# The EC2 disk image of a NixOS system, built only if its root filesystem holds the system's closure byte for byte
# and it boots as EC2 boots it, on a larger disk with a state disk attached, in each role and with the controller state disk mounted, with no unit failed.
{
  pkgs,
  nixos,
}: let
  inherit (pkgs) lib;
  image = nixos.config.system.build.ec2Image;
  closure = pkgs.closureInfo {rootPaths = [nixos.config.system.build.toplevel];};
  qemu = "${pkgs.qemu_kvm}/bin/qemu-system-${pkgs.stdenv.hostPlatform.qemuArch}";
  # The machine, and the bus of the disk the firmware boots: one that both the firmware and the image's initrd drive, and not NVMe (see the boot below).
  # x86_64's initrd loads AHCI but has no virtio driver; aarch64's kernel has virtio built in, and its firmware has no AHCI driver.
  platform =
    {
      x86_64 = {
        machine = "q35";
        bootDisk = "-device ich9-ahci,id=sata -device ide-hd,drive=root,bus=sata.0,bootindex=0";
      };
      aarch64 = {
        machine = "virt";
        bootDisk = "-device virtio-blk-pci,drive=root,bootindex=0";
      };
    }
    .${
      pkgs.stdenv.hostPlatform.parsed.cpu.name
    };

  # The units that cannot run in a VM, because they need the EC2 instance metadata service (IMDS), which only EC2 serves, in every role and in each role's own.
  # Each role's boot masks its list (at runtime, on the kernel command line) and fails if any other unit is masked at runtime.
  metadataUnits = {
    "fetch-ec2-metadata.service" = "copies the instance's user data, host name and SSH key from IMDS";
    "gang-config.service" = "reads the name of its SSM parameter from the user data and the region from IMDS";
  };
  roleUnits = {
    controller = {
      "gang-server.service" = "requires gang-config.service and runs on the gang.toml it writes";
    };
    worker = {
      "gang-scratch.service" = "requires gang-config.service and reads the scratch mount point from the gang.toml it writes";
      "gang-worker.service" = "requires gang-config.service and runs on the gang.toml it writes";
    };
  };
  roles = nixos.config.gang.roles;
  runs = {
    controller = {
      role = "controller";
      state = false;
    };
    controller-state = {
      role = "controller";
      state = true;
    };
    worker = {
      role = "worker";
      state = false;
    };
  };
  volumes = [(lib.removePrefix "/dev/" nixos.config.gang.stateDevice)];
  masks = role: lib.attrNames (metadataUnits // roleUnits.${role});
  missingUnits = lib.filter (unit: !(nixos.config.systemd.services.${lib.removeSuffix ".service" unit}.enable or false)) (lib.unique (lib.attrNames metadataUnits ++ lib.concatLists (map lib.attrNames (lib.attrValues roleUnits))));

  # The verdict, printed on the console by the booted system once its multi-user target is reached.
  verdict = run: let
    inherit (runs.${run}) role state;
  in
    pkgs.writeText "image-check-${run}" ''
      export PATH=/run/current-system/sw/bin LC_ALL=C
      problems=()

      # The boot finished: systemd's job queue drained, so no job of the boot is left (one that never finishes keeps the check waiting until its hard deadline).
      # Jobs that systemd or udev queue after the boot, such as a console setup restart for a new virtual terminal, are not part of it.
      state=$(systemctl is-system-running --wait)
      [ "$state" = running ] || problems+=("the system is $state, not running")
      # No unit failed, and the only runtime masks are the check's (the image's own masks are the image's).
      failed=$(systemctl list-units --all --state=failed --plain --no-legend --no-pager | awk '{print $1}' | xargs)
      [ -z "$failed" ] || problems+=("failed units: $failed")
      masked=$(systemctl list-unit-files --no-legend --no-pager | awk '$2 == "masked-runtime" {print $1}' | sort | xargs)
      [ "$masked" = ${lib.escapeShellArg (lib.concatStringsSep " " (masks role))} ] || problems+=("masked units: ''${masked:-none}; the check masks only ${lib.concatStringsSep " " (masks role)}")

      # Nothing in the boot depends on the EFI system partition, which only the firmware reads.
      esp=$(lsblk -rpno PATH,PARTTYPE | awk '$2 == "c12a7328-f81f-11d2-ba4b-00a0c93ec93b" {print $1}')
      if [ -z "$esp" ]; then
        problems+=("no EFI system partition")
      else
        for name in "$esp" $(udevadm info --query=symlink "$esp"); do
          dependents=$(systemctl list-dependencies --reverse --plain --no-pager "$(systemd-escape --path --suffix=device "/dev/''${name#/dev/}")" | tail -n +2 | xargs)
          [ -z "$dependents" ] || problems+=("units depend on the EFI system partition (/dev/''${name#/dev/}): $dependents")
        done
      fi

      # growpart and growfs extended the root to the disk.
      size=$(findmnt -bno SIZE /)
      [ "$size" -gt @rootBytes@ ] || problems+=("the root filesystem did not grow: $size bytes, the image's root partition @rootBytes@")

      ${lib.concatMapStrings (volume: ''
          [ -b /dev/${volume} ] || problems+=("the volume /dev/${volume} is not a block device")
          ${
            if state
            then ''
              [ "$(findmnt -no FSTYPE,TARGET --source /dev/${volume})" = "ext4 /var/lib/gang" ] || problems+=("the volume /dev/${volume} is not mounted as ext4 at /var/lib/gang")
            ''
            else ''
              if blkid -p /dev/${volume} >/dev/null; then
                problems+=("the volume /dev/${volume} is not blank")
              else
                status=$?
                [ "$status" -eq 2 ] || problems+=("probing /dev/${volume} failed with status $status")
              fi
              if mountpoint -q /var/lib/gang; then
                problems+=("/var/lib/gang is mounted")
              fi
            ''
          }
        '')
        volumes}
      ${
        if state
        then ''
          [ "$(systemctl is-active var-lib-gang.mount)" = active ] || problems+=("var-lib-gang.mount is not active")
        ''
        else ''
          [ "$(systemctl show -P LoadState var-lib-gang.mount)" = not-found ] || problems+=("var-lib-gang.mount exists without a state disk token")
        ''
      }

      # The user data's role started its target, and no other role's target was ever active.
      [ "$(systemctl is-active gang-${role}.target)" = active ] || problems+=("gang-${role}.target is not active")
      for other in ${lib.escapeShellArgs (lib.filter (other: other != role) roles)}; do
        [ "$(systemctl show -P ActiveEnterTimestampMonotonic "gang-$other.target")" = 0 ] || problems+=("gang-$other.target was active on a ${role}")
      done

      # Check installed target wiring for every unit, and dependencies of the other roles' unmasked units.
      ${lib.concatStrings (lib.mapAttrsToList (unitRole: units:
        lib.concatStrings (lib.mapAttrsToList (unit: _: ''
            wanted_by=$(cd /etc/systemd/system && for link in *.wants/${lib.escapeShellArg unit}; do [ -e "$link" ] || [ -L "$link" ] && printf '%s\n' "''${link%%.wants/*}"; done | sort | xargs)
            [ "$wanted_by" = ${lib.escapeShellArg "gang-${unitRole}.target"} ] || problems+=("${unit} is wanted by '$wanted_by', not gang-${unitRole}.target alone")
            ${lib.optionalString (unitRole != role) ''
              case " $(systemctl show -P Requires ${lib.escapeShellArg unit}) " in *" gang-config.service "*) ;; *) problems+=("${unit} does not require gang-config.service") ;; esac
              case " $(systemctl show -P After ${lib.escapeShellArg unit}) " in *" gang-config.service "*) ;; *) problems+=("${unit} does not order after gang-config.service") ;; esac
            ''}
          '')
          units))
      roleUnits)}

      if [ ''${#problems[@]} -eq 0 ]; then
        echo "image-check: passed"
      else
        printf 'image-check: %s\n' "''${problems[@]}"
        [ -z "$failed" ] || systemctl status --no-pager --lines=20 $failed
        echo "image-check: failed"
      fi
    '';

  # SMBIOS supplies the check's kernel command line, units and udev rule without changing the image's bytes.
  # Extra units and drop-ins reach the initrd too, so the check's are conditioned out of it.
  kernelCommandLine = run: lib.concatStringsSep " " ((map (unit: "systemd.mask=${unit}") (masks runs.${run}.role)) ++ ["systemd.wants=gang-${runs.${run}.role}.target"] ++ lib.optional runs.${run}.state "systemd.wants=gang-state-disk.service");
  credentials = run: {
    # QEMU's NVMe controller lacks EC2's vendor-specific device name, so name its volumes by serial number.
    "udev.rules.70-ec2-volumes" = pkgs.writeText "70-ec2-volumes.rules" (lib.concatMapStrings (volume: ''
        KERNEL=="nvme*", ENV{DEVTYPE}=="disk", ATTRS{serial}=="${volume}", SYMLINK+="${volume}"
      '')
      volumes);
    "systemd.extra-unit.image-check-udev.service" = pkgs.writeText "image-check-udev.service" ''
      [Unit]
      Description=Load the udev rules of the image boot check
      DefaultDependencies=no
      ConditionPathExists=!/etc/initrd-release
      After=systemd-udevd.service
      Before=systemd-udev-trigger.service

      [Service]
      Type=oneshot
      ImportCredential=udev.rules.*
      ExecStart=/run/current-system/sw/bin/udevadm control --load-credentials --reload
    '';
    "systemd.unit-dropin.systemd-udev-trigger.service~image-check" = pkgs.writeText "image-check.conf" ''
      [Unit]
      Wants=image-check-udev.service
    '';
    "systemd.extra-unit.image-check.service" = pkgs.writeText "image-check.service" ''
      [Unit]
      Description=Image boot check
      ConditionPathExists=!/etc/initrd-release
      After=multi-user.target
      Wants=${lib.concatMapStringsSep " " (volume: "dev-${volume}.device") volumes}
      After=${lib.concatMapStringsSep " " (volume: "dev-${volume}.device") volumes}

      [Service]
      # Its start job finishes once the check runs, so the boot's job queue can drain while the check waits for it.
      Type=exec
      ImportCredential=image-check
      ExecStart=/run/current-system/sw/bin/bash %d/image-check
      StandardOutput=journal+console
      StandardError=journal+console
    '';
    "systemd.unit-dropin.multi-user.target~image-check" = pkgs.writeText "image-check.conf" ''
      [Unit]
      Wants=image-check.service
    '';
    # Written with the image's root partition size, below.
    "image-check" = "image-check-${run}";
  };
in
  assert lib.assertMsg (lib.attrNames roleUnits == roles) "verified-image.nix boots every role of gang.roles";
  assert lib.assertMsg (lib.all (role: lib.any (run: run.role == role) (lib.attrValues runs)) roles) "verified-image.nix has a run for every role of gang.roles";
  assert lib.assertMsg (missingUnits == []) "verified-image.nix masks and checks only enabled units: ${lib.concatStringsSep " " missingUnits}";
    pkgs.runCommand "${image.name}-verified" {
      nativeBuildInputs = [pkgs.diffutils pkgs.e2fsprogs pkgs.jq pkgs.libarchive pkgs.qemu-utils pkgs.util-linux];
      # KVM where the builder has it, qemu's TCG otherwise.
      requiredSystemFeatures = ["kvm"];
      # The disk alone, which the release builds a second time to check that it is reproducible.
      passthru.disk = nixos.config.system.build.image;
    } ''
      raw=$(jq -r .file ${image}/nix-support/image-info.json)
      root=$(sfdisk -J "$raw" | jq '[.partitiontable.partitions[]] | max_by(.size)')
      rootBytes=$(($(jq .size <<<"$root") * 512))
      dd if="$raw" of=root.img bs=4M iflag=skip_bytes,count_bytes \
        skip=$(($(jq .start <<<"$root") * 512)) count=$rootBytes status=none
      e2fsck -fn root.img
      mkdir tree
      debugfs -R "rdump /nix/store $PWD/tree" root.img
      rm root.img
      missing=0
      differing=0
      while read -r path; do
        name=''${path#/nix/store/}
        if [ ! -e "tree/store/$name" ] && [ ! -L "tree/store/$name" ]; then
          echo "missing from the image: $path" >&2
          missing=$((missing + 1))
        elif ! diff -r --no-dereference "tree/store/$name" "$path" >&2; then
          differing=$((differing + 1))
        fi
      done <${closure}/store-paths
      if [ "$missing" -ne 0 ] || [ "$differing" -ne 0 ]; then
        echo "the image's store does not hold the system closure: $missing missing, $differing differing" >&2
        exit 1
      fi

      # The boot below does not run from NVMe, but EC2's root is an NVMe namespace and its network an ENA device: the initrd (which the unified kernel image embeds)
      # must carry the NVMe driver to find the root, and the system the ENA driver.
      if ! bsdtar -tf ${nixos.config.system.build.initialRamdisk}/${nixos.config.system.boot.loader.initrdFile} | grep -q '/nvme\.ko'; then
        echo "the image's initrd has no NVMe driver, so it cannot find its root on EC2" >&2
        exit 1
      fi
      if ! find -L ${nixos.config.system.build.toplevel}/kernel-modules/lib/modules -name 'ena.ko*' | grep -q .; then
        echo "the image's system has no ENA driver, so it has no network on EC2" >&2
        exit 1
      fi

      # The boot: the firmware boots the image from a disk twice its size, so growpart and growfs extend the root as on a larger EBS volume.
      # It is not an NVMe disk because the firmware's NVMe driver can stall on QEMU's NVMe controller and never boot, so no NVMe controller exists while the firmware runs.
      # Attach each blank volume after the initrd starts the system, as EC2 does after instance launch.
      # Units that need instance metadata are masked; the check unit then judges the booted system. The timeout is a hard deadline only.
      boot() {
        local run=$1
        shift
        qemu-img create -q -f qcow2 -b "$raw" -F raw "disk-$run.qcow2" $((2 * $(stat -c %s "$raw")))
        local -a attach=()
        local chassis=0 volume
        for volume in ${lib.escapeShellArgs volumes}; do
          chassis=$((chassis + 1))
          qemu-img create -q -f qcow2 "$volume-$run.qcow2" 1G
          attach+=(-drive "if=none,id=$volume,format=qcow2,file=$volume-$run.qcow2" -device "pcie-root-port,id=port-$volume,chassis=$chassis")
        done
        install -m 0644 ${pkgs.OVMF.variables} "vars-$run.fd"
        coproc vm {
          exec timeout 1800 ${qemu} -machine ${platform.machine} -accel kvm -accel tcg -cpu max -smp 2 -m 2048 \
            -drive if=pflash,format=raw,readonly=on,file=${pkgs.OVMF.firmware} -drive "if=pflash,format=raw,file=vars-$run.fd" \
            -drive "if=none,id=root,format=qcow2,file=disk-$run.qcow2" ${platform.bootDisk} \
            "''${attach[@]}" \
            "$@" \
            -nic user,model=virtio-net-pci,restrict=on -display none -serial mon:stdio
        }
        local verdict=none line stage2=false
        while IFS= read -r line <&"''${vm[0]}"; do
          # The console's lines carry CRs, which Nix's log takes as a return to the line start, hiding the text.
          line=''${line//$'\r'/}
          echo "$line"
          case $line in
            *"Welcome to "*"(Initrd)"*) ;;
            *"Welcome to "*)
              if ! $stage2; then
                stage2=true
                # Ctrl-A c switches the shared serial console between the guest and QEMU monitor.
                for volume in ${lib.escapeShellArgs volumes}; do
                  printf '\001cdevice_add nvme,id=%s,drive=%s,serial=%s,bus=port-%s\n\001c' "$volume" "$volume" "$volume" "$volume" >&"''${vm[1]}"
                done
              fi
              ;;
            *"image-check: passed")
              verdict=passed
              break
              ;;
            *"image-check: failed")
              verdict=failed
              break
              ;;
            *"Kernel panic"*)
              verdict=panic
              break
              ;;
          esac
        done
        kill "$vm_PID" 2>/dev/null || true
        wait "$vm_PID" 2>/dev/null || true
        if [ "$verdict" != passed ]; then
          echo "the image failed its $run boot check ($verdict verdict)" >&2
          exit 1
        fi
      }
      ${lib.concatMapStrings (run: ''
        substitute ${verdict run} image-check-${run} --subst-var rootBytes
        boot ${lib.escapeShellArg run} \
          -smbios ${lib.escapeShellArg "type=11,value=io.systemd.stub.kernel-cmdline-extra=${kernelCommandLine run}"} \
          ${lib.concatStringsSep " \\\n        " (lib.mapAttrsToList (name: file: ''-smbios "type=11,value=io.systemd.credential.binary:${name}=$(base64 -w0 ${file})"'') (credentials run))}
      '') (lib.attrNames runs)}

      mkdir $out
      ln -s ${image}/* $out/
    ''
