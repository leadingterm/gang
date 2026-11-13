# The EC2 disk image of a NixOS system, built only if its root filesystem holds the system's closure byte for byte
# and it boots to the basic target under UEFI firmware from an NVMe disk, as EC2 starts it.
{
  pkgs,
  nixos,
}: let
  image = nixos.config.system.build.ec2Image;
  closure = pkgs.closureInfo {rootPaths = [nixos.config.system.build.toplevel];};
  qemu = "${pkgs.qemu_kvm}/bin/qemu-system-${pkgs.stdenv.hostPlatform.qemuArch}";
  machine =
    {
      x86_64 = "q35";
      aarch64 = "virt";
    }
    .${
      pkgs.stdenv.hostPlatform.parsed.cpu.name
    };
in
  pkgs.runCommand "${image.name}-verified" {
    nativeBuildInputs = [pkgs.diffutils pkgs.e2fsprogs pkgs.jq pkgs.qemu-utils pkgs.util-linux];
    # KVM where the builder has it, qemu's TCG otherwise.
    requiredSystemFeatures = ["kvm"];
    # The disk alone, which the release builds a second time to check that it is reproducible.
    passthru.disk = nixos.config.system.build.image;
  } ''
    raw=$(jq -r .file ${image}/nix-support/image-info.json)
    root=$(sfdisk -J "$raw" | jq '[.partitiontable.partitions[]] | max_by(.size)')
    dd if="$raw" of=root.img bs=4M iflag=skip_bytes,count_bytes \
      skip=$(($(jq .start <<<"$root") * 512)) count=$(($(jq .size <<<"$root") * 512)) status=none
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

    # Boot a copy-on-write overlay of the disk until the system's own systemd (after the initrd's) reports the basic target, which needs every local filesystem:
    # the EC2 services that order the multi-user target wait for instance metadata, which a VM has not. The timeout is a hard deadline only.
    qemu-img create -q -f qcow2 -b "$raw" -F raw disk.qcow2
    install -m 0644 ${pkgs.OVMF.variables} vars.fd
    coproc vm {
      exec timeout 1800 ${qemu} -machine ${machine} -accel kvm -accel tcg -cpu max -smp 2 -m 2048 \
        -drive if=pflash,format=raw,readonly=on,file=${pkgs.OVMF.firmware} -drive if=pflash,format=raw,file=vars.fd \
        -drive if=none,id=root,file=disk.qcow2 -device nvme,drive=root,serial=root \
        -nic none -display none -monitor none -serial stdio </dev/null
    }
    booted=false
    stage2=false
    while IFS= read -r line <&"''${vm[0]}"; do
      # The console's lines carry CRs, which Nix's log takes as a return to the line start, hiding the text.
      line=''${line//$'\r'/}
      echo "$line"
      case $line in
        *"Welcome to "*"(Initrd)"*) ;;
        *"Welcome to "*) stage2=true ;;
        *"Reached target"*"Basic System"*)
          if $stage2; then
            booted=true
            break
          fi
          ;;
      esac
    done
    kill "$vm_PID" 2>/dev/null || true
    if ! $booted; then
      echo "the image did not reach the basic target" >&2
      exit 1
    fi

    mkdir $out
    ln -s ${image}/* $out/
  ''
