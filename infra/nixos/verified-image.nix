# The EC2 disk image of a NixOS system, built only if its root filesystem holds the system's closure byte for byte:
# nixpkgs copies the store into the image with lkl's cptofs, which reports a failed directory read on stderr and carries on.
{
  pkgs,
  nixos,
}: let
  image = nixos.config.system.build.amazonImage;
  closure = pkgs.closureInfo {rootPaths = [nixos.config.system.build.toplevel];};
in
  pkgs.runCommand "${image.name}-verified" {
    nativeBuildInputs = [pkgs.diffutils pkgs.e2fsprogs pkgs.jq pkgs.util-linux];
  } ''
    raw=$(jq -r .file ${image}/nix-support/image-info.json)
    root=$(sfdisk -J "$raw" | jq '[.partitiontable.partitions[]] | max_by(.size)')
    dd if="$raw" of=root.img bs=4M iflag=skip_bytes,count_bytes \
      skip=$(($(jq .start <<<"$root") * 512)) count=$(($(jq .size <<<"$root") * 512)) status=none
    e2fsck -fn root.img
    mkdir tree
    debugfs -R "rdump /nix/store $PWD/tree" root.img
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
    mkdir $out
    ln -s ${image}/* $out/
  ''
