{
  pkgs,
  gang,
}: let
  runtime = pkgs.pkgsCross.aarch64-multiplatform;
  dnsmasq = (runtime.dnsmasq.override {dbusSupport = false;}).overrideAttrs (_: {
    buildInputs = [];
    preBuild = ''
      makeFlagsArray=("COPTS=-DNO_DHCP -DNO_DHCP6 -DNO_TFTP -DNO_SCRIPT -DNO_IPSET -DNO_AUTH -DNO_DUMPFILE")
    '';
    postBuild = "";
    postInstall = "";
  });
  policy = pkgs.writeText "gang-local-image-policy.json" (builtins.toJSON {
    default = [{type = "reject";}];
    transports.docker-archive."" = [{type = "insecureAcceptAnything";}];
  });
  root = pkgs.runCommand "gang-controller-root" {} ''
    mkdir -p $out/bin $out/usr/local/bin $out/etc/ssl/certs
    ln -s ${runtime.busybox}/bin/* $out/bin/
    ln -s ${dnsmasq}/bin/dnsmasq $out/bin/dnsmasq
    ln -s ${runtime.jq}/bin/jq $out/bin/jq
    ln -s ${runtime.su-exec}/bin/su-exec $out/bin/su-exec
    ln -s ${gang}/bin/gang $out/usr/local/bin/gang
    install -m 0755 ${./entrypoint.sh} $out/usr/local/bin/gang-entrypoint
    ln -s ${runtime.cacert}/etc/ssl/certs/ca-bundle.crt $out/etc/ssl/certs/ca-certificates.crt
    mkdir -p $out/etc/gang
    install -m 0644 ${../nixos/rds-ca-bundle.pem} $out/etc/gang/rds-ca-bundle.pem
    printf 'root:x:0:0:root:/root:/bin/sh\nnobody:x:65532:65532:nobody:/:/bin/sh\n' > $out/etc/passwd
    printf 'root:x:0:\nnobody:x:65532:\n' > $out/etc/group
  '';
  dockerImage = pkgs.dockerTools.buildLayeredImage {
    name = "gang-controller";
    tag = "release";
    architecture = "arm64";
    created = "1970-01-01T00:00:01Z";
    contents = [root];
    fakeRootCommands = ''
      mkdir -p tmp var/lib/gang
      chmod 1777 tmp
      chmod 0700 var/lib/gang
      chown 65532:65532 var/lib/gang
      chmod 0755 etc/gang
    '';
    config = {
      Entrypoint = ["/usr/local/bin/gang-entrypoint"];
      Env = ["PATH=/bin" "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"];
    };
  };
  image =
    pkgs.runCommand "gang-controller-oci.tar" {
      nativeBuildInputs = [pkgs.skopeo pkgs.gnutar];
    } ''
      skopeo --tmpdir "$TMPDIR" --policy ${policy} copy docker-archive:${dockerImage} oci:oci:release
      tar --sort=name --mtime='@1' --owner=0 --group=0 --numeric-owner -cf "$out" -C oci blobs index.json oci-layout
    '';
in {
  inherit image;
  check = pkgs.runCommand "gang-controller-oci-check" {nativeBuildInputs = [pkgs.python3];} ''
    python3 - ${image} ${gang}/bin/gang ${./entrypoint.sh} <<'PY'
    import hashlib
    import io
    import json
    import posixpath
    import sys
    import tarfile

    with tarfile.open(sys.argv[1]) as archive:
        files = {member.name.removeprefix("./"): member for member in archive}
        def data(path):
            return archive.extractfile(files[path]).read()
        def blob(descriptor):
            algorithm, digest = descriptor["digest"].split(":")
            assert algorithm == "sha256"
            content = data("blobs/sha256/" + digest)
            assert hashlib.sha256(content).hexdigest() == digest
            assert len(content) == descriptor["size"]
            return content
        index = json.loads(data("index.json"))
        assert len(index["manifests"]) == 1
        manifest = json.loads(blob(index["manifests"][0]))
        config = json.loads(blob(manifest["config"]))
        assert config["architecture"] == "arm64" and config["os"] == "linux"
        assert config["created"] == "1970-01-01T00:00:01Z"
        assert config["config"]["Entrypoint"] == ["/usr/local/bin/gang-entrypoint"]
        assert "PATH=/bin" in config["config"]["Env"]
        members = {}
        for layer in manifest["layers"]:
            with tarfile.open(fileobj=io.BytesIO(blob(layer))) as contents:
                for item in contents:
                    name = item.name.removeprefix("./").lstrip("/")
                    members[name] = (item, contents.extractfile(item).read() if item.isfile() else None)
        def image_file(path):
            seen = set()
            while True:
                assert path not in seen, f"image symlink cycle at {path}"
                seen.add(path)
                item, content = members[path]
                if item.isfile():
                    return content
                assert item.issym(), f"image path is not a file or symlink: {path}"
                target = item.linkname
                path = posixpath.normpath(
                    target if target.startswith("/") else posixpath.join(posixpath.dirname(path), target)
                ).lstrip("/")
        entry = image_file("usr/local/bin/gang-entrypoint")
        with open(sys.argv[3], "rb") as expected:
            assert entry == expected.read()
        binary_link = members["usr/local/bin/gang"][0]
        assert binary_link.issym()
        binary = image_file("usr/local/bin/gang")
        assert binary[:4] == b"\x7fELF" and binary[18:20] == b"\xb7\x00"
        with open(sys.argv[2], "rb") as expected:
            assert binary == expected.read()
        for path, mode in [("etc/gang", 0o755), ("tmp", 0o1777), ("var/lib/gang", 0o700)]:
            assert members[path][0].mode == mode
        state = members["var/lib/gang"][0]
        assert (state.uid, state.gid) == (65532, 65532)
    PY
    touch "$out"
  '';
}
