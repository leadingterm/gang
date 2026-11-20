{pkgs, ...}: {
  environment.etc."gang-action-ca.crt".source = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
}
