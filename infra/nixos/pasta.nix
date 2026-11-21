{pkgs}:
pkgs.passt.overrideAttrs (_: {
  version = "2026-11-11-d22d512";
  src = pkgs.fetchurl {
    url = "https://passt.top/passt/snapshot/passt-d22d51229f2a8a9b594ad3fc642f2845b4ef44e8.tar.gz";
    hash = "sha256-7gbPiWYCsfPX6j++NZbWFYUYqovb1SltwqnmSMM3tJ0=";
  };
})
