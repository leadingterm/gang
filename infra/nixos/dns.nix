{
  # EC2 link-local DNS shares a per-ENI PPS limit; static/musl binaries bypass NSS caches.
  services.resolved = {
    enable = true;
    settings.Resolve = {
      Cache = "yes";
      DNSSEC = false;
      DNSOverTLS = false;
      FallbackDNS = "";
    };
  };
}
