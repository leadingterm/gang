{
  config,
  lib,
  ...
}: {
  # dhcpcd's resolvconf compatibility hook cannot register resolver data unprivileged;
  # native networkd integration avoids privileged hooks.
  networking.useNetworkd = true;
  systemd.network.config.networkConfig.UseDomains = true;

  # Preserve dhcpcd's application of the DHCP-advertised MTU on the default network.
  systemd.network.networks = lib.mkIf config.networking.useDHCP {
    "99-ethernet-default-dhcp".dhcpV4Config.UseMTU = true;
  };

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
