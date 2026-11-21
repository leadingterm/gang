{pkgs}: let
  upstreamIP = "192.168.77.1";
  dnsmasq = "${pkgs.dnsmasq}/bin/dnsmasq";
  dig = "${pkgs.dnsutils}/bin/dig";
  getent = "${pkgs.glibc.bin}/bin/getent";
  grep = "${pkgs.gnugrep}/bin/grep";
  resolvectl = "${pkgs.systemd}/bin/resolvectl";
in
  pkgs.testers.runNixOSTest {
    name = "dns-cache";
    globalTimeout = 300;

    nodeDefaults.virtualisation = {
      memorySize = 1024;
      cores = 1;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
      assignIPAddresses = false;
      vlans = [1];
    };

    nodes = {
      upstream = {
        networking = {
          useDHCP = false;
          firewall.enable = false;
          interfaces.eth1.ipv4.addresses = [
            {
              address = upstreamIP;
              prefixLength = 24;
            }
          ];
        };

        systemd.services.dhcp-fixture = {
          wantedBy = ["multi-user.target"];
          after = ["network-online.target"];
          wants = ["network-online.target"];
          serviceConfig.ExecStart = pkgs.writeShellScript "dhcp-fixture" ''
            exec ${dnsmasq} --keep-in-foreground --pid-file= --port=0 --interface=eth1 \
              --dhcp-leasefile=/run/dhcp-fixture.leases \
              --bind-interfaces --dhcp-range=192.168.77.100,192.168.77.120,255.255.255.0,1h \
              --dhcp-option=option:dns-server,${upstreamIP} \
              --dhcp-option=option:domain-name,cache.test \
              --dhcp-option=option:domain-search,cache.test \
              --dhcp-option=option:router
          '';
        };

        systemd.services.dns-fixture = {
          wantedBy = ["multi-user.target"];
          after = ["network-online.target"];
          wants = ["network-online.target"];
          serviceConfig.ExecStart = pkgs.writeShellScript "dns-fixture" ''
            exec ${dnsmasq} --keep-in-foreground --pid-file= --no-resolv --no-hosts \
              --interface=eth1 --bind-interfaces --local-ttl=300 \
              --address=/cached.cache.test/192.0.2.80 \
              --address=/stub.cache.test/192.0.2.81
          '';
        };
      };

      client = {
        imports = [../dns.nix];
        networking.useDHCP = false;
        networking.interfaces.eth1.useDHCP = true;
        services.nscd.enable = false;
      };
    };

    testScript = ''
      from datetime import timedelta

      upstream.start()
      upstream.wait_for_unit("dhcp-fixture.service")
      upstream.wait_for_unit("dns-fixture.service")
      client.start()
      client.wait_for_unit("systemd-resolved.service")
      client.wait_until_succeeds("${pkgs.iproute2}/bin/ip -4 address show dev eth1 | ${grep} -q 'inet 192.168.77.'", timeout=timedelta(seconds=30))
      client.wait_until_succeeds("${resolvectl} dns eth1 | ${grep} -Fq '${upstreamIP}'", timeout=timedelta(seconds=30))
      client.wait_until_succeeds("${resolvectl} domain eth1 | ${grep} -Fq 'cache.test'", timeout=timedelta(seconds=30))
      client.succeed("${grep} -Eq '^nameserver[[:space:]]+127\\.0\\.0\\.53$' /etc/resolv.conf")

      assert client.succeed("${dig} +time=1 +tries=1 +short @${upstreamIP} cached.cache.test A").strip() == "192.0.2.80"
      assert "192.0.2.80" in client.succeed("${getent} ahostsv4 cached")
      assert client.succeed("${dig} +time=1 +tries=1 +short stub.cache.test A").strip() == "192.0.2.81"

      upstream.succeed("${pkgs.systemd}/bin/systemctl stop dns-fixture.service")
      assert "192.0.2.80" in client.succeed("${getent} ahostsv4 cached")
      assert client.succeed("${dig} +time=1 +tries=1 +short stub.cache.test A").strip() == "192.0.2.81"
    '';
  }
