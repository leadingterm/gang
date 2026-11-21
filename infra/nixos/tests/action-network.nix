{
  pkgs,
  gang,
  helper,
}: let
  inherit (pkgs) lib;
  pasta = import ../pasta.nix {inherit pkgs;};
  probe = pkgs.runCommandCC "action-network-probe" {nativeBuildInputs = [pkgs.patchelf];} ''
    cat > probe.c <<'EOF'
    #include <arpa/inet.h>
    #include <fcntl.h>
    #include <signal.h>
    #include <stdio.h>
    #include <string.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <unistd.h>
    static int marker(const char *name) {
      int fd = open(name, O_WRONLY|O_CREAT|O_TRUNC, 0644);
      if (fd < 0) return -1;
      return close(fd);
    }
    int main(int argc, char **argv) {
      if (argc != 3 || marker("started")) return 2;
      alarm(20);
      int fd = socket(AF_INET, SOCK_STREAM, 0);
      if (fd < 0) return 2;
      struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(18080)};
      if (inet_pton(AF_INET, argv[2], &addr.sin_addr) != 1) return 2;
      if (!strcmp(argv[1], "connect")) return connect(fd, (void *)&addr, sizeof(addr)) ? 1 : 0;
      if (strcmp(argv[1], "barrier")) return 2;
      addr.sin_port = htons(30000);
      if (bind(fd, (void *)&addr, sizeof(addr)) || listen(fd, 1)) return 2;
      int control = socket(AF_UNIX, SOCK_STREAM, 0);
      struct sockaddr_un un = {.sun_family = AF_UNIX, .sun_path = "control.sock"};
      char byte = 'x';
      if (control < 0 || connect(control, (void *)&un, sizeof(un)) ||
          write(control, &byte, 1) != 1 || read(control, &byte, 1) != 1) return 2;
      return 0;
    }
    EOF
    $CC -O2 -Wall -Wextra -Werror probe.c -o "$out"
    patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 --remove-rpath "$out"
  '';
  configFile = pkgs.writeText "gang-network-test.toml" ((builtins.readFile ../../../configs/gang.local.plaintext.toml)
    + ''
      [execution.egress]
      protected_networks = ["10.0.0.0/8", "11.0.0.1/32", "169.254.0.0/16"]
      dns_servers = ["1.1.1.1"]
    '');
in
  pkgs.testers.runNixOSTest {
    name = "action-network";
    globalTimeout = 300;
    nodeDefaults.virtualisation = {
      memorySize = 2048;
      cores = 2;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = lib.mkForce {};
      vlans = [1];
    };
    nodes.peer = {pkgs, ...}: {
      networking.firewall.enable = false;
      networking.interfaces.eth1.ipv4.addresses = [
        {
          address = "11.0.0.2";
          prefixLength = 24;
        }
      ];
      systemd.services.fixture = {
        after = ["network-online.target"];
        wants = ["network-online.target"];
        wantedBy = ["multi-user.target"];
        serviceConfig.ExecStart = "${pkgs.socat}/bin/socat TCP-LISTEN:18080,bind=11.0.0.2,reuseaddr,fork EXEC:${pkgs.coreutils}/bin/true";
      };
    };
    nodes.worker = {pkgs, ...}: {
      imports = [../worker.nix];
      _module.args.gang = gang;
      networking.interfaces.eth0.useDHCP = lib.mkForce false;
      networking.defaultGateway = lib.mkForce {
        address = "11.0.0.2";
        interface = "eth1";
      };
      networking.interfaces.eth1.ipv4.addresses = lib.mkForce [
        {
          address = "11.0.0.1";
          prefixLength = 24;
        }
      ];
      networking.interfaces.lo.ipv4.addresses = [
        {
          address = "169.254.169.254";
          prefixLength = 32;
        }
      ];
      users.groups.gang = {};
      users.users.gang = {
        isSystemUser = true;
        group = "gang";
      };
      environment.systemPackages = [pkgs.netcat pkgs.socat];
      systemd.services.fixture = {
        wantedBy = ["multi-user.target"];
        after = ["network-online.target"];
        wants = ["network-online.target"];
        serviceConfig.ExecStart = "${pkgs.socat}/bin/socat TCP-LISTEN:18080,bind=169.254.169.254,reuseaddr,fork EXEC:${pkgs.coreutils}/bin/true";
      };
      systemd.services.gang-config = lib.mkForce {
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''mkdir -p /etc/gang; cp ${configFile} /etc/gang/gang.toml'';
      };
      systemd.services.gang-scratch = lib.mkForce {
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = "mkdir -p /mnt/scratch";
      };
      systemd.services.gang-worker = {
        wantedBy = lib.mkForce [];
        serviceConfig = {
          Type = "simple";
          RemainAfterExit = true;
          Restart = lib.mkForce "no";
          MemoryMax = "1G";
          ExecStart = lib.mkForce "${helper}";
          Environment = ["GANG=${gang}/bin/gang" "PROBE=${probe}" "ROOTFS=/etc/gang/action-rootfs" "BWRAP=${pkgs.bubblewrap}/bin/bwrap" "PASTA=${pasta}/bin/pasta"];
        };
      };
    };
    testScript = ''
      peer.start()
      peer.wait_for_unit("fixture.service")
      worker.start()
      worker.wait_for_unit("fixture.service")
      worker.succeed("nc -z -w 3 11.0.0.2 18080")
      worker.succeed("nc -z -w 3 169.254.169.254 18080")
      worker.succeed("systemctl start gang-worker.service")
      worker.wait_until_succeeds("test -f /var/lib/gang-home/network-test/first-done")
      worker.succeed("systemctl restart firewall.service")
      worker.succeed("printf x | socat - UNIX-CONNECT:/var/lib/gang-home/network-test/reload.sock")
      worker.wait_until_succeeds("test -f /var/lib/gang-home/network-test/remove-stamp")
      worker.succeed("mv /run/gang/action-egress.json /run/gang/action-egress.json.saved")
      worker.succeed("printf x | socat - UNIX-CONNECT:/var/lib/gang-home/network-test/stamp.sock")
      worker.wait_until_succeeds("test -f /var/lib/gang-home/network-test/all-done")
      worker.succeed("rm -f /run/gang/action-egress.json.saved")
    '';
  }
