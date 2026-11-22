{
  pkgs,
  controllerImage,
}: let
  config = pkgs.writeText "controller-test.toml" ''
    poll_interval = "500ms"
    client_action_timeout = "1m"
    default_isa = "x86-64"
    listen = "127.0.0.1:50051"
    health_listen = "0.0.0.0:8080"
    worker_listen = "127.0.0.1:9090"
    endpoint = "http://127.0.0.1:50051"
    worker_endpoint = "grpc://127.0.0.1:9090"

    [[fleets]]
    name = "local-worker-x86-64"
    isa = "x86-64"
    sandbox = "noop"
    action_environment = "host:local"
    min_instances = 0
    max_instances = 1
    vcpus_per_instance = 1
    # Match the local sample's capacities so default action budgets fit.
    memory_per_instance = "8GiB"
    disk_per_instance = "20GiB"
    idle_timeout = "15s"
    max_drain_wait = "1m"
    boot_grace = "1m"

    [provisioner.local]
    state_dir = "/var/lib/gang/local"
    mock_imds_listen = "127.0.0.1:16925"
    [cache.filesystem]
    path = "/var/lib/gang/cache"
    [signer.file]
    key_file = "/var/lib/gang/ca.key"
    [database.sqlite]
    path = "/var/lib/gang/gang.db"
    [sandbox.noop]
    environment = "host:local"
    [server]
    transport = "plaintext"
    [authentication.clients.none]
    [authentication.workers.none]
  '';
in
  pkgs.testers.runNixOSTest {
    name = "controller-container";
    globalTimeout = 300;
    nodeDefaults.virtualisation = {
      memorySize = 2048;
      cores = 2;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
      vlans = [1];
    };
    nodes = {
      upstream = {
        networking.useDHCP = false;
        networking.firewall.enable = false;
        networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkForce [
          {
            address = "192.168.77.1";
            prefixLength = 24;
          }
        ];
        systemd.services.dns-fixture = {
          wantedBy = ["multi-user.target"];
          after = ["network-online.target"];
          wants = ["network-online.target"];
          serviceConfig.ExecStart = pkgs.writeShellScript "dns-fixture" ''
            exec ${pkgs.dnsmasq}/bin/dnsmasq --keep-in-foreground --pid-file= --no-resolv --no-hosts \
              --interface=eth1 --bind-interfaces --local-ttl=300 --address=/cached.cache.test/192.0.2.80
          '';
        };
      };
      client = {
        boot.binfmt.emulatedSystems = ["aarch64-linux"];
        # Pin a static emulator before Podman enters the ARM container's root.
        boot.binfmt.preferStaticEmulators = true;
        virtualisation.podman.enable = true;
        networking.useDHCP = false;
        networking.firewall.enable = false;
        networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkForce [
          {
            address = "192.168.77.2";
            prefixLength = 24;
          }
        ];
        environment.systemPackages = [pkgs.curl pkgs.podman];
      };
    };
    testScript = ''
      from datetime import timedelta

      upstream.start()
      upstream.wait_for_unit("dns-fixture.service")
      client.start()
      client.wait_for_unit("multi-user.target")
      client.succeed("podman load -i ${controllerImage}")
      image = client.succeed("podman images -q | sort -u").strip()
      assert image
      client.succeed(f"podman run -d --name controller -p 127.0.0.1:8080:8080 --dns 192.168.77.1 --env \"GANG_CONFIG=$(cat ${config})\" --env GANG_SECRET_FILES=0=test-secret --env GANG_SECRET_0=secret-value {image}")
      try:
          client.wait_until_succeeds("curl -fsS http://127.0.0.1:8080/ready", timeout=timedelta(seconds=60))
      except Exception:
          print(client.execute("podman logs --tail 100 controller"))
          print(client.execute("podman inspect --format '{{json .State}}' controller"))
          raise
      client.succeed("podman exec controller grep -Eq '^Uid:[[:space:]]+65532[[:space:]]+65532' /proc/1/status")
      client.succeed("podman exec controller sh -c 'test $(stat -c %u:%g:%a /etc/gang/gang.toml) = 65532:65532:600 && test $(stat -c %u:%g:%a /etc/gang/secrets/test-secret) = 65532:65532:600'")
      # Host root can inspect the dropped-UID process without adding container capabilities.
      controller_pid = int(client.succeed("podman inspect --format '{{.State.Pid}}' controller"))
      assert controller_pid > 0
      environment = client.succeed(f"cat /proc/{controller_pid}/environ | tr '\\000' '\\n'")
      assert "GANG_CONFIG=" not in environment and "GANG_SECRET" not in environment
      client.succeed("podman exec controller nslookup -type=A cached.cache.test 127.0.0.1 | grep -q 192.0.2.80")
      upstream.succeed("systemctl stop dns-fixture.service")
      client.succeed("podman exec controller nslookup -type=A cached.cache.test 127.0.0.1 | grep -q 192.0.2.80")
      client.succeed("podman kill --signal TERM controller")
      client.wait_until_succeeds("test $(podman inspect -f '{{.State.Running}}' controller) = false", timeout=timedelta(seconds=60))
      client.succeed("test $(podman inspect -f '{{.State.ExitCode}}' controller) = 0")
    '';
  }
