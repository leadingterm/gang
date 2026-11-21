{pkgs}: let
  inherit (pkgs) lib;
  controllerService =
    (import (pkgs.path + "/nixos/lib/eval-config.nix") {
      system = pkgs.stdenv.hostPlatform.system;
      modules = [
        ../controller.nix
        {_module.args.gang = pkgs.hello;}
      ];
    }).config.systemd.services.gang-server.serviceConfig;

  # One HTTP server answers EC2 metadata on :80 and SSM GetParameter on :8080.
  fakeAws = pkgs.writeText "fake-aws.py" ''
    import http.server
    import json
    import os
    import threading

    PARAMETERS_FILE = "/run/fake-aws/parameters.json"
    TOKEN = b"test-token"
    REGION = b"eu-north-1"
    SSM_TYPE = "application/x-amz-json-1.1"


    class Handler(http.server.BaseHTTPRequestHandler):
        def reply(self, status, body, content_type="text/plain"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass


    class Metadata(Handler):
        def do_PUT(self):
            if self.path == "/latest/api/token":
                self.reply(200, TOKEN)
            else:
                self.reply(404, b"")

        def do_GET(self):
            if self.path == "/latest/user-data":
                self.reply(200, open(os.environ["USER_DATA_FILE"], "rb").read())
            elif self.path == "/latest/meta-data/placement/region":
                self.reply(200, REGION)
            else:
                self.reply(404, b"")


    class Ssm(Handler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length) or b"{}")
            if self.headers.get("X-Amz-Target") != "AmazonSSM.GetParameter":
                body = {"__type": "UnsupportedOperation", "message": "unsupported"}
                self.reply(400, json.dumps(body).encode(), SSM_TYPE)
                return
            reference = request.get("Name", "")
            name, separator, selector = reference.rpartition(":")
            if not separator:
                name = reference
            with open(PARAMETERS_FILE, encoding="utf-8") as parameters_file:
                parameters = json.load(parameters_file)
            if name not in parameters:
                body = {"__type": "ParameterNotFound", "message": "not found"}
                self.reply(400, json.dumps(body).encode(), SSM_TYPE)
                return
            parameter = parameters[name]
            if isinstance(parameter, str):
                parameter = {"latest": 1, "versions": {"1": parameter}}
            version = selector if separator else str(parameter["latest"])
            if version not in parameter["versions"]:
                body = {"__type": "ParameterVersionNotFound", "message": "version not found"}
                self.reply(400, json.dumps(body).encode(), SSM_TYPE)
                return
            result = {
                "Name": name,
                "Type": "String",
                "Value": parameter["versions"][version],
                "Version": int(version),
            }
            if separator:
                result["Selector"] = ":" + selector
            body = {"Parameter": result}
            self.reply(200, json.dumps(body).encode(), SSM_TYPE)


    ssm = http.server.ThreadingHTTPServer(("0.0.0.0", 8080), Ssm)
    metadata = http.server.ThreadingHTTPServer(("0.0.0.0", 80), Metadata)
    print("fake-aws listening", flush=True)
    threading.Thread(target=ssm.serve_forever, daemon=True).start()
    metadata.serve_forever()
  '';

  tomlText = "[server]\ntrust_domain = \"example.com\"\n";
  generation = "/legacy/generations/12345678-1234-4234-8234-123456789abc";
  nextGeneration = "/legacy/generations/87654321-4321-4321-8321-cba987654321";
  generationParameters = {
    "/legacy" = tomlText;
    "/gang/secrets/client" = "s3cr3t";
    ${generation} = {
      latest = 1;
      versions."1" = tomlText;
    };
  };
  publishedParameters =
    generationParameters
    // {
      ${generation} = {
        latest = 2;
        versions = {
          "1" = tomlText;
          "2" = "changed version";
        };
      };
      ${nextGeneration} = {
        latest = 1;
        versions."1" = "new generation";
      };
    };
  laterParameters =
    publishedParameters
    // {
      ${generation} = {
        latest = 3;
        versions = publishedParameters.${generation}.versions // {"3" = "later version";};
      };
    };
  publishedFile = pkgs.writeText "published-parameters.json" (builtins.toJSON publishedParameters);
  laterFile = pkgs.writeText "later-parameters.json" (builtins.toJSON laterParameters);

  stateDisk = {
    virtualisation.emptyDiskImages = [64];
    gang.stateDevice = "/dev/vdc";
  };

  baseModule = {
    userData,
    parameters,
  }: {
    imports = [../gang-config.nix];

    systemd.services = {
      gang-config.environment = {
        AWS_ENDPOINT_URL_SSM = "http://169.254.169.254:8080";
        AWS_ACCESS_KEY_ID = "test";
        AWS_SECRET_ACCESS_KEY = "test";
        AWS_DEFAULT_REGION = "eu-north-1";
      };

      metadata-address = {
        description = "Assign the fake metadata address to lo";
        wantedBy = ["multi-user.target"];
        before = ["fake-aws.service" "gang-config.service"];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          ExecStart = "${pkgs.iproute2}/bin/ip addr replace 169.254.169.254/32 dev lo";
        };
      };

      fake-aws = {
        description = "Fake EC2 metadata and SSM endpoints";
        before = ["gang-config.service"];
        after = ["metadata-address.service"];
        wantedBy = ["multi-user.target"];
        environment = {
          USER_DATA_FILE = "/run/fake-aws/user-data";
        };
        serviceConfig = {
          ExecStart = "${pkgs.python3}/bin/python3 ${fakeAws}";
          Restart = "on-failure";
          RuntimeDirectory = "fake-aws";
          StateDirectory = "fake-aws";
          ExecStartPre = pkgs.writeShellScript "fake-aws-setup" ''
            install -m 0644 ${pkgs.writeText "user-data" userData} /run/fake-aws/user-data
            if [ ! -e /var/lib/fake-aws/parameters.json ]; then
              install -m 0644 ${pkgs.writeText "parameters.json" (builtins.toJSON parameters)} /var/lib/fake-aws/parameters.json
            fi
            install -m 0644 /var/lib/fake-aws/parameters.json /run/fake-aws/parameters.json
          '';
        };
      };

      probe-controller = {
        requires = ["gang-config.service"];
        after = ["gang-config.service"];
        wantedBy = ["gang-controller.target"];
        script = "stat -c '%a' /etc/gang/gang.toml >> /run/probe-controller";
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          StateDirectory = "gang";
        };
      };

      probe-worker = {
        requires = ["gang-config.service"];
        after = ["gang-config.service"];
        wantedBy = ["gang-worker.target"];
        script = "stat -c '%a' /etc/gang/gang.toml >> /run/probe-worker";
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
      };
    };
  };
in
  assert lib.assertMsg (controllerService.Restart == "always") "gang-server must restart after handover";
  assert lib.assertMsg (!(controllerService ? RestartPreventExitStatus)) "gang-server must not suppress restart by exit status";
  assert lib.assertMsg (controllerService.TimeoutStopSec == "infinity") "gang-server shutdown must not have a systemd deadline";
    pkgs.testers.runNixOSTest {
      name = "gang-config";
      globalTimeout = 600;

      nodeDefaults = {
        # At most four guests run at once: 4 GiB guest memory and one vCPU each.
        virtualisation.memorySize = 1024;
        virtualisation.cores = 1;
        # The store image avoids asynchronous virtiofs socket startup; no host shares are needed.
        virtualisation.useNixStoreImage = true;
        virtualisation.mountHostNixStore = false;
        virtualisation.sharedDirectories = lib.mkForce {};
      };

      nodes = {
        withSecrets = {
          imports = [
            stateDisk
            (baseModule {
              userData = "controller /legacy\ndashboard-client-secret=/gang/secrets/client\n# config-digest 0123456789abcdef";
              parameters = generationParameters;
            })
          ];
        };

        controllerstate = {
          imports = [
            stateDisk
            (baseModule {
              userData = "controller ${generation}:1 state=/dev/vdc";
              parameters = generationParameters;
            })
          ];
        };

        workerstate = {
          imports = [
            stateDisk
            (baseModule {
              userData = "worker /gang/gang.toml state=/dev/vdc";
              parameters = {"/gang/gang.toml" = tomlText;};
            })
          ];
          systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
        };

        wrongdevice = {
          imports = [
            stateDisk
            (baseModule {
              userData = "controller /gang/gang.toml state=/dev/sdf";
              parameters = {"/gang/gang.toml" = tomlText;};
            })
          ];
          systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
        };

        plain = baseModule {
          userData = "worker ${generation}:1";
          parameters = generationParameters;
        };

        badname = baseModule {
          userData = "controller /gang/gang.toml\n../escape=/gang/secrets/client";
          parameters = {
            "/gang/gang.toml" = tomlText;
            "/gang/secrets/client" = "s3cr3t";
          };
        };

        missing = {
          imports = [
            (baseModule {
              userData = "worker /gang/absent";
              parameters = generationParameters;
            })
          ];
          systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
          systemd.services.gang-config.serviceConfig.TimeoutStartSec = 150;
        };

        badrole = {
          imports = [
            (baseModule {
              userData = "builder /gang/gang.toml";
              parameters = {"/gang/gang.toml" = tomlText;};
            })
          ];
          systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
        };

        noparameter = {
          imports = [
            (baseModule {
              userData = "worker";
              parameters = {};
            })
          ];
          systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
        };

        extratoken = {
          imports = [
            (baseModule {
              userData = "controller /gang/gang.toml state=/dev/vdc extra";
              parameters = {"/gang/gang.toml" = tomlText;};
            })
          ];
          systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
        };
      };

      testScript = ''
        from collections.abc import Generator
        from contextlib import contextmanager
        import json
        import shutil


        @contextmanager
        def guest(machine: QemuMachine) -> Generator[None]:
            # Keep failed scenarios for the driver to diagnose and clean up.
            yield
            machine.shutdown()
            # Store images belong to one scenario, not the whole test.
            if not machine.keep_machine_state:
                shutil.rmtree(machine.state_dir)
            next_machine = next(pending, None)
            if next_machine is not None:
                next_machine.start(allow_reboot=next_machine in (controllerstate, plain))


        TOML = ${builtins.toJSON tomlText}
        JOURNAL = "journalctl -u gang-config.service"
        SECRET = "/etc/gang/secrets/dashboard-client-secret"

        def publish(machine: QemuMachine, source: str) -> None:
            machine.succeed(
                f"install -m 0644 {source} /var/lib/fake-aws/parameters.json && "
                f"install -m 0644 {source} /run/fake-aws/parameters.json"
            )


        def check_generation_across_restart_and_reboot(machine: QemuMachine, role: str) -> None:
            other = "worker" if role == "controller" else "controller"
            publish(machine, "${publishedFile}")
            machine.succeed("systemctl restart gang-config.service")
            machine.wait_for_unit(f"gang-{role}.target")
            assert machine.succeed("cat /etc/gang/gang.toml") == TOML
            assert machine.succeed(f"systemctl show -P ActiveEnterTimestampMonotonic gang-{other}.target").strip() == "0"
            publish(machine, "${laterFile}")
            machine.reboot()
            machine.wait_for_unit(f"probe-{role}.service")
            machine.wait_for_unit(f"gang-{role}.target")
            assert machine.succeed("cat /etc/gang/gang.toml") == TOML
            assert machine.succeed(f"systemctl show -P ActiveEnterTimestampMonotonic gang-{other}.target").strip() == "0"

        # Bound concurrent boots to the CI runner's resource budget.
        boot_parallelism = 4
        pending = iter([
            withSecrets,
            controllerstate,
            plain,
            badname,
            missing,
            badrole,
            noparameter,
            extratoken,
            workerstate,
            wrongdevice,
        ])
        for _ in range(boot_parallelism):
            machine = next(pending)
            machine.start(allow_reboot=machine in (controllerstate, plain))

        with guest(withSecrets):
            with subtest("withSecrets installs gang.toml and secrets before dependents start"):
                withSecrets.wait_for_unit("probe-controller.service")
                withSecrets.wait_for_unit("gang-controller.target")
                assert withSecrets.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"
                assert withSecrets.succeed("systemctl show -P ActiveEnterTimestampMonotonic probe-worker.service").strip() == "0"
                assert withSecrets.succeed("cat /etc/gang/gang.toml") == TOML
                assert withSecrets.succeed("stat -c '%a' /etc/gang/gang.toml").strip() == "644"
                assert withSecrets.succeed("cat /run/probe-controller") == "644\n"
                assert withSecrets.succeed("stat -c '%a %U:%G' /etc/gang/secrets").strip() == "700 gang:gang"
                assert withSecrets.succeed(f"stat -c '%a %U:%G %s' {SECRET}").strip() == "600 gang:gang 6"
                assert withSecrets.succeed(f"cat {SECRET}") == "s3cr3t"
                withSecrets.succeed("rc=0; blkid -p /dev/vdc >/dev/null || rc=$?; test \"$rc\" -eq 2")
                withSecrets.fail("mountpoint -q /var/lib/gang")
                assert withSecrets.succeed("systemctl show -P LoadState var-lib-gang.mount").strip() == "not-found"

            with subtest("restart removes stale secret files"):
                withSecrets.succeed("touch /etc/gang/secrets/stale")
                publish(withSecrets, "${publishedFile}")
                withSecrets.succeed("systemctl restart gang-config.service")
                withSecrets.wait_until_succeeds("test \"$(wc -l < /run/probe-controller)\" -eq 2")
                withSecrets.wait_for_unit("gang-controller.target")
                assert withSecrets.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"
                withSecrets.fail("test -e /etc/gang/secrets/stale")
                assert withSecrets.succeed(f"cat {SECRET}") == "s3cr3t"
                assert withSecrets.succeed("cat /etc/gang/gang.toml") == TOML

            with subtest("restart without configured secrets removes every secret file"):
                withSecrets.succeed("echo 'controller /legacy' > /run/fake-aws/user-data")
                withSecrets.succeed("systemctl restart gang-config.service")
                withSecrets.succeed("test -z \"$(ls -A /etc/gang/secrets)\"")

        with guest(controllerstate):
            with subtest("controller state disk is mounted before the role and stays mounted on restart"):
                controllerstate.wait_for_unit("probe-controller.service")
                assert controllerstate.succeed("cat /etc/gang/gang.toml") == TOML
                assert controllerstate.succeed("systemctl is-active var-lib-gang.mount").strip() == "active"
                assert controllerstate.succeed("cat /run/probe-controller") == "644\n"
                assert controllerstate.succeed("findmnt -no FSTYPE,SOURCE /var/lib/gang").strip() == "ext4 /dev/vdc"
                controllerstate.succeed("systemctl restart gang-config.service")
                controllerstate.wait_until_succeeds("test \"$(wc -l < /run/probe-controller)\" -eq 2")
                assert controllerstate.succeed("findmnt -no FSTYPE,SOURCE /var/lib/gang").strip() == "ext4 /dev/vdc"
                assert controllerstate.succeed("systemctl is-active var-lib-gang.mount").strip() == "active"

            with subtest("existing controller state survives an unmount and restart"):
                controllerstate.succeed("echo marker > /var/lib/gang/marker")
                controllerstate.succeed("umount /var/lib/gang")
                controllerstate.fail("mountpoint -q /var/lib/gang")
                controllerstate.succeed("systemctl restart gang-config.service")
                assert controllerstate.succeed("findmnt -no FSTYPE,SOURCE /var/lib/gang").strip() == "ext4 /dev/vdc"
                assert controllerstate.succeed("cat /var/lib/gang/marker") == "marker\n"

            with subtest("controller keeps its selected generation across restart and reboot"):
                check_generation_across_restart_and_reboot(controllerstate, "controller")
                assert controllerstate.succeed("cat /var/lib/gang/marker") == "marker\n"

        with guest(plain):
            with subtest("plain installs gang.toml without a secrets directory"):
                plain.wait_for_unit("probe-worker.service")
                plain.wait_for_unit("gang-worker.target")
                assert plain.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert plain.succeed("systemctl show -P ActiveEnterTimestampMonotonic probe-controller.service").strip() == "0"
                assert plain.succeed("cat /run/probe-worker") == "644\n"
                assert plain.succeed("cat /etc/gang/gang.toml") == TOML
                plain.fail("test -e /etc/gang/secrets")

            with subtest("worker keeps its selected generation across restart and reboot"):
                check_generation_across_restart_and_reboot(plain, "worker")

            with subtest("fake SSM returns native version selection metadata"):
                # The pinned production AWS CLI verifies the fake's SSM response contract.
                aws = (
                    "AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test "
                    "${pkgs.awscli2}/bin/aws --region eu-north-1 "
                    "--endpoint-url http://169.254.169.254:8080 ssm get-parameter"
                )
                selected = json.loads(plain.succeed(f"{aws} --name '${generation}:1'"))["Parameter"]
                latest = json.loads(plain.succeed(f"{aws} --name '${generation}'"))["Parameter"]
                assert selected["Name"] == "${generation}"
                assert selected["Version"] == 1
                assert selected["Selector"] == ":1"
                assert selected["Value"] == TOML
                assert latest["Version"] == 3
                assert latest["Value"] == "later version"
                status, output = plain.execute(f"{aws} --name '${generation}:99' 2>&1")
                assert status == 254, output
                assert "ParameterVersionNotFound" in output, output

        with guest(badname):
            with subtest("invalid secret file name is rejected before anything is written"):
                badname.wait_until_succeeds(f"{JOURNAL} | grep -q 'invalid secret file name'")
                badname.fail("test -e /etc/gang/gang.toml")
                assert badname.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert badname.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"

        with guest(missing):
            with subtest("missing SSM parameter fails without writing gang.toml"):
                missing.wait_until_succeeds(f"{JOURNAL} | grep -q 'SSM parameter reference /gang/absent is empty or unreadable'")
                missing.wait_until_succeeds("systemctl is-failed gang-config.service")
                missing.fail("test -e /etc/gang/gang.toml")
                assert missing.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert missing.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"

            with subtest("an absent selected version fails rather than reading latest"):
                missing.succeed("echo 'worker ${generation}:99' > /run/fake-aws/user-data")
                missing.fail("systemctl start gang-config.service")
                missing.wait_until_succeeds(f"{JOURNAL} | grep -q 'SSM parameter reference ${generation}:99 is empty or unreadable'")
                missing.fail("test -e /etc/gang/gang.toml")
                assert missing.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"

        with guest(badrole):
            with subtest("unknown role fails before writing gang.toml or starting a target"):
                badrole.wait_until_succeeds(f"{JOURNAL} | grep -q \"role 'builder' is not one of: controller worker\"")
                badrole.wait_until_succeeds("systemctl is-failed gang-config.service")
                badrole.fail("test -e /etc/gang/gang.toml")
                assert badrole.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert badrole.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"

        with guest(noparameter):
            with subtest("missing parameter fails before writing gang.toml"):
                noparameter.wait_until_succeeds(f"{JOURNAL} | grep -q \"must be '<role> <config-parameter-reference>' or 'controller <config-parameter-reference> state=<stateDevice>'\"")
                noparameter.wait_until_succeeds("systemctl is-failed gang-config.service")
                noparameter.fail("test -e /etc/gang/gang.toml")
                assert noparameter.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert noparameter.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"

        with guest(extratoken):
            with subtest("extra token fails before writing gang.toml"):
                extratoken.wait_until_succeeds(f"{JOURNAL} | grep -q \"must be '<role> <config-parameter-reference>' or 'controller <config-parameter-reference> state=<stateDevice>'\"")
                extratoken.wait_until_succeeds("systemctl is-failed gang-config.service")
                extratoken.fail("test -e /etc/gang/gang.toml")
                assert extratoken.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert extratoken.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"

        with guest(workerstate):
            with subtest("state token on a worker is rejected"):
                workerstate.wait_until_succeeds(f"{JOURNAL} | grep -q \"only a controller's first EC2 user-data line may end with state=/dev/vdc\"")
                workerstate.wait_until_succeeds("systemctl is-failed gang-config.service")
                workerstate.fail("test -e /etc/gang/gang.toml")
                assert workerstate.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert workerstate.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"
                workerstate.succeed("rc=0; blkid -p /dev/vdc >/dev/null || rc=$?; test \"$rc\" -eq 2")
                workerstate.fail("mountpoint -q /var/lib/gang")
                assert workerstate.succeed("systemctl show -P LoadState var-lib-gang.mount").strip() == "not-found"

        with guest(wrongdevice):
            with subtest("wrong state device is rejected"):
                wrongdevice.wait_until_succeeds(f"{JOURNAL} | grep -q \"only a controller's first EC2 user-data line may end with state=/dev/vdc\"")
                wrongdevice.wait_until_succeeds("systemctl is-failed gang-config.service")
                wrongdevice.fail("test -e /etc/gang/gang.toml")
                assert wrongdevice.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-controller.target").strip() == "0"
                assert wrongdevice.succeed("systemctl show -P ActiveEnterTimestampMonotonic gang-worker.target").strip() == "0"
                wrongdevice.succeed("rc=0; blkid -p /dev/vdc >/dev/null || rc=$?; test \"$rc\" -eq 2")
                wrongdevice.fail("mountpoint -q /var/lib/gang")
                assert wrongdevice.succeed("systemctl show -P LoadState var-lib-gang.mount").strip() == "not-found"
      '';
    }
