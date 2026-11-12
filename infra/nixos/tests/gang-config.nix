{pkgs}: let
  inherit (pkgs) lib;

  # One HTTP server answers the EC2 metadata routes on :80 and the SSM JSON
  # 1.1 GetParameter route on :8080. Values come from USER_DATA and PARAMETERS.
  fakeAws = pkgs.writeText "fake-aws.py" ''
    import http.server
    import json
    import os
    import threading

    PARAMETERS = json.loads(os.environ["PARAMETERS"])
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
            name = request.get("Name")
            if name not in PARAMETERS:
                body = {"__type": "ParameterNotFound", "message": "not found"}
                self.reply(400, json.dumps(body).encode(), SSM_TYPE)
                return
            body = {
                "Parameter": {
                    "Name": name,
                    "Type": "String",
                    "Value": PARAMETERS[name],
                    "Version": 1,
                }
            }
            self.reply(200, json.dumps(body).encode(), SSM_TYPE)


    ssm = http.server.ThreadingHTTPServer(("0.0.0.0", 8080), Ssm)
    metadata = http.server.ThreadingHTTPServer(("0.0.0.0", 80), Metadata)
    print("fake-aws listening", flush=True)
    threading.Thread(target=ssm.serve_forever, daemon=True).start()
    metadata.serve_forever()
  '';

  tomlText = "[server]\ntrust_domain = \"example.com\"\n";

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
          PARAMETERS = builtins.toJSON parameters;
        };
        serviceConfig = {
          ExecStart = "${pkgs.python3}/bin/python3 ${fakeAws}";
          Restart = "on-failure";
          RuntimeDirectory = "fake-aws";
          ExecStartPre = "${pkgs.coreutils}/bin/install -m 0644 ${pkgs.writeText "user-data" userData} /run/fake-aws/user-data";
        };
      };

      probe = {
        requires = ["gang-config.service"];
        after = ["gang-config.service"];
        wantedBy = ["multi-user.target"];
        script = "stat -c '%a' /etc/gang/gang.toml > /run/probe-mode";
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
      };
    };
  };
in
  pkgs.testers.runNixOSTest {
    name = "gang-config";

    nodes = {
      withSecrets = baseModule {
        userData = "/gang/gang.toml\ndashboard-client-secret=/gang/secrets/client";
        parameters = {
          "/gang/gang.toml" = tomlText;
          "/gang/secrets/client" = "s3cr3t";
        };
      };

      plain = baseModule {
        userData = "/gang/gang.toml";
        parameters = {
          "/gang/gang.toml" = tomlText;
        };
      };

      badname = baseModule {
        userData = "/gang/gang.toml\n../escape=/gang/secrets/client";
        parameters = {
          "/gang/gang.toml" = tomlText;
          "/gang/secrets/client" = "s3cr3t";
        };
      };

      missing = {
        imports = [
          (baseModule {
            userData = "/gang/absent";
            parameters = {};
          })
        ];
        systemd.services.gang-config.serviceConfig.Restart = lib.mkForce "no";
      };
    };

    testScript = ''
      TOML = ${builtins.toJSON tomlText}
      JOURNAL = "journalctl -u gang-config.service"
      SECRET = "/etc/gang/secrets/dashboard-client-secret"

      start_all()

      with subtest("withSecrets installs gang.toml and secrets before dependents start"):
          withSecrets.wait_for_unit("probe.service")
          assert withSecrets.succeed("cat /etc/gang/gang.toml") == TOML
          assert withSecrets.succeed("stat -c '%a' /etc/gang/gang.toml").strip() == "644"
          assert withSecrets.succeed("cat /run/probe-mode").strip() == "644"
          assert withSecrets.succeed("stat -c '%a %U:%G' /etc/gang/secrets").strip() == "700 gang:gang"
          assert withSecrets.succeed(f"stat -c '%a %U:%G %s' {SECRET}").strip() == "600 gang:gang 6"
          assert withSecrets.succeed(f"cat {SECRET}") == "s3cr3t"

      with subtest("restart removes stale secret files"):
          withSecrets.succeed("touch /etc/gang/secrets/stale")
          withSecrets.succeed("systemctl restart gang-config.service")
          withSecrets.fail("test -e /etc/gang/secrets/stale")
          assert withSecrets.succeed(f"cat {SECRET}") == "s3cr3t"

      with subtest("restart without configured secrets removes every secret file"):
          withSecrets.succeed("echo /gang/gang.toml > /run/fake-aws/user-data")
          withSecrets.succeed("systemctl restart gang-config.service")
          withSecrets.succeed("test -z \"$(ls -A /etc/gang/secrets)\"")

      with subtest("plain installs gang.toml without a secrets directory"):
          plain.wait_for_unit("probe.service")
          assert plain.succeed("cat /etc/gang/gang.toml") == TOML
          plain.fail("test -e /etc/gang/secrets")

      with subtest("invalid secret file name is rejected before anything is written"):
          badname.wait_until_succeeds(f"{JOURNAL} | grep -q 'invalid secret file name'")
          badname.fail("test -e /etc/gang/gang.toml")

      with subtest("missing SSM parameter fails without writing gang.toml"):
          missing.wait_until_succeeds(f"{JOURNAL} | grep -q 'SSM parameter /gang/absent is empty or unreadable'")
          missing.wait_until_succeeds("systemctl is-failed gang-config.service")
          missing.fail("test -e /etc/gang/gang.toml")
    '';
  }
