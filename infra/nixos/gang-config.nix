{
  lib,
  pkgs,
  ...
}: let
  imds = "http://169.254.169.254/latest";
  gangConfig = pkgs.writeShellApplication {
    name = "gang-config";
    runtimeInputs = [
      pkgs.awscli2
      pkgs.curl
      pkgs.coreutils
    ];
    text = ''
      token=$(curl --fail --silent --show-error --retry 10 --retry-all-errors \
        -X PUT "${imds}/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60")

      imds_get() {
        curl --fail --silent --show-error --retry 10 --retry-all-errors \
          -H "X-aws-ec2-metadata-token: $token" \
          "${imds}/$1"
      }

      user_data=$(imds_get user-data)
      name=$(printf '%s\n' "$user_data" | head -n 1)
      if [ -z "$name" ]; then
        echo "gang-config: EC2 user-data (parameter name) is empty" >&2
        exit 1
      fi

      region=$(imds_get meta-data/placement/region)
      if [ -z "$region" ]; then
        echo "gang-config: placement region is empty" >&2
        exit 1
      fi

      fetch_parameter() {
        local value=""
        for _ in $(seq 1 30); do
          if value=$(aws ssm get-parameter --region "$region" --name "$1" \
            --with-decryption --query Parameter.Value --output text) && [ -n "$value" ]; then
            break
          fi
          sleep 2
        done
        if [ -z "$value" ]; then
          echo "gang-config: SSM parameter $1 is empty or unreadable" >&2
          exit 1
        fi
        printf '%s' "$value"
      }

      tmp=""
      trap 'rm -f -- "$tmp"' EXIT

      # Lines after the first are `<file>=<parameter>` secrets. Validate all of
      # them before writing anything.
      secret_lines=$(printf '%s\n' "$user_data" | tail -n +2)
      files=()
      params=()
      while IFS='=' read -r file parameter; do
        if [ -z "$file$parameter" ]; then
          continue
        fi
        case "$file" in
          "" | .* | */*)
            echo "gang-config: invalid secret file name '$file'" >&2
            exit 1
            ;;
        esac
        if [ -z "$parameter" ]; then
          echo "gang-config: secret '$file' has no SSM parameter" >&2
          exit 1
        fi
        files+=("$file")
        params+=("$parameter")
      done <<< "$secret_lines"

      if [ ''${#files[@]} -gt 0 ]; then
        install -d -m 0700 -o gang -g gang /etc/gang/secrets
        for i in "''${!files[@]}"; do
          secret=$(fetch_parameter "''${params[$i]}")
          tmp=$(mktemp -p /etc/gang/secrets)
          printf '%s' "$secret" > "$tmp"
          chown gang:gang "$tmp"
          chmod 0600 "$tmp"
          mv -f -- "$tmp" "/etc/gang/secrets/''${files[$i]}"
          tmp=""
        done

      fi

      # Secrets not named in the current user-data are removed on every run,
      # including when none are configured.
      if [ -d /etc/gang/secrets ]; then
        declare -A wanted=()
        for file in "''${files[@]}"; do
          wanted[$file]=1
        done
        for path in /etc/gang/secrets/*; do
          [ -e "$path" ] || [ -L "$path" ] || continue
          if [ -z "''${wanted[''${path##*/}]:-}" ]; then
            rm -f -- "$path"
          fi
        done
      fi

      toml=$(fetch_parameter "$name")
      tmp=$(mktemp -p /etc/gang)
      printf '%s\n' "$toml" > "$tmp"
      chown root:root "$tmp"
      chmod 0644 "$tmp"
      mv -f -- "$tmp" /etc/gang/gang.toml
    '';
  };
in {
  users.groups.gang = {};
  users.users.gang = {
    isSystemUser = true;
    group = "gang";
    createHome = false;
  };

  systemd.tmpfiles.rules = ["d /etc/gang 0755 root root -"];

  systemd.services.gang-config = {
    description = "Write gang.toml from the SSM parameter named in EC2 user-data";
    wantedBy = ["multi-user.target"];
    wants = ["network-online.target"];
    after = ["network-online.target"];
    environment = {
      AWS_MAX_ATTEMPTS = "10";
      AWS_RETRY_MODE = "standard";
    };
    serviceConfig = {
      Type = "oneshot";
      RemainAfterExit = true;
      ExecStart = lib.getExe gangConfig;
      Restart = "on-failure";
      RestartSec = 5;
    };
  };
}
