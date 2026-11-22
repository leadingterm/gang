#!/bin/sh
set -eu
umask 077

mkdir -p /etc/gang/secrets
chmod 0755 /etc/gang
chmod 0700 /etc/gang/secrets
printf '%s' "${GANG_CONFIG:?missing GANG_CONFIG}" > /etc/gang/gang.toml
unset GANG_CONFIG

while IFS= read -r record; do
    [ -n "$record" ] || continue
    index=${record%%=*}
    name=${record#*=}
    case "$record" in *=*) ;; *) echo 'invalid secret record' >&2; exit 1 ;; esac
    case "$index" in ''|*[!0-9]*) echo 'invalid secret index' >&2; exit 1 ;; esac
    case "$name" in ''|.|..|*[!a-zA-Z0-9._-]*) echo 'invalid secret filename' >&2; exit 1 ;; esac
    eval 'value=${GANG_SECRET_'"$index"'?'"'missing secret'"'}'
    printf '%s' "$value" > "/etc/gang/secrets/$name"
    chmod 0600 "/etc/gang/secrets/$name"
    unset value "GANG_SECRET_$index"
done <<EOF
${GANG_SECRET_FILES-}
EOF
unset GANG_SECRET_FILES
chown -R 65532:65532 /etc/gang/secrets /etc/gang/gang.toml
chmod 0600 /etc/gang/gang.toml
if [ -n "${ECS_CONTAINER_METADATA_URI_V4-}" ]; then
    metadata=$(wget -q -T 10 -O - "$ECS_CONTAINER_METADATA_URI_V4/task")
    task_arn=$(printf '%s' "$metadata" | jq -er '.TaskARN | strings | select(startswith("arn:") and length > 4)')
    GANG_CONTROLLER_LAUNCHED_AT=$(printf '%s' "$metadata" | jq -er '
        [.Containers[] | select(.Name == "controller") | .CreatedAt] |
        select(length == 1) | .[0] | strings |
        select(test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$"))
    ')
    GANG_CONTROLLER_IDENTITY=$(printf '%s' "$task_arn" | sha256sum)
    GANG_CONTROLLER_IDENTITY=${GANG_CONTROLLER_IDENTITY%% *}
    export GANG_CONTROLLER_IDENTITY GANG_CONTROLLER_LAUNCHED_AT
    unset metadata task_arn
fi
if [ ! -e /etc/gang/upstream-resolv.conf ]; then
    cp /etc/resolv.conf /etc/gang/upstream-resolv.conf
fi
chmod 0644 /etc/gang/upstream-resolv.conf
# Keep search domains and resolver options while directing all lookups through the cache.
{ printf 'nameserver 127.0.0.1\n'; grep -E '^[[:space:]]*(search|domain|options)[[:space:]]' /etc/gang/upstream-resolv.conf || true; } > /etc/resolv.conf
dnsmasq --listen-address=127.0.0.1 --bind-interfaces --resolv-file=/etc/gang/upstream-resolv.conf --user=gang-dns --group=gang-dns --pid-file=
exec su-exec 65532:65532 /usr/local/bin/gang server --config /etc/gang/gang.toml
