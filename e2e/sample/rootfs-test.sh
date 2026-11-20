#!/bin/bash
set -eu

[ "${GANG_E2E_EXACT_ENV:-}" = declared ]
[ "$(cat "$TEST_SRCDIR/$TEST_WORKSPACE/rootfs_environment.witness")" = ok ]
[ "${LD_LIBRARY_PATH+x}" != x ]
[ "$(id -u)" = 1000 ] && [ "$(id -g)" = 1000 ]
[ "$(id -un)" = builder ] && [ "$(id -gn)" = builder ]
[ ! -e /nix/store ] && [ ! -e /run/current-system ]
[ -s /etc/ssl/certs/ca-certificates.crt ]
[ -r /etc/hosts ] && [ -r /etc/nsswitch.conf ]
[ "$(stat -f -c %T /tmp)" = tmpfs ]
if touch /etc/gang-write-probe 2>/dev/null; then exit 1; fi
if touch /usr/gang-write-probe 2>/dev/null; then exit 1; fi
PATH=/bin:/usr/bin /bin/bash -c 'set -eu; command -v bash >/dev/null; command -v awk >/dev/null; [ "$(printf ok)" = ok ]'
env -u PATH /bin/bash -c 'set -eu; [ "$(printf ok)" = ok ]'
"$TEST_SRCDIR/$TEST_WORKSPACE/action-environment-probe" --gang-e2e-action-environment-probe roots
if SSL_CERT_FILE=/missing-ca-file SSL_CERT_DIR=/missing-ca-dir "$TEST_SRCDIR/$TEST_WORKSPACE/action-environment-probe" --gang-e2e-action-environment-probe roots; then exit 1; fi
# Exercise Bazel test-setup's undeclared-output zip handling.
printf 'rootfs\n' > "$TEST_UNDECLARED_OUTPUTS_DIR/rootfs.txt"
