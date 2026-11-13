#!/usr/bin/env bash
# A script as toolchains ship them (rules_js's launcher): its interpreter through /usr/bin/env, its
# tools through the action's PATH, and the shells actions name by absolute path.
set -eu
/bin/sh -c 'exit 0'
/bin/bash -c 'exit 0'
printf 'RAN\n' | tr A-Z a-z
