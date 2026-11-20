"""Parameters of one end-to-end run; the harness overwrites this file in its staged copy."""

SALT = ""
ACTION_PATH = ""
ACTION_SHELL = "/bin/bash"
ACTION_IPC = False
HOST_ACTION_PATH = ""
HOST_ACTION_SHELL = "/bin/bash"
SIGNAL = ""
ATTEMPTS = ""
ISA = "x86-64"
MEMORY_KB = 65536
CPU_COUNT = 1
LARGE_FLEET = "local-large"
LARGE_MEMORY_KB = 16777216
REQUESTED_SANDBOX = "noop"
SIGNAL_COMMAND = "printf s >\"$E2E_SIGNAL\""
