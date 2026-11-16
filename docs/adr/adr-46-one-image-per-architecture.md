# ADR-46: One image per architecture; the role comes from the user data

Status: Accepted.

## Context
- Separate controller and worker images doubled the images to copy, their quota and their cost. The role is a fact of the launch, known to the module that launches the instance.

## Decision
- One NixOS image holds both roles. The first user-data line is `<role> <parameter>`, optionally followed for a controller by `state=<device>`, written by the module that launches the instance. `gang-config.service`, the one reader of the user data, validates it and starts `gang-<role>.target`, the only parent of that role's units.
- The controller's state disk is an explicit fact of the launch: the module that attaches it names it on line 1 (`state=/dev/sdg`), and gang-config mounts it before the role starts. A controller without the token (shared database engines) mounts nothing; no device is detected, and the state device is not a worker's scratch device.
- The boot check runs three times: controller without the state token, controller with it, and worker. It asserts the role's target is active and no other role's target was ever active, and that a blank state disk is formatted and mounted only when named.

## Consequences
- The controller's closure gains the worker's inactive tools (bubblewrap, nix-ld loader, mdadm, xfsprogs, yq-go, RAID0/XFS modules and action tools), about 70 MB.
- Nothing changes on workers except two firewall ports with no listener.
- One image name and one AMI per architecture; a custom image passes the same three-run check.
- The controller module that renders the state token ships in the same release as the image.

## Rejected alternatives
- **The role in `gang.toml`:** one file for all nodes.
- **Instance tags:** a second owner and IMDS tag access.
- **Per-unit conditions:** systemd still starts a skipped unit's requirements.
- **A per-role firewall unit:** no exposure gained; security groups filter.
- **Mounting the state disk when present:** detection, a second owner of the launch's facts.
