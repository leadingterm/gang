# ADR-47: Publish public AMIs in every image region

Status: Accepted.

## Context
- External teams need to deploy in their own accounts and regions without importing images. Public-image guardrails are loosened only for the publishing account.

## Decision
- A tag-triggered job assumes a tag-trusted role, verifies and uploads each architecture's image once in the home region, and registers `gang-<arch>-<sha16>` with shared tags and deregistration protection. It copies each image to every image region and makes each copy public; one control-plane owner maintains the region list. The attested `gang-amis.json` lists the regional AMI IDs and image SHA-256s.
- A successor supersedes its predecessor only after it is available, public and protected in every image region. The predecessor is deprecated 184 days after the successor's publication and retired 92 days after deprecation, following the protection cooldown. Gang never retires the latest release.
- `gang release ami-steps` makes pure decisions from observed state; every run converges, and a backfill into new regions is the same run without a new release.
- The deployment module resolves the image of its own version: the SHA256SUMS of its release, then the publisher's AMI named after the SHA-256 whose shared tag matches; images and fleets[].image override per architecture or fleet.

## Consequences
- Retaining a release costs about $4.2 per month, with about $1.6 per release for copies.
- Outsiders cannot prove byte equality of an AMI with the released image; snapshots remain private. Importing the released image is the remedy when byte verification is required.
- S3 image publishing stops; existing objects stay.

## Rejected alternatives
- **Import by default:** every deploying team repeats the image work.
- **One region plus user copies:** every team must copy and maintain images in its own regions.
- **Marketplace:** adds a distribution path to a public image.
- **Requester-pays S3:** consumers still have to import images.
