# ADR-48: Each database backend is a separate plugin crate

Status: Accepted.

## Context
- Durable controller state needs the same semantics in local and shared databases. Embedding a database implementation in a cloud provisioner couples unrelated plugins.

## Decision
- Keep each database backend (`gang-sqlite`, `gang-postgres`, `gang-dynamodb`) a separate plugin crate behind the state repository, leader lease and expiring record ports. Backend-specific configuration, schema and error mapping belong to the backend.
- One database contract suite exercises every backend.

## Consequences
- Callers use database ports rather than backend-specific operations. The database copy command can migrate repository records between configured plugins while both databases are quiescent.
- The AWS SDK plumbing moves to `gang-aws-sdk`, shared by `gang-aws` and `gang-dynamodb`.
- The PostgreSQL contract test runs a real server binary pinned by checksum.

## Rejected alternatives
- **Put DynamoDB in the AWS provisioner:** couples database choice to cloud provisioner choice.
- **Backend capability flags:** move semantic differences to every caller instead of providing a common contract.
- **sqlx:** compile-time database access and macros add a database dependency to compilation.
- **A fake PostgreSQL:** it would test the fake rather than the server.
