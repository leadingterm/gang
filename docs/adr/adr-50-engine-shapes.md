# ADR-50: The database engine selects the deployment shape

Status: Accepted. Builds on ADR-43 and ADR-45.

## Context
- Small deployments need a single controller without the cost of a shared database and load balancers. Highly available deployments need replaceable controllers and stable endpoints.

## Decision
- The deployment module selects the controller shape from the database engine: SQLite runs one instance with its state volume; a shared database runs a controller Auto Scaling group behind the load balancers of ADR-45.
- PostgreSQL uses either `postgres.rds` (an RDS for PostgreSQL instance, Multi-AZ by default, with its password passed through write-only arguments into the instance and SSM) or `postgres.external`.

## Consequences
- One module supports both shapes. Switching engines is a planned migration using the database copy command, with both databases quiescent.
- SQLite retains the single-attach volume constraint; the group can replace controllers without tying their state to an instance.
- The group uses EC2 health rather than ELB health; a ready newer contender can serve shared requests before leadership, as described in ADR-59.

## Rejected alternatives
- **Separate deployment modules:** duplicates network, identity and fleet wiring.
- **An Auto Scaling group for SQLite:** the single-attach state volume still requires stop-before-start replacement.
- **Aurora Serverless v2:** about three times the idle cost; lease renewals never let it pause.
- **IAM database authentication:** puts the AWS SDK into `gang-postgres` and needs a token every 15 minutes.
