### Fixed

- Return REAPI `FAILED_PRECONDITION` with `google.rpc.PreconditionFailure` (`type = "MISSING"`, `subject = "blobs/<hash>/<size>"`) immediately when an action's `Action`, `Command`, `Directory`, input tree, or input file blob is absent from CAS, without retrying on the worker.
- Verify output and directory digest completeness during `GetActionResult` and `Execute` action-cache lookups so stale action-cache entries referencing expired CAS blobs are treated as cache misses.
- Invalidate the controller's `S3ExistenceCache` with a generation guard on ownership-validated worker missing-blob reports so expired S3 objects are not resurrected by concurrent `HeadObject` probes.
