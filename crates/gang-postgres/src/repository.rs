use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PortError, PrincipalRecord, RecordedScalingEvent,
    RevocationEpoch, ScalingAuditEvent, ScalingEventSeq, StateRepositoryPort, Subject,
};
use gang_types::{CertSerial, Identity, ScalingCause};
use tokio_postgres::types::Type;

use crate::codec::{
    at, certificate_from_row, corrupt, get, page, principal_from_row, scaling_from_row, signed,
    unsigned,
};
use crate::plugin::{Finish, Postgres};

fn fetch_count(limit: NonZeroUsize) -> i64 {
    i64::try_from(limit.get().saturating_add(1)).unwrap_or(i64::MAX)
}

impl StateRepositoryPort for Postgres {
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
        let issued_at = at(record.issued_at)?;
        let expires_at = at(record.expires_at)?;
        let revoked_at = match record.status {
            CertStatus::Active => None,
            CertStatus::Revoked { at: value } => Some(at(value)?),
        };
        self.mutate(async |client| {
            client.execute_typed(
                "INSERT INTO issued_certificates (serial, provider, principal, issued_at, expires_at, revoked_at) VALUES ($1, $2, $3, $4, $5, $6)",
                &[(&record.serial.as_str(), Type::TEXT), (&record.identity.provider.as_str(), Type::TEXT), (&record.identity.principal.as_str(), Type::TEXT), (&issued_at, Type::TIMESTAMPTZ), (&expires_at, Type::TIMESTAMPTZ), (&revoked_at, Type::TIMESTAMPTZ)],
            ).await?;
            Ok(())
        }).await?;
        Ok(())
    }

    async fn certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        Ok(self.run(async |client| {
            client.query_typed_opt(
                "SELECT serial, provider, principal, issued_at, expires_at, revoked_at FROM issued_certificates WHERE serial = $1",
                &[(&serial.as_str(), Type::TEXT)],
            ).await?.as_ref().map(certificate_from_row).transpose()
        }).await?)
    }

    async fn revoke(
        &self,
        serial: &CertSerial,
        when: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        let when = at(when)?;
        self.transaction(async |tx| {
            let row = tx.query_typed_opt(
                "UPDATE issued_certificates SET revoked_at = COALESCE(revoked_at, $1) WHERE serial = $2 RETURNING serial, provider, principal, issued_at, expires_at, revoked_at",
                &[(&when, Type::TIMESTAMPTZ), (&serial.as_str(), Type::TEXT)],
            ).await?;
            let record = row.as_ref().map(certificate_from_row).transpose()?;
            if record.is_some() {
                tx.query_typed_opt("UPDATE counters SET value = value + 1 WHERE name = 'revocation' RETURNING value", &[]).await?.ok_or_else(|| corrupt("counters", "missing revocation counter"))?;
                Ok(Finish::Commit(record))
            } else {
                Ok(Finish::Rollback(None))
            }
        }).await?.ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        Ok(self.run(async |client| {
            client.query_typed(
                "SELECT serial, provider, principal, issued_at, expires_at, revoked_at FROM issued_certificates WHERE revoked_at IS NOT NULL ORDER BY revoked_at, serial", &[],
            ).await?.iter().map(certificate_from_row).collect()
        }).await?)
    }

    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        Ok(self.run(async |client| {
            client.query_typed(
                "SELECT serial, provider, principal, issued_at, expires_at, revoked_at FROM issued_certificates WHERE provider = $1 AND principal = $2 AND revoked_at IS NULL ORDER BY issued_at, serial",
                &[(&identity.provider.as_str(), Type::TEXT), (&identity.principal.as_str(), Type::TEXT)],
            ).await?.iter().map(certificate_from_row).collect()
        }).await?)
    }

    async fn certificates(
        &self,
        after: Option<&CertSerial>,
        limit: NonZeroUsize,
        into: &mut Vec<IssuedCertRecord>,
    ) -> Result<bool, PortError> {
        let after = after.map(CertSerial::as_str);
        let count = fetch_count(limit);
        let mut rows: Vec<_> = self.run(async |client| {
            client.query_typed(
                "SELECT serial, provider, principal, issued_at, expires_at, revoked_at FROM issued_certificates WHERE ($1::text IS NULL OR serial > $1) ORDER BY serial LIMIT $2",
                &[(&after, Type::TEXT), (&count, Type::INT8)],
            ).await?.iter().map(certificate_from_row).collect()
        }).await?;
        let more = page(&mut rows, limit);
        into.clear();
        into.extend(rows);
        Ok(more)
    }

    async fn revocation_epoch(&self) -> Result<RevocationEpoch, PortError> {
        let value = self
            .run(async |client| {
                let row = client
                    .query_typed_opt("SELECT value FROM counters WHERE name = 'revocation'", &[])
                    .await?
                    .ok_or_else(|| corrupt("counters", "missing revocation counter"))?;
                unsigned("value", get(&row, "value")?)
            })
            .await?;
        Ok(RevocationEpoch::new(value))
    }

    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        event
            .kind
            .validate()
            .map_err(|error| corrupt(error.field(), &error))?;
        let when = at(event.at)?;
        let kind = event.kind.stored();
        let from = kind.from_capacity.map(i64::from);
        let to = kind.to_capacity.map(i64::from);
        let cause = event.cause.map(ScalingCause::as_str);
        let detail = kind.detail.map(str::as_bytes);
        Ok(self.transaction(async |tx| {
            let row = tx.query_typed_opt("UPDATE counters SET value = value + 1 WHERE name = 'audit' RETURNING value", &[]).await?.ok_or_else(|| corrupt("counters", "missing audit counter"))?;
            let sequence: i64 = get(&row, "value")?;
            let seq = ScalingEventSeq::new(unsigned("sequence", sequence)?);
            tx.execute_typed(
                "INSERT INTO scaling_audit (sequence, at, fleet, kind, from_capacity, to_capacity, subject, cause, detail) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[(&sequence, Type::INT8), (&when, Type::TIMESTAMPTZ), (&event.fleet.as_str(), Type::TEXT), (&kind.name, Type::TEXT), (&from, Type::INT8), (&to, Type::INT8), (&kind.subject, Type::TEXT), (&cause, Type::TEXT), (&detail, Type::BYTEA)],
            ).await?;
            Ok(Finish::Commit(seq))
        }).await?)
    }

    async fn scaling_events(
        &self,
        since: ScalingEventSeq,
        before: Option<ScalingEventSeq>,
        limit: NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError> {
        let since = signed("sequence", since.get())?;
        let before = before
            .map(|seq| signed("sequence", seq.get()))
            .transpose()?;
        let count = fetch_count(limit);
        let mut rows: Vec<_> = self.run(async |client| {
            client.query_typed(
                "SELECT sequence, at, fleet, kind, from_capacity, to_capacity, subject, cause, detail FROM scaling_audit WHERE sequence > $1 AND ($2::bigint IS NULL OR sequence < $2) ORDER BY sequence DESC LIMIT $3",
                &[(&since, Type::INT8), (&before, Type::INT8), (&count, Type::INT8)],
            ).await?.iter().map(scaling_from_row).collect()
        }).await?;
        let more = page(&mut rows, limit);
        rows.reverse();
        into.clear();
        into.extend(rows);
        Ok(more)
    }

    async fn observe_principal(
        &self,
        identity: &Identity,
        when: Timestamp,
    ) -> Result<(), PortError> {
        let when = at(when)?;
        self.mutate(async |client| {
            client.execute_typed(
                "INSERT INTO principals (provider, principal, first_seen, last_seen) VALUES ($1, $2, $3, $3) ON CONFLICT (provider, principal) DO UPDATE SET last_seen = GREATEST(principals.last_seen, EXCLUDED.last_seen)",
                &[(&identity.provider.as_str(), Type::TEXT), (&identity.principal.as_str(), Type::TEXT), (&when, Type::TIMESTAMPTZ)],
            ).await?;
            Ok(())
        }).await?;
        Ok(())
    }

    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        Ok(self.run(async |client| {
            client.query_typed_opt(
                "SELECT provider, principal, first_seen, last_seen, revoked_at FROM principals WHERE provider = $1 AND principal = $2",
                &[(&identity.provider.as_str(), Type::TEXT), (&identity.principal.as_str(), Type::TEXT)],
            ).await?.as_ref().map(principal_from_row).transpose()
        }).await?)
    }

    async fn revoke_principal(
        &self,
        identity: &Identity,
        when: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        let when = at(when)?;
        Ok(self.transaction(async |tx| {
            let row = tx.query_typed_one(
                "INSERT INTO principals (provider, principal, first_seen, last_seen, revoked_at) VALUES ($1, $2, $3, $3, $3) ON CONFLICT (provider, principal) DO UPDATE SET revoked_at = COALESCE(principals.revoked_at, EXCLUDED.revoked_at) RETURNING provider, principal, first_seen, last_seen, revoked_at",
                &[(&identity.provider.as_str(), Type::TEXT), (&identity.principal.as_str(), Type::TEXT), (&when, Type::TIMESTAMPTZ)],
            ).await?;
            let record = principal_from_row(&row)?;
            tx.query_typed_opt("UPDATE counters SET value = value + 1 WHERE name = 'revocation' RETURNING value", &[]).await?.ok_or_else(|| corrupt("counters", "missing revocation counter"))?;
            Ok(Finish::Commit(record))
        }).await?)
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        self.transaction(async |tx| {
            let row = tx.query_typed_opt(
                "UPDATE principals SET revoked_at = NULL WHERE provider = $1 AND principal = $2 RETURNING provider, principal, first_seen, last_seen, revoked_at",
                &[(&identity.provider.as_str(), Type::TEXT), (&identity.principal.as_str(), Type::TEXT)],
            ).await?;
            let record = row.as_ref().map(principal_from_row).transpose()?;
            if record.is_some() {
                tx.query_typed_opt("UPDATE counters SET value = value + 1 WHERE name = 'revocation' RETURNING value", &[]).await?.ok_or_else(|| corrupt("counters", "missing revocation counter"))?;
                Ok(Finish::Commit(record))
            } else {
                Ok(Finish::Rollback(None))
            }
        }).await?.ok_or_else(|| PortError::NotFound(Subject::Principal(identity.clone())))
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        Ok(self.run(async |client| {
            client.query_typed(
                "SELECT provider, principal, first_seen, last_seen, revoked_at FROM principals WHERE revoked_at IS NOT NULL ORDER BY revoked_at, provider, principal", &[],
            ).await?.iter().map(principal_from_row).collect()
        }).await?)
    }

    async fn principals(
        &self,
        after: Option<&Identity>,
        limit: NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        let provider = after.map(|identity| identity.provider.as_str());
        let principal = after.map(|identity| identity.principal.as_str());
        let count = fetch_count(limit);
        let mut rows: Vec<_> = self.run(async |client| {
            client.query_typed(
                "SELECT provider, principal, first_seen, last_seen, revoked_at FROM principals WHERE ($1::text IS NULL OR provider > $1 OR (provider = $1 AND principal > $2)) AND (NOT $3 OR revoked_at IS NOT NULL) ORDER BY provider, principal LIMIT $4",
                &[(&provider, Type::TEXT), (&principal, Type::TEXT), (&only_revoked, Type::BOOL), (&count, Type::INT8)],
            ).await?.iter().map(principal_from_row).collect()
        }).await?;
        let more = page(&mut rows, limit);
        into.clear();
        into.extend(rows);
        Ok(more)
    }
}
