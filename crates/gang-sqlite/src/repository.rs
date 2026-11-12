use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PortError, PrincipalRecord, RecordedScalingEvent,
    ScalingAuditEvent, ScalingEventSeq, StateRepositoryPort, Subject,
};
use gang_types::{CertSerial, Identity};
use rusqlite::{OptionalExtension, Params, Row, named_params, params};

use crate::codec::{
    ScalingRow, certificate_columns, certificate_from_row, millis, page, principal_columns,
    principal_from_row, scaling_columns, scaling_from_row, scaling_seq, sequence_bound,
};
use crate::{Sqlite, SqliteError};

/// The certificate with the serial bound to `?1`.
const SELECT_CERTIFICATE: &str = concat!(
    "SELECT ",
    certificate_columns!(),
    " FROM issued_certificates WHERE serial = ?1"
);

/// Certificates that carry a revocation time, in revocation order.
const SELECT_REVOKED_CERTIFICATES: &str = concat!(
    "SELECT ",
    certificate_columns!(),
    " FROM issued_certificates ",
    "WHERE revoked_at IS NOT NULL ORDER BY revoked_at, serial"
);

/// The unrevoked certificates of one identity, oldest first.
const SELECT_CERTIFICATES_OF: &str = concat!(
    "SELECT ",
    certificate_columns!(),
    " FROM issued_certificates ",
    "WHERE provider = ?1 AND principal = ?2 AND revoked_at IS NULL ",
    "ORDER BY issued_at, serial"
);

/// Scaling events newest first, bounded by the `since` and `before` ids and a row limit.
const SELECT_SCALING_EVENTS: &str = concat!(
    "SELECT ",
    scaling_columns!(),
    " FROM scaling_audit ",
    "WHERE id > ?1 AND (?2 IS NULL OR id < ?2) ORDER BY id DESC LIMIT ?3"
);

/// The principal with the identity bound to `?1` and `?2`.
const SELECT_PRINCIPAL: &str = concat!(
    "SELECT ",
    principal_columns!(),
    " FROM principals ",
    "WHERE provider = ?1 AND principal = ?2"
);

/// Revoked principals in revocation order.
const SELECT_REVOKED_PRINCIPALS: &str = concat!(
    "SELECT ",
    principal_columns!(),
    " FROM principals ",
    "WHERE revoked_at IS NOT NULL ORDER BY revoked_at, provider, principal"
);

/// One page of principals in identity order, optionally only revoked ones.
const SELECT_PRINCIPALS_PAGE: &str = concat!(
    "SELECT ",
    principal_columns!(),
    " FROM principals ",
    "WHERE ((?1 IS NULL) OR provider > ?1 OR (provider = ?1 AND principal > ?2)) ",
    "AND (?3 = 0 OR revoked_at IS NOT NULL) ",
    "ORDER BY provider, principal LIMIT ?4"
);

/// The fetch size for a page of `limit` rows: one row past the limit reveals whether more exist.
fn fetch_count(limit: NonZeroUsize) -> i64 {
    i64::try_from(limit.get().saturating_add(1)).unwrap_or(i64::MAX)
}

impl StateRepositoryPort for Sqlite {
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
        let serial = record.serial.as_str().to_owned();
        let provider = record.identity.provider.as_str().to_owned();
        let principal = record.identity.principal.as_str().to_owned();
        let issued_at = millis(record.issued_at)?;
        let expires_at = millis(record.expires_at)?;
        let revoked_at = match record.status {
            CertStatus::Active => None,
            CertStatus::Revoked { at } => Some(millis(at)?),
        };
        self.run(move |db| {
            db.prepare_cached(
                "INSERT INTO issued_certificates \
                 (serial, provider, principal, issued_at, expires_at, revoked_at) \
                 VALUES (:serial, :provider, :principal, :issued_at, :expires_at, :revoked_at)",
            )?
            .execute(named_params! {
                ":serial": serial,
                ":provider": provider,
                ":principal": principal,
                ":issued_at": issued_at,
                ":expires_at": expires_at,
                ":revoked_at": revoked_at,
            })?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        let serial = serial.as_str().to_owned();
        Ok(self
            .run(move |db| {
                db.prepare_cached(SELECT_CERTIFICATE)?
                    .query_row(params![serial], |row| Ok(certificate_from_row(row)))
                    .optional()?
                    .transpose()
            })
            .await?)
    }

    /// A revoked certificate keeps its first revocation time: a later revocation is a no-op.
    async fn revoke(
        &self,
        serial: &CertSerial,
        at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        let key = serial.as_str().to_owned();
        let at = millis(at)?;
        self.run(move |db| {
            db.prepare_cached(concat!(
                "UPDATE issued_certificates SET revoked_at = COALESCE(revoked_at, :at) ",
                "WHERE serial = :serial RETURNING ",
                certificate_columns!()
            ))?
            .query_row(named_params! { ":at": at, ":serial": key }, |row| {
                Ok(certificate_from_row(row))
            })
            .optional()?
            .transpose()
        })
        .await?
        .ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        Ok(self
            .query_all(SELECT_REVOKED_CERTIFICATES, [], certificate_from_row)
            .await?)
    }

    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        let params = (
            identity.provider.as_str().to_owned(),
            identity.principal.as_str().to_owned(),
        );
        Ok(self
            .query_all(SELECT_CERTIFICATES_OF, params, certificate_from_row)
            .await?)
    }

    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        let row = ScalingRow::try_from(event)?;
        Ok(self
            .run(move |db| {
                db.prepare_cached(
                    "INSERT INTO scaling_audit \
                     (at, fleet, kind, from_capacity, to_capacity, subject, cause, detail) \
                     VALUES (:at, :fleet, :kind, :from_capacity, :to_capacity, :subject, \
                     :cause, :detail) RETURNING id",
                )?
                .query_row(
                    named_params! {
                        ":at": row.at,
                        ":fleet": row.fleet,
                        ":kind": row.kind,
                        ":from_capacity": row.from_capacity,
                        ":to_capacity": row.to_capacity,
                        ":subject": row.subject,
                        ":cause": row.cause,
                        ":detail": row.detail,
                    },
                    |returned| Ok(scaling_seq(returned)),
                )?
            })
            .await?)
    }

    /// Reads the events after `since` and before `before`, up to `limit`, into `into` in
    /// ascending id order. `true` means older events in the window were left out; the newest
    /// events are kept.
    async fn scaling_events(
        &self,
        since: ScalingEventSeq,
        before: Option<ScalingEventSeq>,
        limit: NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError> {
        let since = sequence_bound(since)?;
        let before = before.map(sequence_bound).transpose()?;
        let params = (since, before, fetch_count(limit));
        let mut rows = self
            .query_all(SELECT_SCALING_EVENTS, params, scaling_from_row)
            .await?;
        let more = page(&mut rows, limit);
        rows.reverse();
        into.clear();
        into.extend(rows);
        Ok(more)
    }

    /// Registers the principal on first sight and advances `last_seen`, never backwards.
    async fn observe_principal(&self, identity: &Identity, at: Timestamp) -> Result<(), PortError> {
        let provider = identity.provider.as_str().to_owned();
        let principal = identity.principal.as_str().to_owned();
        let at = millis(at)?;
        self.run(move |db| {
            db.prepare_cached(
                "INSERT INTO principals (provider, principal, first_seen, last_seen) \
                 VALUES (:provider, :principal, :at, :at) \
                 ON CONFLICT (provider, principal) \
                 DO UPDATE SET last_seen = max(last_seen, excluded.last_seen)",
            )?
            .execute(named_params! {
                ":provider": provider,
                ":principal": principal,
                ":at": at,
            })?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        let provider = identity.provider.as_str().to_owned();
        let principal = identity.principal.as_str().to_owned();
        Ok(self
            .run(move |db| {
                db.prepare_cached(SELECT_PRINCIPAL)?
                    .query_row(params![provider, principal], |row| {
                        Ok(principal_from_row(row))
                    })
                    .optional()?
                    .transpose()
            })
            .await?)
    }

    /// Registers the principal if unknown and revokes it. The upsert and the read-back are one
    /// statement, so the returned time is the stored one: an already revoked principal keeps its
    /// first revocation time.
    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        let provider = identity.provider.as_str().to_owned();
        let principal = identity.principal.as_str().to_owned();
        let at = millis(at)?;
        Ok(self
            .run(move |db| {
                db.prepare_cached(concat!(
                    "INSERT INTO principals ",
                    "(provider, principal, first_seen, last_seen, revoked_at) ",
                    "VALUES (:provider, :principal, :at, :at, :at) ",
                    "ON CONFLICT (provider, principal) ",
                    "DO UPDATE SET revoked_at = COALESCE(revoked_at, excluded.revoked_at) ",
                    "RETURNING ",
                    principal_columns!()
                ))?
                .query_row(
                    named_params! {
                        ":provider": provider,
                        ":principal": principal,
                        ":at": at,
                    },
                    |row| Ok(principal_from_row(row)),
                )?
            })
            .await?)
    }

    /// Clears the revocation of a principal; fails with `NotFound` when it was never seen.
    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        let provider = identity.provider.as_str().to_owned();
        let principal = identity.principal.as_str().to_owned();
        self.run(move |db| {
            db.prepare_cached(concat!(
                "UPDATE principals SET revoked_at = NULL ",
                "WHERE provider = :provider AND principal = :principal RETURNING ",
                principal_columns!()
            ))?
            .query_row(
                named_params! { ":provider": provider, ":principal": principal },
                |row| Ok(principal_from_row(row)),
            )
            .optional()?
            .transpose()
        })
        .await?
        .ok_or_else(|| PortError::NotFound(Subject::Principal(identity.clone())))
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        Ok(self
            .query_all(SELECT_REVOKED_PRINCIPALS, [], principal_from_row)
            .await?)
    }

    /// Reads one page of principals in `(provider, principal)` order after `after`. `true`
    /// means more principals follow the page.
    async fn principals(
        &self,
        after: Option<&Identity>,
        limit: NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        let (after_provider, after_principal) = match after {
            Some(identity) => (
                Some(identity.provider.as_str().to_owned()),
                Some(identity.principal.as_str().to_owned()),
            ),
            None => (None, None),
        };
        let params = (
            after_provider,
            after_principal,
            i64::from(only_revoked),
            fetch_count(limit),
        );
        let mut rows = self
            .query_all(SELECT_PRINCIPALS_PAGE, params, principal_from_row)
            .await?;
        let more = page(&mut rows, limit);
        into.clear();
        into.extend(rows);
        Ok(more)
    }
}

impl Sqlite {
    /// Runs one query on the database thread and collects every row through `read`.
    async fn query_all<P: Params + Send + 'static, T: Send + 'static>(
        &self,
        sql: &'static str,
        params: P,
        read: fn(&Row<'_>) -> Result<T, SqliteError>,
    ) -> Result<Vec<T>, SqliteError> {
        self.run(move |db| {
            let mut statement = db.prepare_cached(sql)?;
            let mut rows = statement.query(params)?;
            let mut records = Vec::new();
            while let Some(row) = rows.next()? {
                records.push(read(row)?);
            }
            Ok(records)
        })
        .await
    }
}
