//! Bounded single-flight sharing of browser refresh grants.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Tick;
use gang_spi::{LoginGrant, PortError};
use gang_types::RefreshToken;
use parking_lot::Mutex;
use ring::digest::{SHA256, digest};
use tokio::sync::OnceCell;

use crate::ttl_map::TtlMap;

/// Why a shared refresh grant failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenewalFailure {
    /// The provider refused the refresh token.
    Rejected,
    /// The provider could not be reached or failed.
    Unavailable,
}

/// One shared refresh grant: its result once the first renewal of the token finishes.
type Flight = Arc<OnceCell<Result<LoginGrant, RenewalFailure>>>;

/// A bounded table of shared refresh grant results.
pub(crate) struct RenewalFlights {
    /// Cells are retained from flight start, not last access.
    flights: Mutex<TtlMap<[u8; 32], Flight>>,
}

impl RenewalFlights {
    /// Creates a flight table with the given capacity and reuse window.
    pub(crate) fn new(capacity: NonZeroUsize, reuse: Duration) -> Self {
        Self {
            flights: Mutex::new(TtlMap::new(capacity, reuse)),
        }
    }

    /// The result of the flight for `token` that started within `reuse` before `now`, or of a
    /// new flight running `grant`. `PortError::Rejected` maps to `Rejected`, every other error to
    /// `Unavailable`.
    pub(crate) async fn renew<F>(
        &self,
        token: &RefreshToken,
        now: Tick,
        grant: impl FnOnce() -> F,
    ) -> Result<LoginGrant, RenewalFailure>
    where
        F: Future<Output = Result<LoginGrant, PortError>>,
    {
        let mut key = [0; 32];
        key.copy_from_slice(digest(&SHA256, token.expose().as_bytes()).as_ref());
        let cell = {
            let mut flights = self.flights.lock();
            if let Some(cell) = flights.get_live(&key, now) {
                Arc::clone(cell)
            } else {
                let cell = Arc::new(OnceCell::new());
                flights.insert(key, Arc::clone(&cell), now);
                cell
            }
        };
        cell.get_or_init(|| async {
            grant().await.map_err(|error| match error {
                PortError::Rejected { .. } => RenewalFailure::Rejected,
                _ => RenewalFailure::Unavailable,
            })
        })
        .await
        .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gang_clock::Tick;
    use gang_spi::{LoginGrant, PortError, Rejection};
    use gang_types::{Proof, RefreshToken};
    use tokio::sync::Notify;

    use super::{RenewalFailure, RenewalFlights};

    /// Constructs a small table with a ten-second reuse window.
    fn flights() -> RenewalFlights {
        RenewalFlights::new(
            NonZeroUsize::new(4).expect("four is non-zero"),
            Duration::from_secs(10),
        )
    }

    /// Returns a test tick at the given second.
    fn at(seconds: u64) -> Tick {
        Tick::ORIGIN + Duration::from_secs(seconds)
    }

    /// Constructs a refresh grant for a proof.
    fn grant(proof: &str) -> LoginGrant {
        LoginGrant {
            proof: Proof::new(proof),
            refresh_token: Some(RefreshToken::new("rotated")),
        }
    }

    /// Concurrent requests for one token run only one grant.
    #[tokio::test]
    async fn concurrent_renewals_of_one_token_share_one_grant() {
        let flights = flights();
        let token = RefreshToken::new("token");
        let count = AtomicUsize::new(0);
        let release = Notify::new();
        let first = flights.renew(&token, at(0), || async {
            count.fetch_add(1, Ordering::SeqCst);
            release.notified().await;
            Ok(grant("proof"))
        });
        let second = flights.renew(&token, at(1), || async {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(grant("different"))
        });
        let (first, second, ()) = tokio::join!(first, second, async {
            tokio::task::yield_now().await;
            release.notify_one();
        });
        let first = first.expect("first succeeds");
        let second = second.expect("second succeeds");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(first.proof.matches(second.proof.expose()));
        assert!(
            first
                .refresh_token
                .expect("rotated")
                .matches(second.refresh_token.expect("rotated").expose())
        );
    }

    /// A flight expires after its reuse interval.
    #[tokio::test]
    async fn a_renewal_after_the_reuse_window_runs_a_new_grant() {
        let flights = flights();
        let token = RefreshToken::new("token");
        let count = AtomicUsize::new(0);
        for second in [0, 11] {
            flights
                .renew(&token, at(second), || async {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(grant("proof"))
                })
                .await
                .expect("grant succeeds");
        }
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    /// Distinct refresh tokens create distinct flights.
    #[tokio::test]
    async fn different_tokens_do_not_share() {
        let flights = flights();
        let count = AtomicUsize::new(0);
        for token in ["first", "second"] {
            let result = flights
                .renew(&RefreshToken::new(token), at(0), || async {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(grant(token))
                })
                .await
                .expect("grant succeeds");
            assert!(result.proof.matches(token));
        }
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    /// Refused grants are shared and unavailable grants retain their classification.
    #[tokio::test]
    async fn a_rejected_grant_is_shared_as_rejected() {
        let flights = flights();
        let token = RefreshToken::new("rejected");
        let count = AtomicUsize::new(0);
        let release = Notify::new();
        let first = flights.renew(&token, at(0), || async {
            count.fetch_add(1, Ordering::SeqCst);
            release.notified().await;
            Err(PortError::refused(Rejection::InvalidProof))
        });
        let second = flights.renew(&token, at(1), || async {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(grant("unexpected"))
        });
        let (first, second, ()) = tokio::join!(first, second, async {
            tokio::task::yield_now().await;
            release.notify_one();
        });
        assert!(matches!(first, Err(RenewalFailure::Rejected)));
        assert!(matches!(second, Err(RenewalFailure::Rejected)));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let unavailable = flights
            .renew(&RefreshToken::new("unavailable"), at(0), || async {
                Err(PortError::unavailable("provider unavailable"))
            })
            .await;
        assert!(matches!(unavailable, Err(RenewalFailure::Unavailable)));
    }
}
