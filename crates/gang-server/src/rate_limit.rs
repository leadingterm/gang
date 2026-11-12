//! The rate limit of the open routes: the server info, the session exchange and the browser
//! sign-in. Each caller is held to its own bucket and every open request to one shared bucket.
//! An IPv6 caller is keyed by the first `ipv6_prefix_length` bits of its address, so the
//! addresses of one subscriber share a bucket.
//! Buckets are generic cell rate limiters (GCRA): a bucket's state is the earliest instant its
//! next request would be admitted without spending the burst.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::{Clock, Tick};
use gang_config::{BucketConfig, RateLimitConfig};
use parking_lot::Mutex;

/// One bucket's parameters.
#[derive(Debug, Clone, Copy)]
struct Rate {
    /// The time one request takes from the bucket: the refill interval of one token.
    interval: Duration,
    /// How many requests the bucket admits at once when full.
    burst: NonZeroU32,
}

impl From<&BucketConfig> for Rate {
    fn from(config: &BucketConfig) -> Self {
        Self {
            interval: config.refill_interval.get(),
            burst: config.burst,
        }
    }
}

impl Rate {
    /// Admits one request arriving at `now` on a bucket whose state is `tat`; returns the new
    /// state, or how long to wait before the request is admitted.
    fn admit(self, tat: Tick, now: Tick) -> Result<Tick, Duration> {
        let next = tat.max(now) + self.interval;
        let horizon = now + self.interval.saturating_mul(self.burst.get());
        if next > horizon {
            Err(next.saturating_duration_since(horizon))
        } else {
            Ok(next)
        }
    }
}

/// Admits or refuses the requests of the open routes, per caller address and in total.
#[derive(Debug)]
pub(crate) struct RateLimiter<C> {
    /// The clock every bucket reads its time from.
    clock: C,
    /// The rate of each caller's bucket.
    client: Rate,
    /// The rate of the bucket every request shares.
    global: Rate,
    /// The leading bits of an IPv6 caller's address that name its subscriber.
    ipv6_prefix_length: u8,
    /// The most callers tracked at once.
    capacity: usize,
    /// The state of every bucket.
    state: Mutex<State>,
}

/// The state of every bucket a limiter holds. Idle callers are forgotten only when the table of
/// callers is full.
#[derive(Debug)]
struct State {
    /// The state of the shared bucket.
    global: Tick,
    /// The state of each tracked caller's bucket, keyed by its canonical subscriber address.
    clients: HashMap<IpAddr, Tick>,
    /// A lower bound of the state of every tracked caller's bucket: no tracked bucket is idle
    /// before this instant. Set from the first insert whenever the table is empty.
    earliest_free: Tick,
}

impl<C: Clock> RateLimiter<C> {
    /// Builds the limiter `config` describes, reading time from `clock`. Every bucket starts full.
    #[must_use]
    pub(crate) fn new(config: &RateLimitConfig, clock: C) -> Self {
        let capacity = config.max_tracked_clients.get();
        Self {
            clock,
            client: Rate::from(&config.client),
            global: Rate::from(&config.global),
            capacity,
            ipv6_prefix_length: config.ipv6_prefix_length.get(),
            state: Mutex::new(State {
                global: Tick::ORIGIN,
                clients: HashMap::with_capacity(capacity),
                earliest_free: Tick::ORIGIN,
            }),
        }
    }

    /// Admits one request from `client`, or returns how long to wait before it is admitted.
    /// `None` means the caller's address is unknown, so only the shared bucket applies. A
    /// refused request spends no bucket.
    ///
    /// # Errors
    ///
    /// Returns how long to wait before the request is admitted when it is refused.
    pub(crate) fn check(&self, client: Option<IpAddr>) -> Result<(), Duration> {
        let now = self.clock.monotonic();
        let mut state = self.state.lock();
        let client_next = match client {
            None => None,
            Some(address) => {
                let key = subscriber(address, self.ipv6_prefix_length);
                let existing = state.clients.get(&key).copied();
                let tat = existing.unwrap_or(now);
                let next = self.client.admit(tat, now)?;
                if existing.is_none() && state.clients.len() >= self.capacity {
                    // An idle bucket is full, so forgetting it changes no answer. No bucket is
                    // idle before the earliest stored instant, so the table is not scanned then.
                    if now >= state.earliest_free {
                        state.clients.retain(|_, tat| *tat > now);
                        let earliest = state.clients.values().copied().min();
                        state.earliest_free = earliest.unwrap_or(Tick::ORIGIN);
                    }
                    if state.clients.len() >= self.capacity {
                        return Err(self.client.interval);
                    }
                }
                Some((key, next))
            }
        };
        let global_next = self.global.admit(state.global, now)?;
        state.global = global_next;
        if let Some((key, next)) = client_next {
            state.earliest_free = if state.clients.is_empty() {
                next
            } else {
                state.earliest_free.min(next)
            };
            state.clients.insert(key, next);
        }
        Ok(())
    }
}

/// The address whose bucket a caller shares: its canonical form, with an IPv6 address reduced to
/// its first `prefix_length` bits, the part that names one subscriber.
fn subscriber(address: IpAddr, prefix_length: u8) -> IpAddr {
    match address.to_canonical() {
        v4 @ IpAddr::V4(_) => v4,
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & prefix_mask(prefix_length))),
    }
}

/// The mask keeping the first `prefix_length` bits of a 128-bit address. A zero prefix keeps none.
fn prefix_mask(prefix_length: u8) -> u128 {
    u128::MAX
        .checked_shl(u128::BITS.saturating_sub(u32::from(prefix_length)))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use gang_clock::{TestClock, Timestamp};
    use gang_config::{BucketConfig, RateLimitConfig};
    use gang_types::NonZeroDuration;

    use super::*;

    fn new_clock() -> Arc<TestClock> {
        Arc::new(TestClock::new(Timestamp::from_unix_millis(0)))
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().expect("test address parses")
    }

    const ALICE: &str = "192.0.2.1";
    const BOB: &str = "192.0.2.2";
    const CAROL: &str = "192.0.2.3";

    fn bucket(burst: u32, interval_ms: u64) -> BucketConfig {
        BucketConfig {
            burst: NonZeroU32::new(burst).expect("test burst is nonzero"),
            refill_interval: NonZeroDuration::from_millis(interval_ms),
        }
    }

    fn config(client: BucketConfig, global: BucketConfig, max_tracked: usize) -> RateLimitConfig {
        RateLimitConfig {
            client,
            global,
            max_tracked_clients: NonZeroUsize::new(max_tracked).expect("test capacity is nonzero"),
            trusted_proxies: Vec::new(),
            ipv6_prefix_length: gang_config::Ipv6PrefixLength::DEFAULT,
        }
    }

    fn limiter(
        client: BucketConfig,
        global: BucketConfig,
        max_tracked: usize,
    ) -> (RateLimiter<Arc<TestClock>>, Arc<TestClock>) {
        let clock = new_clock();
        let limiter = RateLimiter::new(&config(client, global, max_tracked), Arc::clone(&clock));
        (limiter, clock)
    }

    fn limiter_with_prefix(
        ipv6_prefix_length: u8,
    ) -> (RateLimiter<Arc<TestClock>>, Arc<TestClock>) {
        let clock = new_clock();
        let cfg = RateLimitConfig {
            ipv6_prefix_length: gang_config::Ipv6PrefixLength::try_new(ipv6_prefix_length)
                .expect("valid test prefix"),
            ..config(bucket(1, 1000), bucket(100, 1), 16)
        };
        let limiter = RateLimiter::new(&cfg, Arc::clone(&clock));
        (limiter, clock)
    }

    #[test]
    fn burst_is_admitted_at_once_then_refused_with_the_exact_wait() {
        let (limiter, clock) = limiter(bucket(3, 100), bucket(1000, 1), 16);
        for _ in 0..3 {
            assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        }
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(100)));
        clock.advance(ms(99));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(1)));
        clock.advance(ms(1));
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(100)));
    }

    #[test]
    fn sustained_requests_are_admitted_at_the_refill_rate() {
        let (limiter, clock) = limiter(bucket(3, 100), bucket(1000, 1), 16);
        for _ in 0..3 {
            assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        }
        for _ in 0..20 {
            clock.advance(ms(100));
            assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
            assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(100)));
        }
    }

    #[test]
    fn refused_by_the_client_bucket_spends_no_global_capacity() {
        let (limiter, _clock) = limiter(bucket(1, 1000), bucket(2, 1000), 16);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(1000)));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
    }

    #[test]
    fn refused_by_the_global_bucket_spends_no_client_capacity() {
        let (limiter, clock) = limiter(bucket(1, 1000), bucket(1, 100), 16);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(BOB))), Err(ms(100)));
        clock.advance(ms(100));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
    }

    #[test]
    fn clients_have_independent_buckets() {
        let (limiter, _clock) = limiter(bucket(1, 1000), bucket(10, 1), 16);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(1000)));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
    }

    #[test]
    fn the_global_bucket_refuses_a_fresh_client_once_exhausted() {
        let (limiter, _clock) = limiter(bucket(5, 1000), bucket(2, 1000), 16);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
        assert_eq!(limiter.check(Some(ip(CAROL))), Err(ms(1000)));
    }

    #[test]
    fn an_unknown_client_uses_only_the_global_bucket() {
        let (limiter, _clock) = limiter(bucket(1, 1000), bucket(2, 1000), 16);
        assert_eq!(limiter.check(None), Ok(()));
        assert_eq!(limiter.check(None), Ok(()));
        assert_eq!(limiter.check(None), Err(ms(1000)));
    }

    #[test]
    fn ipv6_addresses_in_one_slash_64_share_a_bucket() {
        let (limiter, _clock) = limiter(bucket(1, 1000), bucket(100, 1), 16);
        assert_eq!(limiter.check(Some(ip("2001:db8:0:1::1"))), Ok(()));
        assert_eq!(
            limiter.check(Some(ip("2001:db8:0:1:ffff::2"))),
            Err(ms(1000))
        );
        assert_eq!(limiter.check(Some(ip("2001:db8:0:2::1"))), Ok(()));
    }

    #[test]
    fn ipv4_mapped_ipv6_shares_the_bucket_of_plain_ipv4() {
        let (limiter, _clock) = limiter(bucket(1, 1000), bucket(100, 1), 16);
        assert_eq!(limiter.check(Some(ip("::ffff:192.0.2.1"))), Ok(()));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(1000)));
    }

    #[test]
    fn ipv6_prefix_48_shares_a_bucket_across_slash_64s_of_one_slash_48() {
        let (limiter, _clock) = limiter_with_prefix(48);
        assert_eq!(limiter.check(Some(ip("2001:db8:0:1::1"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("2001:db8:0:2::1"))), Err(ms(1000)));
        assert_eq!(limiter.check(Some(ip("2001:db8:1:1::1"))), Ok(()));
    }

    #[test]
    fn ipv6_prefix_128_gives_each_address_of_one_slash_64_its_own_bucket() {
        let (limiter, _clock) = limiter_with_prefix(128);
        assert_eq!(limiter.check(Some(ip("2001:db8:0:1::1"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("2001:db8:0:1::2"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("2001:db8:0:1::1"))), Err(ms(1000)));
    }

    #[test]
    fn ipv6_clients_share_one_bucket_when_the_prefix_covers_none() {
        // The shortest prefix the config allows is one bit; both addresses start with a zero
        // bit, so that bit cannot tell their networks apart.
        let (limiter, _clock) = limiter_with_prefix(1);
        assert_eq!(limiter.check(Some(ip("2001:db8::1"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("2a00:1450::1"))), Err(ms(1000)));
    }

    #[test]
    fn ipv6_clients_of_different_networks_have_independent_buckets_at_prefix_128() {
        let (limiter, _clock) = limiter_with_prefix(128);
        assert_eq!(limiter.check(Some(ip("2001:db8::1"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("2a00:1450::1"))), Ok(()));
    }

    #[test]
    fn ipv4_addresses_are_keyed_whole_whatever_the_ipv6_prefix() {
        let (limiter, _clock) = limiter_with_prefix(8);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(1000)));
    }

    #[test]
    fn ipv4_mapped_addresses_are_keyed_whole_whatever_the_ipv6_prefix() {
        let (limiter, _clock) = limiter_with_prefix(48);
        assert_eq!(limiter.check(Some(ip("::ffff:192.0.2.1"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("::ffff:192.0.2.2"))), Ok(()));
        assert_eq!(limiter.check(Some(ip("::ffff:192.0.2.1"))), Err(ms(1000)));
        assert_eq!(limiter.check(Some(ip(ALICE))), Err(ms(1000)));
    }

    #[test]
    fn a_full_table_drops_idle_clients_before_refusing_a_new_one() {
        let (limiter, clock) = limiter(bucket(1, 1000), bucket(100, 1), 2);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
        assert_eq!(limiter.check(Some(ip(CAROL))), Err(ms(1000)));
        clock.advance(ms(1000));
        assert_eq!(limiter.check(Some(ip(CAROL))), Ok(()));
    }

    #[test]
    fn a_full_table_admits_a_new_client_exactly_when_the_earliest_bucket_goes_idle() {
        let (limiter, clock) = limiter(bucket(1, 1000), bucket(100, 1), 2);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        clock.advance(ms(500));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
        assert_eq!(limiter.check(Some(ip(CAROL))), Err(ms(1000)));
        clock.advance(ms(499));
        assert_eq!(limiter.check(Some(ip(CAROL))), Err(ms(1000)));
        clock.advance(ms(1));
        assert_eq!(limiter.check(Some(ip(CAROL))), Ok(()));
        assert_eq!(limiter.check(Some(ip("192.0.2.4"))), Err(ms(1000)));
        clock.advance(ms(500));
        assert_eq!(limiter.check(Some(ip("192.0.2.4"))), Ok(()));
    }

    #[test]
    fn a_refused_new_client_spends_no_bucket() {
        let (limiter, clock) = limiter(bucket(1, 1000), bucket(2, 1000), 2);
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(BOB))), Ok(()));
        assert_eq!(limiter.check(Some(ip(CAROL))), Err(ms(1000)));
        clock.advance(ms(1000));
        assert_eq!(limiter.check(Some(ip(ALICE))), Ok(()));
        assert_eq!(limiter.check(Some(ip(BOB))), Err(ms(1000)));
    }
}
