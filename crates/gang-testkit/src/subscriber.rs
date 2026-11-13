//! Scoped tracing subscribers that every event of their own thread reaches.

use std::sync::LazyLock;

use tracing::Dispatch;
use tracing::subscriber::NoSubscriber;

/// A dispatcher registered for the whole test process and never installed as anyone's default.
///
/// tracing caches each callsite's interest process-wide. While a single dispatcher is
/// registered, it computes that interest from the default of whichever thread first reaches the
/// callsite (tokio-rs/tracing#3611): a test thread without a subscriber caches `never`, and a
/// scoped subscriber on another thread then loses that callsite's events. Registered next to a
/// scoped subscriber, the witness makes tracing compute every interest from all registered
/// dispatchers under its registry lock, and a subscriber's registration recomputes them all.
static WITNESS: LazyLock<Dispatch> = LazyLock::new(|| Dispatch::new(NoSubscriber::default()));

/// Runs `f` with `subscriber` as this thread's default, so that `subscriber` receives every
/// event `f` emits on this thread whatever other test threads log meanwhile.
pub fn with_subscriber<T>(
    subscriber: impl tracing::Subscriber + Send + Sync + 'static,
    f: impl FnOnce() -> T,
) -> T {
    LazyLock::force(&WITNESS);
    tracing::subscriber::with_default(subscriber, f)
}
