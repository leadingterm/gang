use std::convert::Infallible;
use std::time::Duration;

use crate::actions::{ActionsCache, ActionsRefresher};
use gang_clock::{Clock, Timestamp};
use gang_spi::{ActionInfo, ActionsView, ListingLimits, QueuedActionView};
use gang_testkit::FakeScheduler;
use gang_types::{FleetSet, NonZeroDuration};

use crate::test_support::start_clock;

const REFRESH: NonZeroDuration = NonZeroDuration::from_secs(1);
const INTERVAL: Duration = REFRESH.get();
const LIMITS: ListingLimits = ListingLimits {
    running: 10,
    queued: 10,
};

fn queued(op: &str) -> QueuedActionView {
    QueuedActionView {
        info: ActionInfo {
            op: op.to_owned(),
            digest: format!("{op}/1"),
            target: "//pkg:target".to_owned(),
            mnemonic: "Javac".to_owned(),
            priority: 0,
        },
        queued_since: Timestamp::UNIX_EPOCH,
        candidates: FleetSet::EMPTY,
    }
}

/// A view holding the queued actions `ops`.
fn view_of(ops: &[&str]) -> ActionsView {
    ActionsView {
        queued: ops.iter().copied().map(queued).collect(),
        ..ActionsView::default()
    }
}

/// The sample time and content the cache holds, or `None` before the first success.
fn sample_of(cache: &ActionsCache) -> Option<(Timestamp, ActionsView)> {
    cache.read(|sample| sample.map(|sample| (sample.at, sample.view.clone())))
}

/// Runs `body` while `refresher` runs. The refresher never finishes, so the result is `body`'s.
async fn alongside<T>(
    refresher: impl Future<Output = Infallible>,
    body: impl Future<Output = T>,
) -> T {
    tokio::select! {
        never = refresher => match never {},
        output = body => output,
    }
}

#[tokio::test]
async fn refreshes_once_per_interval_without_viewers() {
    let clock = start_clock();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    scheduler.set_actions(view_of(&["a"]));
    let calls = scheduler.actions_calls();
    let cache = ActionsCache::default();
    let refresher = ActionsRefresher::new(&scheduler, &clock, cache.clone(), LIMITS, REFRESH);

    alongside(refresher.run(), async {
        // The first tick is immediate.
        calls.reached(1).await;
        assert_eq!(calls.get(), 1);
        assert_eq!(
            sample_of(&cache).map(|(_, view)| view),
            Some(view_of(&["a"]))
        );

        clock.advance(INTERVAL);
        calls.reached(2).await;
        assert_eq!(calls.get(), 2);

        clock.advance(INTERVAL);
        calls.reached(3).await;
        assert_eq!(calls.get(), 3);
    })
    .await;
}

#[tokio::test]
async fn read_is_none_until_the_first_success() {
    let clock = start_clock();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    scheduler.fail_actions(true);
    let calls = scheduler.actions_calls();
    let cache = ActionsCache::default();
    let refresher = ActionsRefresher::new(&scheduler, &clock, cache.clone(), LIMITS, REFRESH);

    alongside(refresher.run(), async {
        calls.reached(1).await;
        assert_eq!(calls.get(), 1);
        assert_eq!(sample_of(&cache), None);

        scheduler.fail_actions(false);
        scheduler.set_actions(view_of(&["a"]));
        clock.advance(INTERVAL);
        calls.reached(2).await;
        assert_eq!(calls.get(), 2);
        assert_eq!(sample_of(&cache), Some((clock.wall(), view_of(&["a"]))));
    })
    .await;
}

#[tokio::test]
async fn failed_refresh_keeps_previous_sample_and_its_time() {
    let clock = start_clock();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    scheduler.set_actions(view_of(&["a"]));
    let calls = scheduler.actions_calls();
    let cache = ActionsCache::default();
    let refresher = ActionsRefresher::new(&scheduler, &clock, cache.clone(), LIMITS, REFRESH);

    alongside(refresher.run(), async {
        // The tick at 0 succeeds at the start time.
        let start = clock.wall();
        calls.reached(1).await;
        assert_eq!(sample_of(&cache), Some((start, view_of(&["a"]))));

        // The tick at 1 fails: the sample and its time stay.
        scheduler.fail_actions(true);
        scheduler.set_actions(view_of(&["b"]));
        clock.advance(INTERVAL);
        calls.reached(2).await;
        assert_eq!(calls.get(), 2);
        assert_eq!(sample_of(&cache), Some((start, view_of(&["a"]))));

        // The tick at 2 succeeds at the wall time of that refresh, replacing the content.
        scheduler.fail_actions(false);
        clock.advance(INTERVAL);
        calls.reached(3).await;
        assert_eq!(calls.get(), 3);
        assert_eq!(sample_of(&cache), Some((clock.wall(), view_of(&["b"]))));
    })
    .await;
}

#[tokio::test]
async fn sample_carries_the_wall_time_of_its_refresh() {
    let clock = start_clock();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    scheduler.set_actions(view_of(&["a"]));
    let calls = scheduler.actions_calls();
    let cache = ActionsCache::default();
    let refresher = ActionsRefresher::new(&scheduler, &clock, cache.clone(), LIMITS, REFRESH);

    alongside(refresher.run(), async {
        calls.reached(1).await;
        assert_eq!(sample_of(&cache).map(|(at, _)| at), Some(clock.wall()));

        // The refresh at the next tick reads the clock as it stands then. A wake a whole
        // period late delays the next tick, so exactly two refreshes run.
        clock.advance(Duration::from_secs(5));
        scheduler.set_actions(view_of(&["c", "d"]));
        calls.reached(2).await;
        assert_eq!(calls.get(), 2);
        assert_eq!(
            sample_of(&cache),
            Some((clock.wall(), view_of(&["c", "d"])))
        );
    })
    .await;
}
