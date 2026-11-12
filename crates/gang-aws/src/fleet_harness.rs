//! Shared test harness for the fleet adapter: a fake AWS account, the `Fleets` under test on a
//! deterministic clock, and the accessors tests use to seed and inspect the account.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use aws_config::SdkConfig;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_types::{Bytes, Fleet, FleetName, Isa, Resources};

use crate::config::{AwsFleetOptions, Capacity, CooldownPolicy};
use crate::fake_aws::{FakeActivity, FakeAws, FakeFailure, FakeGroup};
use crate::fleet::Fleets;

/// The name of the fleet, and of its Auto Scaling group, that tests use.
pub(crate) const FLEET: &str = "gang-fleet";
/// The wall-clock time the test clock starts at, in Unix milliseconds (2027-01-15T08:00:00Z).
pub(crate) const NOW_MILLIS: u64 = 1_800_000_000_000;

/// The harness: a running fake account, the adapter under test, and its clock.
pub(crate) struct Harness {
    /// The fake AWS account; tests seed and inspect it through its state.
    pub fake: FakeAws,
    /// The adapter under test.
    pub fleets: Fleets,
    /// The clock the adapter reads; tests advance it explicitly.
    pub clock: Arc<TestClock>,
    /// The SDK configuration the adapter was built from; kept so a restart rebuilds it.
    sdk: SdkConfig,
    /// The cooldown policy the adapter was built with; kept so a restart keeps it.
    cooldown: CooldownPolicy,
}

impl Harness {
    /// Starts a fake account and an adapter that applies capacity changes at once.
    pub(crate) async fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_with(CooldownPolicy::Ignore).await
    }

    /// Starts a fake account and an adapter with the given cooldown policy.
    pub(crate) async fn start_with(cooldown: CooldownPolicy) -> Result<Self, Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(NOW_MILLIS)));
        let fleets = Fleets::new(&sdk, clock.clone(), cooldown);
        Ok(Self {
            fake,
            fleets,
            clock,
            sdk,
            cooldown,
        })
    }

    /// Replaces the adapter with a fresh one over the same fake account, as a controller
    /// restart does: nothing it held in memory survives.
    pub(crate) fn restart(&mut self) {
        let clock: Arc<dyn Clock> = self.clock.clone();
        self.fleets = Fleets::new(&self.sdk, clock, self.cooldown);
    }

    /// Adds an unprotected group with the given members and no warm pool; the fake applies no
    /// capacity changes to it.
    pub(crate) fn add_group(&self, name: &str, desired: i32, instances: &[(&str, &str)]) {
        self.add_group_with(FakeGroup::new(name, desired, instances));
    }

    /// Adds a group built by the test, with its own protection, warm pool and simulation.
    pub(crate) fn add_group_with(&self, group: FakeGroup) {
        self.fake.state().groups.push(group);
    }

    /// Records the EC2 launch time of an instance, RFC 3339.
    pub(crate) fn add_launch(&self, id: &str, launched: &str) {
        self.fake
            .state()
            .launch_times
            .insert(id.to_owned(), launched.to_owned());
    }

    /// Records a scaling activity of a group, so scale-out failure history can be queried.
    pub(crate) fn add_activity(&self, group: &str, status: &str, start_time: &str) {
        self.fake.state().activities.push(FakeActivity {
            group: group.to_owned(),
            status_code: status.to_owned(),
            start_time: start_time.to_owned(),
            message: String::new(),
        });
    }

    /// Answers every call to `operation` with `failure` until it is cleared.
    pub(crate) fn fail(&self, operation: &'static str, failure: FakeFailure) {
        self.fake.state().failures.insert(operation, failure);
    }

    /// Stops answering `operation` with a failure.
    pub(crate) fn clear_failure(&self, operation: &str) {
        self.fake.state().failures.remove(operation);
    }

    /// The desired capacity of a group, or `None` when the group does not exist.
    pub(crate) fn desired(&self, group: &str) -> Option<i32> {
        self.fake
            .state()
            .groups
            .iter()
            .find(|candidate| candidate.name == group)
            .map(|candidate| candidate.desired)
    }

    /// The ids of the members of a group, in the order the fake holds them.
    pub(crate) fn instance_ids(&self, group: &str) -> Vec<String> {
        self.fake
            .state()
            .groups
            .iter()
            .find(|candidate| candidate.name == group)
            .map(|candidate| {
                candidate
                    .instances
                    .iter()
                    .map(|instance| instance.id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The ids in the warm pool of a group, in the order the fake holds them.
    pub(crate) fn warm_ids(&self, group: &str) -> Vec<String> {
        self.fake
            .state()
            .groups
            .iter()
            .find(|candidate| candidate.name == group)
            .map(|candidate| candidate.warm_pool.clone())
            .unwrap_or_default()
    }

    /// Whether a member of a group is protected from scale-in; `None` when the member is absent.
    pub(crate) fn is_protected(&self, group: &str, id: &str) -> Option<bool> {
        self.fake
            .state()
            .groups
            .iter()
            .find(|candidate| candidate.name == group)
            .and_then(|candidate| {
                candidate
                    .instances
                    .iter()
                    .find(|instance| instance.id == id)
            })
            .map(|instance| instance.protected)
    }

    /// How many requests for `operation` the fake has received, failed ones included.
    pub(crate) fn requests(&self, operation: &str) -> usize {
        self.fake
            .state()
            .requests
            .iter()
            .filter(|request| request.operation == operation)
            .count()
    }

    /// Every request the fake has received, in order: its operation and decoded parameters.
    pub(crate) fn request_log(&self) -> Vec<(String, BTreeMap<String, String>)> {
        self.fake
            .state()
            .requests
            .iter()
            .map(|request| (request.operation.clone(), request.parameters.clone()))
            .collect()
    }
}

/// A fleet the test owns; [`TestFleet::view`] lends it to the adapter as a `Fleet`.
pub(crate) struct TestFleet {
    name: FleetName,
    options: AwsFleetOptions,
}

impl TestFleet {
    /// Lends the fleet to the adapter.
    pub(crate) fn view(&self) -> Fleet<'_, AwsFleetOptions> {
        Fleet {
            name: &self.name,
            isa: Isa::Aarch64,
            instance: Resources {
                vcpus: 4,
                memory: Bytes::new(29 << 30),
                disk: Bytes::new(100 << 30),
            },
            options: &self.options,
        }
    }
}

/// A fleet with on-demand capacity and no launch-time options.
pub(crate) fn fleet(name: &str) -> Result<TestFleet, Box<dyn Error>> {
    fleet_with(
        name,
        AwsFleetOptions {
            capacity: Capacity::OnDemand,
            machine_class: None,
            fallback: None,
        },
    )
}

/// A fleet with the given options.
pub(crate) fn fleet_with(
    name: &str,
    options: AwsFleetOptions,
) -> Result<TestFleet, Box<dyn Error>> {
    Ok(TestFleet {
        name: FleetName::try_new(name)?,
        options,
    })
}

/// The RFC 3339 instant `secs` seconds before [`NOW_MILLIS`], in UTC with a `Z` suffix.
pub(crate) fn rfc3339_ago(secs: u64) -> String {
    Timestamp::from_unix_millis(NOW_MILLIS.saturating_sub(secs * 1_000)).to_rfc3339()
}
