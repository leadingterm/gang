//! Pure capacity split between a Spot group and its on-demand twin.
//!
//! Works on numbers only: no AWS types, no I/O. The fleet layer reads the
//! groups, calls [`reported`] for the supply view and [`raise`] for the
//! desired-capacity writes it must issue, in order.

/// Desired and live member counts of one Auto Scaling group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counts {
    /// Desired capacity of the group.
    pub desired: u32,
    /// Members whose lifecycle is `Pending` or `InService`.
    pub live: u32,
}

impl Counts {
    /// Desired capacity not yet backed by a live member: `desired - live`, floored at zero.
    pub(crate) fn gap(self) -> u32 {
        self.desired.saturating_sub(self.live)
    }

    /// Capacity this group reports: `max(desired, live)`.
    pub(crate) fn reported(self) -> u32 {
        self.desired.max(self.live)
    }
}

/// Auto Scaling group that receives a desired-capacity write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    /// The Spot group.
    Spot,
    /// The on-demand twin group.
    OnDemand,
}

/// One `SetDesiredCapacity` write: the target tier and its new desired capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Write {
    /// Group to write.
    pub tier: Tier,
    /// Desired capacity to set on that group.
    pub desired: u32,
}

/// The capacity writes one raise needs: at most two, in the order they are applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Writes([Option<Write>; 2]);

impl Writes {
    /// Appends a write; `raise` pushes at most two writes.
    fn push(&mut self, write: Write) {
        if let Some(slot) = self.0.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(write);
        }
    }
}

impl IntoIterator for Writes {
    type Item = Write;
    type IntoIter = std::iter::Flatten<std::array::IntoIter<Option<Write>, 2>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter().flatten()
    }
}

/// Capacity a fleet reports: the Spot part plus the on-demand twin's reported count.
///
/// Rule: `spot_part = if down { spot.live } else { spot.reported() }`, and the
/// result is `spot_part + on_demand.reported()`, saturating at `u32::MAX`.
pub(crate) fn reported(spot: Counts, on_demand: Counts, down: bool) -> u32 {
    let spot_part = if down { spot.live } else { spot.reported() };
    spot_part.saturating_add(on_demand.reported())
}

/// Up to two writes that raise the fleet's reported capacity to `to`, in issue order.
///
/// Rule:
/// - `d = reported(spot, on_demand, down)`; if `d >= to`, no writes.
/// - `add = to - d`.
/// - Not `down`: one write, `Spot <- spot.reported() + add`.
/// - `down`: if the Spot group still wants capacity (`spot.desired > spot.live`),
///   first withdraw it with `Spot <- spot.live`; then `OnDemand <- on_demand.reported() + add`.
#[must_use]
pub(crate) fn raise(spot: Counts, on_demand: Counts, down: bool, to: u32) -> Writes {
    let d = reported(spot, on_demand, down);
    if d >= to {
        return Writes::default();
    }
    let add = to.saturating_sub(d);
    let mut writes = Writes::default();
    if !down {
        writes.push(Write {
            tier: Tier::Spot,
            desired: spot.reported().saturating_add(add),
        });
        return writes;
    }
    if spot.gap() > 0 {
        writes.push(Write {
            tier: Tier::Spot,
            desired: spot.live,
        });
    }
    writes.push(Write {
        tier: Tier::OnDemand,
        desired: on_demand.reported().saturating_add(add),
    });
    writes
}

#[cfg(test)]
mod tests {
    use super::{Counts, Tier, Write, raise, reported};

    /// One `raise` table row: the group states, the target, and the expected writes.
    struct Case {
        name: &'static str,
        spot: Counts,
        on_demand: Counts,
        down: bool,
        to: u32,
        expected: Vec<Write>,
    }

    /// One `reported` table row: the group states and the expected reported count.
    struct ReportedCase {
        name: &'static str,
        spot: Counts,
        on_demand: Counts,
        down: bool,
        expected: u32,
    }

    /// Builds the counts of one group.
    fn c(desired: u32, live: u32) -> Counts {
        Counts { desired, live }
    }

    /// Builds one expected write.
    fn w(tier: Tier, desired: u32) -> Write {
        Write { tier, desired }
    }

    /// Builds one `raise` row.
    fn case(
        name: &'static str,
        spot: Counts,
        on_demand: Counts,
        down: bool,
        to: u32,
        expected: Vec<Write>,
    ) -> Case {
        Case {
            name,
            spot,
            on_demand,
            down,
            to,
            expected,
        }
    }

    /// Asserts that every row's `raise` output matches its expected writes.
    fn check(cases: &[Case]) {
        for case in cases {
            assert_eq!(
                raise(case.spot, case.on_demand, case.down, case.to)
                    .into_iter()
                    .collect::<Vec<_>>(),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    /// `reported` follows the down/non-down rule and saturates.
    #[test]
    fn reported_uses_live_only_while_down() {
        let cases = [
            ReportedCase {
                name: "spot surplus, not down",
                spot: c(10, 6),
                on_demand: c(0, 0),
                down: false,
                expected: 10,
            },
            ReportedCase {
                name: "spot surplus, down",
                spot: c(10, 6),
                on_demand: c(0, 0),
                down: true,
                expected: 6,
            },
            ReportedCase {
                name: "spot live above desired, not down",
                spot: c(2, 6),
                on_demand: c(0, 0),
                down: false,
                expected: 6,
            },
            ReportedCase {
                name: "spot live above desired, down",
                spot: c(2, 6),
                on_demand: c(0, 0),
                down: true,
                expected: 6,
            },
            ReportedCase {
                name: "twin counted via reported",
                spot: c(6, 6),
                on_demand: c(4, 2),
                down: false,
                expected: 10,
            },
            ReportedCase {
                name: "twin counted via reported, down",
                spot: c(6, 6),
                on_demand: c(4, 2),
                down: true,
                expected: 10,
            },
            ReportedCase {
                name: "twin live above desired",
                spot: c(0, 0),
                on_demand: c(1, 3),
                down: true,
                expected: 3,
            },
            ReportedCase {
                name: "both groups empty",
                spot: c(0, 0),
                on_demand: c(0, 0),
                down: true,
                expected: 0,
            },
            ReportedCase {
                name: "saturates at u32::MAX",
                spot: c(u32::MAX, 0),
                on_demand: c(u32::MAX, 0),
                down: false,
                expected: u32::MAX,
            },
        ];
        for row in &cases {
            assert_eq!(
                reported(row.spot, row.on_demand, row.down),
                row.expected,
                "{}",
                row.name
            );
        }
    }

    /// Healthy (not down) raises write the Spot group only.
    #[test]
    fn raise_healthy_writes_spot_only() {
        check(&[
            case(
                "healthy raise",
                c(4, 4),
                c(0, 0),
                false,
                6,
                vec![w(Tier::Spot, 6)],
            ),
            case(
                "healthy raise above spot surplus",
                c(10, 6),
                c(0, 0),
                false,
                12,
                vec![w(Tier::Spot, 12)],
            ),
        ]);
    }

    /// Targets already met write nothing, whether met by the group itself or by a surplus.
    #[test]
    fn raise_idempotent_when_target_met() {
        check(&[
            case(
                "healthy target already met by spot surplus",
                c(10, 6),
                c(0, 0),
                false,
                10,
                Vec::new(),
            ),
            case(
                "idempotent when reported equals target",
                c(10, 6),
                c(4, 4),
                true,
                10,
                Vec::new(),
            ),
            case(
                "idempotent when reported exceeds target",
                c(10, 6),
                c(4, 4),
                true,
                8,
                Vec::new(),
            ),
            case(
                "twin desired already meets target",
                c(6, 6),
                c(5, 2),
                true,
                11,
                Vec::new(),
            ),
        ]);
    }

    /// A down fleet withdraws Spot surplus, then raises the on-demand twin.
    #[test]
    fn raise_down_withdraws_spot_then_raises_twin() {
        check(&[
            case(
                "first detection withdraws spot and raises twin",
                c(10, 6),
                c(0, 0),
                true,
                10,
                vec![w(Tier::Spot, 6), w(Tier::OnDemand, 4)],
            ),
            case(
                "restart after withdraw raises twin only",
                c(6, 6),
                c(0, 0),
                true,
                10,
                vec![w(Tier::OnDemand, 4)],
            ),
            case(
                "twin already holds capacity, spot withdrawn",
                c(10, 6),
                c(3, 3),
                true,
                10,
                vec![w(Tier::Spot, 6), w(Tier::OnDemand, 4)],
            ),
            case(
                "twin already holds capacity, no spot gap",
                c(6, 6),
                c(3, 3),
                true,
                10,
                vec![w(Tier::OnDemand, 4)],
            ),
            case(
                "twin desired above live counts as held",
                c(6, 6),
                c(5, 2),
                true,
                12,
                vec![w(Tier::OnDemand, 6)],
            ),
            case(
                "down with no spot gap and a target",
                c(6, 6),
                c(0, 0),
                true,
                8,
                vec![w(Tier::OnDemand, 2)],
            ),
        ]);
    }

    /// Zero targets and live counts above desired never underflow or write spuriously.
    #[test]
    fn raise_no_underflow_and_zero_target() {
        check(&[
            case(
                "down with zero target writes nothing",
                c(0, 0),
                c(0, 0),
                true,
                0,
                Vec::new(),
            ),
            case(
                "zero target writes nothing",
                c(4, 4),
                c(0, 0),
                false,
                0,
                Vec::new(),
            ),
            case(
                "live above desired does not underflow gap",
                c(2, 6),
                c(0, 0),
                true,
                7,
                vec![w(Tier::OnDemand, 1)],
            ),
            case(
                "live above desired does not underflow spot write",
                c(2, 6),
                c(0, 0),
                false,
                7,
                vec![w(Tier::Spot, 7)],
            ),
        ]);
    }

    /// Saturated reported capacity at the maximum target writes nothing.
    #[test]
    fn raise_saturated_reported_writes_nothing() {
        check(&[case(
            "saturated reported writes nothing",
            c(u32::MAX, 0),
            c(u32::MAX, 0),
            false,
            u32::MAX,
            Vec::new(),
        )]);
    }
}
