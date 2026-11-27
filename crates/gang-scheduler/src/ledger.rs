//! Per-worker resource capacity and reservation conservation ledger.
//!
//! Tracks `capacity`, `reserved`, and `free = capacity - reserved` across CPU (millicores), memory
//! (bytes), and disk (bytes), plus exact integer cross-product CPU utilization comparisons for
//! distributed placement.

use std::cmp::Ordering;
use std::num::NonZeroU32;

use gang_types::{ActionResources, Bytes, Resources, property};
use thiserror::Error;

/// Errors returned when a ledger reservation or release violates capacity conservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LedgerError {
    /// A worker lacks sufficient free capacity in at least one resource dimension.
    #[error("insufficient free {resource} capacity on worker")]
    InsufficientCapacity {
        /// The resource dimension (`cpu_millicores`, `memory`, or `disk`) that overflowed.
        resource: &'static str,
    },
    /// A release exceeded the currently reserved quantity.
    #[error("cannot release more {resource} than currently reserved")]
    Underflow {
        /// The resource dimension (`cpu_millicores`, `memory`, or `disk`) that underflowed.
        resource: &'static str,
    },
}

/// Computes a worker's ledger capacity from physical instance resources and CPU oversubscription.
///
/// CPU capacity equals `physical.vcpus * 1000 * cpu_oversubscription`; memory and disk match the
/// physical/post-reserve worker capacities.
#[must_use]
pub fn ledger_capacity(physical: Resources, cpu_oversubscription: NonZeroU32) -> ActionResources {
    let cpu_millicores = u64::from(physical.vcpus)
        .saturating_mul(property::MILLICORES_PER_CORE)
        .saturating_mul(u64::from(cpu_oversubscription.get()));
    ActionResources {
        cpu_millicores,
        memory: physical.memory,
        disk: physical.disk,
    }
}

/// Exact integer cross-product comparison of two workers' reserved CPU fractions.
///
/// Compares `left_reserved / left_capacity` against `right_reserved / right_capacity` without
/// floating-point error using `u128` cross-products (`u64 * u64` never overflows `u128`). Workers
/// with zero CPU capacity sort after workers with positive capacity.
#[must_use]
pub const fn cmp_reserved_cpu_fraction(
    left_reserved: u64,
    left_capacity: u64,
    right_reserved: u64,
    right_capacity: u64,
) -> Ordering {
    match (left_capacity, right_capacity) {
        (0, 0) => {
            if left_reserved < right_reserved {
                Ordering::Less
            } else if left_reserved > right_reserved {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (0, _) => Ordering::Greater,
        (_, 0) => Ordering::Less,
        (l_cap, r_cap) => {
            let lhs = (left_reserved as u128) * (r_cap as u128);
            let rhs = (right_reserved as u128) * (l_cap as u128);
            if lhs < rhs {
                Ordering::Less
            } else if lhs > rhs {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
    }
}

/// Per-worker capacity and reservation conservation ledger.
///
/// Invariant: reserved CPU and memory equal the sum over `Offered`/`Running` attempts on this
/// worker, while reserved disk equals the sum over non-`Completed` attempts (retained across
/// `Executed` until `Completed`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerLedger {
    capacity: ActionResources,
    reserved: ActionResources,
}

impl WorkerLedger {
    /// Creates a ledger with zero reservations and the given `capacity`.
    #[must_use]
    pub const fn new(capacity: ActionResources) -> Self {
        Self {
            capacity,
            reserved: ActionResources {
                cpu_millicores: 0,
                memory: Bytes::new(0),
                disk: Bytes::new(0),
            },
        }
    }

    /// Returns the worker's total capacity.
    #[must_use]
    pub const fn capacity(&self) -> ActionResources {
        self.capacity
    }

    /// Returns the sum of active reservations held on this worker.
    #[must_use]
    pub const fn reserved(&self) -> ActionResources {
        self.reserved
    }

    /// Returns unreserved capacity (`capacity - reserved`), saturating at zero if a dynamic
    /// memory capacity reduction lowered capacity below active reservations.
    #[must_use]
    pub const fn free(&self) -> ActionResources {
        ActionResources {
            cpu_millicores: self
                .capacity
                .cpu_millicores
                .saturating_sub(self.reserved.cpu_millicores),
            memory: self.capacity.memory.saturating_sub(self.reserved.memory),
            disk: self.capacity.disk.saturating_sub(self.reserved.disk),
        }
    }

    /// Returns `true` when `request` fits within [`Self::free`] across all three dimensions.
    #[must_use]
    pub const fn fits(&self, request: ActionResources) -> bool {
        let free = self.free();
        request.cpu_millicores <= free.cpu_millicores
            && request.memory.get() <= free.memory.get()
            && request.disk.get() <= free.disk.get()
    }

    /// Returns `true` when every dimension of [`Self::free`] is zero.
    #[must_use]
    pub const fn is_completely_full(&self) -> bool {
        let free = self.free();
        free.cpu_millicores == 0 && free.memory.get() == 0 && free.disk.get() == 0
    }

    /// Compares `self`'s reserved CPU fraction against `other`'s using exact integer
    /// cross-products.
    #[must_use]
    pub const fn cmp_cpu_fraction(&self, other: &Self) -> Ordering {
        cmp_reserved_cpu_fraction(
            self.reserved.cpu_millicores,
            self.capacity.cpu_millicores,
            other.reserved.cpu_millicores,
            other.capacity.cpu_millicores,
        )
    }

    /// Atomically reserves `request` against free capacity.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::InsufficientCapacity`] if any dimension exceeds [`Self::free`],
    /// leaving `self` unchanged.
    pub fn try_reserve(&mut self, request: ActionResources) -> Result<(), LedgerError> {
        let free = self.free();
        if request.cpu_millicores > free.cpu_millicores {
            return Err(LedgerError::InsufficientCapacity {
                resource: "cpu_millicores",
            });
        }
        if request.memory.get() > free.memory.get() {
            return Err(LedgerError::InsufficientCapacity { resource: "memory" });
        }
        if request.disk.get() > free.disk.get() {
            return Err(LedgerError::InsufficientCapacity { resource: "disk" });
        }
        let Some(cpu_millicores) = self
            .reserved
            .cpu_millicores
            .checked_add(request.cpu_millicores)
        else {
            return Err(LedgerError::InsufficientCapacity {
                resource: "cpu_millicores",
            });
        };
        let Some(memory) = self.reserved.memory.checked_add(request.memory) else {
            return Err(LedgerError::InsufficientCapacity { resource: "memory" });
        };
        let Some(disk) = self.reserved.disk.checked_add(request.disk) else {
            return Err(LedgerError::InsufficientCapacity { resource: "disk" });
        };
        self.reserved = ActionResources {
            cpu_millicores,
            memory,
            disk,
        };
        Ok(())
    }

    /// Releases CPU and memory reservations on `Executed`, retaining disk until `Completed`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::Underflow`] if `request` exceeds reserved CPU or memory, leaving
    /// `self` unchanged.
    pub fn release_compute(&mut self, request: ActionResources) -> Result<(), LedgerError> {
        let Some(cpu_millicores) = self
            .reserved
            .cpu_millicores
            .checked_sub(request.cpu_millicores)
        else {
            return Err(LedgerError::Underflow {
                resource: "cpu_millicores",
            });
        };
        let Some(memory) = self
            .reserved
            .memory
            .get()
            .checked_sub(request.memory.get())
            .map(Bytes::new)
        else {
            return Err(LedgerError::Underflow { resource: "memory" });
        };
        self.reserved.cpu_millicores = cpu_millicores;
        self.reserved.memory = memory;
        Ok(())
    }

    /// Releases a disk reservation on `Completed` after `Executed`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::Underflow`] if `disk` exceeds reserved disk, leaving `self`
    /// unchanged.
    pub fn release_disk(&mut self, disk: Bytes) -> Result<(), LedgerError> {
        let Some(remaining) = self
            .reserved
            .disk
            .get()
            .checked_sub(disk.get())
            .map(Bytes::new)
        else {
            return Err(LedgerError::Underflow { resource: "disk" });
        };
        self.reserved.disk = remaining;
        Ok(())
    }

    /// Atomically releases CPU, memory, and disk reservations (e.g. on `Decline` or worker loss
    /// before `Executed`).
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::Underflow`] if any dimension exceeds [`Self::reserved`], leaving
    /// `self` unchanged.
    pub fn release_all(&mut self, request: ActionResources) -> Result<(), LedgerError> {
        let Some(cpu_millicores) = self
            .reserved
            .cpu_millicores
            .checked_sub(request.cpu_millicores)
        else {
            return Err(LedgerError::Underflow {
                resource: "cpu_millicores",
            });
        };
        let Some(memory) = self
            .reserved
            .memory
            .get()
            .checked_sub(request.memory.get())
            .map(Bytes::new)
        else {
            return Err(LedgerError::Underflow { resource: "memory" });
        };
        let Some(disk) = self
            .reserved
            .disk
            .get()
            .checked_sub(request.disk.get())
            .map(Bytes::new)
        else {
            return Err(LedgerError::Underflow { resource: "disk" });
        };
        self.reserved = ActionResources {
            cpu_millicores,
            memory,
            disk,
        };
        Ok(())
    }

    /// Updates the worker's authoritative memory capacity from an observed capacity fact.
    pub fn update_memory_capacity(&mut self, memory: Bytes) {
        self.capacity.memory = memory;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(cpu_millicores: u64, mem_gib: u64, disk_gib: u64) -> ActionResources {
        ActionResources {
            cpu_millicores,
            memory: Bytes::gib(mem_gib),
            disk: Bytes::gib(disk_gib),
        }
    }

    #[test]
    fn ledger_capacity_applies_oversubscription_to_cpu_only() {
        let physical = Resources {
            vcpus: 8,
            memory: Bytes::gib(32),
            disk: Bytes::gib(200),
        };
        let factor = NonZeroU32::new(2).expect("2 is nonzero");
        assert_eq!(ledger_capacity(physical, factor), res(16_000, 32, 200));
    }

    #[test]
    fn ledger_conserves_across_reserve_executed_and_completed() {
        let cap = res(8_000, 16, 100);
        let mut ledger = WorkerLedger::new(cap);
        let req1 = res(3_000, 4, 10);
        let req2 = res(5_000, 12, 20);

        ledger.try_reserve(req1).expect("fits");
        assert_eq!(ledger.free(), res(5_000, 12, 90));

        // Exceeds memory -> atomic failure, ledger untouched.
        assert_eq!(
            ledger.try_reserve(res(1_000, 13, 1)),
            Err(LedgerError::InsufficientCapacity { resource: "memory" })
        );
        assert_eq!(ledger.reserved(), req1);

        ledger.try_reserve(req2).expect("exact fit");
        assert!(ledger.is_completely_full() || ledger.free().cpu_millicores == 0);

        // Executed frees CPU & memory while retaining disk.
        ledger.release_compute(req1).expect("release compute");
        assert_eq!(ledger.reserved(), res(5_000, 12, 30));

        // Completed frees disk.
        ledger.release_disk(req1.disk).expect("release disk");
        assert_eq!(ledger.reserved(), req2);

        // Decline / worker loss releases all remaining dimensions atomically.
        ledger.release_all(req2).expect("release all");
        assert_eq!(ledger.reserved(), ActionResources::default());
        assert_eq!(ledger.free(), cap);
    }

    #[test]
    fn cross_product_cpu_fraction_is_exact_without_floating_point_error() {
        // 1/3 vs 3333333333333333/10000000000000000 (1/3 > 0.3333333333333333)
        assert_eq!(
            cmp_reserved_cpu_fraction(1, 3, 3_333_333_333_333_333, 10_000_000_000_000_000),
            Ordering::Greater
        );
        // 2/6 == 1/3 exactly.
        assert_eq!(cmp_reserved_cpu_fraction(2, 6, 1, 3), Ordering::Equal);
        // Large u64 values near u64::MAX must not overflow u128 cross-product.
        assert_eq!(
            cmp_reserved_cpu_fraction(u64::MAX - 1, u64::MAX, u64::MAX, u64::MAX),
            Ordering::Less
        );
        // Zero capacity sorts behind positive capacity.
        assert_eq!(cmp_reserved_cpu_fraction(0, 0, 100, 100), Ordering::Greater);
        assert_eq!(cmp_reserved_cpu_fraction(100, 100, 0, 0), Ordering::Less);
    }

    #[test]
    fn dynamic_memory_fact_reduction_saturates_free_safely() {
        let mut ledger = WorkerLedger::new(res(4_000, 16, 50));
        ledger.try_reserve(res(2_000, 12, 10)).expect("fits");
        ledger.update_memory_capacity(Bytes::gib(8));
        assert_eq!(ledger.free().memory, Bytes::new(0));
        assert!(!ledger.fits(res(1_000, 1, 0)));
        assert!(ledger.fits(res(1_000, 0, 10)));
    }
}
