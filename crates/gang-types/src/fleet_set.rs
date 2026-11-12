//! A set of fleet indices, stored as one bit per fleet.

use std::iter::FusedIterator;
use std::ops::{BitAnd, BitAndAssign, BitOrAssign};

use crate::{FleetCountError, FleetIndexError};

/// The most fleets a catalog can hold, and so the largest index a [`FleetSet`] can contain.
pub const MAX_FLEETS: usize = 64;

/// A set of fleet indices in `0..MAX_FLEETS`. Bit `i` is the `i`-th fleet in declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct FleetSet(u64);

impl FleetSet {
    /// The set with no fleets.
    pub const EMPTY: Self = Self(0);

    /// Returns the set of indices `0..count`.
    ///
    /// # Errors
    ///
    /// Returns [`FleetCountError`] when `count` is greater than [`MAX_FLEETS`].
    pub fn prefix(count: usize) -> Result<Self, FleetCountError> {
        Self::try_from_indices(0..count).map_err(|_| FleetCountError { count })
    }

    /// Returns the set of `indices`.
    ///
    /// # Errors
    ///
    /// Returns [`FleetIndexError`] when an index is not less than [`MAX_FLEETS`].
    pub fn try_from_indices(
        indices: impl IntoIterator<Item = usize>,
    ) -> Result<Self, FleetIndexError> {
        let mut set = Self::EMPTY;
        for index in indices {
            set.0 |= bit(index).ok_or(FleetIndexError { index })?;
        }
        Ok(set)
    }

    /// Adds `index` to the set and returns whether it was newly added. An index of
    /// [`MAX_FLEETS`] or more changes nothing and returns `false`.
    pub fn insert(&mut self, index: usize) -> bool {
        let Some(mask) = bit(index) else {
            return false;
        };
        let added = self.0 & mask == 0;
        self.0 |= mask;
        added
    }

    /// Removes `index` from the set and returns whether it was present. An index of
    /// [`MAX_FLEETS`] or more changes nothing and returns `false`.
    pub fn remove(&mut self, index: usize) -> bool {
        let Some(mask) = bit(index) else {
            return false;
        };
        let present = self.0 & mask != 0;
        self.0 &= !mask;
        present
    }

    /// Returns whether `index` is in the set. Indices of [`MAX_FLEETS`] or more are never in it.
    #[must_use]
    pub const fn contains(self, index: usize) -> bool {
        if let Some(mask) = bit(index) {
            self.0 & mask != 0
        } else {
            false
        }
    }

    /// Returns whether the set has no fleets.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns the number of fleets in the set.
    #[must_use]
    pub const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    /// Iterates over the indices in the set, in ascending order.
    #[must_use]
    pub fn iter(self) -> FleetSetIter {
        FleetSetIter(self.0)
    }
}

/// An iterator over a set's indices in ascending order.
#[derive(Debug, Clone)]
pub struct FleetSetIter(u64);

impl Iterator for FleetSetIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0 == 0 {
            return None;
        }
        let index = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1;
        Some(index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.count_ones() as usize;
        (len, Some(len))
    }
}

impl ExactSizeIterator for FleetSetIter {}
impl FusedIterator for FleetSetIter {}

impl IntoIterator for FleetSet {
    type Item = usize;
    type IntoIter = FleetSetIter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl BitAnd for FleetSet {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

impl BitAndAssign for FleetSet {
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

impl BitOrAssign for FleetSet {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Returns the bit for `index`, or `None` when `index` is not less than [`MAX_FLEETS`].
const fn bit(index: usize) -> Option<u64> {
    if index < MAX_FLEETS {
        Some(1_u64 << index)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_contains_leading_indices_in_order() -> Result<(), FleetCountError> {
        assert_eq!(FleetSet::prefix(3)?.iter().collect::<Vec<_>>(), [0, 1, 2]);
        assert!(FleetSet::prefix(0)?.is_empty());
        assert_eq!(FleetSet::prefix(MAX_FLEETS)?.len(), MAX_FLEETS);
        Ok(())
    }

    #[test]
    fn prefix_past_max_fleets_is_an_error() {
        assert_eq!(
            FleetSet::prefix(MAX_FLEETS + 1),
            Err(FleetCountError {
                count: MAX_FLEETS + 1
            })
        );
    }

    #[test]
    fn insert_and_remove_update_membership_and_length() {
        let mut set = FleetSet::EMPTY;
        assert!(set.is_empty());

        assert!(set.insert(5));
        assert!(set.insert(63));
        assert!(set.contains(5));
        assert!(set.contains(63));
        assert!(!set.contains(6));
        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());

        assert!(set.remove(5));
        assert!(!set.contains(5));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn insert_and_remove_report_whether_membership_changed() {
        let mut set = FleetSet::EMPTY;
        assert!(set.insert(2));
        assert!(!set.insert(2));
        assert!(set.remove(2));
        assert!(!set.remove(2));
    }

    #[test]
    fn out_of_range_index_changes_nothing() {
        let mut set = FleetSet::EMPTY;
        assert!(!set.insert(MAX_FLEETS));
        assert!(set.is_empty());
        assert!(!set.remove(MAX_FLEETS));
        assert!(!set.contains(MAX_FLEETS));
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn try_from_indices_iterates_ascending() -> Result<(), FleetIndexError> {
        let set = FleetSet::try_from_indices([9, 1, 4])?;
        assert_eq!(set.iter().collect::<Vec<_>>(), [1, 4, 9]);
        assert_eq!(set.len(), 3);
        Ok(())
    }

    #[test]
    fn highest_index_is_supported() -> Result<(), FleetIndexError> {
        let set = FleetSet::try_from_indices([MAX_FLEETS - 1])?;
        assert!(set.contains(MAX_FLEETS - 1));
        assert_eq!(set.iter().last(), Some(MAX_FLEETS - 1));
        Ok(())
    }

    #[test]
    fn try_from_indices_rejects_index_past_the_last() {
        assert_eq!(
            FleetSet::try_from_indices([0, MAX_FLEETS]),
            Err(FleetIndexError { index: MAX_FLEETS })
        );
    }

    #[test]
    fn iter_yields_the_set_bits_including_both_edges() -> Result<(), FleetIndexError> {
        let set = FleetSet::try_from_indices([63, 0, 5])?;
        assert_eq!(set.iter().collect::<Vec<_>>(), [0, 5, 63]);
        Ok(())
    }

    #[test]
    fn iterator_length_tracks_remaining_indices() -> Result<(), FleetIndexError> {
        let set = FleetSet::try_from_indices([63, 2, 7])?;
        let mut iter = set.into_iter();
        assert_eq!(iter.len(), set.len());
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.len(), set.len() - 1);
        assert_eq!(iter.collect::<Vec<_>>(), [7, 63]);
        Ok(())
    }

    #[test]
    fn bitwise_intersection_keeps_shared_indices() -> Result<(), FleetIndexError> {
        let mut left = FleetSet::try_from_indices([0, 5, 63])?;
        let right = FleetSet::try_from_indices([5, 8, 63])?;
        let expected = FleetSet::try_from_indices([5, 63])?;
        assert_eq!(left & right, expected);
        left &= right;
        assert_eq!(left, expected);
        Ok(())
    }
}
