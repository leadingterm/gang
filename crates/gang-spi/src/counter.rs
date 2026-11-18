//! Counters the database advances: each is a `u64` newtype that only the database moves forward,
//! so ports compare and order them but never compute with them.

/// Declares a counter: a `Copy`, ordered newtype over `u64` with `new` and `get`, whose `0`
/// (its `Default`) precedes every value the database assigns.
macro_rules! counter {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(u64);

        impl $name {
            /// The counter at `value`.
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// The counter's value.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

pub(crate) use counter;
