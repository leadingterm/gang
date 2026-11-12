//! #[settings]: a composed configuration enum reaches every method of the selected plugin's settings, including a method added with a default body.

use gang_macros::settings;

/// Settings with one method of each shape.
#[settings]
pub trait Tuning {
    /// The level.
    fn level(&self) -> u8 {
        0
    }

    /// Sets the level.
    fn set_level(&mut self, level: u8) {
        let _ = level;
    }

    /// A method added later, with a default body.
    fn added(&self) -> &'static str {
        "default"
    }

    /// Sets the level and returns the settings.
    #[must_use]
    fn with_level(mut self, level: u8) -> Self
    where
        Self: Sized,
    {
        self.set_level(level);
        self
    }
}

struct Plain;

impl Tuning for Plain {}

struct Custom(u8);

impl Tuning for Custom {
    fn level(&self) -> u8 {
        self.0
    }

    fn set_level(&mut self, level: u8) {
        self.0 = level;
    }

    fn added(&self) -> &'static str {
        "custom"
    }
}

enum Selected {
    Plain(Plain),
    Custom(Custom),
}

__gang_delegate_Tuning!(self::Selected { Plain, Custom });

#[test]
fn an_added_method_reaches_the_selected_override() {
    assert_eq!(Selected::Custom(Custom(3)).added(), "custom");
    assert_eq!(Selected::Plain(Plain).added(), "default");
}

#[test]
fn reads_and_writes_reach_the_selected_settings() {
    let mut custom = Selected::Custom(Custom(1));
    custom.set_level(7);
    assert_eq!(custom.level(), 7);

    let mut plain = Selected::Plain(Plain);
    plain.set_level(7);
    assert_eq!(plain.level(), 0);
}

#[test]
fn by_value_methods_use_the_delegated_ones() {
    assert_eq!(Selected::Custom(Custom(0)).with_level(4).level(), 4);
}
