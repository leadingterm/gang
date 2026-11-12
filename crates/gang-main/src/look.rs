//! How one output stream looks: colored or not, decorated or not, decided from the environment.

use std::ffi::{OsStr, OsString};

use anstream::ColorChoice;

use crate::palette::Palette;

/// How one stream looks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Look {
    /// The palette when the stream is colored.
    pub(crate) palette: Option<Palette>,
    /// Whether the stream carries decorations and progress.
    pub(crate) decorate: bool,
}

impl Look {
    /// How anstream treats the escape codes of a stream with this look.
    pub(crate) fn color_choice(self) -> ColorChoice {
        if self.palette.is_some() {
            ColorChoice::Always
        } else {
            ColorChoice::Never
        }
    }
}

/// Decides the look of a stream from the environment and whether it is a terminal.
///
/// A terminal gets color and decorations. `NO_COLOR` (non-empty) or `CLICOLOR=0` removes the
/// color only. `CLICOLOR_FORCE` (non-empty, not `0`) adds color to a pipe but never
/// decorations. `TERM=dumb` removes both, whatever else is set. The palette is truecolor when
/// `COLORTERM` says `truecolor` or `24bit`.
pub(crate) fn decide(env: impl Fn(&str) -> Option<OsString>, terminal: bool) -> Look {
    let set = |key: &str| env(key).filter(|value| !value.is_empty());
    let monochrome =
        set("NO_COLOR").is_some() || env("CLICOLOR").as_deref() == Some(OsStr::new("0"));
    let forced = set("CLICOLOR_FORCE").is_some_and(|value| value != OsStr::new("0"));
    let dumb = env("TERM").as_deref() == Some(OsStr::new("dumb"));
    let palette = (!monochrome && !dumb && (forced || terminal)).then(|| {
        let colorterm = env("COLORTERM");
        let truecolor = colorterm
            .as_deref()
            .is_some_and(|value| value == OsStr::new("truecolor") || value == OsStr::new("24bit"));
        if truecolor {
            Palette::Truecolor
        } else {
            Palette::Ansi
        }
    });
    Look {
        palette,
        decorate: terminal && !dumb,
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use anstream::ColorChoice;

    use crate::palette::Palette;

    use super::{Look, decide};

    /// The decision for an environment made of `env`.
    fn look(env: &[(&str, &str)], terminal: bool) -> (Option<Palette>, bool) {
        let lookup = |key: &str| {
            env.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| OsString::from(*value))
        };
        let decided = decide(lookup, terminal);
        (decided.palette, decided.decorate)
    }

    #[test]
    fn a_terminal_gets_color_and_decorations() {
        assert_eq!(look(&[], true), (Some(Palette::Ansi), true));
        assert_eq!(
            look(&[("TERM", "xterm-256color")], true),
            (Some(Palette::Ansi), true)
        );
    }

    #[test]
    fn colorterm_picks_the_truecolor_palette() {
        assert_eq!(
            look(&[("COLORTERM", "truecolor")], true),
            (Some(Palette::Truecolor), true)
        );
        assert_eq!(
            look(&[("COLORTERM", "24bit")], true),
            (Some(Palette::Truecolor), true)
        );
        assert_eq!(
            look(&[("COLORTERM", "yes")], true),
            (Some(Palette::Ansi), true)
        );
    }

    #[test]
    fn no_color_removes_color_and_keeps_decorations() {
        assert_eq!(look(&[("NO_COLOR", "1")], true), (None, true));
        assert_eq!(look(&[("NO_COLOR", "")], true), (Some(Palette::Ansi), true));
    }

    #[test]
    fn clicolor_zero_removes_color_and_keeps_decorations() {
        assert_eq!(look(&[("CLICOLOR", "0")], true), (None, true));
        assert_eq!(
            look(&[("CLICOLOR", "1")], true),
            (Some(Palette::Ansi), true)
        );
    }

    #[test]
    fn clicolor_force_colors_a_pipe_without_decorations() {
        assert_eq!(
            look(&[("CLICOLOR_FORCE", "1")], false),
            (Some(Palette::Ansi), false)
        );
        assert_eq!(
            look(
                &[("CLICOLOR_FORCE", "1"), ("COLORTERM", "truecolor")],
                false
            ),
            (Some(Palette::Truecolor), false)
        );
        assert_eq!(look(&[("CLICOLOR_FORCE", "0")], false), (None, false));
        assert_eq!(look(&[("CLICOLOR_FORCE", "")], false), (None, false));
    }

    #[test]
    fn no_color_beats_clicolor_force() {
        assert_eq!(
            look(&[("NO_COLOR", "1"), ("CLICOLOR_FORCE", "1")], false),
            (None, false)
        );
    }

    #[test]
    fn a_dumb_terminal_gets_neither() {
        assert_eq!(look(&[("TERM", "dumb")], true), (None, false));
    }

    #[test]
    fn a_dumb_terminal_beats_clicolor_force() {
        assert_eq!(
            look(&[("TERM", "dumb"), ("CLICOLOR_FORCE", "1")], true),
            (None, false)
        );
        assert_eq!(
            look(&[("TERM", "dumb"), ("CLICOLOR_FORCE", "1")], false),
            (None, false)
        );
    }

    #[test]
    fn a_pipe_gets_neither() {
        assert_eq!(look(&[], false), (None, false));
        assert_eq!(look(&[("COLORTERM", "truecolor")], false), (None, false));
        assert_eq!(look(&[("TERM", "dumb")], false), (None, false));
    }

    #[test]
    fn only_a_colored_stream_passes_escape_codes_through() {
        let colored = Look {
            palette: Some(Palette::Ansi),
            decorate: true,
        };
        let plain = Look {
            palette: None,
            decorate: true,
        };
        assert_eq!(colored.color_choice(), ColorChoice::Always);
        assert_eq!(plain.color_choice(), ColorChoice::Never);
    }
}
