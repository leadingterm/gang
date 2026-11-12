//! The colors of the `gang` command line: one role per meaning, two renderings of each.

use anstyle::{AnsiColor, Color, Effects, RgbColor, Style};

/// What a span of output means; the palette decides how it looks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// Default ink, no style.
    Plain,
    /// A healthy outcome: the `ok` word.
    Ok,
    /// The brand color: `wait`, `fix`, `next`, the mark, help literals.
    Accent,
    /// A deviation worth a look: the `warn` word.
    Warn,
    /// A failure: the `fail` word.
    Fail,
    /// Secondary text: causes, field keys, help placeholders, the elapsed time.
    Muted,
    /// Bold default ink: titles.
    Strong,
    /// An explicit style that ignores the palette: only the mark's pixels use it.
    Fixed(Style),
}

/// The two renderings of the roles; a stream without color has no palette at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Palette {
    /// Fixed 24-bit values, used when the terminal announces truecolor support.
    Truecolor,
    /// The terminal's own 16 colors.
    Ansi,
}

impl Palette {
    /// The terminal style of `role` in this palette.
    pub(crate) fn style(self, role: Role) -> Style {
        let plain = Style::new();
        let bold = Effects::BOLD;
        match (self, role) {
            (_, Role::Plain) => plain,
            (_, Role::Strong) => plain.effects(bold),
            (_, Role::Fixed(style)) => style,
            (Self::Truecolor, Role::Ok) => plain.fg_color(Some(rgb(0x26, 0x89, 0x46))),
            (Self::Truecolor, Role::Accent) => {
                plain.fg_color(Some(rgb(0x26, 0x89, 0x46))).effects(bold)
            }
            (Self::Truecolor, Role::Warn) => {
                plain.fg_color(Some(rgb(0xa2, 0x6d, 0x06))).effects(bold)
            }
            (Self::Truecolor, Role::Fail) => {
                plain.fg_color(Some(rgb(0xcb, 0x4e, 0x46))).effects(bold)
            }
            (Self::Truecolor, Role::Muted) => plain.fg_color(Some(rgb(0x70, 0x7b, 0x6d))),
            (Self::Ansi, Role::Ok) => plain.fg_color(Some(AnsiColor::Green.into())),
            (Self::Ansi, Role::Accent) => {
                plain.fg_color(Some(AnsiColor::Green.into())).effects(bold)
            }
            (Self::Ansi, Role::Warn) => {
                plain.fg_color(Some(AnsiColor::Yellow.into())).effects(bold)
            }
            (Self::Ansi, Role::Fail) => plain.fg_color(Some(AnsiColor::Red.into())).effects(bold),
            (Self::Ansi, Role::Muted) => plain.effects(Effects::DIMMED),
        }
    }

    /// The help and usage-error styles of clap: headers and usage bold, literals accent,
    /// placeholders muted, errors fail, valid values ok, invalid values warn.
    pub(crate) fn clap_styles(self) -> clap::builder::Styles {
        clap::builder::Styles::plain()
            .header(self.style(Role::Strong))
            .usage(self.style(Role::Strong))
            .literal(self.style(Role::Accent))
            .placeholder(self.style(Role::Muted))
            .error(self.style(Role::Fail))
            .valid(self.style(Role::Ok))
            .invalid(self.style(Role::Warn))
    }
}

/// A 24-bit color.
const fn rgb(r: u8, g: u8, b: u8) -> Color {
    Color::Rgb(RgbColor(r, g, b))
}

/// Removes complete ANSI CSI sequences, preserving other text and incomplete sequences.
#[cfg(test)]
#[must_use]
pub(crate) fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            let mut sequence = chars.clone();
            sequence.next();
            while sequence
                .peek()
                .is_some_and(|c| ('\x30'..='\x3f').contains(c))
            {
                sequence.next();
            }
            while sequence
                .peek()
                .is_some_and(|c| ('\x20'..='\x2f').contains(c))
            {
                sequence.next();
            }
            if sequence
                .peek()
                .is_some_and(|c| ('\x40'..='\x7e').contains(c))
            {
                sequence.next();
                chars = sequence;
                continue;
            }
        }
        plain.push(c);
    }
    plain
}

#[cfg(test)]
mod tests {
    use super::{Palette, Role, strip_ansi};

    const RESET: &str = "\x1b[0m";
    const ESC: &str = "\u{1b}[";

    #[test]
    fn strip_ansi_only_removes_complete_csi_sequences() {
        assert_eq!(strip_ansi("a\x1b[?25;1 $~b"), "ab");
        assert_eq!(strip_ansi("a\x1b[m\x1b[@b"), "ab");
        assert_eq!(strip_ansi("a\x1b[31"), "a\x1b[31");
        assert_eq!(strip_ansi("a\x1b[ 1mb"), "a\x1b[ 1mb");
        assert_eq!(strip_ansi("a\x1b]title\x07b"), "a\x1b]title\x07b");
    }

    /// The number of escape sequences `text` opens.
    fn escapes(text: &str) -> usize {
        text.matches(ESC).count()
    }

    fn sgr(palette: Palette, role: Role) -> String {
        format!("{}x{}", palette.style(role).render(), RESET)
    }

    #[test]
    fn plain_has_no_style_in_either_palette() {
        for palette in [Palette::Truecolor, Palette::Ansi] {
            assert!(palette.style(Role::Plain).is_plain());
            assert!(
                !palette
                    .style(Role::Plain)
                    .render()
                    .to_string()
                    .contains('\u{1b}')
            );
        }
    }

    #[test]
    fn truecolor_roles_render_their_escape_strings() {
        let palette = Palette::Truecolor;
        // Each styled role is one escape that opens its text and one reset that closes it.
        for (role, introducers) in [
            (Role::Ok, 1),
            (Role::Accent, 2),
            (Role::Warn, 2),
            (Role::Fail, 2),
            (Role::Muted, 1),
            (Role::Strong, 1),
        ] {
            let styled = sgr(palette, role);
            assert!(styled.starts_with(ESC), "{role:?} opens with an escape");
            assert_eq!(strip_ansi(&styled), "x", "{role:?} keeps its text");
            assert_eq!(
                escapes(&styled),
                introducers + 1,
                "{role:?}: its style and reset"
            );
        }
        assert_ne!(sgr(palette, Role::Ok), sgr(palette, Role::Fail));
        assert_ne!(sgr(palette, Role::Warn), sgr(palette, Role::Fail));
    }

    #[test]
    fn ansi_roles_render_their_escape_strings() {
        let palette = Palette::Ansi;
        for (role, introducers) in [
            (Role::Ok, 1),
            (Role::Accent, 2),
            (Role::Warn, 2),
            (Role::Fail, 2),
            (Role::Muted, 1),
            (Role::Strong, 1),
        ] {
            let styled = sgr(palette, role);
            assert!(styled.starts_with(ESC), "{role:?} opens with an escape");
            assert_eq!(strip_ansi(&styled), "x", "{role:?} keeps its text");
            assert_eq!(
                escapes(&styled),
                introducers + 1,
                "{role:?}: its style and reset"
            );
        }
        assert_ne!(sgr(palette, Role::Ok), sgr(palette, Role::Fail));
        assert_ne!(sgr(palette, Role::Warn), sgr(palette, Role::Fail));
    }

    #[test]
    fn a_fixed_role_ignores_the_palette() {
        let style = anstyle::Style::new().fg_color(Some(anstyle::AnsiColor::Cyan.into()));
        for palette in [Palette::Truecolor, Palette::Ansi] {
            assert_eq!(palette.style(Role::Fixed(style)), style);
        }
    }

    #[test]
    fn clap_styles_follow_the_roles() {
        let palette = Palette::Truecolor;
        let styles = palette.clap_styles();
        assert_eq!(*styles.get_header(), palette.style(Role::Strong));
        assert_eq!(*styles.get_usage(), palette.style(Role::Strong));
        assert_eq!(*styles.get_literal(), palette.style(Role::Accent));
        assert_eq!(*styles.get_placeholder(), palette.style(Role::Muted));
        assert_eq!(*styles.get_error(), palette.style(Role::Fail));
        assert_eq!(*styles.get_valid(), palette.style(Role::Ok));
        assert_eq!(*styles.get_invalid(), palette.style(Role::Warn));
    }
}
