//! The raccoon mark and the fleet tape: the only place that holds their artwork.
//!
//! A picture is a pixel grid, one character per pixel and two pixel rows per terminal row (the
//! top pixel and the bottom pixel of a half-block cell). The legend:
//!
//! - `G` fur, `D` mask and nose, `W` brow and muzzle, `A` eyes: fixed colors in a truecolor
//!   picture; in a fallback picture `A` is the accent role;
//! - `M` the muted role, `K` the default ink with no style;
//! - `.` empty.

use anstyle::{Color, RgbColor, Style};

use crate::palette::Role;
use crate::record::Line;
use crate::wrap::columns;

/// A pixel grid; every row is one string of pixel characters.
type Picture = &'static [&'static str];

/// The banner in truecolor: fixed fur colors that read on any background.
const STICKER: Picture = &[
    "..GG........GG..",
    ".GDDG......GDDG.",
    ".GGGGGGGGGGGGGG.",
    "GWWWWGGGGGGWWWWG",
    "GDDDDDWGGWDDDDDG",
    "DDADDDWWWWDDDADD",
    "GDDDWWWWWWWWDDDG",
    ".GGWWWWDDWWWWGG.",
    "...GGWWWWWWGG...",
    "......GGGG......",
];

/// The banner without truecolor: foreground only, no mixed cells.
const LINE: Picture = &[
    "..MM........MM..",
    ".M..M......M..M.",
    ".M..MMMMMMMM..M.",
    "M..............M",
    "KKKKKK....KKKKKK",
    "KK.KKK....KKK.KK",
    ".KK..........KK.",
    "...M...KK...M...",
    "....MMM..MMM....",
    "......MMMM......",
];

/// The inline mark in truecolor: brow and mask with two eye holes.
const MASK: Picture = &["GWWWWG", "DADDAD"];

/// The inline mark without truecolor: foreground only.
const MASK_LINE: Picture = &["K.KK.K", ".A..A."];

/// The picture of the banner decoration (truecolor form; `banner` picks the fallback).
const BANNER: Picture = STICKER;

/// Columns left of the banner.
pub(crate) const BANNER_INDENT: usize = 2;

/// Columns between the banner's right edge and its text.
pub(crate) const BANNER_GAP: usize = 4;

/// The banner as terminal rows without text, in the truecolor form or its fallback.
pub(crate) fn banner(truecolor: bool) -> Vec<Line> {
    render(if truecolor { BANNER } else { LINE }, truecolor)
}

/// The width in columns of the banner picture; both forms have the same size.
pub(crate) fn banner_width() -> usize {
    picture_width(BANNER)
}

/// The inline mark as one terminal row, in the truecolor form or its fallback.
pub(crate) fn inline(truecolor: bool) -> Line {
    render(if truecolor { MASK } else { MASK_LINE }, truecolor)
        .into_iter()
        .next()
        .unwrap_or_default()
}

/// The widest row of a picture, in pixels (one column each).
fn picture_width(picture: Picture) -> usize {
    picture.iter().map(|row| columns(row)).max().unwrap_or(0)
}

/// What a pixel looks like.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ink {
    /// A fixed truecolor value.
    Fixed(RgbColor),
    /// A role of the palette.
    Role(Role),
}

impl Ink {
    /// The look of a cell that shows this ink in the foreground.
    fn foreground(self) -> Role {
        match self {
            Self::Fixed(color) => Role::Fixed(Style::new().fg_color(Some(Color::Rgb(color)))),
            Self::Role(role) => role,
        }
    }
}

/// The ink of a pixel character; `None` for an empty pixel.
fn ink(pixel: char, truecolor: bool) -> Option<Ink> {
    match pixel {
        'G' => Some(Ink::Fixed(RgbColor(0x8b, 0x94, 0x8a))),
        'D' => Some(Ink::Fixed(RgbColor(0x1d, 0x22, 0x1f))),
        'W' => Some(Ink::Fixed(RgbColor(0xee, 0xf1, 0xea))),
        'A' if truecolor => Some(Ink::Fixed(RgbColor(0x3f, 0xae, 0x63))),
        'A' => Some(Ink::Role(Role::Accent)),
        'M' => Some(Ink::Role(Role::Muted)),
        'K' => Some(Ink::Role(Role::Plain)),
        _ => None,
    }
}

/// The glyph and look of the cell holding `top` over `bottom`. A cell of two different fixed
/// inks is the only place gang sets a background color.
fn cell(top: char, bottom: char, truecolor: bool) -> (Role, char) {
    match (ink(top, truecolor), ink(bottom, truecolor)) {
        (None, None) => (Role::Plain, ' '),
        (None, Some(lower)) => (lower.foreground(), '▄'),
        (Some(upper), Some(lower)) if upper == lower => (upper.foreground(), '█'),
        (Some(Ink::Fixed(upper)), Some(Ink::Fixed(lower))) => (
            Role::Fixed(
                Style::new()
                    .fg_color(Some(Color::Rgb(upper)))
                    .bg_color(Some(Color::Rgb(lower))),
            ),
            '▀',
        ),
        (Some(upper), _) => (upper.foreground(), '▀'),
    }
}

/// Renders a picture into terminal rows, never padding a row with trailing spaces.
fn render(picture: Picture, truecolor: bool) -> Vec<Line> {
    let width = picture_width(picture);
    picture
        .chunks(2)
        .map(|pair| {
            let chars = |row: Option<&&str>| -> Vec<char> {
                row.map_or_else(Vec::new, |row| row.chars().collect())
            };
            let top = chars(pair.first());
            let bottom = chars(pair.get(1));
            let mut cells: Vec<(Role, char)> = (0..width)
                .map(|x| {
                    cell(
                        top.get(x).copied().unwrap_or('.'),
                        bottom.get(x).copied().unwrap_or('.'),
                        truecolor,
                    )
                })
                .collect();
            while cells.last().is_some_and(|&(_, glyph)| glyph == ' ') {
                cells.pop();
            }
            cells
                .into_iter()
                .fold(Line::new(), |line, (role, glyph)| line.span(role, glyph))
        })
        .collect()
}

/// The most cells a fleet tape draws before it ends with an ellipsis.
const TAPE_CELLS: usize = 12;

/// The fleet tape: one cell per possible instance, the first `live` of them filled, capped at
/// twelve cells and then `…`.
pub(crate) fn tape(max: usize, live: usize) -> String {
    let cells = max.min(TAPE_CELLS);
    let filled = live.min(cells);
    let mut tape = "■".repeat(filled);
    tape.push_str(&"□".repeat(cells - filled));
    if max > TAPE_CELLS {
        tape.push('…');
    }
    tape
}

#[cfg(test)]
mod tests {
    use crate::palette::{Palette, strip_ansi};
    use crate::record::Line;

    use super::{
        BANNER, LINE, MASK, MASK_LINE, Picture, banner, banner_width, inline, render, tape,
    };

    const ESC: &str = "\u{1b}[";

    fn painted(rows: Picture, truecolor: bool) -> Vec<String> {
        let palette = Palette::Truecolor;
        render(rows, truecolor)
            .iter()
            .map(|line| line.paint(Some(palette)))
            .collect()
    }

    /// The number of escape sequences `text` opens.
    fn escapes(text: &str) -> usize {
        text.matches(ESC).count()
    }

    /// The rows of a picture without a palette: plain text only.
    fn unpainted(rows: Picture, truecolor: bool) -> Vec<String> {
        render(rows, truecolor)
            .iter()
            .map(|line| line.paint(None))
            .collect()
    }

    #[test]
    fn an_empty_cell_is_a_space_and_trailing_spaces_are_dropped() {
        assert_eq!(painted(&["..", ".."], true), [""]);
        let rows = painted(&["G.G", "..."], true);
        assert_eq!(rows.len(), 1);
        assert_eq!(strip_ansi(&rows[0]), "▀ ▀");
        // Two styled cells, each an escape and a reset; the empty cell between them is unstyled.
        assert_eq!(escapes(&rows[0]), 4);
    }

    #[test]
    fn a_half_filled_cell_is_a_half_block_in_the_filled_ink() {
        let top = painted(&["W", "."], true);
        let bottom = painted(&[".", "W"], true);
        assert_eq!(strip_ansi(&top[0]), "▀");
        assert_eq!(strip_ansi(&bottom[0]), "▄");
        assert_eq!(escapes(&top[0]), 2);
        assert_eq!(escapes(&bottom[0]), 2);
        // Both halves show the filled pixel's ink, so only the glyph differs.
        assert_eq!(top[0].replace('▀', ""), bottom[0].replace('▄', ""));
    }

    #[test]
    fn two_equal_pixels_are_a_full_block() {
        let full = painted(&["D", "D"], true);
        assert_eq!(full.len(), 1);
        assert_eq!(strip_ansi(&full[0]), "█");
        assert_eq!(escapes(&full[0]), 2);
    }

    #[test]
    fn two_different_fixed_inks_set_a_background() {
        let split = painted(&["G", "D"], true);
        let single = painted(&["G", "."], true);
        assert_eq!(strip_ansi(&split[0]), "▀");
        // A foreground and a background: one escape more than a single ink.
        assert_eq!(escapes(&split[0]), 3);
        assert_eq!(escapes(&single[0]), 2);
        assert_ne!(split, single);
        assert_eq!(unpainted(&["G", "D"], true), ["▀"]);
    }

    #[test]
    fn the_eyes_are_a_fixed_green_in_truecolor_and_the_accent_role_without() {
        let truecolor_eye = painted(&["A", "."], true);
        let fallback_eye = painted(&["A", "."], false);
        assert_eq!(strip_ansi(&truecolor_eye[0]), "▀");
        assert_eq!(strip_ansi(&fallback_eye[0]), "▀");
        assert_ne!(truecolor_eye, fallback_eye);
        assert_eq!(escapes(&truecolor_eye[0]), 2);
        // The accent role is bold and colored.
        assert_eq!(escapes(&fallback_eye[0]), 3);
    }

    #[test]
    fn muted_pixels_use_the_muted_role_and_ink_pixels_no_style() {
        let muted = painted(&["M", "K"], false);
        assert_eq!(strip_ansi(&muted[0]), "▀");
        assert_eq!(escapes(&muted[0]), 2);
        // An ink pixel has the plain role: no escape at all, even with a palette.
        let ink = painted(&["K", "."], false);
        assert_eq!(ink, ["▀"]);
        assert_eq!(escapes(&ink[0]), 0);
        assert!(!unpainted(&["M", "K"], false)[0].contains('\u{1b}'));
    }

    #[test]
    fn both_banners_have_five_rows_of_the_same_width() {
        assert_eq!(banner_width(), 16);
        for truecolor in [true, false] {
            let rows = banner(truecolor);
            assert_eq!(rows.len(), 5);
        }
        assert_eq!(BANNER.len(), LINE.len());
    }

    #[test]
    fn the_inline_mark_is_one_six_column_row() {
        assert_eq!(inline(false).text(), "▀▄▀▀▄▀");
        assert_eq!(inline(true).text(), "▀▀▀▀▀▀");
        assert_eq!(MASK.len(), 2);
        assert_eq!(MASK_LINE.len(), 2);
        assert!(!inline(true).paint(None).contains('\u{1b}'));
    }

    #[test]
    fn the_fallback_banner_matches_the_design_text() {
        let rows: Vec<String> = banner(false).iter().map(Line::text).collect();
        assert_eq!(
            rows,
            [
                " ▄▀▀▄      ▄▀▀▄",
                "▄▀  ▀▀▀▀▀▀▀▀  ▀▄",
                "██▀███    ███▀██",
                " ▀▀▄   ▄▄   ▄▀▀",
                "    ▀▀█▄▄█▀▀",
            ]
        );
    }

    #[test]
    fn tape_of_an_idle_fleet_is_hollow() {
        assert_eq!(tape(4, 0), "□□□□");
    }

    #[test]
    fn tape_fills_the_live_cells_first() {
        assert_eq!(tape(4, 3), "■■■□");
        assert_eq!(tape(4, 9), "■■■■");
    }

    #[test]
    fn tape_of_a_large_fleet_is_capped_with_an_ellipsis() {
        assert_eq!(tape(12, 0), "□□□□□□□□□□□□");
        assert_eq!(tape(40, 2), "■■□□□□□□□□□□…");
    }

    #[test]
    fn tape_of_no_instances_is_empty() {
        assert_eq!(tape(0, 0), "");
    }
}
