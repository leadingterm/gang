//! Consent and terminal policy for the setup wizard.

/// How setup may interact with the user and the filesystem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mode {
    /// Ask for consent before making changes.
    Interactive,
    /// Apply Gang-owned changes without prompting.
    Yes,
    /// Show changes without signing in or writing.
    DryRun,
    /// Show changes and require explicit consent in a terminal or via `--yes`.
    PreviewOnly,
}

/// Whether the input and diagnostic streams are terminals.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Terminals {
    /// Standard input is a terminal.
    pub stdin: bool,
    /// Standard error is a terminal.
    pub stderr: bool,
}

/// Selects the requested mode, with explicit flags taking precedence over terminals.
pub(crate) fn mode(yes: bool, dry_run: bool, terminals: Terminals) -> Mode {
    if yes {
        Mode::Yes
    } else if dry_run {
        Mode::DryRun
    } else if terminals.stdin && terminals.stderr {
        Mode::Interactive
    } else {
        Mode::PreviewOnly
    }
}

impl Mode {
    /// Whether Gang-owned changes may be written.
    pub(crate) fn writes(self) -> bool {
        matches!(self, Self::Interactive | Self::Yes)
    }

    /// Whether user input may be requested.
    pub(crate) fn prompts(self) -> bool {
        self == Self::Interactive
    }

    /// Whether authentication may be initiated.
    pub(crate) fn signs_in(self) -> bool {
        matches!(self, Self::Interactive | Self::Yes)
    }
}

#[cfg(test)]
mod tests {
    use super::{Mode, Terminals, mode};

    /// Flags win over terminals, and only two terminals allow prompting.
    #[test]
    fn flags_win_and_only_two_terminals_prompt() {
        let both = Terminals {
            stdin: true,
            stderr: true,
        };
        let piped = Terminals {
            stdin: false,
            stderr: true,
        };
        assert_eq!(mode(true, true, both), Mode::Yes);
        assert_eq!(mode(false, true, both), Mode::DryRun);
        assert_eq!(mode(false, false, both), Mode::Interactive);
        assert_eq!(mode(false, false, piped), Mode::PreviewOnly);
        assert_eq!(
            mode(
                false,
                false,
                Terminals {
                    stdin: true,
                    stderr: false
                }
            ),
            Mode::PreviewOnly
        );
    }
}
