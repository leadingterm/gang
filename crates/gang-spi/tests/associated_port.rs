//! A port with an associated type: the dispatch enum wraps each plugin's scope in its scope enum,
//! narrows scopes back to the selected plugin, and refuses a scope made by another plugin.

/// The meter port, re-exported at the root for the plugins and tests below.
pub use ports::Meter;

#[macro_use]
mod ports {
    use std::fmt::Debug;

    use gang_spi::port;

    use crate::__private::PortError;

    /// A port whose scope is an associated type that the dispatch enum narrows.
    #[port]
    pub trait Meter: Send + Sync {
        /// The scope one plugin hands out for a label.
        type Scope: Debug + Send;

        /// Opens a scope for the label.
        async fn enter(&self, label: &str) -> Result<Self::Scope, PortError>;

        /// Reads the scope's value without closing it.
        async fn peek(&self, scope: &Self::Scope) -> Result<u32, PortError>;

        /// Closes the scope and returns its report.
        async fn leave(&self, scope: Self::Scope) -> Result<String, PortError>;
    }
}

/// Hides the `PortError` path that the delegation macro resolves through `$crate::__private`.
mod __private {
    pub use gang_spi::PortError;
}

/// The scope of the first plugin.
#[derive(Debug)]
pub struct AlphaScope(pub u32);

/// The scope of the second plugin.
#[derive(Debug)]
pub struct BetaScope(pub String);

/// A plugin whose scope counts the label's bytes.
pub struct Alpha;

impl Meter for Alpha {
    type Scope = AlphaScope;

    async fn enter(&self, label: &str) -> Result<AlphaScope, gang_spi::PortError> {
        Ok(AlphaScope(u32::try_from(label.len()).unwrap_or(u32::MAX)))
    }

    async fn peek(&self, scope: &AlphaScope) -> Result<u32, gang_spi::PortError> {
        Ok(scope.0)
    }

    async fn leave(&self, scope: AlphaScope) -> Result<String, gang_spi::PortError> {
        Ok(format!("alpha:{}", scope.0))
    }
}

/// A plugin whose scope keeps the label itself.
pub struct Beta;

impl Meter for Beta {
    type Scope = BetaScope;

    async fn enter(&self, label: &str) -> Result<BetaScope, gang_spi::PortError> {
        Ok(BetaScope(label.to_owned()))
    }

    async fn peek(&self, scope: &BetaScope) -> Result<u32, gang_spi::PortError> {
        Ok(u32::try_from(scope.0.len()).unwrap_or(u32::MAX))
    }

    async fn leave(&self, scope: BetaScope) -> Result<String, gang_spi::PortError> {
        Ok(scope.0)
    }
}

/// The dispatch enum that selects a plugin at run time.
pub enum Dispatch {
    /// The `Alpha` plugin's dispatch arm.
    Alpha(Alpha),
    /// The `Beta` plugin's dispatch arm.
    Beta(Beta),
}

/// The dispatch enum of the `Scope` associated type: one variant per plugin.
#[derive(Debug)]
pub enum DispatchScope {
    /// The `Alpha` plugin's scope.
    Alpha(AlphaScope),
    /// The `Beta` plugin's scope.
    Beta(BetaScope),
}

impl DispatchScope {
    /// The scope if it came from the `Alpha` plugin.
    #[must_use]
    pub fn as_alpha(&self) -> Option<&AlphaScope> {
        match self {
            Self::Alpha(scope) => Some(scope),
            Self::Beta(_) => None,
        }
    }

    /// The scope if it came from the `Alpha` plugin, otherwise the value back.
    ///
    /// # Errors
    /// The unchanged value when it belongs to another plugin.
    pub fn into_alpha(self) -> Result<AlphaScope, Self> {
        match self {
            Self::Alpha(scope) => Ok(scope),
            Self::Beta(scope) => Err(Self::Beta(scope)),
        }
    }

    /// The scope if it came from the `Beta` plugin.
    #[must_use]
    pub fn as_beta(&self) -> Option<&BetaScope> {
        match self {
            Self::Beta(scope) => Some(scope),
            Self::Alpha(_) => None,
        }
    }

    /// The scope if it came from the `Beta` plugin, otherwise the value back.
    ///
    /// # Errors
    /// The unchanged value when it belongs to another plugin.
    pub fn into_beta(self) -> Result<BetaScope, Self> {
        match self {
            Self::Beta(scope) => Ok(scope),
            Self::Alpha(scope) => Err(Self::Alpha(scope)),
        }
    }
}

__gang_delegate_Meter!(self::Dispatch { Alpha => as_alpha, into_alpha, Beta => as_beta, into_beta } [ DispatchScope ]);

/// A dispatch enum with a single plugin: its match has one arm, which must still compile.
pub enum Solo {
    /// The `Alpha` plugin, the only plugin this dispatch enum selects.
    Alpha(Alpha),
}

/// The scope enum of the single-plugin dispatch enum.
#[derive(Debug)]
pub enum SoloScope {
    /// The `Alpha` plugin's scope, the only one `SoloScope` can hold.
    Alpha(AlphaScope),
}

impl SoloScope {
    /// The scope if it came from the `Alpha` plugin.
    #[must_use]
    pub fn as_alpha(&self) -> Option<&AlphaScope> {
        match self {
            Self::Alpha(scope) => Some(scope),
        }
    }

    /// The scope if it came from the `Alpha` plugin.
    ///
    /// # Errors
    /// Never: this enum has only the `Alpha` variant.
    pub fn into_alpha(self) -> Result<AlphaScope, Self> {
        match self {
            Self::Alpha(scope) => Ok(scope),
        }
    }
}

__gang_delegate_Meter!(self::Solo { Alpha => as_alpha, into_alpha } [ SoloScope ]);

/// Uses any meter: opens a scope, peeks it, and reports the value.
async fn use_meter<M: Meter>(meter: &M) -> Result<u32, gang_spi::PortError> {
    let scope = meter.enter("abcd").await?;
    meter.peek(&scope).await
}

/// Extracts the message of a configuration error, or fails on any other outcome.
fn config_message<T: std::fmt::Debug>(
    result: Result<T, gang_spi::PortError>,
) -> Result<String, Box<dyn std::error::Error>> {
    match result {
        Err(gang_spi::PortError::Config(text)) => Ok(text.to_string()),
        other => Err(format!("expected a configuration error, got {other:?}").into()),
    }
}

#[tokio::test]
async fn enter_through_alpha_returns_alpha_scope() {
    assert!(matches!(
        Dispatch::Alpha(Alpha).enter("ab").await,
        Ok(DispatchScope::Alpha(AlphaScope(2)))
    ));
}

#[tokio::test]
async fn enter_through_beta_returns_beta_scope() {
    assert!(matches!(
        Dispatch::Beta(Beta).enter("ab").await,
        Ok(DispatchScope::Beta(BetaScope(label))) if label == "ab"
    ));
}

#[tokio::test]
async fn alpha_peek_and_leave_reach_the_plugin() {
    let meter = Dispatch::Alpha(Alpha);
    let scope = meter.enter("abc").await.expect("enter");
    assert_eq!(meter.peek(&scope).await.ok(), Some(3));
    assert_eq!(meter.leave(scope).await.ok(), Some("alpha:3".to_owned()));
}

#[tokio::test]
async fn beta_peek_and_leave_reach_the_plugin() {
    let meter = Dispatch::Beta(Beta);
    let scope = meter.enter("abcd").await.expect("enter");
    assert_eq!(meter.peek(&scope).await.ok(), Some(4));
    assert_eq!(meter.leave(scope).await.ok(), Some("abcd".to_owned()));
}

#[tokio::test]
async fn peek_with_another_plugins_scope_is_a_config_error()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Dispatch::Beta(Beta).enter("x").await.expect("enter");
    let message = config_message(Dispatch::Alpha(Alpha).peek(&scope).await)?;
    assert_eq!(message, "the scope belongs to another plugin");
    Ok(())
}

#[tokio::test]
async fn leave_with_another_plugins_scope_is_a_config_error()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Dispatch::Alpha(Alpha).enter("x").await.expect("enter");
    let message = config_message(Dispatch::Beta(Beta).leave(scope).await)?;
    assert_eq!(message, "the scope belongs to another plugin");
    Ok(())
}

#[tokio::test]
async fn arc_of_dispatch_is_a_meter() {
    let meter = std::sync::Arc::new(Dispatch::Alpha(Alpha));
    assert_eq!(use_meter(&meter).await.ok(), Some(4));
}

#[tokio::test]
async fn arc_of_plugin_is_a_meter() {
    let meter = std::sync::Arc::new(Alpha);
    assert_eq!(use_meter(&meter).await.ok(), Some(4));
}

#[tokio::test]
async fn single_plugin_dispatch_round_trips() {
    let meter = Solo::Alpha(Alpha);
    let scope = meter.enter("ab").await.expect("enter");
    assert!(matches!(scope, SoloScope::Alpha(AlphaScope(2))));
    assert_eq!(meter.peek(&scope).await.ok(), Some(2));
    assert_eq!(meter.leave(scope).await.ok(), Some("alpha:2".to_owned()));
}
