//! Synchronous entry point into a multi-threaded tokio runtime.

use crate::errors::{MainError, Result};

/// Runs `future` to completion on a fresh multi-threaded runtime with all drivers enabled;
/// its error converts into `MainError`.
pub(crate) fn block_on<T, E>(future: impl Future<Output = std::result::Result<T, E>>) -> Result<T>
where
    MainError: From<E>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(MainError::Runtime)?;
    Ok(runtime.block_on(future)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_returns_future_output() {
        let value = block_on(async { Ok::<_, MainError>(7_u32) }).expect("runtime builds");
        assert_eq!(value, 7);
    }

    #[test]
    fn block_on_converts_the_futures_error() {
        let result: Result<()> = block_on(async { Err::<(), _>(MainError::NoServer) });
        assert!(matches!(result, Err(MainError::NoServer)));
    }
}
