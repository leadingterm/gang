//! Stops admission and releases queued client streams during a controller hand-off.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::standing::Standing;

/// Persistent, cloneable hand-off notifications.
#[derive(Clone)]
pub(crate) struct HandOff {
    begun: watch::Sender<bool>,
    workers_released: watch::Sender<bool>,
    public: Arc<Mutex<Vec<Weak<Standing>>>>,
}

impl HandOff {
    #[allow(dead_code)] // Wired by controller runtime (WP D2).
    pub(crate) fn new() -> Self {
        Self {
            begun: watch::channel(false).0,
            workers_released: watch::channel(false).0,
            public: Arc::default(),
        }
    }

    #[allow(dead_code)] // Wired by controller runtime (WP D2).
    pub(crate) fn begin(&self) {
        let public = self.public.lock();
        if self.is_begun() {
            return;
        }
        self.begun.send_replace(true);
        for standing in public.iter().filter_map(Weak::upgrade) {
            standing.hand_off();
        }
    }

    /// Registers only pre-handoff connections; fresh refusal connections must first send 503.
    pub(crate) fn public_connection(&self, standing: &Arc<Standing>) {
        let mut public = self.public.lock();
        if !self.is_begun() {
            public.retain(|entry| entry.strong_count() != 0);
            public.push(Arc::downgrade(standing));
        }
    }

    #[allow(dead_code)] // Wired by controller runtime (WP D2).
    pub(crate) fn public_complete(&self) -> bool {
        self.public
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .all(|standing| !standing.is_busy())
    }

    pub(crate) fn is_begun(&self) -> bool {
        *self.begun.borrow()
    }

    pub(crate) async fn begun(&self) {
        let mut receiver = self.begun.subscribe();
        let _ = receiver.wait_for(|begun| *begun).await;
    }

    #[allow(dead_code)] // Wired by controller runtime (WP D2).
    pub(crate) fn release_workers(&self) {
        self.workers_released.send_replace(true);
    }

    pub(crate) async fn workers_released(&self) {
        let mut receiver = self.workers_released.subscribe();
        let _ = receiver.wait_for(|released| *released).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn begin_retires_pre_handoff_admitted_connections_and_completes_when_idle() {
        let handoff = HandOff::new();
        let pre = Arc::new(Standing::default());
        assert!(pre.admit());
        let busy = pre.begin();
        handoff.public_connection(&pre);
        assert!(!handoff.public_complete());

        handoff.begin();
        assert!(handoff.is_begun());
        let retry = pre.retry_requested();
        tokio::pin!(retry);
        assert!(futures::poll!(&mut retry).is_ready());

        // Post-handoff refusal connections are not registered in `public`.
        let post = Arc::new(Standing::default());
        let _post_busy = post.begin();
        handoff.public_connection(&post);
        assert!(!handoff.public_complete());

        drop(busy);
        assert!(handoff.public_complete());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn begun_and_workers_released_notify_waiters() {
        let handoff = HandOff::new();
        let begun = handoff.begun();
        let released = handoff.workers_released();
        tokio::pin!(begun);
        tokio::pin!(released);
        assert!(futures::poll!(&mut begun).is_pending());
        assert!(futures::poll!(&mut released).is_pending());

        handoff.begin();
        assert!(futures::poll!(&mut begun).is_ready());
        assert!(futures::poll!(&mut released).is_pending());

        handoff.release_workers();
        assert!(futures::poll!(&mut released).is_ready());
    }
}
