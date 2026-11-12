//! In-memory listeners and their dialers: the connections a test hands to the server.

use std::io;

use gang_tls::Listener;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

/// Capacity of each in-memory pipe, in bytes.
const PIPE_BUFFER: usize = 64 * 1024;

/// A listener fed by [`MemoryDialer`]: each dialed stream is one accepted connection.
pub struct MemoryListener {
    /// The dialed streams, one per connection.
    dials: mpsc::UnboundedReceiver<DuplexStream>,
    /// How many connections have been accepted; the address of the next one.
    accepted: u32,
}

impl Listener for MemoryListener {
    type Io = DuplexStream;
    type Addr = u32;

    async fn accept(&mut self) -> io::Result<(DuplexStream, u32)> {
        match self.dials.recv().await {
            Some(stream) => {
                self.accepted += 1;
                Ok((stream, self.accepted))
            }
            None => std::future::pending().await,
        }
    }
}

/// Dials a [`MemoryListener`]; each dial hands the listener one end of a fresh in-memory pipe.
#[derive(Clone)]
pub struct MemoryDialer {
    /// The sending end of the listener's dial queue.
    dials: mpsc::UnboundedSender<DuplexStream>,
}

impl MemoryDialer {
    /// Opens a connection to the listener and returns the dialing end. When the listener is gone
    /// the returned stream reads end of file.
    #[must_use]
    pub fn dial(&self) -> DuplexStream {
        let (dialing, listening) = tokio::io::duplex(PIPE_BUFFER);
        let _ = self.dials.send(listening);
        dialing
    }
}

/// A listener and the dialer that connects to it. The listener accepts forever once every dialer
/// has been dropped; it never returns an error.
#[must_use]
pub fn memory_listener() -> (MemoryListener, MemoryDialer) {
    let (sender, dials) = mpsc::unbounded_channel();
    (
        MemoryListener { dials, accepted: 0 },
        MemoryDialer { dials: sender },
    )
}
