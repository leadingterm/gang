//! The permissions an API caller holds, as written on the wire.

wire_enum! {
    /// A permission an API caller holds, as written on the wire.
    pub enum Permission from gang_types::Permission {
        /// Reads fleet state.
        StateRead = "state.read",
        /// Reads principals.
        PrincipalsRead = "principals.read",
        /// Reads the scaling event history.
        EventsRead = "events.read",
        /// Revokes principals and restores revoked principals.
        PrincipalsRevoke = "principals.revoke",
    }
}
