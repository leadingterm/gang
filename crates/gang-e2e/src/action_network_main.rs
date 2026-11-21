//! Entrypoint for the NixOS action-network VM fixture.
fn main() {
    if let Err(error) = gang_e2e::run_action_network() {
        eprintln!("action network: {error}");
        std::process::exit(1);
    }
}
