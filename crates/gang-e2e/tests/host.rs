//! Behaviour of the host namespace observation on the platform the tests run on.

use gang_e2e::Host;

#[cfg(target_os = "linux")]
#[test]
fn observes_the_net_and_mnt_namespaces() {
    let host = Host::observe();

    let net = host.net.expect("the net namespace is readable on linux");
    let mnt = host.mnt.expect("the mnt namespace is readable on linux");
    assert!(net.starts_with("net:["), "net namespace: {net}");
    assert!(mnt.starts_with("mnt:["), "mnt namespace: {mnt}");
}

#[cfg(not(target_os = "linux"))]
#[test]
fn reports_no_namespaces_where_they_are_not_readable() {
    let host = Host::observe();

    assert_eq!(host.net, None);
    assert_eq!(host.mnt, None);
}
