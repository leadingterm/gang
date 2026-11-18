//! Shared checks for provisioner abandoned-request withdrawal.

use gang_spi::{FleetSupply, ProvisionerPort};
use gang_types::Fleet;

use crate::TestResult;

/// Checks that changing desired capacity preserves abandoned requests, while repeating it
/// withdraws them. The fleet must report abandoned requests and have room to raise desired by one.
///
/// # Errors
///
/// Returns an error if a provisioner operation fails or desired capacity cannot be raised.
///
/// # Panics
///
/// Panics if the preconditions or the provisioner contract are not met.
pub async fn abandoned_requests_contract<P: ProvisionerPort>(
    provisioner: &P,
    fleet: Fleet<'_, P::FleetOptions>,
) -> TestResult {
    let mut supply = FleetSupply::default();
    provisioner.supply(fleet, &mut supply).await?;
    let abandoned = supply.abandoned;
    assert!(abandoned > 0, "the fleet must have abandoned requests");
    let desired = supply
        .desired
        .checked_add(1)
        .ok_or("the fleet must have room to increase desired capacity")?;

    crate::fence::with_held(async |held| {
        provisioner.set_desired_capacity(held, fleet, desired).await
    })
    .await?;
    provisioner.supply(fleet, &mut supply).await?;
    assert_eq!(
        supply.desired, desired,
        "the new desired capacity is reported"
    );
    assert_eq!(
        supply.abandoned, abandoned,
        "a call that changes desired changes only the requested count"
    );

    crate::fence::with_held(async |held| {
        provisioner.set_desired_capacity(held, fleet, desired).await
    })
    .await?;
    provisioner.supply(fleet, &mut supply).await?;
    assert_eq!(
        supply.desired, desired,
        "withdrawal keeps desired unchanged"
    );
    assert_eq!(
        supply.abandoned, 0,
        "a call at the unchanged desired withdraws the abandoned requests"
    );
    Ok(())
}
