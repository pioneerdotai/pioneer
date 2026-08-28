use anyhow::Result;
use pioneer_crud::{
    CrudStore, load_gateway_singleton, scan_authorization_persistence_invariants_cooperative,
};
use tracing::{info, warn};

/// Audits invariants that current authorization writes must continue to
/// preserve. Unlike the retired legacy access-class backfill, this remains
/// useful for detecting corruption produced by current runtime paths.
pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    let database = crud_store.database_connection();
    let gateway = match load_gateway_singleton(&database).await {
        Ok(Some(gateway)) => gateway,
        Ok(None) => {
            warn!("authorization background audit skipped because Gateway identity is missing");
            return Ok(());
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "authorization background audit could not load Gateway identity"
            );
            return Err(error.into());
        }
    };

    match scan_authorization_persistence_invariants_cooperative(
        &database,
        &gateway.id,
        128,
        super::maintenance_checkpoint,
    )
    .await
    {
        Ok(report) if report.is_valid() => {
            if report.ineligible_active_learned_versions > 0 {
                info!(
                    ineligible_active_learned_versions = report.ineligible_active_learned_versions,
                    "active learned versions remain Superuser-only after authorization background audit"
                );
            }
        }
        Ok(report) => warn!(
            violations = %report.safe_diagnostic(),
            "Gateway authorization background audit found persistence invariant violations"
        ),
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "Gateway authorization background audit failed"
            );
            return Err(error);
        }
    }
    Ok(())
}
