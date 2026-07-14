use fips_core::FipsEndpoint;
use fips_core::config::NostrDiscoveryConfig;
use fips_core::discovery::nostr::{ADVERT_IDENTIFIER, ADVERT_KIND};
use nostr::Kind;
use nostr_pubsub::{EventRetentionPolicy, Filter, Result, VerifiedEvent};

use super::storage_error;

/// Convert FIPS's transport-neutral discovery configuration into the bounded
/// retention policy used by a pubsub cache.
#[must_use]
pub fn fips_discovery_retention_policy(
    config: &NostrDiscoveryConfig,
) -> Option<EventRetentionPolicy> {
    config.enabled.then(|| {
        let app = config.app.trim();
        let identifier = if app.is_empty() {
            ADVERT_IDENTIFIER
        } else {
            app
        };
        EventRetentionPolicy::new(
            config.advert_cache_max_entries,
            vec![
                Filter::new()
                    .kind(Kind::Custom(ADVERT_KIND))
                    .identifier(identifier),
            ],
        )
    })
}

/// Feed a verified pubsub event into FIPS's normal Nostr discovery validator.
pub async fn ingest_fips_discovery_event(
    endpoint: &FipsEndpoint,
    event: VerifiedEvent,
) -> Result<bool> {
    endpoint
        .ingest_nostr_discovery_event(event.into_event())
        .await
        .map_err(|error| storage_error("ingest FIPS Nostr discovery event", error))
}
