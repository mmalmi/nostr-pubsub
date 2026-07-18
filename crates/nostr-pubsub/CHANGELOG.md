# Changelog

## 0.1.12 - 2026-07-18

- Add subscription-scoped `INV` and `WANT` extensions to the existing Nostr
  `REQ`/`EVENT`/`CLOSE` FIPS wire codec.
- Group all matching subscription IDs for one peer into one `INV`; keep full
  event transfer as an ordinary subscription `EVENT`, allowing live
  mesh receivers to select one provider without changing subscription IDs,
  filters, or close semantics.
- Keep each `WANT` scoped to exactly one event ID; the provider selects a
  still-open matching subscription for the ordinary `EVENT` response.
- Expose exact peer-subscription lookup for validating requested live events.

## 0.1.11 - 2026-07-16

- Expose bounded mesh snapshots and verified snapshot seeding so reliable
  carriers can reconnect and replay without duplicating pubsub state.
- Include the configured Inv/WANT envelope in the maximum wire-size bound.
