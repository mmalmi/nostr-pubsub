use std::borrow::Cow;

use nostr::Kind;
use nostr_pubsub::{PubsubError, Result, VerifiedEvent};
use nostr_social_graph::Rating;
use nostr_social_memory::RATING_KIND;
use uuid::Uuid;

const SCHEMA_VERSION: &str = "1";
const TYPE_RATING: &str = "rating";
const SUBJECT_MARKER: &str = "subject";
const RESERVED_TAGS: [&str; 5] = ["d", "e", "i", "p", "ms"];

type FactEntry<'a> = (&'a str, Vec<String>);

/// Parses a `nostr-social-memory` rating from an event whose Nostr ID and
/// Schnorr signature were already verified at the trust boundary.
pub fn parse_verified_rating(verified: &VerifiedEvent) -> Result<Rating> {
    let event = verified.as_event();
    if event.kind != Kind::from(RATING_KIND) {
        return Err(validation(format!(
            "wrong fact op kind: expected {RATING_KIND}, got {:?}",
            event.kind
        )));
    }
    if !event.content.is_empty() {
        return Err(validation("fact events must have empty content"));
    }

    let (raw_subject, mut facts) = parse_rating_tags(event.tags.as_slice())?;
    let id = parse_record_uuid(raw_subject).ok_or_else(|| {
        validation(format!(
            "social-memory fact record id must be a UUID: {raw_subject}"
        ))
    })?;
    facts.sort();
    facts.dedup();
    build_rating_from_facts(id, &facts)
}

fn parse_rating_tags(tags: &[nostr::Tag]) -> Result<(&str, Vec<FactEntry<'_>>)> {
    let mut subject: Option<&str> = None;
    let mut facts: Vec<FactEntry<'_>> = Vec::new();

    for tag in tags {
        let parts = tag.as_slice();
        let Some(kind) = parts.first().map(String::as_str) else {
            continue;
        };
        match kind {
            "d" => return Err(validation("fact op event must not use d tag")),
            "ms" => return Err(validation("fact op event must not use ms tag")),
            "i" => {
                let value = parts
                    .get(1)
                    .ok_or_else(|| validation("fact i tag is missing value"))?;
                if parts.get(2).is_some_and(|marker| marker == SUBJECT_MARKER) {
                    validate_subject(value)?;
                    if subject.replace(value.as_str()).is_some() {
                        return Err(validation("fact event has multiple subject i tags"));
                    }
                } else {
                    validate_external_identifier(value)?;
                }
            }
            "p" => {
                let value = parts
                    .get(1)
                    .ok_or_else(|| validation("fact p tag is missing pubkey"))?;
                if !is_hex_len(value.trim(), 64) {
                    return Err(validation(format!("invalid fact p tag pubkey: {value}")));
                }
            }
            "e" => {
                let value = parts
                    .get(1)
                    .ok_or_else(|| validation("fact e tag is missing event id"))?;
                if !is_hex_len(value.trim(), 64) {
                    return Err(validation(format!("invalid linked event id: {value}")));
                }
                match parts.get(3).map(String::as_str) {
                    Some("prev" | "replace" | "dispute") => {}
                    Some(marker) => {
                        return Err(validation(format!(
                            "unsupported fact e tag marker: {marker}"
                        )));
                    }
                    None => return Err(validation("fact e tag is missing marker")),
                }
            }
            _ => {
                if kind.chars().count() == 1 {
                    continue;
                }
                let predicate = normalize_predicate(kind)?;
                let values = parts
                    .iter()
                    .skip(1)
                    .map(|value| normalize_value(value).into_owned())
                    .collect();
                facts.push((predicate, values));
            }
        }
    }

    let raw_subject = subject.ok_or_else(|| validation("fact event is missing subject i tag"))?;
    Ok((raw_subject, facts))
}

fn build_rating_from_facts(id: String, facts: &[FactEntry<'_>]) -> Result<Rating> {
    let record_type = required_scalar(facts, "type")?;
    if record_type != TYPE_RATING {
        return Err(validation(format!(
            "unexpected social-memory fact event type {record_type}"
        )));
    }
    let schema = required_scalar(facts, "schema")?;
    if schema != SCHEMA_VERSION {
        return Err(validation(format!(
            "unsupported social-memory fact schema {schema}"
        )));
    }

    let rating = Rating {
        id,
        rater: required_scalar(facts, "rater")?.to_owned(),
        subject: required_scalar(facts, "subject")?.to_owned(),
        scope: optional_scalar(facts, "scope")?.map(ToOwned::to_owned),
        rating: required_i64(facts, "rating")?,
        min_rating: required_i64(facts, "min_rating")?,
        max_rating: required_i64(facts, "max_rating")?,
        sample_count: optional_u64(facts, "sample_count")?,
        window_start: optional_u64(facts, "window_start")?,
        window_end: optional_u64(facts, "window_end")?,
        evidence: scalar_values(facts, "evidence")?,
        reason: optional_scalar(facts, "reason")?.map(ToOwned::to_owned),
        tags: scalar_values(facts, "tag")?,
        created_at: required_u64(facts, "created_at")?,
    };
    rating
        .validate()
        .map_err(|error| validation(format!("invalid rating: {error}")))?;
    Ok(rating)
}

fn validate_subject(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(validation(
            "fact subject must be a non-empty identifier without control characters",
        ));
    }
    Ok(())
}

fn validate_external_identifier(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(validation("external identifier cannot be empty"));
    }
    if is_hyphenated_uuid(trimmed) {
        return Ok(());
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err(validation(format!(
            "external identifier cannot contain whitespace: {trimmed}"
        )));
    }
    Ok(())
}

fn normalize_predicate(value: &str) -> Result<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(validation("fact predicate cannot be empty"));
    }
    if trimmed.chars().count() < 2 {
        return Err(validation(format!(
            "fact predicate must be at least two characters: {trimmed}"
        )));
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err(validation(format!(
            "fact predicate cannot contain whitespace: {trimmed}"
        )));
    }
    if RESERVED_TAGS.contains(&trimmed) {
        return Err(validation(format!(
            "{trimmed} is a reserved fact event tag"
        )));
    }
    Ok(trimmed)
}

fn normalize_value(value: &str) -> Cow<'_, str> {
    let trimmed = value.trim();
    if is_hyphenated_uuid(trimmed) || is_hex_len(trimmed, 64) {
        if trimmed.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(trimmed.to_ascii_lowercase())
        } else {
            Cow::Borrowed(trimmed)
        }
    } else {
        Cow::Borrowed(trimmed)
    }
}

fn is_hex_len(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_hyphenated_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes[8] == b'-'
        && bytes[13] == b'-'
        && bytes[18] == b'-'
        && bytes[23] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, byte)| matches!(i, 8 | 13 | 18 | 23) || byte.is_ascii_hexdigit())
}

fn parse_record_uuid(value: &str) -> Option<String> {
    // Match the unverified social-memory parser exactly, using the same UUID
    // dependency instead of accepting extra combinations of wrappers.
    Uuid::parse_str(value).ok().map(|id| id.to_string())
}

fn scalar_iter<'a>(
    facts: &'a [(&'a str, Vec<String>)],
    predicate: &'a str,
) -> impl Iterator<Item = Result<&'a str>> + 'a {
    facts
        .iter()
        .filter(move |(name, _)| *name == predicate)
        .map(move |(_, values)| match values.as_slice() {
            [value] => Ok(value.as_str()),
            _ => Err(validation(format!(
                "social-memory fact {predicate} must be a scalar"
            ))),
        })
}

fn required_scalar<'a>(facts: &'a [(&'a str, Vec<String>)], predicate: &'a str) -> Result<&'a str> {
    let mut iter = scalar_iter(facts, predicate);
    let first = iter
        .next()
        .ok_or_else(|| validation(format!("missing social-memory fact {predicate}")))??;
    if iter.next().is_some() {
        return Err(validation(format!(
            "social-memory fact {predicate} must appear once"
        )));
    }
    Ok(first)
}

fn optional_scalar<'a>(
    facts: &'a [(&'a str, Vec<String>)],
    predicate: &'a str,
) -> Result<Option<&'a str>> {
    let mut iter = scalar_iter(facts, predicate);
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    let first = first?;
    if iter.next().is_some() {
        return Err(validation(format!(
            "social-memory fact {predicate} must appear once"
        )));
    }
    Ok(Some(first))
}

fn scalar_values(facts: &[(&str, Vec<String>)], predicate: &str) -> Result<Vec<String>> {
    scalar_iter(facts, predicate)
        .map(|value| value.map(ToOwned::to_owned))
        .collect()
}

fn required_i64(facts: &[(&str, Vec<String>)], predicate: &str) -> Result<i64> {
    required_scalar(facts, predicate)?.parse().map_err(|error| {
        validation(format!(
            "invalid i64 social-memory fact {predicate}: {error}"
        ))
    })
}

fn required_u64(facts: &[(&str, Vec<String>)], predicate: &str) -> Result<u64> {
    required_scalar(facts, predicate)?.parse().map_err(|error| {
        validation(format!(
            "invalid u64 social-memory fact {predicate}: {error}"
        ))
    })
}

fn optional_u64(facts: &[(&str, Vec<String>)], predicate: &str) -> Result<Option<u64>> {
    optional_scalar(facts, predicate)?
        .map(|value| {
            value.parse().map_err(|error| {
                validation(format!(
                    "invalid u64 social-memory fact {predicate}: {error}"
                ))
            })
        })
        .transpose()
}

fn validation(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
    use nostr_pubsub::VerifiedEvent;
    use nostr_social_graph::Rating;
    use nostr_social_memory::{RATING_KIND, RatingEventExt, rating_from_event};

    use super::parse_verified_rating;

    #[test]
    fn verified_rating_preserves_record_uuid_validation() {
        let keys = Keys::generate();
        let rating = Rating::new(
            keys.public_key().to_hex(),
            keys.public_key().to_hex(),
            1,
            0,
            2,
        );
        let original = rating.to_event(&keys).unwrap();
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let simple = id.replace('-', "");
        for subject in [
            id.to_owned(),
            id.to_uppercase(),
            simple.clone(),
            format!("{{{id}}}"),
            format!("urn:uuid:{id}"),
            format!("{{{simple}}}"),
            format!("urn:uuid:{simple}"),
            format!("urn:uuid:{{{id}}}"),
            format!("URN:UUID:{id}"),
        ] {
            let tags = original.tags.iter().map(|tag| {
                let mut parts = tag.as_slice().to_vec();
                if parts.first().is_some_and(|name| name == "i")
                    && parts.get(2).is_some_and(|marker| marker == "subject")
                {
                    parts[1] = subject.clone();
                }
                Tag::parse(parts).unwrap()
            });
            let event = EventBuilder::new(Kind::from(RATING_KIND), "")
                .tags(tags)
                .sign_with_keys(&keys)
                .unwrap();
            let verified = VerifiedEvent::try_from(event.clone()).unwrap();
            assert_eq!(
                parse_verified_rating(&verified).ok(),
                rating_from_event(&event).ok(),
                "record subject: {subject}",
            );
        }
    }

    #[test]
    fn parse_verified_rating_matches_rating_from_event_for_valid_and_invalid_events() {
        let keys = Keys::generate();
        let rater = keys.public_key().to_hex();
        let subject = Keys::generate().public_key().to_hex();

        let mut full = Rating::new(&rater, &subject, 75, 0, 100);
        full.scope = Some("fips.peer".to_string());
        full.sample_count = Some(9);
        full.window_start = Some(100);
        full.window_end = Some(200);
        full.evidence = vec!["ev-b".to_string(), "ev-a".to_string()];
        full.reason = Some("reliable".to_string());
        full.tags = vec!["peer".to_string(), "fast".to_string()];
        full.created_at = 1_700_000_123;

        let valid_event = full.to_event(&keys).unwrap();
        let verified = VerifiedEvent::try_from(valid_event.clone()).unwrap();
        assert_eq!(
            parse_verified_rating(&verified).unwrap(),
            rating_from_event(&valid_event).unwrap()
        );

        let dup_tags_event = EventBuilder::new(valid_event.kind, "")
            .tags(
                valid_event
                    .tags
                    .iter()
                    .cloned()
                    .chain(std::iter::once(Tag::parse(["rating", "75"]).unwrap())),
            )
            .custom_created_at(Timestamp::from(1_700_000_123))
            .sign_with_keys(&keys)
            .unwrap();
        assert_eq!(
            parse_verified_rating(&VerifiedEvent::try_from(dup_tags_event.clone()).unwrap())
                .unwrap(),
            rating_from_event(&dup_tags_event).unwrap()
        );

        let invalid_cases = [
            EventBuilder::text_note("not a rating")
                .sign_with_keys(&keys)
                .unwrap(),
            EventBuilder::new(Kind::from(RATING_KIND), "non-empty")
                .tags(valid_event.tags.clone())
                .sign_with_keys(&keys)
                .unwrap(),
            EventBuilder::new(Kind::from(RATING_KIND), "")
                .tags(
                    valid_event
                        .tags
                        .iter()
                        .cloned()
                        .chain(std::iter::once(Tag::parse(["d", "bad"]).unwrap())),
                )
                .sign_with_keys(&keys)
                .unwrap(),
            EventBuilder::new(Kind::from(RATING_KIND), "")
                .tags(
                    valid_event
                        .tags
                        .iter()
                        .cloned()
                        .chain(std::iter::once(Tag::parse(["rating", "10"]).unwrap())),
                )
                .sign_with_keys(&keys)
                .unwrap(),
            EventBuilder::new(Kind::from(RATING_KIND), "")
                .tags(
                    valid_event
                        .tags
                        .iter()
                        .cloned()
                        .chain(std::iter::once(Tag::parse(["p", "not-hex"]).unwrap())),
                )
                .sign_with_keys(&keys)
                .unwrap(),
            EventBuilder::new(Kind::from(RATING_KIND), "")
                .tags(valid_event.tags.iter().cloned().chain(std::iter::once(
                    Tag::parse(["e", &"ab".repeat(32), "", "bad"]).unwrap(),
                )))
                .sign_with_keys(&keys)
                .unwrap(),
        ];

        for invalid in invalid_cases {
            let verified = VerifiedEvent::try_from(invalid.clone()).unwrap();
            assert_eq!(
                parse_verified_rating(&verified).is_ok(),
                rating_from_event(&invalid).is_ok(),
                "mismatch for event tags: {:?}",
                invalid.tags
            );
        }
    }
}
