use nostr::{
    Alphabet, Event, EventBuilder, Filter, Keys, Kind, PublicKey, SingleLetterTag, Tag, TagKind,
    Timestamp, event::builder::Error as EventBuilderError,
};

const HASHTREE_ROOT_KIND: u16 = 30_064;
const GIT_REPO_ANNOUNCEMENT_KIND: u16 = 30_617;
const IRIS_DRIVE_ROOT_KIND: u16 = 30_078;
const TARGETED_APPROVAL_RATING_KIND: u16 = 7_368;
const FIPS_ADVERT_KIND: u16 = 37_195;
const FIPS_PAID_OFFER_KIND: u16 = 37_196;
const HASHTREE_LABEL: &str = "hashtree";
const FIPS_PAID_OFFER_APP: &str = "fips/paid-route-offer";
const FIPS_PAID_OFFER_VERSION: &str = "1";

/// Representative subscription shapes exercised by the simulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionClass {
    AuthorFeed,
    HashtagTopic,
    HashtreeUpdate,
    TargetedApprovalRating,
    IrisDriveBroadRoot,
    FipsAdvert,
    FipsPaidOffer,
    GitRepoAnnouncement,
}

impl SubscriptionClass {
    pub const ALL: [Self; 8] = [
        Self::AuthorFeed,
        Self::HashtagTopic,
        Self::HashtreeUpdate,
        Self::TargetedApprovalRating,
        Self::IrisDriveBroadRoot,
        Self::FipsAdvert,
        Self::FipsPaidOffer,
        Self::GitRepoAnnouncement,
    ];
}

/// A real Nostr filter paired with a deterministically signed matching event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionWorkload {
    pub class: SubscriptionClass,
    pub filter: Filter,
    pub event: Event,
}

pub type WorkloadResult = std::result::Result<SubscriptionWorkload, EventBuilderError>;

/// A kind-1 feed restricted to one author.
pub fn build_author_feed(signer: &Keys, created_at: u64) -> WorkloadResult {
    let filter = Filter::new()
        .kind(Kind::TextNote)
        .author(signer.public_key());
    signed_workload(
        SubscriptionClass::AuthorFeed,
        filter,
        EventBuilder::new(Kind::TextNote, "author feed event"),
        signer,
        created_at,
    )
}

/// A kind-1 topic feed selected by its lower-case NIP-12 hashtag.
pub fn build_hashtag_topic(signer: &Keys, topic: &str, created_at: u64) -> WorkloadResult {
    let topic = topic.trim_start_matches('#').to_lowercase();
    let filter = Filter::new().kind(Kind::TextNote).hashtag(topic.clone());
    signed_workload(
        SubscriptionClass::HashtagTopic,
        filter,
        EventBuilder::new(Kind::TextNote, format!("#{topic}")).tags([Tag::hashtag(topic)]),
        signer,
        created_at,
    )
}

/// An exact Hashtree update root: kind 30064, author, `d`, and `l=hashtree`.
pub fn build_hashtree_update(signer: &Keys, tree: &str, created_at: u64) -> WorkloadResult {
    let root_hash = format!("{created_at:064x}");
    let filter = Filter::new()
        .kind(Kind::Custom(HASHTREE_ROOT_KIND))
        .author(signer.public_key())
        .identifier(tree)
        .custom_tag(SingleLetterTag::lowercase(Alphabet::L), HASHTREE_LABEL);
    signed_workload(
        SubscriptionClass::HashtreeUpdate,
        filter,
        EventBuilder::new(Kind::Custom(HASHTREE_ROOT_KIND), root_hash.clone()).tags([
            Tag::identifier(tree),
            custom_tag("l", HASHTREE_LABEL),
            custom_tag("hash", root_hash),
        ]),
        signer,
        created_at,
    )
}

/// A targeted kind-7368 approval/rating selected by its recipient `p` tag.
///
/// The `i=scope` tag models rating and application context while the subscription
/// remains recipient-targeted, as in the join-approval production path.
pub fn build_targeted_approval_rating(
    signer: &Keys,
    recipient: PublicKey,
    scope: &str,
    created_at: u64,
) -> WorkloadResult {
    let filter = Filter::new()
        .kind(Kind::Custom(TARGETED_APPROVAL_RATING_KIND))
        .pubkey(recipient);
    signed_workload(
        SubscriptionClass::TargetedApprovalRating,
        filter,
        EventBuilder::new(Kind::Custom(TARGETED_APPROVAL_RATING_KIND), "").tags([
            Tag::public_key(recipient),
            custom_tag("i", scope),
            custom_tag("type", "approval-or-rating"),
        ]),
        signer,
        created_at,
    )
}

/// An Iris Drive kind-30078 root selected only by its `d` scope.
///
/// The missing author constraint is intentional: roster changes can authorize a
/// different application key, and Iris Drive performs that authorization after
/// broad event delivery.
pub fn build_iris_drive_broad_root(
    signer: &Keys,
    root_scope: &str,
    created_at: u64,
) -> WorkloadResult {
    let filter = Filter::new()
        .kind(Kind::Custom(IRIS_DRIVE_ROOT_KIND))
        .identifier(root_scope);
    signed_workload(
        SubscriptionClass::IrisDriveBroadRoot,
        filter,
        EventBuilder::new(
            Kind::Custom(IRIS_DRIVE_ROOT_KIND),
            format!("iris-drive-root:{root_scope}:{created_at}"),
        )
        .tags([Tag::identifier(root_scope)]),
        signer,
        created_at,
    )
}

/// A FIPS peer advert selected by kind 37195 and its application-specific `d` tag.
pub fn build_fips_advert(signer: &Keys, advert_scope: &str, created_at: u64) -> WorkloadResult {
    let filter = Filter::new()
        .kind(Kind::Custom(FIPS_ADVERT_KIND))
        .identifier(advert_scope);
    signed_workload(
        SubscriptionClass::FipsAdvert,
        filter,
        EventBuilder::new(
            Kind::Custom(FIPS_ADVERT_KIND),
            format!("fips-advert:{advert_scope}:{created_at}"),
        )
        .tags([
            Tag::identifier(advert_scope),
            custom_tag("protocol", advert_scope),
            custom_tag("version", "1"),
        ]),
        signer,
        created_at,
    )
}

/// A nostr-vpn paid-exit offer discovered through the production kind-wide filter.
///
/// The broad filter is intentional: production discovery selects kind 37196 and
/// applies its optional retention window and limit separately. Signed offers use
/// `d`, `app`, and `v` plus service/payment metadata for validation after delivery.
pub fn build_fips_paid_offer(signer: &Keys, offer_id: &str, created_at: u64) -> WorkloadResult {
    let filter = Filter::new().kind(Kind::Custom(FIPS_PAID_OFFER_KIND));
    signed_workload(
        SubscriptionClass::FipsPaidOffer,
        filter,
        EventBuilder::new(
            Kind::Custom(FIPS_PAID_OFFER_KIND),
            format!(r#"{{"offer_id":"{offer_id}","service":"internet_exit"}}"#),
        )
        .tags([
            Tag::identifier(offer_id),
            custom_tag("app", FIPS_PAID_OFFER_APP),
            custom_tag("v", FIPS_PAID_OFFER_VERSION),
            custom_tag("service", "internet_exit"),
            custom_tag("payment", "cashu_spilman"),
            custom_tag("meter", "bytes"),
        ]),
        signer,
        created_at,
    )
}

/// A NIP-34 repository announcement selected by kind, owner, and repository `d` tag.
pub fn build_git_repo_announcement(
    signer: &Keys,
    repo_name: &str,
    created_at: u64,
) -> WorkloadResult {
    let filter = Filter::new()
        .kind(Kind::Custom(GIT_REPO_ANNOUNCEMENT_KIND))
        .author(signer.public_key())
        .identifier(repo_name)
        .limit(50);
    signed_workload(
        SubscriptionClass::GitRepoAnnouncement,
        filter,
        EventBuilder::new(Kind::Custom(GIT_REPO_ANNOUNCEMENT_KIND), "").tags([
            Tag::identifier(repo_name),
            custom_tag("name", repo_name),
            custom_tag(
                "clone",
                format!("htree://{}/{repo_name}", signer.public_key()),
            ),
        ]),
        signer,
        created_at,
    )
}

fn signed_workload(
    class: SubscriptionClass,
    filter: Filter,
    builder: EventBuilder,
    signer: &Keys,
    created_at: u64,
) -> WorkloadResult {
    let event = builder
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(signer)?;
    Ok(SubscriptionWorkload {
        class,
        filter,
        event,
    })
}

fn custom_tag(name: &str, value: impl Into<String>) -> Tag {
    Tag::custom(TagKind::custom(name), [value.into()])
}

#[cfg(test)]
mod tests {
    use nostr_pubsub::{PubsubPeerInterest, VerifiedEvent};

    use super::*;

    #[test]
    fn every_production_shape_matches_its_own_verified_event() {
        let signer = test_keys(1);
        let recipient = test_keys(2).public_key();
        let workloads = [
            build_author_feed(&signer, 1).unwrap(),
            build_hashtag_topic(&signer, "#Nostr", 2).unwrap(),
            build_hashtree_update(&signer, "iris/releases", 3).unwrap(),
            build_targeted_approval_rating(&signer, recipient, "fips.peer", 4).unwrap(),
            build_iris_drive_broad_root(&signer, "iris-drive/profile/drive/root", 5).unwrap(),
            build_fips_advert(&signer, "nvpn", 6).unwrap(),
            build_fips_paid_offer(&signer, "paid-exit-fi", 7).unwrap(),
            build_git_repo_announcement(&signer, "iris", 8).unwrap(),
        ];

        assert_eq!(
            workloads
                .iter()
                .map(|workload| workload.class)
                .collect::<Vec<_>>(),
            SubscriptionClass::ALL
        );
        for workload in workloads {
            assert_interest(
                &workload.filter,
                workload.event,
                PubsubPeerInterest::Subscribed,
            );
        }
    }

    #[test]
    fn author_feed_rejects_another_author() {
        let interested_author = test_keys(1);
        let other_author = test_keys(2);
        let workload = build_author_feed(&interested_author, 1).unwrap();
        let near_miss = build_author_feed(&other_author, 1).unwrap();

        assert_interest(
            &workload.filter,
            near_miss.event,
            PubsubPeerInterest::Unsubscribed,
        );
    }

    #[test]
    fn hashtag_topic_rejects_another_topic() {
        let signer = test_keys(1);
        let workload = build_hashtag_topic(&signer, "nostr", 1).unwrap();
        let near_miss = build_hashtag_topic(&signer, "spam", 2).unwrap();

        assert_interest(
            &workload.filter,
            near_miss.event,
            PubsubPeerInterest::Unsubscribed,
        );
    }

    #[test]
    fn hashtree_update_requires_author_tree_and_label() {
        let signer = test_keys(1);
        let other_signer = test_keys(2);
        let workload = build_hashtree_update(&signer, "iris/releases", 1).unwrap();
        let wrong_author = build_hashtree_update(&other_signer, "iris/releases", 2).unwrap();
        let wrong_tree = build_hashtree_update(&signer, "other/releases", 3).unwrap();
        let missing_label = signed_event(
            &signer,
            Kind::Custom(HASHTREE_ROOT_KIND),
            [Tag::identifier("iris/releases")],
            4,
        );

        for event in [wrong_author.event, wrong_tree.event, missing_label] {
            assert_interest(&workload.filter, event, PubsubPeerInterest::Unsubscribed);
        }
    }

    #[test]
    fn targeted_approval_rejects_another_recipient() {
        let signer = test_keys(1);
        let recipient = test_keys(2).public_key();
        let other_recipient = test_keys(3).public_key();
        let workload = build_targeted_approval_rating(&signer, recipient, "fips.peer", 1).unwrap();
        let near_miss =
            build_targeted_approval_rating(&signer, other_recipient, "fips.peer", 2).unwrap();

        assert_interest(
            &workload.filter,
            near_miss.event,
            PubsubPeerInterest::Unsubscribed,
        );
    }

    #[test]
    fn iris_drive_root_is_broad_across_authors_but_exact_on_scope() {
        let signer = test_keys(1);
        let newly_authorized_signer = test_keys(2);
        let workload =
            build_iris_drive_broad_root(&signer, "iris-drive/profile/drive/root", 1).unwrap();
        let same_scope = build_iris_drive_broad_root(
            &newly_authorized_signer,
            "iris-drive/profile/drive/root",
            2,
        )
        .unwrap();
        let wrong_scope =
            build_iris_drive_broad_root(&signer, "iris-drive/other/drive/root", 3).unwrap();

        assert_interest(
            &workload.filter,
            same_scope.event,
            PubsubPeerInterest::Subscribed,
        );
        assert_interest(
            &workload.filter,
            wrong_scope.event,
            PubsubPeerInterest::Unsubscribed,
        );
    }

    #[test]
    fn fips_advert_rejects_another_application_scope() {
        let signer = test_keys(1);
        let workload = build_fips_advert(&signer, "nvpn", 1).unwrap();
        let near_miss = build_fips_advert(&signer, "iris-drive", 2).unwrap();

        assert_interest(
            &workload.filter,
            near_miss.event,
            PubsubPeerInterest::Unsubscribed,
        );
    }

    #[test]
    fn fips_paid_offer_uses_kind_wide_discovery_and_production_tags() {
        let signer = test_keys(1);
        let other_signer = test_keys(2);
        let workload = build_fips_paid_offer(&signer, "paid-exit-fi", 1).unwrap();
        let another_valid_offer = build_fips_paid_offer(&other_signer, "paid-exit-se", 2).unwrap();
        let wrong_kind = signed_event(
            &signer,
            Kind::Custom(FIPS_ADVERT_KIND),
            [Tag::identifier("paid-exit-fi")],
            3,
        );

        assert_interest(
            &workload.filter,
            another_valid_offer.event,
            PubsubPeerInterest::Subscribed,
        );
        assert_interest(
            &workload.filter,
            wrong_kind,
            PubsubPeerInterest::Unsubscribed,
        );
        assert_event_tag(&workload.event, "d", "paid-exit-fi");
        assert_event_tag(&workload.event, "app", FIPS_PAID_OFFER_APP);
        assert_event_tag(&workload.event, "v", FIPS_PAID_OFFER_VERSION);
        assert_event_tag(&workload.event, "payment", "cashu_spilman");
    }

    #[test]
    fn git_repo_announcement_requires_owner_and_repo_identifier() {
        let signer = test_keys(1);
        let other_signer = test_keys(2);
        let workload = build_git_repo_announcement(&signer, "iris", 1).unwrap();
        let wrong_author = build_git_repo_announcement(&other_signer, "iris", 2).unwrap();
        let wrong_repo = build_git_repo_announcement(&signer, "nostr-vpn", 3).unwrap();
        let missing_identifier = signed_event(
            &signer,
            Kind::Custom(GIT_REPO_ANNOUNCEMENT_KIND),
            [custom_tag("name", "iris")],
            4,
        );

        for event in [wrong_author.event, wrong_repo.event, missing_identifier] {
            assert_interest(&workload.filter, event, PubsubPeerInterest::Unsubscribed);
        }
        assert_event_tag(&workload.event, "d", "iris");
        assert_event_tag(&workload.event, "name", "iris");
    }

    fn assert_interest(filter: &Filter, event: Event, expected: PubsubPeerInterest) {
        let event = VerifiedEvent::try_from(event).expect("workload event must verify");
        assert_eq!(
            PubsubPeerInterest::from_filters(std::slice::from_ref(filter), &event),
            expected
        );
    }

    fn signed_event<const N: usize>(
        signer: &Keys,
        kind: Kind,
        tags: [Tag; N],
        created_at: u64,
    ) -> Event {
        EventBuilder::new(kind, "near miss")
            .tags(tags)
            .custom_created_at(Timestamp::from(created_at))
            .sign_with_keys(signer)
            .unwrap()
    }

    fn assert_event_tag(event: &Event, name: &str, value: &str) {
        assert!(event.tags.iter().any(|tag| {
            let parts = tag.as_slice();
            parts.first().is_some_and(|part| part == name)
                && parts.get(1).is_some_and(|part| part == value)
        }));
    }

    fn test_keys(value: u8) -> Keys {
        Keys::parse(&format!("{value:064x}")).unwrap()
    }
}
