#![cfg(feature = "cashu-integration")]

use std::str::FromStr;
use std::sync::Arc;

use cashu_credit::{
    AcceptanceMode, AccountPolicy, BackedCreditSettlement, BackingDeposit, CreditAccount,
    ExternalSettlementRequest, IssuerPolicy, ReceiptApplication, ServiceReceiptClaim, ValueClass,
};
use cashu_service::simulation::{
    InvoiceStatus, IssuerMode, LocalMint, OrchestratorFunding, PaymentNetwork, VirtualClock,
};
use cashu_service::{
    CashuIssuerRoute, CreditAccountStore, create_topup_quote, execute_cashu_settlement,
    load_mint_balance, open_wallet_repository, receive_payment_token, send_lightning_payment,
    send_payment_token,
};
use cdk::mint_url::MintUrl;
use cdk::nuts::{CurrencyUnit, MintQuoteState, SecretKey, Token};
use nostr_pubsub_sim::{
    NodeRole, PeerSelectionMode, SimulationConfig, SimulationReport, VerifiedDeliveryRecord,
    run_simulation,
};

const START_TIME: u64 = 1_700_000_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_cashu_proofs_reject_forgery_and_replay() {
    let temp = tempfile::tempdir().expect("create isolated Cashu state");
    let sender = temp.path().join("sender");
    let forged_receiver = temp.path().join("forged-receiver");
    let paid_receiver = temp.path().join("paid-receiver");
    let replay_receiver = temp.path().join("replay-receiver");
    let clock = Arc::new(VirtualClock::new(START_TIME));
    let network = PaymentNetwork::new(42, 1, clock);
    let funding = network.orchestrator_funding();
    let mint = LocalMint::start(
        temp.path(),
        network.clone(),
        "pubsub-closed-loop",
        IssuerMode::ClosedLoop,
    )
    .await
    .expect("start real CDK mint with simulated Lightning");

    let _ = fund_wallet(&sender, &mint, &network, &funding, 16).await;
    let payment = send_payment_token(&sender, mint.url(), 8)
        .await
        .expect("create real Cashu payment token");

    let forged = forge_proof_signature(&payment.token);
    let forged_error = receive_payment_token(&forged_receiver, &forged)
        .await
        .expect_err("mint must reject a syntactically valid token with a forged signature");
    assert!(
        forged_error
            .to_string()
            .contains("Failed to receive Cashu payment token")
    );
    assert_balance(&forged_receiver, mint.url(), 0).await;

    let received = receive_payment_token(&paid_receiver, &payment.token)
        .await
        .expect("mint must accept the untouched proof once");
    assert_eq!(received.amount_sat, 8);
    assert_balance(&paid_receiver, mint.url(), 8).await;

    let replay_error = receive_payment_token(&replay_receiver, &payment.token)
        .await
        .expect_err("mint must reject an already-spent proof");
    assert!(
        replay_error
            .to_string()
            .contains("Failed to receive Cashu payment token")
    );
    assert_balance(&replay_receiver, mint.url(), 0).await;
    assert_balance(&sender, mint.url(), 8).await;

    let accounting = network.accounting().expect("read simulated reserves");
    assert_eq!(accounting.mint_reserve("pubsub-closed-loop"), Some(16));
    assert_eq!(accounting.fee_sink_sat, 0);
    assert!(accounting.is_conserved());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_delivery_credit_only_cashouts_through_real_withdrawable_backing_once() {
    let report = small_adversarial_report();
    let (first, second) = two_verified_services_from_one_provider(&report);
    let counterparty = format!("node:{}", first.provider);
    let payout_sat = first.payload_bytes.div_ceil(128).clamp(4, 16);
    let scenario = SettlementScenario::start(payout_sat).await;
    let mut account =
        accept_bounded_verified_credit(&report, first, second, &scenario, &counterparty);
    bind_backing_once(&mut account, &scenario, &counterparty);
    let authorization = authorize_exact_cashout(&mut account, first, &scenario, &counterparty);
    let transfer = scenario.execute_once(&authorization).await;
    scenario
        .deliver_once_and_compare_mint_modes(&mut account, &authorization, &transfer)
        .await;

    let reserve = account.sat_reserve(scenario.issuer()).unwrap();
    assert_eq!(reserve.total_deposited_sat(), scenario.credit_cap_sat);
    assert_eq!(reserve.settled_external_sat(), scenario.credit_cap_sat);
    assert_eq!(reserve.conserved_sat().unwrap(), scenario.credit_cap_sat);
    let final_accounting = scenario.network.accounting().expect("read final reserves");
    assert!(final_accounting.is_conserved());
    assert_eq!(
        final_accounting.total_accounted_sat,
        final_accounting.external_funding_sat
    );
}

struct SettlementScenario {
    temp: tempfile::TempDir,
    payer_wallet: std::path::PathBuf,
    provider_wallet: std::path::PathBuf,
    replay_wallet: std::path::PathBuf,
    closed_loop_wallet: std::path::PathBuf,
    network: PaymentNetwork,
    source: LocalMint,
    provider_mint: LocalMint,
    closed_loop: LocalMint,
    backing_id: String,
    payout_sat: u64,
    fee_sat: u64,
    credit_cap_sat: u64,
}

impl SettlementScenario {
    async fn start(payout_sat: u64) -> Self {
        let fee_sat = 1;
        let credit_cap_sat = payout_sat + fee_sat;
        let temp = tempfile::tempdir().expect("create isolated settlement state");
        let payer_wallet = temp.path().join("payer-wallet");
        let provider_wallet = temp.path().join("provider-wallet");
        let replay_wallet = temp.path().join("replay-wallet");
        let closed_loop_wallet = temp.path().join("closed-loop-wallet");
        let clock = Arc::new(VirtualClock::new(START_TIME));
        let network = PaymentNetwork::new(77, fee_sat, clock);
        let funding = network.orchestrator_funding();
        let source = start_mint(&temp, &network, "reserve-backed-source", true).await;
        let provider_mint = start_mint(&temp, &network, "provider-withdrawable", true).await;
        let closed_loop = start_mint(&temp, &network, "service-only", false).await;
        let backing_id =
            fund_wallet(&payer_wallet, &source, &network, &funding, credit_cap_sat).await;
        let _ = fund_wallet(&closed_loop_wallet, &closed_loop, &network, &funding, 4).await;
        Self {
            temp,
            payer_wallet,
            provider_wallet,
            replay_wallet,
            closed_loop_wallet,
            network,
            source,
            provider_mint,
            closed_loop,
            backing_id,
            payout_sat,
            fee_sat,
            credit_cap_sat,
        }
    }

    fn issuer(&self) -> &str {
        self.source.url()
    }

    async fn execute_once(
        &self,
        authorization: &cashu_credit::ExternalSettlementAuthorization,
    ) -> cashu_service::CashuCrossMintTransfer {
        let route = CashuIssuerRoute {
            issuer: self.issuer().to_string(),
            source_mint_url: self.source.url().to_string(),
        };
        let transfer =
            execute_cashu_settlement(&self.payer_wallet, authorization, &route, START_TIME + 10)
                .await
                .expect("execute the one authorized CDK route");
        let paid_once = self.network.accounting().expect("read settlement reserves");
        let replay =
            execute_cashu_settlement(&self.payer_wallet, authorization, &route, START_TIME + 121)
                .await
                .expect("expired retry resumes the durable transfer instead of paying twice");
        assert_eq!(replay, transfer);
        assert_eq!(self.network.accounting().unwrap(), paid_once);
        transfer
    }

    async fn deliver_once_and_compare_mint_modes(
        &self,
        account: &mut CreditAccount,
        authorization: &cashu_credit::ExternalSettlementAuthorization,
        transfer: &cashu_service::CashuCrossMintTransfer,
    ) {
        let payout = send_payment_token(
            &self.payer_wallet,
            self.provider_mint.url(),
            self.payout_sat,
        )
        .await
        .expect("create the exact journalable provider payout token");
        let received = receive_payment_token(&self.provider_wallet, &payout.token)
            .await
            .expect("provider accepts genuine proofs once");
        assert_eq!(received.amount_sat, self.payout_sat);
        assert!(
            receive_payment_token(&self.replay_wallet, &payout.token)
                .await
                .is_err(),
            "spent bearer proofs must not fund a second payout"
        );
        account
            .complete_external_settlement(&authorization.settlement_id, transfer.fee_paid_sat)
            .expect("complete exact reserved principal and actual fee");
        account
            .complete_external_settlement(&authorization.settlement_id, transfer.fee_paid_sat)
            .expect("completion replay is idempotent");
        self.compare_cashout_modes().await;
    }

    async fn compare_cashout_modes(&self) {
        let closed_invoice = create_topup_quote(
            &self.temp.path().join("closed-loop-cashout-target"),
            self.source.url(),
            1,
        )
        .await
        .expect("create a fake-Lightning withdrawal target");
        assert!(
            send_lightning_payment(
                &self.closed_loop_wallet,
                self.closed_loop.url(),
                &closed_invoice.payment_request,
            )
            .await
            .is_err(),
            "closed-loop proofs buy issuer service but never cash out"
        );
        let withdrawable = create_topup_quote(
            &self.temp.path().join("withdrawable-cashout-target"),
            self.source.url(),
            1,
        )
        .await
        .expect("create provider's fake-Lightning withdrawal target");
        send_lightning_payment(
            &self.provider_wallet,
            self.provider_mint.url(),
            &withdrawable.payment_request,
        )
        .await
        .expect("withdrawable provider mint pays one real local Lightning invoice");
    }
}

async fn start_mint(
    temp: &tempfile::TempDir,
    network: &PaymentNetwork,
    id: &str,
    withdrawable: bool,
) -> LocalMint {
    let mode = if withdrawable {
        IssuerMode::Withdrawable
    } else {
        IssuerMode::ClosedLoop
    };
    LocalMint::start(temp.path(), network.clone(), id, mode)
        .await
        .unwrap_or_else(|error| panic!("start {id} mint: {error}"))
}

fn accept_bounded_verified_credit(
    report: &SimulationReport,
    first: &VerifiedDeliveryRecord,
    second: &VerifiedDeliveryRecord,
    scenario: &SettlementScenario,
    counterparty: &str,
) -> CreditAccount {
    let mut account = CreditAccount::new(account_policy(
        counterparty,
        scenario.issuer(),
        scenario.credit_cap_sat,
    ))
    .expect("create bounded peer account");
    let revision = account.revision();
    assert!(
        verified_service_claim(
            report,
            "not-an-observed-event",
            first.receiver,
            scenario.issuer(),
            counterparty,
            1,
        )
        .is_err(),
        "an unsigned claim without a production-path delivery fact must not enter accounting"
    );
    assert_eq!(account.revision(), revision);

    let service_credit = verified_service_claim(
        report,
        &first.event_id,
        first.receiver,
        scenario.issuer(),
        counterparty,
        scenario.credit_cap_sat,
    )
    .expect("derive receipt only from a first-accepted interested delivery");
    assert_eq!(
        account
            .apply_receipt(
                &service_credit,
                scenario.issuer(),
                AcceptanceMode::OfflineDeferred,
                START_TIME + 10,
            )
            .expect("accept credit within the configured relationship cap"),
        ReceiptApplication::Applied
    );
    assert_eq!(
        account
            .apply_receipt(
                &service_credit,
                scenario.issuer(),
                AcceptanceMode::OfflineDeferred,
                START_TIME + 10,
            )
            .expect("exact receipt replay is idempotent"),
        ReceiptApplication::AlreadyApplied
    );
    assert_eq!(account.total_peer_credit_sat(), scenario.credit_cap_sat);

    let over_cap = verified_service_claim(
        report,
        &second.event_id,
        second.receiver,
        scenario.issuer(),
        counterparty,
        1,
    )
    .expect("second receipt also has an observed delivery fact");
    assert!(
        account
            .apply_receipt(
                &over_cap,
                scenario.issuer(),
                AcceptanceMode::OfflineDeferred,
                START_TIME + 10,
            )
            .is_err(),
        "the next service unit must require backed payment at the credit cap"
    );
    assert_eq!(account.total_peer_credit_sat(), scenario.credit_cap_sat);
    account
}

fn bind_backing_once(
    account: &mut CreditAccount,
    scenario: &SettlementScenario,
    counterparty: &str,
) {
    let backing = BackingDeposit {
        deposit_id: format!("mint-quote:{}", scenario.backing_id),
        issuer: scenario.issuer().to_string(),
        amount_sat: scenario.credit_cap_sat,
        value_class: ValueClass::ReserveBackedWithdrawable,
    };
    assert_eq!(
        account
            .record_backing_deposit(&backing, scenario.issuer())
            .expect("bind the verified mint quote once"),
        ReceiptApplication::Applied
    );
    assert_eq!(
        account
            .record_backing_deposit(&backing, scenario.issuer())
            .expect("same-account backing replay is idempotent"),
        ReceiptApplication::AlreadyApplied
    );

    let mut store = CreditAccountStore::open(scenario.temp.path().join("credit.sqlite3"))
        .expect("open global backing-claim store");
    store
        .create("provider-account", account)
        .expect("persist the first backing owner");
    let mut attacker = CreditAccount::new(account_policy(
        "attacker",
        scenario.issuer(),
        scenario.credit_cap_sat,
    ))
    .unwrap();
    attacker
        .record_backing_deposit(&backing, scenario.issuer())
        .expect("an isolated account cannot see the global claim before persistence");
    assert!(
        store.create("attacker-account", &attacker).is_err(),
        "one mint quote must never back two persisted accounts"
    );

    account
        .settle_peer_credit_with_backing(
            &BackedCreditSettlement {
                settlement_id: format!("back-{}", scenario.backing_id),
                counterparty: counterparty.to_string(),
                from_issuer: scenario.issuer().to_string(),
                backing_issuer: scenario.issuer().to_string(),
                amount_sat: scenario.credit_cap_sat,
                value_class: ValueClass::ReserveBackedWithdrawable,
                expires_at_unix: START_TIME + 120,
            },
            scenario.issuer(),
            START_TIME + 10,
        )
        .expect("replace bounded peer credit with verified withdrawable backing");
    assert_eq!(account.total_peer_credit_sat(), 0);
}

fn authorize_exact_cashout(
    account: &mut CreditAccount,
    first: &VerifiedDeliveryRecord,
    scenario: &SettlementScenario,
    counterparty: &str,
) -> cashu_credit::ExternalSettlementAuthorization {
    let request = ExternalSettlementRequest {
        settlement_id: format!("cashout-{}-{}", first.event_id, first.receiver),
        issuer: scenario.issuer().to_string(),
        counterparty: counterparty.to_string(),
        payout_destination: scenario.provider_mint.url().to_string(),
        amount_sat: scenario.payout_sat,
        max_fee_sat: scenario.fee_sat,
        expires_at_unix: START_TIME + 120,
    };
    let authorization = account
        .authorize_external_settlement(&request, counterparty, START_TIME + 10)
        .expect("reserve exact principal and fee only after backing exists");
    let mut redirected = request.clone();
    redirected.payout_destination = scenario.closed_loop.url().to_string();
    assert!(
        account
            .authorize_external_settlement(&redirected, counterparty, START_TIME + 10)
            .is_err(),
        "a stable settlement id must not redirect its payout"
    );
    authorization
}

async fn fund_wallet(
    data_dir: &std::path::Path,
    mint: &LocalMint,
    network: &PaymentNetwork,
    funding: &OrchestratorFunding,
    amount_sat: u64,
) -> String {
    let quote = create_topup_quote(data_dir, mint.url(), amount_sat)
        .await
        .expect("create real CDK mint quote");
    assert_eq!(
        network
            .invoice(&quote.payment_request)
            .expect("find fake-Lightning invoice")
            .status,
        InvoiceStatus::Unpaid
    );
    funding
        .settle_external(&quote.payment_request)
        .expect("settle through orchestrator-only fake Lightning");

    let repository = open_wallet_repository(data_dir)
        .await
        .expect("open sender wallet");
    let mint_url = MintUrl::from_str(mint.url()).expect("parse local mint URL");
    let wallet = repository
        .get_wallet(&mint_url, &CurrencyUnit::Sat)
        .await
        .expect("load sender wallet");
    let status = wallet
        .check_mint_quote_status(&quote.quote_id)
        .await
        .expect("check funded quote");
    assert_eq!(status.state, MintQuoteState::Paid);
    wallet
        .mint(&quote.quote_id, cdk::amount::SplitTarget::default(), None)
        .await
        .expect("mint genuine proofs");
    quote.quote_id
}

fn small_adversarial_report() -> SimulationReport {
    run_simulation(
        SimulationConfig {
            node_count: 48,
            attacker_count: 8,
            fanout: 4,
            fake_inventories_per_attack_link: 2,
            signed_spam_rounds: 1,
            legitimate_publication_rounds: 4,
            supernode_count: 4,
            adversarial_discovery_candidate_count: 2,
            supernode_links_per_peer: 2,
            supernode_fanout: 24,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::SharedReputation,
    )
    .expect("small production-path adversarial simulation must complete")
}

fn two_verified_services_from_one_provider(
    report: &SimulationReport,
) -> (&VerifiedDeliveryRecord, &VerifiedDeliveryRecord) {
    for first in report.verified_delivery_records.iter().filter(|record| {
        record.final_interested_delivery && report.node_roles[record.provider] != NodeRole::Attacker
    }) {
        if let Some(second) = report.verified_delivery_records.iter().find(|record| {
            record.final_interested_delivery
                && record.provider == first.provider
                && (record.event_id != first.event_id || record.receiver != first.receiver)
        }) {
            return (first, second);
        }
    }
    panic!("simulation must produce two verified interested services from one honest provider");
}

fn verified_service_claim(
    report: &SimulationReport,
    event_id: &str,
    receiver: usize,
    issuer: &str,
    counterparty: &str,
    amount_sat: u64,
) -> Result<ServiceReceiptClaim, String> {
    let record = report
        .verified_delivery_records
        .iter()
        .find(|record| {
            record.event_id == event_id
                && record.receiver == receiver
                && record.final_interested_delivery
        })
        .ok_or_else(|| "no verified interested delivery fact".to_string())?;
    if counterparty != format!("node:{}", record.provider) {
        return Err("receipt counterparty did not provide this delivery".to_string());
    }
    let issued_at_unix = START_TIME.saturating_add(record.accepted_at_ms / 1_000);
    Ok(ServiceReceiptClaim {
        receipt_id: format!("pubsub:{}:{}", record.event_id, record.receiver),
        issuer: issuer.to_string(),
        counterparty: counterparty.to_string(),
        service: "nostr_pubsub_verified_delivery".to_string(),
        resource: format!(
            "nostr:event:{}:receiver:{}",
            record.event_id, record.receiver
        ),
        useful_service_units: record.payload_bytes,
        amount_sat,
        value_class: ValueClass::PeerCredit,
        issued_at_unix,
        expires_at_unix: issued_at_unix.saturating_add(120),
    })
}

fn account_policy(counterparty: &str, issuer: &str, cap_sat: u64) -> AccountPolicy {
    AccountPolicy {
        counterparty: counterparty.to_string(),
        max_total_peer_credit_sat: cap_sat,
        issuers: vec![IssuerPolicy {
            issuer: issuer.to_string(),
            max_peer_credit_sat: cap_sat,
            max_offline_peer_credit_sat: cap_sat,
            max_closed_loop_sat: 0,
            max_withdrawable_sat: cap_sat,
            expires_at_unix: Some(START_TIME + 180),
        }],
    }
}

fn forge_proof_signature(encoded: &str) -> String {
    let mut token = Token::from_str(encoded).expect("parse genuine token before forging it");
    let Token::TokenV4(token_v4) = &mut token else {
        panic!("cashu-service should emit a V4 token");
    };
    let proof = token_v4
        .token
        .first_mut()
        .and_then(|keyset| keyset.proofs.first_mut())
        .expect("payment token must contain a proof");
    proof.c = SecretKey::generate().public_key();
    token.to_string()
}

async fn assert_balance(data_dir: &std::path::Path, mint_url: &str, expected_sat: u64) {
    let balance = load_mint_balance(data_dir, mint_url)
        .await
        .expect("load Cashu wallet balance");
    assert_eq!(balance.balance_sat, expected_sat);
}
