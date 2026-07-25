//! Offline verifier for one node/day durable audit stream.
//!
//! This binary lets an auditor or operator prove, after the fact and without
//! the running server's cooperation, that a day of Zeppelin's durable audit log
//! has not been altered. It selects exactly one `(UTC day, node)` stream, reads
//! it straight from object storage, and prints the verification report.
//!
//! It owns only argument parsing, configuration and store construction, report
//! printing, and the exit-code contract. The verification itself belongs to
//! `verify_audit_day` in `src/security/audit_chain.rs`; this tool adds no
//! independent checking logic, so what it accepts is exactly what the library
//! accepts.
//!
//! ## Where this sits
//!
//! The server's audit runtime writes JSONL audit objects under
//! `_audit/<YYYY-MM-DD>/<node-id>/` in the same bucket the engine uses for
//! namespaces, seals the day with a create-only terminal object, and publishes
//! a signed day anchor at `_audit/anchors/<YYYY-MM-DD>/<node-id>.json`. This
//! tool is the read-only counterpart, reached through the normal storage layer:
//!
//! ```text
//! zeppelin.toml ──> Config ──> ZeppelinStore  (S3 / MinIO; authoritative)
//!                                    |
//!                     LIST _audit/<day>/<node>/ , then GET every object
//!                                    |
//!                     GET _audit/anchors/<day>/<node>.json
//!                     GET _security/signers/          (anchor signer keys)
//!                                    v
//!                            verify_audit_day
//!                    replay hash chain, check terminal seal,
//!                    check signed day anchor (count + tail hash)
//!                                    |
//!                                    v
//!                  AuditChainVerification  (pretty JSON on stdout)
//! ```
//!
//! It reads only. It creates, modifies, and deletes nothing, so it is safe to
//! run against a live production bucket. It needs credentials that can LIST and
//! GET under `_audit/` and GET the published signer inventory under
//! `_security/signers/`; it uses whatever storage configuration
//! `zeppelin.toml` names, so pointing it at a read-only role is the intended
//! way to run it.
//!
//! ## Operator contract
//!
//! ```text
//! zeppelin_audit_verify --config zeppelin.toml \
//!                       --day 2026-07-24 \
//!                       --node <node-id>
//! ```
//!
//! All three flags are required; `--day` is a UTC calendar date in
//! `YYYY-MM-DD`. Streams are per node as well as per day, because each server
//! process writes its own independently sealed chain — verifying a deployment
//! for a day means running this once per node that was writing audit that day.
//! The node id is the audit writer identity recorded in the audit records
//! themselves and reported by the server at boot.
//!
//! ## Reading the report
//!
//! The pretty-printed JSON report is written to stdout in every case where
//! verification actually ran, including failure. `valid` is true only when the
//! records, the terminal seal, and the signed anchor all agree.
//! When it is false, `first_divergence` names the first problem in object-key
//! then line order, `verified_records` says how many records were accepted
//! before it, and `object_key` and `line_index` locate it precisely. The
//! divergence kinds distinguish a broken record chain (`record_decode`,
//! `record_identity`, `previous_hash`, `record_position`) from a broken or
//! missing seal (`terminal_seal_missing`, `terminal_seal_invalid`) from an
//! anchor that does not commit to the stream that is present
//! (`anchor_missing`, `anchor_signature`, `anchor_identity`, `anchor_count`,
//! `anchor_last_hash`). That last family is the one that catches wholesale
//! truncation: removing records changes the recomputed count and tail hash,
//! which the signed anchor still commits to.
//!
//! ## Exit behavior
//!
//! Three distinct outcomes, deliberately not collapsed into two:
//!
//! - `0` — verification ran and the stream is intact. The report on stdout has
//!   `"valid": true`.
//! - `2` — verification ran and the stream is **not** intact. The report on
//!   stdout is the evidence; this is a finding, not a tool malfunction.
//! - `1` — verification could not run. A usage error, an unparseable day, an
//!   unreadable configuration, or a storage failure. The reason is printed to
//!   stderr as `zeppelin_audit_verify: <error>`; stdout stays empty.
//!
//! Scripts must treat `1` and `2` differently: `1` means "try again or fix the
//! invocation", while `2` means "escalate". Collapsing them would let an
//! expired credential look exactly like a tampered audit log.
//!
//! One asymmetry is worth knowing before triaging an exit code: an audit
//! *record* that will not decode is reported as the `record_decode` divergence
//! and exits `2`, but a day *anchor* object that will not decode surfaces as a
//! serialization error and exits `1`. A `1` accompanied by an
//! `invalid audit anchor …` message therefore describes damaged evidence, not a
//! misconfigured invocation, and should be escalated like a `2`.
//!
//! ## Where to start reading
//!
//! `run` is the entire flow in a dozen lines; the substance is in
//! `verify_audit_day`. Read `main` for the exit-code mapping above.

use chrono::NaiveDate;
use zeppelin::config::Config;
use zeppelin::security::verify_audit_day;
use zeppelin::storage::ZeppelinStore;

#[tokio::main]
async fn main() {
    match run().await {
        Ok(true) => {}
        Ok(false) => std::process::exit(2),
        Err(error) => {
            eprintln!("zeppelin_audit_verify: {error}");
            std::process::exit(1);
        }
    }
}

async fn run() -> Result<bool, Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 6 {
        return Err(usage().into());
    }
    let config_path = required(&arguments, "--config")?;
    let day = NaiveDate::parse_from_str(required(&arguments, "--day")?, "%Y-%m-%d")?;
    let node_id = required(&arguments, "--node")?;
    let config = Config::load(Some(config_path))?;
    let store = ZeppelinStore::from_config(&config.storage)?;
    let verification = verify_audit_day(&store, day, node_id).await?;
    println!("{}", serde_json::to_string_pretty(&verification)?);
    Ok(verification.valid)
}

fn required<'a>(arguments: &'a [String], flag: &str) -> Result<&'a str, &'static str> {
    arguments
        .chunks_exact(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].as_str())
        .ok_or_else(usage)
}

fn usage() -> &'static str {
    "usage: zeppelin_audit_verify --config <zeppelin.toml> --day <YYYY-MM-DD> --node <node-id>"
}
