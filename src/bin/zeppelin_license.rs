//! Offline Zeppelin license issuance and verification utility.
//!
//! This binary is the operator- and release-custody-facing front end for the
//! license format defined in `src/security/license.rs`. It has two
//! subcommands: `sign` mints a signed license from a payload and an Ed25519
//! seed, and `verify` re-checks an existing license and prints what it grants.
//!
//! It owns only argument handling, key-file loading, and safe file
//! publication. It deliberately does **not** define the license schema, the
//! canonical signed bytes, or the verification rules — it calls the exact
//! library functions (`canonical_payload_bytes`, `validate_license_payload`,
//! `verify_signed_license_bytes`, `read_key_file`) that the server uses at
//! boot, so an issued license and an accepted license can never diverge.
//!
//! It runs entirely offline. It never contacts object storage, never reads
//! `zeppelin.toml`, and holds no server state.
//!
//! ## The trust anchor is compiled in
//!
//! Verification uses `LICENSE_PUBKEY`, embedded in the library at build time.
//! There is no flag to supply a different trust anchor. Two consequences the
//! operator must internalize:
//!
//! - `sign` derives the public key from the supplied seed and **refuses to
//!   sign** unless it equals the embedded key. This tool cannot mint a license
//!   that its matching server would reject.
//! - The tool and the server it issues for must be built from the same source
//!   revision. Rotating the key is an intentional source change and rebuild,
//!   not a configuration change.
//!
//! The private seed is read from a file, used to sign, and never written
//! anywhere. Custody of that file is outside this program's control.
//!
//! ## Operator contract
//!
//! ```text
//! zeppelin_license sign --payload PAYLOAD.json \
//!                       --private-key ED25519_SEED \
//!                       --output LICENSE.json
//!
//! zeppelin_license verify --license LICENSE.json
//! ```
//!
//! Argument parsing is strict rather than forgiving: the exact set of flags is
//! required, each exactly once, each with a value that does not itself look
//! like a flag. An unknown flag, a duplicate, a missing value, or a wrong
//! count is a usage error, not a silently ignored token.
//!
//! `PAYLOAD.json` is the unsigned payload — customer id and name, issue and
//! expiry timestamps, the explicit feature list, and optional limits. Unknown
//! fields are rejected. Feature names are the stable `snake_case` spellings
//! (`rbac`, `constraints`, `audit_s3`, `delegation`, `preservation`,
//! `audit_streaming`, `cmek`, `branching`).
//!
//! `ED25519_SEED` is a text file holding the 32-byte signing seed as either 64
//! hex characters or unpadded base64url. Surrounding whitespace is trimmed.
//!
//! ## What `sign` does, in order
//!
//! ```text
//!  parse + validate arguments
//!         |
//!  reject: output already exists, or output aliases an input path
//!         |          (canonicalized, so ./a and a are the same file)
//!         v
//!  read PAYLOAD.json  -> LicensePayload (unknown fields rejected)
//!         |
//!  validate_license_payload      schema errors fail BEFORE any signing
//!         |
//!  read seed -> derive public key -> must equal embedded LICENSE_PUBKEY
//!         |
//!  sign canonical payload bytes -> pretty-printed SignedLicense
//!         |
//!  verify_signed_license_bytes(exact output bytes)   self-check
//!         |
//!  write .NAME.<uuid>.tmp (create-new) -> write_all -> sync_all
//!         |
//!  hard_link temp -> LICENSE.json     create-only publish
//!         |
//!  unlink temp
//! ```
//!
//! Three properties of that sequence matter operationally:
//!
//! - **Nothing is published until it has been verified.** The tool verifies the
//!   exact serialized bytes it is about to write, using the same code the
//!   server will use, so a canonicalization or encoding mistake fails here
//!   rather than at a customer's boot.
//! - **Publication never overwrites.** The output must not already exist, and
//!   the final step is a hard link, which fails if the destination appeared in
//!   the meantime. A previously issued license cannot be silently replaced.
//! - **Failures do not leave debris or partial files.** The payload is fully
//!   written and `fsync`ed to a uniquely named temporary file first; if
//!   staging or linking fails, the temporary file is removed and the error is
//!   reported. If cleanup *also* fails, the message names both failures rather
//!   than hiding one.
//!
//! ## What `verify` reports
//!
//! On success it prints one line to stdout:
//!
//! ```text
//! verified customer=<customer_id> expires_at=<RFC3339> features=<csv>
//! ```
//!
//! The feature list is filtered through the library's canonical feature
//! inventory, so it is printed in the same stable order every time and names
//! only what the license actually grants.
//!
//! `verify` proves the signature and the schema. It does **not** compare the
//! expiry against the current time — an expired license verifies successfully
//! and reports its past `expires_at`. That mirrors the server: expiry freezes
//! security *management* after a grace period and never revokes enforcement, so
//! "expired" is information for the operator to read, not a verdict this tool
//! renders.
//!
//! ## Exit behavior
//!
//! - `0` — the requested operation succeeded. For `sign`, the license file now
//!   exists and has been verified. For `verify`, the summary line was printed.
//! - `1` — any failure, printed to stderr as `zeppelin_license: <error>`.
//!   Usage, unreadable input, schema violation, wrong signing key, invalid
//!   signature, an existing output path, and a failed write all land here.
//!
//! There is no partial success and no warning-and-continue path; consistent
//! with the rest of Zeppelin, this tool fails loudly and leaves nothing
//! half-published.
//!
//! ## Where to start reading
//!
//! `run` for dispatch, then `sign` for the issuance pipeline. `write_new_atomic`
//! and `reject_output_aliases` hold the file-safety rules and are worth reading
//! before changing anything about output handling.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use zeppelin::security::{
    canonical_payload_bytes, read_key_file, validate_license_payload, verify_signed_license_bytes,
    LicensePayload, SignedLicense, LICENSE_PUBKEY,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("zeppelin_license: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let Some(command) = arguments.first().map(String::as_str) else {
        return Err(usage().into());
    };
    match command {
        "sign" => sign(&arguments[1..]),
        "verify" => verify(&arguments[1..]),
        _ => Err(usage().into()),
    }
}

fn sign(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    validate_path_arguments(arguments, &["--payload", "--private-key", "--output"])?;
    let payload_path = required_path(arguments, "--payload")?;
    let private_key_path = required_path(arguments, "--private-key")?;
    let output_path = required_path(arguments, "--output")?;
    reject_output_aliases(&output_path, &[&payload_path, &private_key_path])?;

    let payload: LicensePayload = serde_json::from_slice(&std::fs::read(payload_path)?)?;
    validate_license_payload(&payload)?;
    let signing_seed = read_key_file(&private_key_path)?;
    let signing_key = SigningKey::from_bytes(&signing_seed);
    if signing_key.verifying_key().to_bytes() != LICENSE_PUBKEY {
        return Err("private key does not match the public key embedded in this binary".into());
    }
    let signature = signing_key.sign(&canonical_payload_bytes(&payload)?);
    let document = SignedLicense::new(payload, URL_SAFE_NO_PAD.encode(signature.to_bytes()));
    let encoded = serde_json::to_vec_pretty(&document)?;

    // Verify the exact encoded output against the same embedded key the server
    // uses before publishing it.
    verify_signed_license_bytes(&encoded)?;
    write_new_atomic(&output_path, &encoded)?;
    Ok(())
}

fn verify(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    validate_path_arguments(arguments, &["--license"])?;
    let license_path = required_path(arguments, "--license")?;
    let entitlements = verify_signed_license_bytes(&std::fs::read(license_path)?)?;
    let features = zeppelin::security::Feature::ALL
        .into_iter()
        .filter(|feature| entitlements.has(*feature))
        .map(zeppelin::security::Feature::as_str)
        .collect::<Vec<_>>();
    let customer = entitlements
        .customer()
        .ok_or("verified file license did not carry a customer identifier")?;
    let expires_at = entitlements
        .expires_at()
        .ok_or("verified file license did not carry an expiry")?;
    println!(
        "verified customer={} expires_at={} features={}",
        customer.as_str(),
        expires_at.to_rfc3339(),
        features.join(",")
    );
    Ok(())
}

fn validate_path_arguments(
    arguments: &[String],
    expected: &[&'static str],
) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.len() != expected.len() * 2 {
        return Err(usage().into());
    }
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for pair in arguments.chunks_exact(2) {
        let flag = pair[0].as_str();
        if !expected.contains(flag) {
            return Err(format!("unexpected argument {flag}; {}", usage()).into());
        }
        if pair[1].starts_with("--") {
            return Err(format!("missing value for {flag}; {}", usage()).into());
        }
        if !observed.insert(flag) {
            return Err(format!("duplicate {flag}").into());
        }
    }
    if observed != expected {
        return Err(usage().into());
    }
    Ok(())
}

fn reject_output_aliases(
    output: &Path,
    inputs: &[&Path],
) -> Result<(), Box<dyn std::error::Error>> {
    if output.exists() {
        return Err(format!("output already exists: {}", output.display()).into());
    }
    let parent = output.parent().filter(|path| !path.as_os_str().is_empty());
    let parent = std::fs::canonicalize(parent.unwrap_or_else(|| Path::new(".")))?;
    let file_name = output
        .file_name()
        .ok_or_else(|| format!("output path has no file name: {}", output.display()))?;
    let normalized_output = parent.join(file_name);
    for input in inputs {
        if std::fs::canonicalize(input)? == normalized_output {
            return Err(format!("output must not alias input file: {}", input.display()).into());
        }
    }
    Ok(())
}

fn write_new_atomic(output: &Path, body: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let parent = output.parent().filter(|path| !path.as_os_str().is_empty());
    let parent = std::fs::canonicalize(parent.unwrap_or_else(|| Path::new(".")))?;
    let file_name = output
        .file_name()
        .ok_or_else(|| format!("output path has no file name: {}", output.display()))?;
    let temp_path = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        uuid::Uuid::new_v4()
    ));

    let mut temp = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    let staged = temp.write_all(body).and_then(|()| temp.sync_all());
    drop(temp);
    if let Err(error) = staged {
        return match std::fs::remove_file(&temp_path) {
            Ok(()) => Err(error.into()),
            Err(cleanup) => Err(format!(
                "failed to stage license output ({error}); temp cleanup also failed ({cleanup})"
            )
            .into()),
        };
    }

    if let Err(error) = std::fs::hard_link(&temp_path, output) {
        return match std::fs::remove_file(&temp_path) {
            Ok(()) => Err(format!("failed to publish license output: {error}").into()),
            Err(cleanup) => Err(format!(
                "failed to publish license output ({error}); temp cleanup also failed ({cleanup})"
            )
            .into()),
        };
    }
    std::fs::remove_file(temp_path)?;
    Ok(())
}

fn required_path(
    arguments: &[String],
    flag: &'static str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    optional_path(arguments, flag)?.ok_or_else(|| format!("missing {flag}; {}", usage()).into())
}

fn optional_path(
    arguments: &[String],
    flag: &'static str,
) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let mut found = None;
    let mut index = 0;
    while index < arguments.len() {
        if arguments[index] == flag {
            if found.is_some() {
                return Err(format!("duplicate {flag}").into());
            }
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            found = Some(PathBuf::from(value));
            index += 2;
        } else {
            index += 1;
        }
    }
    Ok(found)
}

fn usage() -> &'static str {
    "usage: zeppelin_license sign --payload PAYLOAD.json --private-key ED25519_SEED --output LICENSE.json | zeppelin_license verify --license LICENSE.json"
}
