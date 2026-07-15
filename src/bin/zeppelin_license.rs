//! Offline Zeppelin license issuance and verification utility.

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
