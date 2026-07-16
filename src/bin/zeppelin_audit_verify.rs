//! Offline verifier for one node/day durable audit stream.

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
