//! Deployment sizing and onboarding advisor for Zeppelin operators.
//!
//! Ranks cloud hardware candidates for a customer's data shape using the
//! calibrated analytic model in `zeppelin::sizing`, and emits a tuned,
//! validated `zeppelin.toml` for the selected configuration.
//!
//! Subcommands land incrementally: `plan` and `catalog` arrive with the
//! embedded cloud catalog, `emit-config` with the tuner. Until then this
//! binary only reports its own usage.

use std::process::ExitCode;

const USAGE: &str = "\
zeppelin_advisor — deployment sizing and onboarding advisor

USAGE:
    zeppelin_advisor <SUBCOMMAND> [FLAGS]

SUBCOMMANDS (landing incrementally):
    plan          Rank viable node configurations for a data shape
    catalog       Print the embedded cloud catalog snapshot
    emit-config   Generate a tuned, validated zeppelin.toml

The sizing model itself is available today via zeppelin::sizing.
";

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("plan" | "catalog" | "emit-config") => {
            eprintln!(
                "zeppelin_advisor: `{}` is not implemented yet; the sizing \
                 engine (zeppelin::sizing) landed first and subcommands \
                 follow in upcoming phases",
                args[0]
            );
            ExitCode::FAILURE
        }
        Some(other) => {
            eprintln!("zeppelin_advisor: unknown subcommand {other:?}\n\n{USAGE}");
            ExitCode::FAILURE
        }
        None => {
            eprintln!("{USAGE}");
            ExitCode::FAILURE
        }
    }
}
