//! Root-cause analysis for captured sync traces (see DEBUGGING.md).
//!
//! Usage:
//!   trace_rca <trace.jsonl>                       replay one trace
//!   trace_rca <server.jsonl> <client.jsonl>       replay both + cross-diff
//!   flags: --json (machine-readable), --emit-test <out.rs> (write a repro test)

use anyhow::{Context, Result, bail};
use common::replay::{ClientReplay, ServerReplay, diff_traces, trace_side};
use common::trace::{TraceSide, read_trace};
use std::path::{Path, PathBuf};

struct Args {
    traces: Vec<PathBuf>,
    json: bool,
    emit_test: Option<PathBuf>,
}

fn parse_args() -> Result<Args> {
    let mut traces = Vec::new();
    let mut json = false;
    let mut emit_test = None;

    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--emit-test" => {
                let path = iter.next().context("--emit-test requires an output path")?;
                emit_test = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other if other.starts_with("--") => bail!("Unknown flag: {}", other),
            path => traces.push(PathBuf::from(path)),
        }
    }

    if traces.is_empty() || traces.len() > 2 {
        print_usage();
        bail!("Expected one or two trace paths, got {}", traces.len());
    }

    Ok(Args {
        traces,
        json,
        emit_test,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: trace_rca <server_trace.jsonl> [<client_trace.jsonl>] [--json] [--emit-test <out.rs>]\n\
         Replays captured sync traces deterministically through the real game engine,\n\
         reports the first divergence, and cross-diffs server vs client perspectives.\n\
         See DEBUGGING.md for the full workflow."
    );
}

fn main() -> Result<()> {
    let args = parse_args()?;

    let mut loaded = Vec::new();
    for path in &args.traces {
        let records =
            read_trace(path).with_context(|| format!("Failed to read trace {}", path.display()))?;
        let side = trace_side(&records)
            .with_context(|| format!("{} has no Meta record", path.display()))?;
        loaded.push((path.clone(), side, records));
    }

    // Order as (server, client) when both are present.
    loaded.sort_by_key(|(_, side, _)| match side {
        TraceSide::Server => 0,
        TraceSide::Client => 1,
    });
    if loaded.len() == 2 && loaded[0].1 == loaded[1].1 {
        bail!(
            "Both traces are {:?}-side; need one server and one client trace to diff",
            loaded[0].1
        );
    }

    let mut json_out = serde_json::Map::new();

    for (path, side, records) in &loaded {
        match side {
            TraceSide::Server => {
                let outcome = ServerReplay::from_records(records.clone())?
                    .replay()
                    .with_context(|| format!("Server replay of {} failed", path.display()))?;
                if args.json {
                    json_out.insert("server_replay".into(), serde_json::to_value(&outcome)?);
                } else {
                    println!("=== Server replay: {} ===", path.display());
                    println!("{}", outcome.render());
                    println!();
                }
            }
            TraceSide::Client => {
                let outcome = ClientReplay::from_records(records.clone())?
                    .replay()
                    .with_context(|| format!("Client replay of {} failed", path.display()))?;
                if args.json {
                    json_out.insert("client_replay".into(), serde_json::to_value(&outcome)?);
                } else {
                    println!("=== Client replay: {} ===", path.display());
                    println!("{}", outcome.render());
                    println!();
                }
            }
        }
    }

    if loaded.len() == 2 {
        let report = diff_traces(&loaded[0].2, &loaded[1].2);
        if args.json {
            json_out.insert("diff".into(), serde_json::to_value(&report)?);
            json_out.insert(
                "verdict".into(),
                serde_json::Value::String(report.verdict()),
            );
        } else {
            println!("=== Cross-diff ===");
            println!("{}", report.render());
        }
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&json_out)?);
    }

    if let Some(out) = &args.emit_test {
        emit_repro_test(out, &loaded)?;
        eprintln!(
            "Wrote repro test to {} — move the trace file(s) into the repo (e.g. server/tests/fixtures/) \
             and adjust the embedded paths before committing.",
            out.display()
        );
    }

    Ok(())
}

/// Write a #[test] that replays the trace(s) and asserts on today's outcome:
/// a deterministic trace asserts it stays deterministic; a divergent trace
/// asserts the divergence (so the test starts red and goes green with the fix,
/// at which point the assertion should be flipped to lock in determinism).
fn emit_repro_test(
    out: &Path,
    loaded: &[(PathBuf, TraceSide, Vec<common::trace::TraceRecord>)],
) -> Result<()> {
    let mut body = String::from(
        "// Auto-generated by trace_rca --emit-test. Reproduces a captured sync trace\n\
         // as a local test. See DEBUGGING.md (\"Freeze it as a test\").\n\n",
    );

    for (path, side, records) in loaded {
        let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.clone());
        match side {
            TraceSide::Server => {
                let outcome = ServerReplay::from_records(records.clone())?.replay()?;
                let assertion = if outcome.deterministic {
                    "assert!(outcome.deterministic, \"engine replay diverged from recorded trace: {:?}\", outcome.first_divergence);".to_string()
                } else {
                    let div = outcome
                        .first_divergence
                        .as_ref()
                        .map(|d| format!("tick {} ({})", d.tick, d.kind))
                        .unwrap_or_else(|| "unknown".into());
                    format!(
                        "// Captured divergence: {div}. Flip this assertion once the bug is fixed.\n    \
                         assert!(!outcome.deterministic, \"divergence no longer reproduces — fix confirmed; flip this assertion to lock it in\");"
                    )
                };
                body.push_str(&format!(
                    "#[test]\nfn replay_server_trace_reproduces() {{\n    \
                     let replay = common::replay::ServerReplay::from_file({:?}).unwrap();\n    \
                     let outcome = replay.replay().unwrap();\n    {}\n}}\n\n",
                    abs, assertion
                ));
            }
            TraceSide::Client => {
                let outcome = ClientReplay::from_records(records.clone())?.replay()?;
                let assertion = if outcome.reproduces {
                    "assert!(outcome.reproduces, \"client replay diverged from recorded trace: {:?}\", outcome.first_divergence);".to_string()
                } else {
                    "assert!(!outcome.reproduces, \"divergence no longer reproduces — fix confirmed; flip this assertion to lock it in\");".to_string()
                };
                body.push_str(&format!(
                    "#[test]\nfn replay_client_trace_reproduces() {{\n    \
                     let replay = common::replay::ClientReplay::from_file({:?}).unwrap();\n    \
                     let outcome = replay.replay().unwrap();\n    {}\n}}\n\n",
                    abs, assertion
                ));
            }
        }
    }

    std::fs::write(out, body).with_context(|| format!("Failed to write {}", out.display()))?;
    Ok(())
}
