//! Offline Play-of-the-Game calibration harness.
//!
//! Score existing recordings:
//! `cargo run -p server --bin highlight_tune -- recording-a.json recording-b.json`
//!
//! Generate the deterministic launch corpus and review pack:
//! `cargo run -p server --release --bin highlight_tune -- --bot-corpus-dir docs/qa/play-of-the-game-calibration`

use anyhow::{Context, Result, bail};
use common::{GameRecordingV1, HighlightConfig, score_highlight_candidate};
use serde::Serialize;
use server::highlight_calibration::{
    BotCorpusSpec, DEFAULT_CORPUS_GAME_COUNT, DEFAULT_CORPUS_SEED, DEFAULT_REVIEW_COUNT,
    assert_automatic_acceptance, run_bot_corpus, write_calibration_artifacts,
};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Cli {
    config_path: Option<PathBuf>,
    corpus_dir: Option<PathBuf>,
    games: usize,
    seed: u64,
    review_count: usize,
    recordings: Vec<PathBuf>,
}

impl Cli {
    fn parse() -> Result<Self> {
        let mut args = std::env::args_os().skip(1);
        let mut cli = Self {
            config_path: None,
            corpus_dir: None,
            games: DEFAULT_CORPUS_GAME_COUNT,
            seed: DEFAULT_CORPUS_SEED,
            review_count: DEFAULT_REVIEW_COUNT,
            recordings: Vec::new(),
        };
        while let Some(arg) = args.next() {
            match arg.to_str() {
                Some("--config") => cli.config_path = Some(next_path(&mut args, "--config")?),
                Some("--bot-corpus-dir") => {
                    cli.corpus_dir = Some(next_path(&mut args, "--bot-corpus-dir")?)
                }
                Some("--games") => {
                    cli.games = parse_usize(next_arg(&mut args, "--games")?, "--games")?
                }
                Some("--seed") => {
                    cli.seed = parse_seed(next_arg(&mut args, "--seed")?)?;
                }
                Some("--review-count") => {
                    cli.review_count =
                        parse_usize(next_arg(&mut args, "--review-count")?, "--review-count")?
                }
                Some("--help" | "-h") => {
                    println!("{}", usage());
                    std::process::exit(0);
                }
                Some(flag) if flag.starts_with('-') => bail!("unknown option {flag}\n{}", usage()),
                _ => cli.recordings.push(PathBuf::from(arg)),
            }
        }
        if cli.corpus_dir.is_some() && !cli.recordings.is_empty() {
            bail!("--bot-corpus-dir cannot be combined with recording paths");
        }
        if cli.corpus_dir.is_none() && cli.recordings.is_empty() {
            bail!(
                "pass recording JSON files or --bot-corpus-dir OUTPUT\n{}",
                usage()
            );
        }
        Ok(cli)
    }
}

fn usage() -> &'static str {
    "Usage:\n  highlight_tune [--config CONFIG.json] RECORDING.json...\n  highlight_tune --bot-corpus-dir OUTPUT [--games 200] [--seed U64|0xHEX] [--review-count 20] [--config CONFIG.json]"
}

fn next_arg(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<OsString> {
    args.next().with_context(|| format!("{flag} needs a value"))
}

fn next_path(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(next_arg(args, flag)?))
}

fn parse_usize(value: OsString, flag: &str) -> Result<usize> {
    value
        .to_str()
        .with_context(|| format!("{flag} must be UTF-8"))?
        .parse()
        .with_context(|| format!("{flag} must be a positive integer"))
}

fn parse_seed(value: OsString) -> Result<u64> {
    let value = value.to_str().context("--seed must be UTF-8")?;
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).context("--seed must be a u64 or 0x-prefixed hex u64")
    } else {
        value
            .parse()
            .context("--seed must be a u64 or 0x-prefixed hex u64")
    }
}

fn load_config(path: Option<&Path>) -> Result<HighlightConfig> {
    let Some(path) = path else {
        return Ok(HighlightConfig::default());
    };
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read config {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum TuneOutcome {
    Selected {
        game_id: u32,
        star_user_id: u32,
        star_snake_id: u32,
        reason: common::HighlightReason,
        score: i32,
        window: common::HighlightWindow,
        breakdown: common::HighlightScoreBreakdown,
    },
    BelowThreshold {
        game_id: u32,
        candidate: Option<server::highlight_calibration::SelectedHighlightSummary>,
    },
}

#[derive(Serialize)]
struct TuneResult {
    source: PathBuf,
    outcome: TuneOutcome,
}

fn load_recording(path: &Path) -> Result<GameRecordingV1> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read recording {}", path.display()))?;
    let recording: GameRecordingV1 = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse recording {}", path.display()))?;
    recording
        .verify_end_hash()
        .with_context(|| format!("invalid or desynced recording {}", path.display()))?;
    Ok(recording)
}

fn score_recordings(paths: Vec<PathBuf>, config: &HighlightConfig) -> Result<()> {
    let mut results = Vec::with_capacity(paths.len());
    for source in paths {
        let recording = load_recording(&source)?;
        let candidate = score_highlight_candidate(&recording, config)?;
        let outcome = match candidate {
            Some(clip) if clip.score >= config.minimum_score => TuneOutcome::Selected {
                game_id: clip.game_id,
                star_user_id: clip.star_user_id,
                star_snake_id: clip.star_snake_id,
                reason: clip.reason,
                score: clip.score,
                window: clip.window,
                breakdown: clip.breakdown,
            },
            candidate => TuneOutcome::BelowThreshold {
                game_id: recording.game_id,
                candidate: candidate
                    .as_ref()
                    .map(server::highlight_calibration::SelectedHighlightSummary::from),
            },
        };
        results.push(TuneResult { source, outcome });
    }
    serde_json::to_writer_pretty(std::io::stdout().lock(), &results)?;
    println!();
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse()?;
    let config = load_config(cli.config_path.as_deref())?;
    if let Some(output_dir) = cli.corpus_dir {
        let run = run_bot_corpus(
            &BotCorpusSpec {
                games: cli.games,
                seed_base: cli.seed,
                review_count: cli.review_count,
            },
            &config,
        )?;
        write_calibration_artifacts(&run, &output_dir)?;
        serde_json::to_writer_pretty(std::io::stdout().lock(), &run.summary)?;
        println!();
        assert_automatic_acceptance(&run.summary)?;
        return Ok(());
    }
    score_recordings(cli.recordings, &config)
}
