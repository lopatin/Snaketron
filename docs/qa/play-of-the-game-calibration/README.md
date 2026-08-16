# Play-of-the-Game calibration

This directory is the compact, reproducible launch-calibration pack for the
deterministic Play-of-the-Game scorer. The checked corpus uses seed
`0x534e414b4554524f`, 200 authoritative `GameState` matches, and the same
`common::calculate_ai_command` decision function as the live bot.

Four profiles start the bot policy immediately (Duel and 2v2, Quickmatch and
Competitive). Every fifth match is an explicitly labeled Competitive Duel
that sends canonical `PlayerActivity` commands for 60 seconds before enabling
the same 100 ms bot policy. Stock bots otherwise finish every sampled match in
under two minutes, so that declared profile supplies a deterministic cohort
for the PRD's `>= 2 min` denominator without substituting a mock engine or
scripted score event. Truncated games and games with fewer than two active
players never enter that denominator.

## Reproduce the objective gate

From the repository root, run:

```sh
cargo run -p server --release --bin highlight_tune -- \
  --bot-corpus-dir docs/qa/play-of-the-game-calibration \
  --games 200 \
  --seed 0x534e414b4554524f \
  --review-count 20
```

The command validates the end hash of every full recording, replays every
selected clip, writes `corpus-summary.json`, and exits unsuccessfully unless:

- at least 70% of completed games lasting at least two minutes with at least
  two active players produce a highlight;
- no Demolition, Banking, Combo, or Frenzy category exceeds 60% of winners.

> **Point it at a scratch directory unless you mean to redo the human gate.**
> Writing straight into this directory also rewrites `corpus-summary.json`,
> `top-20-review.json` and `review-template.csv` with blank human-review
> fields, discarding hand-entered verdicts that are not tool output. Nothing
> regenerates those, and `client/web/tests/unit/potgCalibrationArtifacts.test.ts`
> fails once they are gone.
>
> To refresh only the clip data — which is what a `GAMEPLAY_REPLAY_VERSION`
> bump or a change to the serialized shape of `GameState` requires, since
> the browser rejects any clip whose `gameplay_version` differs from the
> protocol it speaks — generate into a scratch directory and copy `clips/`
> back:
>
> ```sh
> cargo run -p server --release --bin highlight_tune -- \
>   --bot-corpus-dir /tmp/potg-regen \
>   --games 200 --seed 0x534e414b4554524f --review-count 20
> cp /tmp/potg-regen/clips/*.json docs/qa/play-of-the-game-calibration/clips/
> ```
>
> Diff the scratch `corpus-summary.json` and `top-20-review.json` against the
> checked-in ones first: apart from the blanked human-review fields they should
> be identical, and any other difference means the corpus itself moved and the
> human gate genuinely has to be redone. Clip regeneration is deterministic in
> content but not byte-stable — `HashMap` fields such as `last_death_causes`
> serialize in arbitrary key order, so expect reordering noise in the diff.

`tuning-rounds.json` records the fixed-seed baseline and the only tuning round.
Round 1 changed only `banked_per_point` (`5 -> 6`) and `combo_step` (`15 ->
21`), promoting the calibrated defaults to rules version 2. The 120-point
minimum, elimination weights, anti-farming rules, and anti-style penalties did
not move.

## Complete the human gate

The top 20 selected clips and exact scorer/event evidence are listed in
`top-20-review.json`; human fields deliberately start as `null`. Render all 20
outside git while the QA web application is running:

```sh
node tools/video/capture-potg-review.mjs \
  --manifest docs/qa/play-of-the-game-calibration/top-20-review.json \
  --capture-vfps 60 \
  --out /tmp/snaketron-potg-review
```

Watch every rendered clip, answer the five rubric questions in the manifest,
and record the verdict in `review-template.csv` or the manifest. A clip is
"deserved" only when all five checks pass. At least 16 of 20 deserved clips
are required.

The launch review was completed against the actual product timing at
1920×1080/60 fps: every master was 12.5 seconds (750 frames), with the focus
at viewer time 8.0 seconds. Three independent visual-QA passes judged 16 of 20
clips deserved, meeting the 80% gate. Ranks 5, 9, 12, and 19 were rejected for
repeated mutual trading, a caption fact outside the clip, an ordinary play,
and an unclear ordinary collision respectively. Full field-level verdicts and
review notes are preserved in `top-20-review.json` and `review-template.csv`.
