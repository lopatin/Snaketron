//! Where the fold resumes from.
//!
//! Object keys are
//! `raw/{dataset}/dt={date}/host={host}/{first_cursor}-{last_cursor}-{hash}.json.gz`,
//! and only the trailing file name carries the cursor. So a key says "written
//! later than" another key **only within one `dt=…/host=…` prefix**. Across
//! prefixes the comparison is noise: `host` is `{region}-{server_id}`
//! (`exporter_service.rs`), so `euw1-*` sorts below `use1-*` and `use1-12`
//! sorts below `use1-3`.
//!
//! A single lexicographic mark therefore cannot express what has been folded.
//! Once it lands on a `use1` key, every EU object written afterwards for that
//! same day sorts beneath it and is skipped forever — an entire region dropped
//! silently, and bounded within the day only because `dt=` sorts first.
//!
//! So the resume state is one mark per prefix, and the listing floor is not
//! derived from those marks at all. It cannot be: a host that starts writing
//! today produces keys that sort BELOW an existing mark, so listing from
//! `min(marks)` would skip them. The floor is a retention window instead —
//! always the same shape, independent of what has been folded — and the marks
//! decide what to skip once the keys are in hand.

use std::collections::BTreeMap;

use chrono::{Days, NaiveDate, Utc};
use snaketron_service_api::ServiceError;

use super::object_store::{dataset_prefix, partition_prefix, prefix_day};

/// How far back a fold looks, in days.
///
/// This is the pipeline's **lateness budget**: an object written for a `dt=`
/// day older than the window is never folded, because the listing never
/// returns it. Three days is chosen against the two cadences that bracket it:
///
/// * The exporter flushes a batch at `BatchLimits::max_batch_age`, 300 s by
///   default (`batch.rs`), so an object normally lands within minutes of the
///   events it holds — the window is slack, not the ordinary path.
/// * The committer ticks hourly (`ICEBERG_COMMIT_INTERVAL` in
///   `game_server.rs`), so three days is 72 consecutive missed ticks: far past
///   any deploy, lease handover, or fenced stand-down.
///
/// The cost of raising it is paid every tick, in keys listed and in marks
/// stored — the floor day is inclusive, so the window spans
/// `RETENTION_DAYS + 1` days of objects. Both stay small because there is
/// exactly ONE exporter per region (`ExporterFactory::exclusion_key` is a
/// regional lease), so a steady day contributes one `host=` per region rather
/// than one per task, and a failover adds one more.
///
/// Raise it if a multi-day exporter outage is ever expected to replay a
/// backlog still carrying its original event dates, since `dt=` is the EVENT
/// date (`exporter::event_date`) and a replayed backlog does not get a fresh
/// one.
pub const RETENTION_DAYS: u64 = 3;

/// Today, in UTC. The partition dates are UTC (`exporter::event_date`), so the
/// window has to be measured in the same calendar or it would drift by a day
/// for part of every day.
pub fn today_utc() -> NaiveDate {
    Utc::now().date_naive()
}

/// The oldest day a fold will look at.
///
/// An underflow falls back to the earliest representable date rather than to
/// `today`, because a too-wide window only costs listing time while a too-narrow
/// one drops objects.
pub fn window_start(today: NaiveDate) -> NaiveDate {
    today
        .checked_sub_days(Days::new(RETENTION_DAYS))
        .unwrap_or(NaiveDate::MIN)
}

/// The `StartAfter` value that admits the whole window.
///
/// `raw/{dataset}/dt={day}` is a PREFIX of every real key of that day and is
/// never itself a key, so it sorts strictly below all of them: a string is
/// less than any string extending it, and the extension here begins with `/`.
/// `StartAfter` is exclusive, so the day it names is included in full — which
/// is what makes the floor and [`ResumeMarks::prune`] agree, both keeping the
/// floor day and dropping everything before it.
pub fn listing_floor(dataset: &str, window_start: NaiveDate) -> String {
    format!(
        "{}dt={}",
        dataset_prefix(dataset),
        window_start.format("%Y-%m-%d")
    )
}

/// The highest folded key, per partition prefix.
///
/// Stored as one JSON table property. A map rather than a single string
/// because the prefix is the only span in which "highest" means "latest"; see
/// the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResumeMarks {
    per_prefix: BTreeMap<String, String>,
    /// The mark left by the single-mark scheme this replaced.
    ///
    /// Honoured as an ADDITIONAL global floor, never written again. Doing so
    /// can only skip more than the per-prefix marks would, never re-fold, so
    /// it cannot duplicate rows on a table carrying one. It stops mattering on
    /// its own: it never advances, so it falls out of the retention window
    /// within [`RETENTION_DAYS`] and every later key is above it.
    legacy: Option<String>,
}

impl ResumeMarks {
    /// Reads both properties. A malformed marks property is an ERROR rather
    /// than an empty resume, for the same reason a malformed epoch is: reading
    /// garbage as absence would re-fold the entire window and duplicate every
    /// row in it.
    pub fn decode(marks: Option<&str>, legacy: Option<&str>) -> Result<Self, ServiceError> {
        let per_prefix = match marks {
            None => BTreeMap::new(),
            Some(raw) => {
                serde_json::from_str::<BTreeMap<String, String>>(raw).map_err(|error| {
                    ServiceError::failed(format!(
                        "the committer resume marks are not a JSON object of \
                         partition prefix -> highest folded key: {error}"
                    ))
                })?
            }
        };
        Ok(Self {
            per_prefix,
            legacy: legacy.map(str::to_owned),
        })
    }

    /// Serializes the per-prefix marks. `BTreeMap` so the bytes are a function
    /// of the content alone — two committers holding the same marks write the
    /// same property.
    pub fn encode(&self) -> Result<String, ServiceError> {
        serde_json::to_string(&self.per_prefix)
            .map_err(|error| ServiceError::failed(format!("encoding resume marks: {error}")))
    }

    /// Whether `key` has already been folded.
    ///
    /// Two floors, either of which is sufficient: the legacy global mark, and
    /// the mark for the key's OWN prefix. A mark from any other prefix says
    /// nothing about this key and is not consulted.
    pub fn already_folded(&self, key: &str) -> bool {
        if self.legacy.as_deref().is_some_and(|mark| key <= mark) {
            return true;
        }
        self.per_prefix
            .get(partition_prefix(key))
            .is_some_and(|mark| key <= mark.as_str())
    }

    /// Records a folded key, keeping the highest.
    ///
    /// Never lowers a mark: a fold walks prefixes interleaved, so a commit for
    /// one prefix routinely lands after a later commit for another, and a
    /// blind overwrite would re-expose keys already folded.
    pub fn record(&mut self, key: &str) {
        let slot = self
            .per_prefix
            .entry(partition_prefix(key).to_owned())
            .or_default();
        if slot.as_str() < key {
            *slot = key.to_owned();
        }
    }

    /// Drops marks for days the listing no longer returns, so the property
    /// cannot grow without bound.
    ///
    /// `window_start` must be the same day [`listing_floor`] was built from.
    /// Both keep that day and drop everything before it, so a pruned mark is
    /// always one whose keys the next listing cannot produce — pruning can
    /// never cause a re-fold.
    pub fn prune(&mut self, window_start: NaiveDate) {
        let floor = window_start.format("%Y-%m-%d").to_string();
        self.per_prefix
            .retain(|prefix, _| match prefix_day(prefix) {
                // `YYYY-MM-DD` sorts chronologically, so the string comparison IS
                // the date comparison and no parse is needed.
                Some(day) => day >= floor.as_str(),
                // The single bucket for keys outside the layout. One entry can
                // never grow the property, and dropping it would re-fold the keys
                // it stands for — they carry no day to be re-listed under.
                None => true,
            });
    }

    /// The greatest folded key across every prefix.
    ///
    /// Observability only: it is NOT what the fold resumes from, and treating
    /// it as such is the bug this type exists to fix.
    pub fn highest(&self) -> Option<&str> {
        self.per_prefix
            .values()
            .map(String::as_str)
            .chain(self.legacy.as_deref())
            .max()
    }

    /// The mark for one prefix, if any.
    pub fn mark_for(&self, prefix: &str) -> Option<&str> {
        self.per_prefix.get(prefix).map(String::as_str)
    }

    /// How many prefixes are tracked. The bound pruning defends.
    pub fn prefix_count(&self) -> usize {
        self.per_prefix.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::object_store::object_key;

    const DATASET: &str = "game-events";

    fn key(date: &str, host: &str, cursor: &str) -> String {
        object_key(DATASET, date, host, cursor, cursor, "hash")
    }

    fn day(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("a date")
    }

    // -- The listing floor ---------------------------------------------

    /// The load-bearing ordering fact. `raw/{dataset}/dt={day}` is a proper
    /// prefix of every key of that day, and a string sorts below any string
    /// that extends it, so an exclusive `StartAfter` on it INCLUDES the whole
    /// day. If this ever stopped holding, the floor would silently amputate
    /// its own oldest day.
    #[test]
    fn the_floor_sorts_below_every_key_of_the_day_it_names() {
        let floor = listing_floor(DATASET, day("2026-08-17"));
        assert_eq!(floor, "raw/game-events/dt=2026-08-17");
        for host in ["euw1-5", "use1-3", "use1-12", "a", "zzzz"] {
            for cursor in ["0-0", "1755600000000-0", "9999999999999-9"] {
                let key = key("2026-08-17", host, cursor);
                assert!(
                    floor < key,
                    "the floor must admit its own day:\n  floor={floor}\n  key={key}"
                );
            }
        }
    }

    /// The other half: the day before the floor is excluded, which is what
    /// makes the window a window rather than a full scan.
    #[test]
    fn the_floor_sorts_above_every_key_of_the_previous_day() {
        let floor = listing_floor(DATASET, day("2026-08-17"));
        for host in ["euw1-5", "use1-3", "zzzz"] {
            let key = key("2026-08-16", host, "9999999999999-9");
            assert!(key < floor, "\n  key={key}\n  floor={floor}");
        }
    }

    /// The floor has to be scoped to the dataset, or the listing prefix and
    /// the `StartAfter` would disagree and the fold would see nothing.
    #[test]
    fn the_floor_lies_under_the_dataset_prefix() {
        let floor = listing_floor(DATASET, day("2026-08-17"));
        assert!(floor.starts_with(&dataset_prefix(DATASET)));
    }

    #[test]
    fn the_window_reaches_back_exactly_the_retention_days() {
        assert_eq!(window_start(day("2026-08-20")), day("2026-08-17"));
        assert_eq!(
            (day("2026-08-20") - window_start(day("2026-08-20"))).num_days(),
            RETENTION_DAYS as i64
        );
        // Across a month boundary, because the arithmetic is calendar
        // arithmetic and not a subtraction on the day-of-month.
        assert_eq!(window_start(day("2026-03-01")), day("2026-02-26"));
    }

    /// Underflow must widen the window, never narrow it: a wide window costs
    /// listing time, a narrow one drops objects.
    #[test]
    fn an_underflowing_window_falls_back_to_the_earliest_date() {
        assert_eq!(window_start(NaiveDate::MIN), NaiveDate::MIN);
        let floor = listing_floor(DATASET, window_start(NaiveDate::MIN));
        assert!(
            floor < key("2026-08-17", "use1-3", "0-0"),
            "the fallback floor must still admit real keys: {floor}"
        );
    }

    // -- The bug ------------------------------------------------------

    /// The two-region loss, at the unit level. A `use1` mark must say nothing
    /// about a `euw1` key, however the two sort against each other.
    #[test]
    fn a_us_mark_does_not_hide_an_eu_key_of_the_same_day() {
        let us = key("2026-08-19", "use1-3", "1000000000000-0");
        let eu = key("2026-08-19", "euw1-5", "2000000000000-0");
        assert!(eu < us, "the premise:\n  eu={eu}\n  us={us}");

        let mut marks = ResumeMarks::default();
        marks.record(&us);
        assert!(marks.already_folded(&us));
        assert!(
            !marks.already_folded(&eu),
            "a US mark must not fold away an EU object"
        );
    }

    /// The same failure inside one region: exporter failover moves the server
    /// id, and `use1-12` sorts below `use1-3`.
    #[test]
    fn a_mark_from_one_server_id_does_not_hide_another() {
        let three = key("2026-08-19", "use1-3", "1000000000000-0");
        let twelve = key("2026-08-19", "use1-12", "2000000000000-0");
        assert!(twelve < three, "the premise:\n  {twelve}\n  {three}");

        let mut marks = ResumeMarks::default();
        marks.record(&three);
        assert!(!marks.already_folded(&twelve));
    }

    /// A host that starts writing today produces keys sorting below marks that
    /// already exist. This is why the floor is a retention window and not
    /// `min(marks)`.
    #[test]
    fn a_new_host_sorting_below_every_existing_mark_is_still_listed_and_folded() {
        let existing = key("2026-08-19", "use1-3", "5000000000000-0");
        let newcomer = key("2026-08-19", "euw1-9", "0-0");

        let mut marks = ResumeMarks::default();
        marks.record(&existing);
        assert!(newcomer < existing);

        let floor = listing_floor(DATASET, day("2026-08-17"));
        assert!(floor < newcomer, "the listing must still reach it");
        assert!(
            !marks.already_folded(&newcomer),
            "and it must not be skipped"
        );
    }

    // -- Recording ----------------------------------------------------

    #[test]
    fn a_mark_advances_within_its_own_prefix() {
        let first = key("2026-08-19", "use1-3", "1000000000000-0");
        let second = key("2026-08-19", "use1-3", "2000000000000-0");
        let mut marks = ResumeMarks::default();
        marks.record(&first);
        marks.record(&second);
        assert!(marks.already_folded(&first));
        assert!(marks.already_folded(&second));
        assert_eq!(marks.prefix_count(), 1, "one prefix, one entry");
    }

    /// Commits arrive interleaved across prefixes, so a later commit for one
    /// prefix routinely follows a higher key in another. A record that lowered
    /// a mark would re-expose everything between.
    #[test]
    fn an_out_of_order_commit_never_lowers_a_mark() {
        let low = key("2026-08-19", "use1-3", "1000000000000-0");
        let high = key("2026-08-19", "use1-3", "9000000000000-0");
        let other = key("2026-08-19", "euw1-5", "5000000000000-0");

        let mut marks = ResumeMarks::default();
        marks.record(&high);
        marks.record(&other);
        marks.record(&low);

        assert_eq!(
            marks.mark_for("dt=2026-08-19/host=use1-3"),
            Some(high.as_str()),
            "the highest key must survive an out-of-order record"
        );
        assert!(marks.already_folded(&low));
        assert!(marks.already_folded(&high));
    }

    #[test]
    fn each_prefix_gets_its_own_entry() {
        let mut marks = ResumeMarks::default();
        marks.record(&key("2026-08-19", "use1-3", "1-0"));
        marks.record(&key("2026-08-19", "euw1-5", "1-0"));
        marks.record(&key("2026-08-20", "use1-3", "1-0"));
        assert_eq!(marks.prefix_count(), 3);
    }

    // -- Pruning ------------------------------------------------------

    /// Without this the property grows by one entry per host per day, forever,
    /// inside a `metadata.json` that bricks the table at 50 MB.
    #[test]
    fn marks_older_than_the_window_are_pruned_and_newer_ones_kept() {
        let mut marks = ResumeMarks::default();
        for date in ["2026-08-15", "2026-08-16", "2026-08-17", "2026-08-20"] {
            marks.record(&key(date, "use1-3", "1-0"));
        }
        assert_eq!(marks.prefix_count(), 4);

        marks.prune(window_start(day("2026-08-20")));

        assert_eq!(marks.mark_for("dt=2026-08-15/host=use1-3"), None);
        assert_eq!(marks.mark_for("dt=2026-08-16/host=use1-3"), None);
        assert!(
            marks.mark_for("dt=2026-08-17/host=use1-3").is_some(),
            "the floor day is inside the window and must be kept"
        );
        assert!(marks.mark_for("dt=2026-08-20/host=use1-3").is_some());
        assert_eq!(marks.prefix_count(), 2);
    }

    /// Pruning and the floor must agree exactly, or a pruned mark's keys come
    /// back in the next listing and are folded twice.
    #[test]
    fn every_pruned_mark_is_one_the_floor_no_longer_admits() {
        let today = day("2026-08-20");
        let start = window_start(today);
        let floor = listing_floor(DATASET, start);

        let mut marks = ResumeMarks::default();
        let dates = ["2026-08-14", "2026-08-16", "2026-08-17", "2026-08-20"];
        for date in dates {
            marks.record(&key(date, "use1-3", "1-0"));
        }
        marks.prune(start);

        for date in dates {
            let key = key(date, "use1-3", "1-0");
            let listed = floor < key;
            let kept = marks.mark_for(&format!("dt={date}/host=use1-3")).is_some();
            assert_eq!(
                listed, kept,
                "a mark must be kept exactly when its keys are still listed: {key}"
            );
        }
    }

    /// The one bucket for keys outside the layout carries no day, so it cannot
    /// be aged out — and it must not be, or those keys would be folded again.
    /// One entry cannot grow the property.
    #[test]
    fn the_unpartitioned_bucket_survives_pruning() {
        let mut marks = ResumeMarks::default();
        marks.record("raw/proof/k1");
        marks.record("a.json.gz");
        assert_eq!(marks.prefix_count(), 1, "they share one bucket");

        marks.prune(window_start(day("2099-01-01")));
        assert_eq!(marks.prefix_count(), 1);
        assert!(marks.already_folded("a.json.gz"));
    }

    // -- The legacy mark ----------------------------------------------

    /// Migration. A table written by the single-mark committer carries one
    /// global mark; honouring it as an extra floor can only skip more than the
    /// per-prefix marks would, so it can never duplicate rows.
    #[test]
    fn the_legacy_mark_is_honoured_as_an_additional_global_floor() {
        let legacy = key("2026-08-19", "use1-3", "5000000000000-0");
        let below = key("2026-08-19", "euw1-5", "1000000000000-0");
        let above = key("2026-08-19", "use1-3", "9000000000000-0");
        let marks = ResumeMarks::decode(None, Some(&legacy)).expect("decode");

        assert!(below < legacy && legacy < above, "the premise");
        assert!(marks.already_folded(&legacy), "the mark itself is folded");
        assert!(marks.already_folded(&below));
        assert!(!marks.already_folded(&above));
    }

    /// The legacy floor never advances, so it ages out of the window on its
    /// own and stops hiding anything.
    #[test]
    fn a_legacy_mark_older_than_the_window_hides_nothing_that_is_listed() {
        let legacy = key("2026-08-16", "use1-3", "9999999999999-9");
        let marks = ResumeMarks::decode(None, Some(&legacy)).expect("decode");
        let floor = listing_floor(DATASET, window_start(day("2026-08-20")));

        for host in ["euw1-5", "use1-3"] {
            let key = key("2026-08-17", host, "0-0");
            assert!(floor < key, "listed");
            assert!(
                !marks.already_folded(&key),
                "and not hidden by the legacy mark"
            );
        }
    }

    /// A legacy mark must not stop the per-prefix marks from advancing past
    /// it, which is what makes the migration a one-way ratchet.
    #[test]
    fn per_prefix_marks_still_advance_above_the_legacy_mark() {
        let legacy = key("2026-08-19", "use1-3", "5000000000000-0");
        let later = key("2026-08-19", "use1-9", "9000000000000-0");
        assert!(legacy < later, "the premise:\n  {legacy}\n  {later}");

        let mut marks = ResumeMarks::decode(None, Some(&legacy)).expect("decode");
        assert!(!marks.already_folded(&later));
        marks.record(&later);
        assert!(marks.already_folded(&later));
    }

    /// The honest cost of the migration, pinned so nobody discovers it in a
    /// query: on the day a legacy mark was set, keys sorting below it stay
    /// hidden. Those are the objects the single-mark scheme had ALREADY
    /// dropped, so honouring the mark loses nothing that was not lost before
    /// — and unlike the old scheme it stops: the legacy mark never advances,
    /// so it leaves the window within `RETENTION_DAYS` and hides nothing
    /// afterwards.
    #[test]
    fn a_legacy_mark_still_hides_its_own_days_lower_keys() {
        let legacy = key("2026-08-19", "use1-3", "5000000000000-0");
        let eu_same_day = key("2026-08-19", "euw1-5", "9000000000000-0");
        assert!(eu_same_day < legacy, "the premise");

        let marks = ResumeMarks::decode(None, Some(&legacy)).expect("decode");
        assert!(marks.already_folded(&eu_same_day));

        // The next day is above the mark, so the loss does not continue.
        let eu_next_day = key("2026-08-20", "euw1-5", "0-0");
        assert!(!marks.already_folded(&eu_next_day));
    }

    // -- The property -------------------------------------------------

    #[test]
    fn marks_round_trip_through_the_property() {
        let mut marks = ResumeMarks::default();
        marks.record(&key("2026-08-19", "use1-3", "1-0"));
        marks.record(&key("2026-08-19", "euw1-5", "2-0"));

        let encoded = marks.encode().expect("encode");
        let restored = ResumeMarks::decode(Some(&encoded), None).expect("decode");
        assert_eq!(restored, marks);
        assert_eq!(
            marks.encode().expect("encode"),
            encoded,
            "the encoding must be a function of the content alone"
        );
    }

    #[test]
    fn an_absent_property_is_an_empty_resume_that_folds_everything() {
        let marks = ResumeMarks::decode(None, None).expect("decode");
        assert_eq!(marks.prefix_count(), 0);
        assert!(!marks.already_folded(&key("2026-08-19", "use1-3", "1-0")));
        assert_eq!(marks.highest(), None);
    }

    /// Garbage must not read as "nothing folded". That would re-fold the whole
    /// retention window and duplicate every row in it.
    #[test]
    fn a_malformed_property_is_an_error_not_an_empty_resume() {
        for raw in ["{", "[]", "\"a\"", "{\"p\":3}"] {
            let error = ResumeMarks::decode(Some(raw), None)
                .expect_err("garbage must not read as an empty resume");
            assert!(error.to_string().contains("resume marks"), "{error}");
        }
    }

    #[test]
    fn the_highest_key_spans_every_prefix_and_the_legacy_mark() {
        let low = key("2026-08-19", "euw1-5", "1-0");
        let high = key("2026-08-20", "use1-3", "1-0");
        let mut marks = ResumeMarks::decode(None, Some(&low)).expect("decode");
        marks.record(&high);
        assert_eq!(marks.highest(), Some(high.as_str()));
    }
}
