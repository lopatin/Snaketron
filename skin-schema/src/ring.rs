//! Whether an animation survives being baked into 32 frames.
//!
//! Everything animated in a skin is sampled at `time = k / ANIMATION_STEPS`,
//! whichever binding tier it lands in, and the renderer *snaps* to the nearest
//! baked frame rather than interpolating. So an expression that completes many
//! cycles per turn does not come out fast — it comes out as a **different,
//! slower motion**, and the author has no way to tell that from a mistake in
//! their arithmetic. `sin(tau * time * 40)` reads as a lazy wobble.
//!
//! ## The measurement
//!
//! **Total variation**: the distance the value travels around the whole cycle.
//! Measured twice — once on the ring the skin will actually be baked into, and
//! once on a dense reference grid standing in for the truth — and compared. If
//! the ring records materially less travel than there is, what plays is not
//! what was written.
//!
//! Total variation rather than interpolation error, because this language is
//! full of functions that are discontinuous *on purpose*: `saw` is a ramp with
//! a cliff, `pulse` is a square wave, `step` and `floor` are stairs. Any
//! smoothness-shaped check refuses all of them forever, and the shipped
//! **Shine** template is a `saw(time)`. A jump contributes the same distance
//! however finely it is sampled, so discontinuity count is invisible here while
//! missed motion is not.
//!
//! ## Why the reference grid is coprime with the ring
//!
//! This is the part that is easy to get wrong, and the first version of this
//! module got it wrong. A reference that is a *multiple* of 32 folds the way
//! the ring folds: at 96 samples, `sin(tau * time * 100)` becomes four cycles
//! on the reference **and** four cycles on the ring, the two totals agree
//! exactly, and a hundred-cycle strobe is reported as fine. Thirty of every
//! ninety-six rates slipped through that way. Tripling the ring does not break
//! the lattice; it moves the blind spot from 32 to 96.
//!
//! So the reference is a *prime* count sharing no factor with 32. A rate can
//! only hide by folding small on both grids at once, and coprime grids do not
//! fold together. `no_rate_in_a_wide_sweep_escapes_unreported` is the check
//! that matters here, because a blind spot is by definition the case nobody
//! thought to write a test for.
//!
//! ## What the bound actually is
//!
//! Not "Nyquist", and the difference is worth stating plainly because it is
//! tempting to claim otherwise. Nyquist describes a band-limited signal; a
//! sawtooth has a cliff and therefore no band limit at all. A cliff needs
//! samples on both sides of it to be recorded, so a ramp runs out of ring
//! sooner than a sinusoid does. Measured, at the threshold below:
//!
//! | shape | last rate that passes | first rate refused |
//! | --- | --- | --- |
//! | `sin` | 15 cycles (1.48) | 17 cycles (1.67) |
//! | `saw` | 12 cycles (1.58) | 13 cycles (1.66) |
//! | `tri` | 12 cycles (1.48) | 13 cycles (1.66) |
//! | `pulse` | 16 cycles (1.00) | 20 cycles (1.67) |
//!
//! `pulse` reaches furthest because a square wave's travel is entirely in its
//! edges, and the ring keeps recording those until two land in one frame.

use crate::ANIMATION_STEPS;
use crate::expr::{Env, Expr};

/// Samples standing in for the true signal.
///
/// Prime, and therefore coprime with the ring's 32 — which is the whole point
/// (see the module docs). Dense enough that the travel it measures is the real
/// travel for any rate an author could plausibly write.
const REFERENCE_SAMPLES: usize = 1009;

/// A second, sparser reference, used only to ask whether the first one is
/// telling the truth.
///
/// Any finite grid aliases eventually — including this module's reference. At
/// 999 cycles the 1009-sample grid folds to ten and reports a calm ratio, which
/// would let the fastest strobe expressible slip through the widest gate. So
/// the two grids are compared with each other first: if a coarser prime and a
/// finer one disagree about how far the value travels, the signal is moving
/// faster than *either* can resolve, which settles the question about the
/// 32-sample ring without needing to know the true answer.
const CONVERGENCE_SAMPLES: usize = 503;

/// How far the two references may differ before the finer one is not to be
/// trusted. Well-resolved signals agree to a fraction of a percent.
const MAX_REFERENCE_DISAGREEMENT: f64 = 0.05;

/// Below this, nothing is visible in any unit the schema uses — a thousandth
/// of an opacity, of a cell, or of a turn.
const MIN_VISIBLE_SWING: f64 = 1e-3;

/// Below this, the reference is reporting less travel than the ring — which
/// sampling cannot legitimately do, since a coarser grid can only miss motion.
/// A little slack for discretisation: a triangle at sixteen cycles measures
/// 0.98 honestly.
const MIN_TRUSTWORTHY_GROWTH: f64 = 0.95;

/// How much more distance the truth may travel than the ring records.
///
/// Chosen from measurement, not from theory. The narrowest real gap is between
/// a 12-cycle sawtooth (1.58, which passes) and a 13-cycle one (1.66, which
/// does not) — and both are computed from `fract` and `floor`, which are exact,
/// so that tightness costs nothing in cross-platform terms. The shapes that
/// *do* run through a libm — the sinusoids — sit at 1.48 and 1.67 either side,
/// and that is the margin which has to survive native and wasm disagreeing in
/// the last bits.
const MAX_MOTION_GROWTH: f64 = 1.6;

/// How an expression fails to survive the ring.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RingAlias {
    /// The value moves, and the ring samples it at exactly the points where it
    /// repeats — so the skin paints a constant.
    Blind,
    /// The ring records only part of the motion, so what plays is a different
    /// and slower movement than the one written.
    TooFast { growth: f64 },
}

/// The distance a cyclic sequence travels, including the wrap.
///
/// The wrap step counts because it is motion a viewer really sees: the ring
/// returns to frame zero, and if that return is a jump, it is a jump.
fn total_variation(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    (0..values.len())
        .map(|index| (values[(index + 1) % values.len()] - values[index]).abs())
        .sum()
}

fn swing(values: &[f64]) -> f64 {
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for value in values {
        low = low.min(*value);
        high = high.max(*value);
    }
    if low.is_finite() && high.is_finite() {
        high - low
    } else {
        0.0
    }
}

fn sample(expr: &Expr, env: &Env, count: usize) -> Vec<f64> {
    (0..count)
        .map(|index| {
            expr.eval(&Env {
                time: index as f64 / count as f64,
                ..*env
            })
        })
        .collect()
}

/// Whether one expression survives the bake, in one environment.
///
/// `env` supplies everything but `time`; the caller varies the rest, because a
/// rate can depend on them — `sin(tau * time * len)` is calm on a short snake
/// and strobes on a long one.
pub fn ring_alias(expr: &Expr, env: &Env) -> Option<RingAlias> {
    let fine = sample(expr, env, REFERENCE_SAMPLES);
    let coarse = sample(expr, env, CONVERGENCE_SAMPLES);

    // Nothing to say about something that does not move. Both references have
    // to agree it is still, because a grid goes blind at exactly its own rate:
    // sampled 1009 times, a 1009-cycle sinusoid is a perfectly flat line.
    if swing(&fine) < MIN_VISIBLE_SWING && swing(&coarse) < MIN_VISIBLE_SWING {
        return None;
    }

    let recorded = total_variation(&sample(expr, env, ANIMATION_STEPS));
    // The ring travels nowhere while the signal does: every sample landed on
    // the same phase, so the skin paints a constant. This is precisely the case
    // a reference sharing the ring's lattice cannot see at all.
    if recorded < MIN_VISIBLE_SWING {
        return Some(RingAlias::Blind);
    }

    // Do the two references agree about how far this travels? If a coarser
    // prime and a finer one disagree, the signal outruns both — and something
    // neither of two grids in the hundreds can resolve is emphatically beyond a
    // ring of thirty-two. Reported at whichever grid saw more, which understates
    // it, because the honest number is unknowable from here.
    let travelled = total_variation(&fine);
    let corroboration = total_variation(&coarse);
    let widest = travelled.max(corroboration);
    if widest > 0.0 && (travelled - corroboration).abs() / widest > MAX_REFERENCE_DISAGREEMENT {
        return Some(RingAlias::TooFast {
            growth: widest / recorded,
        });
    }

    let growth = travelled / recorded;

    // Sampling can only ever *miss* motion, never invent it — so a reference
    // that reports less travel than the 32-sample ring is provably wrong about
    // its own signal, and the only thing that makes it wrong is a rate beyond
    // what it resolves. The one case left after the agreement check: a rate
    // landing exactly on the reference count, where both references fold to
    // the same small number and the ring's own fold happens to be busier.
    if growth < MIN_TRUSTWORTHY_GROWTH {
        return Some(RingAlias::TooFast {
            growth: 1.0 / growth,
        });
    }

    (growth > MAX_MOTION_GROWTH).then_some(RingAlias::TooFast { growth })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verdict(source: &str) -> Option<RingAlias> {
        let expr = Expr::parse(source).unwrap_or_else(|error| panic!("`{source}`: {error}"));
        ring_alias(&expr, &Env::default())
    }

    /// The measured ratio, for a failure message that names a number.
    fn growth_of(source: &str) -> f64 {
        let expr = Expr::parse(source).expect("grammatical");
        let ring = total_variation(&sample(&expr, &Env::default(), ANIMATION_STEPS));
        if ring <= 0.0 {
            return f64::INFINITY;
        }
        total_variation(&sample(&expr, &Env::default(), REFERENCE_SAMPLES)) / ring
    }

    /// The shapes the language exists to express, none of which may be
    /// refused. The discontinuous ones are the point: they fail any
    /// smoothness-shaped check forever, and `saw(time)` is a shipped template.
    #[test]
    fn every_legitimate_waveform_survives_the_ring() {
        for source in [
            "saw(time)",
            "saw(time) - 0.12",
            "saw(time * 3)",
            "saw(time * 8)",
            "pulse(time)",
            "pulse(time, 0.03)",
            "pulse(time * 4)",
            "pulse(time * 8)",
            "pulse(time * 16)",
            "step(0.5, time)",
            "floor(time * 4)",
            "0.5 + 0.5 * sin(tau * time)",
            "sin(tau * time * 8)",
            "sin(tau * time * 15)",
            "tri(time * 8)",
            "abs(sin(tau * time * 6))",
            "0.3 + 0.1 * sin(tau * time) + 0.02 * sin(tau * time * 7)",
            "smoothstep(0.2, 0.8, saw(time))",
            "0.5",
            "len * 2",
        ] {
            assert_eq!(
                verdict(source),
                None,
                "`{source}` was refused at growth {:.3}",
                growth_of(source)
            );
        }
    }

    /// The rates the ring cannot record. The PRD's own example is in here.
    #[test]
    fn motion_the_ring_cannot_record_is_refused() {
        for source in [
            "sin(tau * time * 17)",
            "sin(tau * time * 20)",
            "sin(tau * time * 40)",
            "saw(time * 16)",
            "pulse(time * 20)",
            "tri(time * 15)",
        ] {
            assert!(
                matches!(verdict(source), Some(RingAlias::TooFast { .. })),
                "`{source}` slipped through at growth {:.3}",
                growth_of(source)
            );
        }
    }

    /// **The reason the reference grid is prime.**
    ///
    /// A reference that is a multiple of the ring folds the way the ring does,
    /// so both grids see the same low frequency, the totals agree, and the
    /// strobe is reported as fine. At 96 samples every one of these measured
    /// exactly 1.000 and was accepted — a hundred cycles rendered as four, with
    /// no message.
    #[test]
    fn a_rate_that_folds_on_a_shared_lattice_is_still_caught() {
        for source in [
            "sin(tau * time * 95)",
            "sin(tau * time * 97)",
            "sin(tau * time * 100)",
            "sin(tau * time * 111)",
            "sin(tau * time * 200)",
            "saw(time * 97)",
            "tri(time * 100)",
        ] {
            assert!(
                verdict(source).is_some(),
                "`{source}` slipped through at growth {:.3} — the reference grid \
                 is folding with the ring",
                growth_of(source)
            );
        }
    }

    /// No rate above the ring's own resolution may pass unreported.
    ///
    /// The sweep is the real check, because a blind spot is by definition the
    /// case nobody thought to write a test for: the first version of this
    /// module had thirty per ninety-six rates and a green suite.
    #[test]
    fn no_rate_in_a_wide_sweep_escapes_unreported() {
        for rate in 20..=3_000 {
            for shape in [
                format!("sin(tau * time * {rate})"),
                format!("saw(time * {rate})"),
                format!("tri(time * {rate})"),
            ] {
                assert!(
                    verdict(&shape).is_some(),
                    "`{shape}` passed at growth {:.3}",
                    growth_of(&shape)
                );
            }
        }
    }

    /// A rate that lands exactly on the ring paints a constant, and says so
    /// rather than passing as a still skin nobody asked for.
    #[test]
    fn a_rate_that_lands_on_the_ring_is_reported_as_blind() {
        for source in [
            "sin(tau * time * 32)",
            "sin(tau * time * 64)",
            "sin(tau * time * 96)",
            "saw(time * 32)",
            "pulse(time * 32)",
        ] {
            assert_eq!(
                verdict(source),
                Some(RingAlias::Blind),
                "`{source}` should paint a constant and say so"
            );
        }
    }

    /// The bound is shape-dependent, not one frequency, because a cliff needs
    /// samples either side of it and a curve does not. Pinned so the module's
    /// own table stays true rather than aspirational.
    #[test]
    fn the_bound_depends_on_the_shape_not_only_the_rate() {
        assert_eq!(verdict("sin(tau * time * 15)"), None);
        assert!(verdict("sin(tau * time * 17)").is_some());

        assert_eq!(verdict("saw(time * 12)"), None);
        assert!(verdict("saw(time * 13)").is_some());

        // A square wave reaches furthest: its travel is all in its edges, and
        // the ring keeps recording those until two land in one frame.
        assert_eq!(verdict("pulse(time * 16)"), None);
        assert!(verdict("pulse(time * 20)").is_some());
    }

    /// A rate can depend on the snake, so a verdict is per environment.
    #[test]
    fn a_rate_that_depends_on_the_body_is_judged_per_body() {
        let expr = Expr::parse("sin(tau * time * len)").expect("grammatical");
        assert_eq!(
            ring_alias(
                &expr,
                &Env {
                    len: 4.0,
                    ..Env::default()
                }
            ),
            None,
            "four cycles a turn is ordinary motion"
        );
        assert!(
            ring_alias(
                &expr,
                &Env {
                    len: 40.0,
                    ..Env::default()
                }
            )
            .is_some(),
            "the same expression strobes on a long snake"
        );
    }

    /// Total variation is invariant under affine rescaling, which is what lets
    /// one threshold serve opacity, cell ratios, turns and lengths alike.
    #[test]
    fn the_metric_does_not_care_what_units_a_property_is_in() {
        for source in [
            "sin(tau * time * 40)",
            "0.001 * sin(tau * time * 40) + 9",
            "100 * sin(tau * time * 40) - 50",
        ] {
            assert!(verdict(source).is_some(), "`{source}` slipped through");
        }
    }
}
