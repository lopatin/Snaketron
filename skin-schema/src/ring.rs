//! Whether an animation survives being baked into 32 frames.
//!
//! Everything animated in a skin is sampled at `time = k / ANIMATION_STEPS`,
//! whichever binding tier it lands in, and the renderer *snaps* to the nearest
//! baked frame rather than interpolating. So an expression that completes many
//! cycles per turn does not come out fast — it comes out as a **different,
//! slower motion**, and the author has no way to tell that from a mistake in
//! their arithmetic. `sin(tau * time * 40)` reads as a lazy wobble.
//!
//! ## Why total variation, and not the obvious thing
//!
//! The obvious check — sample twice as finely and compare each midpoint to
//! what the ring would show — measures *interpolation error*, and the skin
//! language is full of functions that are discontinuous on purpose. `saw` is a
//! ramp with a cliff at the wrap; `pulse` is a square wave; `step` and `floor`
//! are stairs. Every one of them fails a smoothness-shaped test forever, and
//! the shipped **Shine** template is a `saw(time)` — so the obvious check
//! rejects a skin this repo ships, on its first run.
//!
//! Counting *how many* intervals look wrong does not separate the classes
//! either: eight legitimate blinks produce sixteen bad intervals, while a
//! genuinely-fine 14-cycle sinusoid produces about nine.
//!
//! **Total variation** — the distance the value travels around the whole cycle
//! — is the quantity that works, for one reason: a jump contributes the same
//! `|Δ|` however finely you sample around it. Discontinuity count is therefore
//! invisible to it, while motion the coarse grid *missed* shows up directly.
//! Since the ring's samples are a subset of the fine grid's, the fine total is
//! never smaller, so the ratio has a meaningful floor of exactly 1.
//!
//! Measured across the language's real waveforms: every legitimate shape sits
//! at or below 1.44 (the worst being `abs(sin(tau * time * 6))`), and every
//! aliasing one at or above 1.59. The threshold sits between them, and the
//! crossover for sinusoids lands exactly on the ring's Nyquist frequency —
//! fifteen cycles a turn passes, sixteen and up are refused.
//!
//! ## Why the fine grid is three times the ring and not two
//!
//! A 2x grid shares the ring's power-of-two lattice, so the purest violations
//! are invisible on *both*: `sin(tau * time * 32)` samples to a constant on the
//! ring and to a constant on a 64-point grid. The author gets a dead layer and
//! no message. Three times keeps the ring a strict subset — which is what makes
//! the ratio's floor exact — while breaking that lattice.
//!
//! The swing that decides "is there anything here at all" is measured on a
//! third, off-grid probe of a prime length, for the same reason.

use crate::ANIMATION_STEPS;
use crate::expr::{Env, Expr};

/// The fine grid: three times the ring, so the ring's samples are a strict
/// subset of it while sitting off the shared power-of-two lattice.
const FINE_STEPS: usize = ANIMATION_STEPS * 3;

/// An off-lattice probe, used only to measure the true swing. Prime, and
/// coprime with both 32 and 96, so an integer frequency can vanish on it and
/// on the ring together only at absurd rates.
const PROBE_SAMPLES: usize = 101;

/// Below this, nothing is visible in any unit the schema uses — a thousandth
/// of an opacity, of a cell, or of a turn.
const MIN_VISIBLE_SWING: f64 = 1e-3;

/// How much more distance the fine grid may travel than the ring records.
///
/// 1.5 sits above every legitimate shape measured (worst: 1.44) and below every
/// aliasing one (lowest: 1.59). Deliberately not parked near either edge: this
/// crate compiles both natively and to wasm, whose libms need not agree to the
/// last bit on `sin`, and a document sitting on the boundary could validate in
/// the Builder and be refused by CI.
const MAX_MOTION_GROWTH: f64 = 1.5;

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
/// The wrap step is included because it is motion a viewer really sees: the
/// ring returns to frame zero, and if that return is a jump, it is a jump.
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

/// Whether one expression survives the bake, in one environment.
///
/// `env` supplies everything but `time`; the caller varies the rest, because a
/// rate can depend on them — `sin(tau * time * len)` is calm on a short snake
/// and strobes on a long one.
pub fn ring_alias(expr: &Expr, env: &Env) -> Option<RingAlias> {
    let at = |time: f64| expr.eval(&Env { time, ..*env });

    let fine: Vec<f64> = (0..FINE_STEPS)
        .map(|index| at(index as f64 / FINE_STEPS as f64))
        .collect();
    // Exactly the values the bake would store: the ring is every third sample.
    let coarse: Vec<f64> = (0..ANIMATION_STEPS).map(|step| fine[step * 3]).collect();
    let probe: Vec<f64> = (0..PROBE_SAMPLES)
        .map(|index| at(index as f64 / PROBE_SAMPLES as f64))
        .collect();

    // Measured against the off-grid probe as well, because the cases that
    // matter most are the ones that vanish on a lattice.
    if swing(&fine).max(swing(&probe)) < MIN_VISIBLE_SWING {
        return None;
    }

    if swing(&coarse) < MIN_VISIBLE_SWING {
        return Some(RingAlias::Blind);
    }

    let recorded = total_variation(&coarse);
    if recorded <= 0.0 {
        // Unreachable: a coarse grid with a visible swing has travelled.
        return Some(RingAlias::Blind);
    }
    let growth = total_variation(&fine) / recorded;
    (growth > MAX_MOTION_GROWTH).then_some(RingAlias::TooFast { growth })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verdict(source: &str) -> Option<RingAlias> {
        let expr = Expr::parse(source).unwrap_or_else(|error| panic!("`{source}`: {error}"));
        ring_alias(&expr, &Env::default())
    }

    /// The shapes the language exists to express, none of which may be
    /// refused. The discontinuous ones are the point: they fail any
    /// smoothness-shaped check forever, and `saw(time)` is a shipped template.
    #[test]
    fn every_legitimate_waveform_survives_the_ring() {
        for source in [
            // The Shine template, and its neighbours.
            "saw(time)",
            "saw(time) - 0.12",
            "saw(time * 3)",
            "saw(time * 12)",
            // Square waves at every duty and rate a blink would use.
            "pulse(time)",
            "pulse(time, 0.03)",
            "pulse(time * 4)",
            "pulse(time * 8)",
            "pulse(time * 16)",
            // Stairs.
            "step(0.5, time)",
            "floor(time * 4)",
            "floor(time * 40)",
            // Ordinary motion, up to and including the last representable rate.
            "0.5 + 0.5 * sin(tau * time)",
            "sin(tau * time * 8)",
            "sin(tau * time * 15)",
            "tri(time * 10)",
            "abs(sin(tau * time * 6))",
            "0.3 + 0.1 * sin(tau * time) + 0.02 * sin(tau * time * 7)",
            "smoothstep(0.2, 0.8, saw(time))",
            // Constants say nothing and are asked nothing.
            "0.5",
            "len * 2",
        ] {
            assert_eq!(verdict(source), None, "`{source}` was refused");
        }
    }

    /// The rates the ring cannot show. The PRD's own example is in here.
    #[test]
    fn motion_the_ring_cannot_record_is_refused() {
        for source in [
            "sin(tau * time * 17)",
            "sin(tau * time * 20)",
            "sin(tau * time * 24)",
            "sin(tau * time * 40)",
            "saw(time * 16)",
            "pulse(time * 20)",
        ] {
            assert!(
                matches!(verdict(source), Some(RingAlias::TooFast { .. })),
                "`{source}` slipped through"
            );
        }
    }

    /// The worst case, and the reason the fine grid is not twice the ring: at
    /// an exact multiple of 32 the value samples to a constant, so the skin
    /// plays nothing at all. A 64-point grid would miss these too and report
    /// no problem whatsoever.
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

    /// The crossover is the ring's own Nyquist frequency, not a number picked
    /// to make the tests pass.
    #[test]
    fn the_crossover_sits_on_the_rings_nyquist_frequency() {
        assert_eq!(verdict("sin(tau * time * 15)"), None);
        assert!(verdict("sin(tau * time * 16)").is_some());
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
