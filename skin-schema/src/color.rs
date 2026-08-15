//! Just enough colour science to hold skins to their promises.
//!
//! Two questions have to be answerable without a browser: "is this readable?"
//! (WCAG relative luminance, the same rule the roster already uses) and "is
//! this the right side's colour?" (hue, in a space where 'same hue, different
//! lightness' actually means that). sRGB HSL fails the second question badly —
//! it thinks a dark navy and a bright cyan are unrelated — so hue comes from
//! OKLCH instead.

/// A parsed `#rrggbb` colour.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rgb {
    pub r: f64,
    pub g: f64,
    pub b: f64,
}

impl Rgb {
    /// Parse a strict 6-digit hex colour. Shorthand and alpha are rejected on
    /// purpose: every colour a skin reports has to be usable verbatim as a CSS
    /// value and as a contrast input.
    pub fn parse(hex: &str) -> Option<Self> {
        let body = hex.strip_prefix('#')?;
        if body.len() != 6 || !body.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let channel = |offset: usize| -> f64 {
            f64::from(u8::from_str_radix(&body[offset..offset + 2], 16).unwrap_or(0)) / 255.0
        };
        Some(Self {
            r: channel(0),
            g: channel(2),
            b: channel(4),
        })
    }

    pub fn to_hex(self) -> String {
        let byte = |value: f64| (value.clamp(0.0, 1.0) * 255.0).round() as u8;
        format!(
            "#{:02x}{:02x}{:02x}",
            byte(self.r),
            byte(self.g),
            byte(self.b)
        )
    }

    /// WCAG relative luminance.
    pub fn relative_luminance(self) -> f64 {
        fn linear(value: f64) -> f64 {
            if value <= 0.04045 {
                value / 12.92
            } else {
                ((value + 0.055) / 1.055).powf(2.4)
            }
        }
        0.2126 * linear(self.r) + 0.7152 * linear(self.g) + 0.0722 * linear(self.b)
    }

    /// Hue in degrees and chroma, via OKLab.
    pub fn oklch_hue_chroma(self) -> (f64, f64) {
        fn linear(value: f64) -> f64 {
            if value <= 0.04045 {
                value / 12.92
            } else {
                ((value + 0.055) / 1.055).powf(2.4)
            }
        }
        let (r, g, b) = (linear(self.r), linear(self.g), linear(self.b));

        let l = (0.412_221_470_8 * r + 0.536_332_536_3 * g + 0.051_445_992_9 * b).cbrt();
        let m = (0.211_903_498_2 * r + 0.680_699_545_1 * g + 0.107_396_956_6 * b).cbrt();
        let s = (0.088_302_461_9 * r + 0.281_718_837_6 * g + 0.629_978_700_5 * b).cbrt();

        let a = 1.977_998_495_1 * l - 2.428_592_205 * m + 0.450_593_709_9 * s;
        let bb = 0.025_904_037_1 * l + 0.782_771_766_2 * m - 0.808_675_766 * s;

        let hue = bb.atan2(a).to_degrees().rem_euclid(360.0);
        (hue, (a * a + bb * bb).sqrt())
    }
}

/// Contrast ratio between two colours, WCAG style.
pub fn contrast_ratio(first: Rgb, second: Rgb) -> f64 {
    let (a, b) = (first.relative_luminance(), second.relative_luminance());
    (a.max(b) + 0.05) / (a.min(b) + 0.05)
}

/// An allowed hue range, in OKLCH degrees, wrapping through 0 where needed.
#[derive(Clone, Copy, Debug)]
pub struct HueWindow {
    pub from: f64,
    pub to: f64,
}

impl HueWindow {
    pub fn contains(self, hue: f64) -> bool {
        if self.from <= self.to {
            hue >= self.from && hue <= self.to
        } else {
            hue >= self.from || hue <= self.to
        }
    }
}

/// Friendly colours must read cool. Measured from the shipped blues, then
/// widened enough to allow teal and violet without letting anything reach red.
pub const FRIENDLY_HUES: HueWindow = HueWindow {
    from: 170.0,
    to: 320.0,
};

/// Enemy colours must read warm.
pub const ENEMY_HUES: HueWindow = HueWindow {
    from: 340.0,
    to: 90.0,
};

/// Below this chroma a colour is effectively gray, and a hue window would be
/// meaningless — so grays are judged by lightness instead of hue.
pub const NEUTRAL_CHROMA: f64 = 0.045;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_rejects_anything_ambiguous() {
        assert_eq!(
            Rgb::parse("#3c8dde").map(Rgb::to_hex).as_deref(),
            Some("#3c8dde")
        );
        assert_eq!(Rgb::parse("3c8dde"), None, "the hash is required");
        assert_eq!(Rgb::parse("#abc"), None, "shorthand is ambiguous");
        assert_eq!(Rgb::parse("#3c8ddeff"), None, "alpha is not a skin colour");
        assert_eq!(Rgb::parse("#gggggg"), None);
    }

    /// The shipped palette is the calibration: whatever the windows are, the
    /// colours players already see have to sit inside them.
    #[test]
    fn shipped_team_colours_sit_inside_their_hue_windows() {
        for blue in ["#70bfe3", "#3c8dde", "#5299bb", "#286eae"] {
            let (hue, chroma) = Rgb::parse(blue).expect("valid hex").oklch_hue_chroma();
            assert!(chroma > NEUTRAL_CHROMA, "{blue} is not a gray");
            assert!(
                FRIENDLY_HUES.contains(hue),
                "{blue} reads at {hue:.0}deg, outside the friendly window"
            );
        }

        for red in ["#ff6b6b", "#e34e5b", "#b84444", "#a92f3a"] {
            let (hue, chroma) = Rgb::parse(red).expect("valid hex").oklch_hue_chroma();
            assert!(chroma > NEUTRAL_CHROMA, "{red} is not a gray");
            assert!(
                ENEMY_HUES.contains(hue),
                "{red} reads at {hue:.0}deg, outside the enemy window"
            );
        }
    }

    /// The rule that matters: no colour may satisfy both windows, or "friendly"
    /// and "enemy" stop meaning anything.
    #[test]
    fn the_two_windows_never_overlap() {
        for degrees in 0..3600 {
            let hue = f64::from(degrees) / 10.0;
            assert!(
                !(FRIENDLY_HUES.contains(hue) && ENEMY_HUES.contains(hue)),
                "{hue} would be legal for both sides"
            );
        }
    }

    #[test]
    fn contrast_matches_the_wcag_extremes() {
        let white = Rgb::parse("#ffffff").unwrap();
        let black = Rgb::parse("#000000").unwrap();
        assert!((contrast_ratio(white, black) - 21.0).abs() < 0.01);
        assert!((contrast_ratio(white, white) - 1.0).abs() < 0.001);
    }
}
