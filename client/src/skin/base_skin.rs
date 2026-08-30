//! Base skins: the picture a team's endzone is painted with.
//!
//! A base skin is a much smaller thing than a [`crate::skin::SnakeSkin`], and
//! deliberately so. It is a **pair of pictures and a text colour** — the art
//! the endzone is painted with, and the colour the players' names are written
//! in on top of it. That is the whole authored surface.
//!
//! # Why there are two pictures
//!
//! Both teams can equip the same base skin. With one picture each, a mirror
//! match would have two identical endzones. So a skin ships a **home** picture
//! for the end the viewer is defending and an **away** picture for the other,
//! the way a club has two kits, and `the_two_ends_are_tellable_apart` measures
//! the committed pixels to keep the pair genuinely different.
//!
//! Which end is which is a separate question, and the art does not answer it.
//! It cannot be trusted to: a base skin *travels*, so the picture on the far
//! end of the arena was chosen by the people you are playing against, and a
//! picture has no colour field to hold to the friendly-cool/enemy-warm hue
//! windows a [`crate::skin::BaseTheme`] is held to — the same problem
//! `specs/skin-shading-prd.md` records for textured snake bodies.
//!
//! Requiring the art to answer it was tried, and it worked: home cool, away
//! warm, measured on the pixels. It also made every home kit a shade of blue
//! and every away kit a shade of red, which is the theme evaporating. So the
//! answer moved off the art entirely and onto the **goal wall** — painted by
//! the renderer from the *viewer's* own theme, hue-locked, over the top of
//! everything, unreachable by any skin — alongside hue-locked snakes, the names
//! written on each endzone, and where a player spawns.
//!
//! # Why this is not a `SnakeSkin`
//!
//! The nineteen catalogue skins each carry a [`crate::skin::BaseTheme`]: six
//! hex colours that recolour the endzone tint, the goal wall, and the endzone
//! lettering. That is still here, still viewer-attributed, and still what a
//! base looks like when no base skin is dressing it. A base skin is a *second,
//! independent* layer painted on top of it, for three reasons:
//!
//! - It is a picture, and `BaseTheme` is six strings. Widening `BaseTheme`
//!   would break the schema-descriptor exhaustiveness guards, every one of the
//!   hand-built `BaseThemeOwned` sites, and the committed golden trace — for
//!   nineteen skins that would all have to declare "no picture".
//! - It is attributed to the **team that owns the endzone**, not to the
//!   viewer, so it travels to every client. `BaseTheme` never leaves the
//!   viewer's own screen.
//! - Its home/away split is the *viewer's* question, answered by the renderer.
//!   A base skin is handed a fully resolved side and never gets to decide which
//!   end is which, exactly as `specs/skins-prd.md` section 7 ruling 11 requires.
//!
//! # Delivery
//!
//! Banners are fetched as versioned relative URLs through the same
//! [`crate::skin::atlas`] image store the textured snake skins use, with the
//! same consequences: the fetch is lazy and starts on the first frame that
//! wants that exact picture, painting is synchronous and never waits, and a
//! picture that has not arrived — or never will — simply leaves the layer
//! beneath showing, which is today's endzone exactly.
//!
//! One banner spans a whole endzone; nothing here tiles. See
//! [`crate::render::paint_base_banner`] for how it is laid along the zone's
//! long axis, and `specs/base-skins-prd.md` section 3.1 for why an earlier
//! tiled version was removed even though it worked.

use crate::skin::atlas;
use std::sync::OnceLock;

/// Which end of the arena a picture is being painted on, from the point of
/// view of whoever is looking.
///
/// Resolved by the renderer from the viewer's own team before a base skin is
/// consulted, so a skin can never reclassify an end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseSide {
    /// The end the viewer is defending.
    Home,
    /// The end the viewer is attacking.
    Away,
}

impl BaseSide {
    /// Which end team `team` owns, for a viewer on `local_team`.
    ///
    /// A spectator has no team and gets the canonical arrangement — team 0 as
    /// home — which is the same answer [`crate::skin::BaseTheme::sides`] gives
    /// them, so the picture and the colours underneath it never disagree about
    /// which end is which.
    pub fn for_team(team: u8, local_team: Option<u8>) -> Self {
        if local_team.unwrap_or(0) == team {
            Self::Home
        } else {
            Self::Away
        }
    }
}

/// One base skin.
pub struct BaseSkin {
    /// The reference that persists on the account and travels in game state.
    pub id: &'static str,
    /// What a player sees in the picker.
    pub name: &'static str,
    /// The picture for the viewer's own end, as a versioned relative URL
    /// resolved against the page's `<base href>` so an embedded build under a
    /// non-root path finds it.
    home_url: &'static str,
    /// The picture for the opposing end.
    away_url: &'static str,
    /// The colour the endzone lettering is filled with.
    ///
    /// One colour for both sides, because it is the team's own name on the
    /// team's own base and the pair is authored to share a value range. The
    /// outline behind those letters is not a second authored colour: it is
    /// derived by [`BaseSkin::text_halo`], because a halo is a readability
    /// mechanism rather than a design choice, and an author who picked it
    /// badly would produce an unreadable name over their own art.
    pub text: &'static str,
    home: OnceLock<usize>,
    away: OnceLock<usize>,
}

impl BaseSkin {
    const fn new(
        id: &'static str,
        name: &'static str,
        home_url: &'static str,
        away_url: &'static str,
        text: &'static str,
    ) -> Self {
        Self {
            id,
            name,
            home_url,
            away_url,
            text,
            home: OnceLock::new(),
            away: OnceLock::new(),
        }
    }

    /// The committed URL of one side's picture.
    ///
    /// Only the tests ask: painting goes through [`Self::handle`], which owns
    /// the URL and the store entry together so no caller can request one
    /// picture and draw another.
    #[cfg(test)]
    pub(crate) fn url(&self, side: BaseSide) -> &'static str {
        match side {
            BaseSide::Home => self.home_url,
            BaseSide::Away => self.away_url,
        }
    }

    /// The image-store handle for one side, starting that fetch if this is the
    /// first ask. Never blocks and never fails.
    ///
    /// The two sides are separate handles on purpose: a match shows the home
    /// picture of one base and the away picture of another, and no client needs
    /// the two it will never draw.
    pub fn handle(&self, side: BaseSide) -> usize {
        let (slot, url) = match side {
            BaseSide::Home => (&self.home, self.home_url),
            BaseSide::Away => (&self.away, self.away_url),
        };
        *slot.get_or_init(|| atlas::request(url))
    }

    /// Whether one side's picture has decoded and can be painted this frame.
    ///
    /// Asking is what *starts* the fetch, the same bargain
    /// [`atlas::Atlas::handle`] strikes: a surface that never paints a base
    /// never downloads one.
    ///
    /// Always `false` outside a browser, so every native test exercises the
    /// no-picture path — the path that has to stay identical to the endzone
    /// that shipped before base skins existed.
    pub fn is_ready(&self, side: BaseSide) -> bool {
        atlas::is_ready(self.handle(side))
    }

    /// The decoded picture for one side, if there is one to paint.
    pub fn image(&self, side: BaseSide) -> Option<web_sys::HtmlImageElement> {
        if !self.is_ready(side) {
            return None;
        }
        atlas::image_element(self.handle(side))
    }

    /// The outline drawn behind the endzone lettering.
    ///
    /// A name is written over a picture, so the outline is the only thing
    /// keeping it legible. Deriving it from the fill rather than authoring it
    /// beside it means the two can never be picked to clash: a light name gets
    /// a near-black halo and a dark name a near-white one, which is the
    /// ordinary outlined-lettering rule and works over any art.
    pub fn text_halo(&self) -> &'static str {
        const LIGHT_HALO: &str = "#f4f7fb";
        const DARK_HALO: &str = "#0b0f14";
        let luminance = skin_schema::color::Rgb::parse(self.text)
            .map(|rgb| rgb.relative_luminance())
            // An unparseable colour is a build bug, not a runtime one, and the
            // tests below catch it. Treating it as light picks the dark halo,
            // which is the safer default over mid-value art.
            .unwrap_or(1.0);
        if luminance > 0.22 {
            DARK_HALO
        } else {
            LIGHT_HALO
        }
    }
}

/// Declare the catalogue and the bytes it ships from one list.
///
/// Two things have to agree: what the registry offers, and what is actually
/// committed under `client/web/public/images/bases/`. Written out twice they
/// drift, and the failure is a blank endzone in production rather than a red
/// build. So the URLs and the `include_bytes!` the tests read are derived from
/// the same line — a skin whose picture is missing does not compile.
macro_rules! base_skins {
    ($(($id:literal, $name:literal, $slug:literal, $text:literal)),+ $(,)?) => {
        /// The base skins compiled into this build, in picker order.
        ///
        /// Pictures are generated by
        /// `client/design/tools/build_base_textures.py` and are build output:
        /// never hand-edited.
        ///
        /// An array rather than a slice reference because [`BaseSkin`] holds a
        /// `OnceLock`, and a `&[..]` static would be a shared borrow of an
        /// interior-mutable temporary, which the compiler refuses.
        pub static BASE_SKINS: [BaseSkin; [$($id),+].len()] = [
            $(BaseSkin::new(
                $id,
                $name,
                concat!("images/bases/", $slug, ".home.v1.png"),
                concat!("images/bases/", $slug, ".away.v1.png"),
                $text,
            )),+
        ];

        /// Every committed picture, read from disk rather than trusted from a
        /// constant.
        #[cfg(test)]
        static COMMITTED: [(&str, &[u8]); [$($id),+].len() * 2] = [
            $(
                (
                    concat!("images/bases/", $slug, ".home.v1.png"),
                    include_bytes!(concat!(
                        "../../web/public/images/bases/", $slug, ".home.v1.png"
                    )),
                ),
                (
                    concat!("images/bases/", $slug, ".away.v1.png"),
                    include_bytes!(concat!(
                        "../../web/public/images/bases/", $slug, ".away.v1.png"
                    )),
                ),
            )+
        ];
    };
}

base_skins![
    ("invaders@1", "Invaders", "invaders", "#ffffff"),
    ("lightcycle@1", "Lightcycle Grid", "lightcycle", "#e8faff"),
    ("python@1", "Python", "python", "#ffffff"),
    ("dragon@1", "Dragon", "dragon", "#d8f4ff"),
    ("sharkbite@1", "Shark Bite", "sharkbite", "#ffd84d"),
    ("aquarium@1", "Aquarium", "aquarium", "#fff3cf"),
    ("surf@1", "Surf", "surf", "#c8ff3c"),
    ("fairway@1", "Fairway", "fairway", "#f4e9d2"),
    ("destroyer@1", "Destroyer", "destroyer", "#fff3e0"),
    ("blockcraft@1", "Blockcraft", "blockcraft", "#fff8e7"),
    ("anime@1", "Anime", "anime", "#eafbff"),
    ("kittens@1", "Kittens", "kittens", "#ffe14d"),
    ("bears@1", "Dancing Bears", "bears", "#ffffff"),
    ("barbershop@1", "Barbershop", "barbershop", "#b6ff3a"),
    ("wizardry@1", "Wizardry", "wizardry", "#dff7ff"),
    ("harvest@1", "Harvest", "harvest", "#2a0f26"),
    ("yuletide@1", "Yuletide", "yuletide", "#fff1c8"),
];

/// The base skin an id names, or `None` for anything this build does not know.
///
/// Unlike [`crate::skin::registry::SkinRegistry::resolve`] this does *not* fall
/// back to a default. There is no default base skin — "no base skin" is a real
/// and common state meaning "paint the endzone the way it has always been
/// painted" — so an unknown id has to stay distinguishable from a known one.
/// Falling back would make an id from a newer build silently paint the wrong
/// team's art.
pub fn resolve_base_skin(id: Option<&str>) -> Option<&'static BaseSkin> {
    let id = id?;
    BASE_SKINS.iter().find(|skin| skin.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use skin_schema::color::{Rgb, contrast_ratio, perceptual_distance};

    fn committed(url: &str) -> &'static [u8] {
        COMMITTED
            .iter()
            .find(|(name, _)| *name == url)
            .unwrap_or_else(|| panic!("{url} is not committed"))
            .1
    }

    /// Mean colour of a picture. Averaging in sRGB rather than in linear light
    /// is deliberate: the questions asked of it — which side does this read as,
    /// is it mid-tone — are about the impression the art gives, which is what
    /// the encoded values already describe.
    fn mean_colour(bytes: &[u8]) -> Rgb {
        let decoded = image::load_from_memory(bytes)
            .expect("a committed base picture must be a decodable image")
            .to_rgb8();
        let (mut r, mut g, mut b) = (0u64, 0u64, 0u64);
        for pixel in decoded.pixels() {
            r += u64::from(pixel[0]);
            g += u64::from(pixel[1]);
            b += u64::from(pixel[2]);
        }
        let total = f64::from(decoded.width()) * f64::from(decoded.height()) * 255.0;
        Rgb {
            r: r as f64 / total,
            g: g as f64 / total,
            b: b as f64 / total,
        }
    }

    /// Mean WCAG luminance and its 98th percentile, over every pixel.
    fn luminance_band(bytes: &[u8]) -> (f64, f64) {
        fn to_linear(channel: u8) -> f64 {
            let value = f64::from(channel) / 255.0;
            if value <= 0.04045 {
                value / 12.92
            } else {
                ((value + 0.055) / 1.055).powf(2.4)
            }
        }

        let decoded = image::load_from_memory(bytes)
            .expect("a committed base picture must be a decodable image")
            .to_rgb8();
        let mut luma: Vec<f64> = decoded
            .pixels()
            .map(|pixel| {
                0.2126 * to_linear(pixel[0])
                    + 0.7152 * to_linear(pixel[1])
                    + 0.0722 * to_linear(pixel[2])
            })
            .collect();
        let mean = luma.iter().sum::<f64>() / luma.len() as f64;
        luma.sort_by(|a, b| a.partial_cmp(b).expect("luminance is never NaN"));
        let top = luma[(luma.len() as f64 * 0.98) as usize];
        (mean, top)
    }

    /// Mean OKLCH chroma, per pixel.
    ///
    /// Averaged over pixels rather than taken from the mean colour: a picture
    /// that is half saturated orange and half saturated teal averages to mud,
    /// and it is not a drab picture.
    fn mean_chroma(bytes: &[u8]) -> f64 {
        let decoded = image::load_from_memory(bytes)
            .expect("a committed base picture must be a decodable image")
            .to_rgb8();
        let total: f64 = decoded
            .pixels()
            .map(|pixel| {
                let rgb = Rgb {
                    r: f64::from(pixel[0]) / 255.0,
                    g: f64::from(pixel[1]) / 255.0,
                    b: f64::from(pixel[2]) / 255.0,
                };
                rgb.oklch_hue_chroma().1
            })
            .sum();
        total / (decoded.width() as f64 * decoded.height() as f64)
    }

    #[test]
    fn ids_are_unique_and_resolvable() {
        for skin in &BASE_SKINS {
            assert!(
                std::ptr::eq(resolve_base_skin(Some(skin.id)).unwrap(), skin),
                "{} did not resolve to itself",
                skin.id
            );
        }
        let mut ids: Vec<&str> = BASE_SKINS.iter().map(|skin| skin.id).collect();
        ids.sort_unstable();
        let count = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), count, "duplicate base skin id");
    }

    #[test]
    fn an_unknown_id_is_absent_rather_than_defaulted() {
        assert!(resolve_base_skin(None).is_none());
        assert!(resolve_base_skin(Some("")).is_none());
        assert!(resolve_base_skin(Some("classic@1")).is_none());
        assert!(resolve_base_skin(Some("from-the-future@9")).is_none());
        // The stored form carries a `base:` prefix; callers strip it, and a
        // caller that forgets must not accidentally match.
        assert!(resolve_base_skin(Some("base:invaders@1")).is_none());
    }

    #[test]
    fn every_picture_is_a_committed_png_and_the_two_sides_differ() {
        assert_eq!(COMMITTED.len(), BASE_SKINS.len() * 2);
        for skin in &BASE_SKINS {
            for side in [BaseSide::Home, BaseSide::Away] {
                assert_eq!(
                    &committed(skin.url(side))[..8],
                    b"\x89PNG\r\n\x1a\n",
                    "{} is not a PNG",
                    skin.url(side)
                );
            }
            assert_ne!(
                committed(skin.url(BaseSide::Home)),
                committed(skin.url(BaseSide::Away)),
                "{} ships the same picture on both ends",
                skin.id
            );
        }
    }

    /// A viewer's own end is `Home` and everything else is `Away`, including
    /// for a spectator, who gets the canonical team-0-is-home arrangement so
    /// the picture agrees with the colour theme underneath it.
    #[test]
    fn sides_follow_the_viewers_team_and_default_to_team_zero() {
        assert_eq!(BaseSide::for_team(0, Some(0)), BaseSide::Home);
        assert_eq!(BaseSide::for_team(1, Some(0)), BaseSide::Away);
        assert_eq!(BaseSide::for_team(0, Some(1)), BaseSide::Away);
        assert_eq!(BaseSide::for_team(1, Some(1)), BaseSide::Home);
        assert_eq!(BaseSide::for_team(0, None), BaseSide::Home);
        assert_eq!(BaseSide::for_team(1, None), BaseSide::Away);
    }

    /// The two ends of an arena must not look like each other.
    ///
    /// Both teams can equip the same base skin, and each viewer then sees the
    /// home picture at their own end and the away picture at the other — so
    /// without this a mirror match would have two identical endzones.
    ///
    /// It deliberately says nothing about *which* is which. An earlier version
    /// did: home had to be cool and away warm, on the reasoning that a picture
    /// cannot be held to the friendly-cool/enemy-warm hue windows a declared
    /// colour is. It worked, and the cost was that every home kit came back a
    /// shade of blue and every away kit a shade of red — the theme stopped
    /// being the theme, which is the entire point of letting a player pick one.
    ///
    /// What carries the side instead is the **goal wall**: the renderer paints
    /// it from the viewer's own [`crate::skin::BaseTheme`], hue-locked, on top
    /// of everything, and no art can reach it. Together with hue-locked snakes,
    /// the names written on each endzone, and where a player spawns, that is
    /// what tells you which end is yours. See `specs/base-skins-prd.md`.
    #[test]
    fn the_two_ends_are_tellable_apart() {
        /// The conformance suite uses 0.15 between two flat colours; a whole
        /// picture averages towards its own middle, so asking the same of a
        /// mean is a stronger requirement than the number suggests.
        const MIN_SIDE_DISTANCE: f64 = 0.10;

        for skin in &BASE_SKINS {
            let home = mean_colour(committed(skin.url(BaseSide::Home)));
            let away = mean_colour(committed(skin.url(BaseSide::Away)));
            let distance = perceptual_distance(home, away);
            assert!(
                distance >= MIN_SIDE_DISTANCE,
                "{}'s two ends are only {distance:.3} apart and would be confusable",
                skin.id
            );
        }
    }

    /// Snakes are painted on top of a base and players score inside it, so a
    /// picture has to live in the middle of the value range.
    ///
    /// Two measurements, because the mean alone is not enough: a dragon cave
    /// that was black corner to corner averaged 0.04 and sailed through a
    /// floor of 0.01, which had only ever been meant to exclude a literally
    /// black image. The percentile is what asks the picture to have a *bright
    /// end* — somewhere in it a dark snake can be seen against — rather than
    /// merely to average above nothing.
    #[test]
    fn every_picture_lives_in_the_middle_of_the_value_range() {
        /// Below this the endzone reads as a hole cut in the arena.
        const MIN_MEAN: f64 = 0.10;
        /// Above this a snake is the dark thing on a bright field, which is
        /// the same problem the other way up.
        const MAX_MEAN: f64 = 0.55;
        /// Where the brightest 2% of the picture has to reach.
        const MIN_TOP: f64 = 0.30;

        for skin in &BASE_SKINS {
            for side in [BaseSide::Home, BaseSide::Away] {
                let (mean, top) = luminance_band(committed(skin.url(side)));
                assert!(
                    (MIN_MEAN..=MAX_MEAN).contains(&mean),
                    "{}'s {side:?} picture averages {mean:.2} luminance, outside \
                     {MIN_MEAN}-{MAX_MEAN}",
                    skin.id
                );
                assert!(
                    top >= MIN_TOP,
                    "{}'s {side:?} picture never gets brighter than {top:.2}; \
                     the whole thing is in shadow",
                    skin.id
                );
            }
        }
    }

    /// A banner has to have a palette.
    ///
    /// Its own gate rather than a note on the value one, because the two pull
    /// against each other: told to stay mid-tone, a generator reaches for grey.
    /// The first batch came back — in the words of the person who asked for
    /// them — "washed out blue" and "washed out green", and every one of those
    /// passed the value band comfortably. Nothing about a snake staying
    /// readable requires the background to be drab.
    #[test]
    fn no_banner_is_washed_out() {
        /// Calibrated against that batch: the light-cycle grid measured 0.034
        /// and the dragon 0.056, while the ones that read as having a palette
        /// were 0.10 and up.
        const MIN_CHROMA: f64 = 0.070;

        for skin in &BASE_SKINS {
            for side in [BaseSide::Home, BaseSide::Away] {
                let chroma = mean_chroma(committed(skin.url(side)));
                assert!(
                    chroma >= MIN_CHROMA,
                    "{}'s {side:?} banner averages {chroma:.3} chroma, below {MIN_CHROMA}; \
                     it has come back washed out",
                    skin.id
                );
            }
        }
    }

    /// A name has to be readable over the picture it sits on. The halo is what
    /// guarantees that, so the pair has to be genuinely far apart.
    #[test]
    fn every_text_colour_is_a_hex_far_from_its_halo() {
        for skin in &BASE_SKINS {
            let text = Rgb::parse(skin.text)
                .unwrap_or_else(|| panic!("{} has an unparseable text colour", skin.id));
            let halo = Rgb::parse(skin.text_halo()).expect("halo is a literal");
            let contrast = contrast_ratio(text, halo);
            assert!(
                contrast >= 4.5,
                "{} reads at only {contrast:.1}:1 against its halo",
                skin.id
            );
        }
    }

    /// Nothing outside a browser can decode an image, so every native surface
    /// must take the no-picture path. This pins that, because the alternative —
    /// a test that silently exercises the browser path natively — is how a
    /// broken fallback ships.
    #[test]
    fn no_picture_is_ever_ready_outside_a_browser() {
        for skin in &BASE_SKINS {
            for side in [BaseSide::Home, BaseSide::Away] {
                assert!(
                    !skin.is_ready(side),
                    "{} claimed a decoded {side:?} picture natively",
                    skin.id
                );
                assert!(skin.image(side).is_none());
            }
        }
    }
}
