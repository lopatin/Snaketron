//! Sprite atlases.
//!
//! One atlas per skin, fetched once and held for the process lifetime.
//!
//! Delivery is a **versioned relative URL, not `include_bytes!`**. Embedding
//! pixels in the wasm binary inflates every player's initial download with art
//! for skins they are not wearing, and wasm is not compressed as well as PNG
//! already is (`specs/skin-shading-prd.md` section 8.1). The descriptor stays
//! embedded so the catalogue is known without a fetch; only pixels are fetched,
//! and only for skins actually in a match.
//!
//! Decoding is asynchronous and painting is not, which is the constraint that
//! shapes everything here. A skin whose atlas has not finished decoding paints
//! its **fallback layers** — a schema requirement rather than a nicety, because
//! a mid-match join must not show a blank snake. The store therefore answers
//! "is this ready?" synchronously and never blocks, and every image layer is
//! written to be skippable.
//!
//! The fallback is not a property of a region. It is simply the layer beneath:
//! a skin's art sits on top of a solid or tiled base, so an atlas that never
//! arrives leaves that base showing. This is the same mechanism that handles a
//! body too short for a sprite's span (`specs/skin-shading-prd.md` section 8.3),
//! and having one mechanism rather than two is why an image layer needs no
//! conditional topology and keeps its op sequence.
//!
//! Natively — in the golden and conformance suites — no image is ever ready.
//! That is deliberate: the no-atlas path is the one that has to be correct on
//! every skin, so it is the one the test suite exercises by default.
//!
//! **Fetching is lazy, and that is a product decision rather than an
//! optimisation.** A skin's pixels are requested the first time that skin
//! actually paints, so a player wearing one texture does not download the other
//! two. The cost is that a surface which paints *once* — a roster glyph, a
//! contact-sheet tile — can render before the pixels arrive and never look
//! again. [`any_pending`] is what those surfaces wait on; the arena, which
//! repaints every frame, needs nothing.

// `image_count`/`region_count` exist for registration-time validation and for
// tests; the paint path reaches regions by index. See the same note in
// `skin::layer`.
#![allow(dead_code)]

/// One skin's atlas: the images it draws from, and the rectangles inside them.
///
/// Images are named by **versioned relative URL**, resolved against the page's
/// `<base href>` so an embedded build under a non-root path finds them too.
/// A URL is turned into a store handle at most once, on the first frame that
/// wants it, which is what keeps the fetch lazy without making the skin carry
/// mutable state.
#[derive(Debug, Default)]
pub struct Atlas {
    images: Vec<Image>,
    regions: Vec<AtlasRegion>,
}

/// One atlas image, and the store handle it resolves to once requested.
#[derive(Debug)]
struct Image {
    url: String,
    handle: std::sync::OnceLock<usize>,
}

impl Atlas {
    /// An atlas over `urls`, whose regions index into that list by position.
    pub fn new(urls: impl IntoIterator<Item = String>, regions: Vec<AtlasRegion>) -> Self {
        Self {
            images: urls
                .into_iter()
                .map(|url| Image {
                    url,
                    handle: std::sync::OnceLock::new(),
                })
                .collect(),
            regions,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty() && self.images.is_empty()
    }

    pub fn image_count(&self) -> usize {
        self.images.len()
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    pub fn region(&self, index: usize) -> Option<&AtlasRegion> {
        self.regions.get(index)
    }

    /// The store handle for one image, requesting it if this is the first ask.
    ///
    /// Painting is synchronous and decoding is not, so this never waits: it
    /// hands back a handle that may not be drawable yet, and the blit is a
    /// no-op until it is.
    pub fn handle(&self, image: usize) -> Option<usize> {
        let image = self.images.get(image)?;
        Some(*image.handle.get_or_init(|| request(&image.url)))
    }
}

/// A named rectangle inside an atlas.
///
/// Regions must be **padded by at least one transparent texel** on every side.
/// Arc lengths are not integers, so fractional source coordinates are
/// unavoidable, and bilinear sampling will otherwise pull a neighbouring
/// region's pixels in along the seams.
///
/// A region tiled along the body ([`crate::skin::layer::Fit::Tile`]) is the one
/// exception on its left and right edges, and it has to be: padding there would
/// put a transparent gap between every repeat. Such a region must instead reach
/// the image's own edges and be authored to **wrap** — the pixels at `x = 0`
/// continuing the pixels at `x = w - 1` — so the clamp canvas applies at an
/// image edge lands on matching colour instead of a seam.
///
/// A sprite sheet's rows are the same exception in `y`, and there the bleed is
/// not merely tolerable but wanted: a row is downsampled hard — sixty source
/// pixels into a fifteen-pixel cell — so a neighbouring row inevitably mixes
/// in, and neighbouring rows are *adjacent moments of the same animation*.
/// What would be a stranger's pixels in an atlas is a frame of motion blur
/// here. Padding the rows apart would replace it with transparent gaps.
#[derive(Clone, Debug, PartialEq)]
pub struct AtlasRegion {
    /// Index into the skin's image list.
    pub image: usize,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    /// Equally sized frames animated by moving the source rectangle. One
    /// `drawImage` with different arguments, so op-count invariance is
    /// satisfied by construction.
    pub frames: Option<FrameStrip>,
}

/// Which way a region's frames are laid out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameAxis {
    /// Side by side along `x`. Each frame is a small whole picture and the
    /// region's width is divided between them — a sprite that plays.
    X,
    /// Stacked down `y`: **one row per moment**, each row spanning the full
    /// width. This is the sprite-sheet layout, and it is a different shape of
    /// art from the one above rather than the same idea rotated. A row is a
    /// whole snake's worth of skin — `x` is distance along the body, from the
    /// head — so playing the rows in order animates the coat *in place*
    /// instead of moving a picture around.
    Y,
}

/// Frames laid out inside one region, and how fast they play.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrameStrip {
    pub count: usize,
    /// Turns through the animation cycle per full pass over the strip.
    pub cycles: f64,
    pub axis: FrameAxis,
}

/// Rows in a sprite sheet when the author has not said otherwise.
///
/// A square sheet at this many rows is also, and not by accident, this many
/// cells long: one row is one cell tall, so the sheet's own aspect makes
/// `Fit::Tile`'s default repeat and `Fit::Clip`'s natural scale agree without
/// either being told the number.
pub const DEFAULT_SPRITE_ROWS: usize = 20;

impl FrameStrip {
    /// A sprite sheet: `count` rows, one cycle per animation period.
    pub const fn rows(count: usize) -> Self {
        Self {
            count,
            cycles: 1.0,
            axis: FrameAxis::Y,
        }
    }
}

impl AtlasRegion {
    /// A whole image read as a sprite sheet of `rows` rows.
    pub fn sheet(image: usize, width: f64, height: f64, rows: usize) -> Self {
        Self {
            image,
            x: 0.0,
            y: 0.0,
            width,
            height,
            frames: Some(FrameStrip::rows(rows)),
        }
    }

    /// How many cells of body one frame of this region covers at its own scale.
    ///
    /// One row is one cell across the body, so a frame's aspect *is* its length
    /// in cells. Both `Fit::Clip` and `Fit::Tile` derive their scale from this,
    /// which is why a square sheet needs no measurements written down anywhere.
    pub fn frame_cells(&self, time: f64) -> f64 {
        let (_, _, width, height) = self.source_rect(time);
        width / height.max(1e-6)
    }

    /// The source rectangle for one moment in the animation cycle.
    ///
    /// `time` is in turns. A region with no strip ignores it entirely, which is
    /// what makes a still sprite genuinely still under reduced motion rather
    /// than merely slow.
    pub fn source_rect(&self, time: f64) -> (f64, f64, f64, f64) {
        let Some(strip) = self.frames else {
            return (self.x, self.y, self.width, self.height);
        };
        let count = strip.count.max(1);
        let phase = (time * strip.cycles).rem_euclid(1.0);
        let index = ((phase * count as f64).floor() as usize).min(count - 1) as f64;
        match strip.axis {
            FrameAxis::X => {
                let frame_width = self.width / count as f64;
                (
                    self.x + index * frame_width,
                    self.y,
                    frame_width,
                    self.height,
                )
            }
            FrameAxis::Y => {
                let frame_height = self.height / count as f64;
                (
                    self.x,
                    self.y + index * frame_height,
                    self.width,
                    frame_height,
                )
            }
        }
    }
}

/// Whether an atlas image has finished decoding.
///
/// Everything outside a browser answers `false`, so the fallback path is what
/// the native suites exercise.
#[cfg(not(target_arch = "wasm32"))]
pub fn is_ready(_image: usize) -> bool {
    false
}

#[cfg(not(target_arch = "wasm32"))]
pub fn request(_url: &str) -> usize {
    0
}

/// Whether any requested image is still in flight.
///
/// Natively this is always `false`, and the two answers are consistent rather
/// than contradictory: nothing outside a browser ever *starts* a fetch, so
/// nothing is pending and the fallback is the permanent state. A surface that
/// paints once waits on this; the arena, which repaints continuously, does not.
#[cfg(not(target_arch = "wasm32"))]
pub fn any_pending() -> bool {
    false
}

#[cfg(target_arch = "wasm32")]
pub use browser::{any_pending, image_element, is_ready, request};

/// A stub so the non-wasm build can still name the symbol the painter calls.
#[cfg(not(target_arch = "wasm32"))]
pub fn image_element(_image: usize) -> Option<web_sys::HtmlImageElement> {
    None
}

#[cfg(target_arch = "wasm32")]
mod browser {
    use std::cell::RefCell;
    use wasm_bindgen::JsCast;

    struct Entry {
        url: String,
        element: web_sys::HtmlImageElement,
    }

    // Single-threaded by construction: wasm has one thread, and the store is
    // only ever touched from it.
    thread_local! {
        static IMAGES: RefCell<Vec<Entry>> = const { RefCell::new(Vec::new()) };
    }

    /// Start loading an atlas image, or return the handle for one already
    /// requested. Never blocks, never fails loudly: an atlas that does not
    /// arrive leaves every layer that wanted it on its fallback.
    pub fn request(url: &str) -> usize {
        IMAGES.with(|images| {
            let mut images = images.borrow_mut();
            if let Some(index) = images.iter().position(|entry| entry.url == url) {
                return index;
            }
            let element = web_sys::window()
                .and_then(|window| window.document())
                .and_then(|document| document.create_element("img").ok())
                .and_then(|element| element.dyn_into::<web_sys::HtmlImageElement>().ok());
            let Some(element) = element else {
                // No document (a worker, a test harness): behave exactly like a
                // never-arriving atlas rather than inventing an error path.
                images.push(Entry {
                    url: url.to_string(),
                    element: web_sys::HtmlImageElement::new().expect("image element"),
                });
                return images.len() - 1;
            };
            // Anonymous CORS *before* the src, because the attribute has to be
            // set when the fetch starts to have any effect. A generated texture
            // is served from the API origin while the game is served from its
            // own, and drawing a cross-origin image without this taints the
            // canvas — after which anything that reads pixels back throws a
            // SecurityError. The byte route already answers
            // `access-control-allow-origin: *`, so this costs a header and
            // buys a canvas that can still be read.
            element.set_cross_origin(Some("anonymous"));
            element.set_src(url);
            images.push(Entry {
                url: url.to_string(),
                element,
            });
            images.len() - 1
        })
    }

    /// Decoded and non-empty. `complete` alone is true for a failed load too,
    /// so the dimensions are what actually distinguish "ready" from "gave up".
    pub fn is_ready(image: usize) -> bool {
        IMAGES.with(|images| {
            images
                .borrow()
                .get(image)
                .is_some_and(|entry| entry.element.complete() && entry.element.natural_width() > 0)
        })
    }

    pub fn image_element(image: usize) -> Option<web_sys::HtmlImageElement> {
        IMAGES.with(|images| {
            images
                .borrow()
                .get(image)
                .map(|entry| entry.element.clone())
        })
    }

    /// Whether any requested image has neither decoded nor given up.
    ///
    /// `complete` covers both outcomes, which is what makes this settle rather
    /// than hang: an atlas that 404s stops being pending, and the surface
    /// waiting on it repaints once and keeps its fallback.
    pub fn any_pending() -> bool {
        IMAGES.with(|images| {
            images
                .borrow()
                .iter()
                .any(|entry| !entry.element.complete())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strip(count: usize, cycles: f64) -> AtlasRegion {
        AtlasRegion {
            image: 0,
            x: 10.0,
            y: 20.0,
            width: 80.0,
            height: 16.0,
            frames: Some(FrameStrip {
                count,
                cycles,
                axis: FrameAxis::X,
            }),
        }
    }

    /// A sprite sheet walks *down* its rows: each row is a whole body's worth
    /// of skin, so the source rectangle is full width and one row tall.
    #[test]
    fn a_sprite_sheet_walks_its_rows_in_order_and_stays_in_bounds() {
        let sheet = AtlasRegion::sheet(0, 1000.0, 500.0, 20);
        assert_eq!(sheet.source_rect(0.0), (0.0, 0.0, 1000.0, 25.0));
        assert_eq!(sheet.source_rect(0.05), (0.0, 25.0, 1000.0, 25.0));
        assert_eq!(sheet.source_rect(0.5), (0.0, 250.0, 1000.0, 25.0));
        // Rows are visited in order, once each, and the cycle closes.
        let rows: Vec<f64> = (0..20)
            .map(|step| sheet.source_rect(step as f64 / 20.0).1)
            .collect();
        assert_eq!(
            rows,
            (0..20).map(|row| row as f64 * 25.0).collect::<Vec<_>>()
        );
        assert_eq!(sheet.source_rect(1.0), sheet.source_rect(0.0));
        for time in [0.0, 0.999_999, 7.25, -2.3] {
            let (_, y, _, height) = sheet.source_rect(time);
            assert!(
                y >= 0.0 && y + height <= 500.0,
                "row at {time} left the sheet: {y} + {height}"
            );
        }
    }

    /// The property that lets a square sheet carry no measurements: one row is
    /// one cell tall, so a frame's aspect is its length in cells. Twenty rows
    /// of a square sheet is twenty cells of body, and both fits agree on it.
    #[test]
    fn a_square_sheet_is_as_many_cells_long_as_it_has_rows() {
        for rows in [4, 12, DEFAULT_SPRITE_ROWS, 32] {
            let sheet = AtlasRegion::sheet(0, 1248.0, 1248.0, rows);
            assert!(
                (sheet.frame_cells(0.0) - rows as f64).abs() < 1e-9,
                "a square sheet of {rows} rows should be {rows} cells long"
            );
        }
        // A non-square sheet is believed rather than corrected: the author
        // chose the aspect, and `cells_per_repeat` is there to override it.
        let wide = AtlasRegion::sheet(0, 2000.0, 1000.0, 20);
        assert!((wide.frame_cells(0.0) - 40.0).abs() < 1e-9);
    }

    /// A frame strip is one `drawImage` whose source rectangle moves. Nothing
    /// about the emitted op changes with the clock except its arguments, which
    /// is why this is the sanctioned way to animate sprite art.
    #[test]
    fn a_frame_strip_walks_the_source_rectangle_and_stays_in_bounds() {
        let region = strip(4, 1.0);
        assert_eq!(region.source_rect(0.0), (10.0, 20.0, 20.0, 16.0));
        assert_eq!(region.source_rect(0.25), (30.0, 20.0, 20.0, 16.0));
        assert_eq!(region.source_rect(0.75), (70.0, 20.0, 20.0, 16.0));
        // The cycle closes rather than running off the end of the strip.
        assert_eq!(region.source_rect(1.0), region.source_rect(0.0));
        // ...including for clocks that have no business being here.
        assert_eq!(region.source_rect(-0.25), (70.0, 20.0, 20.0, 16.0));
        for time in [0.0, 0.999_999, 12.5, -3.7] {
            let (x, _, width, _) = region.source_rect(time);
            assert!(
                x >= 10.0 && x + width <= 90.0,
                "frame at {time} left the region: {x} + {width}"
            );
        }
    }

    #[test]
    fn a_region_without_a_strip_ignores_the_clock_entirely() {
        let mut region = strip(4, 1.0);
        region.frames = None;
        assert_eq!(region.source_rect(0.0), (10.0, 20.0, 80.0, 16.0));
        assert_eq!(region.source_rect(0.37), region.source_rect(0.0));
    }

    /// A single-frame strip is a still sprite, not a division by zero.
    #[test]
    fn a_degenerate_strip_still_produces_a_valid_rectangle() {
        let region = strip(1, 3.0);
        assert_eq!(region.source_rect(0.6), (10.0, 20.0, 80.0, 16.0));
        let zero = strip(0, 1.0);
        assert_eq!(zero.source_rect(0.5), (10.0, 20.0, 80.0, 16.0));
    }

    /// Natively nothing is ever ready, so the fallback path is the one the
    /// golden and conformance suites exercise on every skin.
    #[test]
    fn no_atlas_is_ready_outside_a_browser() {
        assert!(!is_ready(request("skins/example/atlas.v1.png")));
        assert!(image_element(0).is_none());
        assert!(
            !any_pending(),
            "nothing outside a browser ever starts a fetch, so nothing may \
             report as still arriving"
        );
    }

    /// A URL becomes a store handle at most once. Resolving per frame would
    /// turn a lazy fetch into a lookup on the hot path, and the handle is
    /// exactly the kind of thing a skin must not have to hold mutably.
    #[test]
    fn an_atlas_resolves_each_image_once_and_only_on_demand() {
        let atlas = Atlas::new(
            ["images/skins/example.v1.png".to_string()],
            vec![AtlasRegion {
                image: 0,
                x: 0.0,
                y: 0.0,
                width: 96.0,
                height: 16.0,
                frames: None,
            }],
        );

        assert_eq!(atlas.image_count(), 1);
        assert_eq!(atlas.region_count(), 1);
        assert!(!atlas.is_empty());
        let first = atlas.handle(0).expect("image 0 exists");
        assert_eq!(atlas.handle(0), Some(first), "the handle is stable");
        assert_eq!(atlas.handle(1), None, "there is no second image");
        assert!(atlas.region(1).is_none());

        assert!(Atlas::default().is_empty());
    }
}
