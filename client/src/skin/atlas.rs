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

// The compositor's surface runs ahead of the skins that ship on it: every item
// below is implemented and covered by the test suite, but the six catalogue
// skins are all the classic-shaped stack, so the non-test build never reaches
// them. That is the intended state after `specs/skin-shading-prd.md` S7-S8 —
// the engine gained spans, tiling, images and corner policies before any
// first-party skin needed them. Deleting them to satisfy the lint would delete
// the features; the alternative is to say so here.
#![allow(dead_code)]

/// A named rectangle inside an atlas.
///
/// Regions must be **padded by at least one transparent texel** on every side.
/// Arc lengths are not integers, so fractional source coordinates are
/// unavoidable, and bilinear sampling will otherwise pull a neighbouring
/// region's pixels in along the seams.
#[derive(Clone, Debug, PartialEq)]
pub struct AtlasRegion {
    /// Index into the skin's image list.
    pub image: usize,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    /// A strip of equally sized frames laid out along `x`, animated by moving
    /// the source rectangle. One `drawImage` with different arguments, so
    /// op-count invariance is satisfied by construction.
    pub frames: Option<FrameStrip>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrameStrip {
    pub count: usize,
    /// Turns through the animation cycle per full pass over the strip.
    pub cycles: f64,
}

impl AtlasRegion {
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
        let frame_width = self.width / count as f64;
        let phase = (time * strip.cycles).rem_euclid(1.0);
        let index = ((phase * count as f64).floor() as usize).min(count - 1);
        (
            self.x + index as f64 * frame_width,
            self.y,
            frame_width,
            self.height,
        )
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

#[cfg(target_arch = "wasm32")]
pub use browser::{image_element, is_ready, request};

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
            frames: Some(FrameStrip { count, cycles }),
        }
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
    }
}
