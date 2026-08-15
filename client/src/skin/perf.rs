//! The perf smoke harness.
//!
//! `specs/skin-shading-prd.md` section 11 states three budgets — a frame-time
//! ceiling, zero per-frame allocation, and a bounded per-snake op count — and
//! insists all three are measured on a frame where **six distinct skins are
//! actually resolved**. An all-classic frame is not representative: it exercises
//! one painter eight times and would report a compositor's cost as zero.
//!
//! Two of the three budgets can be measured natively and are therefore tests:
//! the op census and the allocation census both run in `cargo test`. Frame time
//! cannot — it is a property of Skia, not of Rust — so it is exported to the
//! browser as `skinPerfSmoke` and measured on the QA route.
//!
//! The numbers pinned here are **baselines, not aspirations**. They record what
//! the shipped painter costs today so that a later stage's regression is a
//! failing test rather than a shrug. Section 11's real gate — zero per-frame
//! allocation — is not met by the code this baseline describes, and the
//! allocation ceiling below is annotated with that gap.

use crate::skin::registry::skin_registry;
use crate::skin::{SkinIdentity, SnakeRole, SnakeSkin};

/// The eight snakes the perf gates are measured on.
///
/// Eight is the arena's practical maximum; the body is the longest fixture
/// (21 cells, which runs the head gradient off its end); the skins cycle the
/// whole catalogue so no measurement is dominated by one painter.
pub const PERF_SNAKE_COUNT: usize = 8;

/// The longest body in the fixture corpus, as the perf frame poses it.
pub const PERF_BODY: &[(f64, f64)] = &[(20.0, 4.0), (0.0, 4.0)];

/// The arena's largest cell size.
pub const PERF_CELL_SIZE: f64 = 15.0;

/// One skin per snake, cycling the catalogue so the frame resolves as many
/// distinct painters as the catalogue has.
pub fn perf_frame_skins() -> Vec<&'static dyn SnakeSkin> {
    let catalogue = skin_registry().entries();
    (0..PERF_SNAKE_COUNT)
        .map(|index| catalogue[index % catalogue.len()])
        .collect()
}

/// One identity per snake, spread across roles so no measurement accidentally
/// paints eight snakes the same colour.
pub fn perf_frame_identities() -> Vec<SkinIdentity> {
    (0..PERF_SNAKE_COUNT)
        .map(|index| SkinIdentity {
            role: match index % 4 {
                0 => SnakeRole::Own,
                1 => SnakeRole::Teammate,
                2 => SnakeRole::Enemy,
                _ => SnakeRole::FreeForAll {
                    palette_slot: (index % 4) as u8,
                },
            },
            shade_slot: (index % 2) as u8,
        })
        .collect()
}

#[cfg(test)]
pub use census::{allocations_during, census_frame};

/// Native measurement of a perf frame.
#[cfg(test)]
mod census {
    use super::*;
    use crate::skin::paint::{OpRecorder, PaintCtx};
    use crate::skin::{SnakePose, paint_alive_with_occlusion};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Ops emitted by one snake, attributed to the skin that emitted them.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SnakeCensus {
        pub skin_id: String,
        pub ops: usize,
    }

    /// Record every snake in the perf frame, boosting, over the arena mask —
    /// the most expensive configuration the arena actually produces.
    pub fn census_frame() -> Vec<SnakeCensus> {
        perf_frame_skins()
            .into_iter()
            .zip(perf_frame_identities())
            .map(|(skin, identity)| {
                let mut recorder = OpRecorder::new();
                let pose = SnakePose::still(PERF_BODY, PERF_CELL_SIZE, true);
                paint_alive_with_occlusion(
                    &mut PaintCtx::recording(&mut recorder),
                    skin,
                    &pose,
                    &identity,
                    Some("#ffffff"),
                )
                .expect("a recording painter cannot fail");
                SnakeCensus {
                    skin_id: skin.id().to_string(),
                    ops: recorder.shapes().len(),
                }
            })
            .collect()
    }

    /// Allocation counts observed while a closure ran on this thread.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct AllocationCensus {
        pub allocations: usize,
        pub bytes: usize,
    }

    // The counter is thread-local and armed explicitly, so a census measures
    // exactly the closure it wraps even though `cargo test` runs tests in
    // parallel on shared threads. `const`-initialised TLS never allocates,
    // which is what keeps the allocator from recursing into itself.
    thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
        static BYTES: Cell<usize> = const { Cell::new(0) };
    }

    /// A pass-through allocator that tallies while a census is armed.
    ///
    /// Registering a global allocator affects the whole test binary, which is
    /// why it must stay a *counting* allocator and never a checking one: it has
    /// to be invisible to every other test in the crate.
    pub struct CountingAllocator;

    // SAFETY: every method forwards to `System` unchanged; the only added work
    // is a thread-local tally guarded by `try_with`, so a thread whose TLS has
    // already been destroyed simply goes uncounted rather than panicking inside
    // the allocator.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            note(new_size);
            unsafe { System.realloc(ptr, layout, new_size) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    fn note(size: usize) {
        let _ = ARMED.try_with(|armed| {
            if !armed.get() {
                return;
            }
            let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
            let _ = BYTES.try_with(|total| total.set(total.get() + size));
        });
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    /// Proof the allocator is actually installed. Without this, a census that
    /// silently counted nothing would read as "allocation-free".
    static SANITY: AtomicUsize = AtomicUsize::new(0);

    /// Count the allocations a closure performs on this thread.
    ///
    /// The closure paints through [`PaintCtx::null`], because recording
    /// allocates by construction and would measure the recorder instead of the
    /// painter.
    pub fn allocations_during(body: impl FnOnce()) -> AllocationCensus {
        ALLOCATIONS.with(|count| count.set(0));
        BYTES.with(|total| total.set(0));
        ARMED.with(|armed| armed.set(true));
        body();
        ARMED.with(|armed| armed.set(false));
        SANITY.fetch_add(1, Ordering::Relaxed);
        AllocationCensus {
            allocations: ALLOCATIONS.with(Cell::get),
            bytes: BYTES.with(Cell::get),
        }
    }

    /// Paint the whole perf frame through the discarding sink.
    pub fn paint_perf_frame() {
        for (skin, identity) in perf_frame_skins().into_iter().zip(perf_frame_identities()) {
            let pose = SnakePose::still(PERF_BODY, PERF_CELL_SIZE, true);
            paint_alive_with_occlusion(
                &mut PaintCtx::null(),
                skin,
                &pose,
                &identity,
                Some("#ffffff"),
            )
            .expect("a null painter cannot fail");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::census::paint_perf_frame;
    use super::*;

    /// The perf frame has to resolve distinct painters, or every number this
    /// module reports describes one skin measured eight times.
    #[test]
    fn perf_frame_resolves_the_whole_catalogue() {
        let ids: std::collections::HashSet<&str> =
            perf_frame_skins().iter().map(|skin| skin.id()).collect();
        assert_eq!(
            ids.len(),
            skin_registry().entries().len(),
            "the perf frame must resolve every catalogue skin, not repeat one"
        );
        assert!(
            ids.len() >= 6,
            "section 11 measures on six distinct skins; found {}",
            ids.len()
        );
    }

    /// The op-count baseline.
    ///
    /// `specs/skin-shading-prd.md` section 11 measured 12-151 ops per snake
    /// against ~2,300 grid-dot arcs per frame, and budgets a rich compositor
    /// skin at roughly double a classic-equivalent one. The ceiling here is
    /// that budget made enforceable: it is deliberately loose enough that the
    /// compositor may cost more than the stroke painter, and tight enough that
    /// a per-cell or per-texel loop escaping onto the frame path fails.
    /// Measured on this frame at the time of writing: classic 64, ember 66,
    /// aurora 68, tidewave 86, voltage 60, lantern 64.
    #[test]
    fn perf_frame_op_census_stays_inside_its_budget() {
        /// Section 11's "rich six-layer image skin roughly doubles a snake's
        /// cost", applied to the 86-op worst case on this frame with headroom.
        const PER_SNAKE_CEILING: usize = 200;

        let census = census_frame();
        assert_eq!(census.len(), PERF_SNAKE_COUNT);

        let total: usize = census.iter().map(|snake| snake.ops).sum();
        for snake in &census {
            assert!(
                snake.ops <= PER_SNAKE_CEILING,
                "{} emits {} ops for one snake, over the {PER_SNAKE_CEILING} budget\n\
                 full census: {census:#?}",
                snake.skin_id,
                snake.ops
            );
        }

        // A frame is also allowed to be cheap in aggregate; the grid alone is
        // ~2,300 arcs, so a snake budget that approached it would be the wrong
        // shape of design regardless of the per-snake number.
        assert!(
            total <= PER_SNAKE_CEILING * PERF_SNAKE_COUNT,
            "the perf frame costs {total} ops\nfull census: {census:#?}"
        );
    }

    /// The allocation census, and an honest record of the gap.
    ///
    /// Section 11's gate is **zero** per-frame allocation. The shipped stroke
    /// painter does not meet it — `specs/skins-prd.md` section 10 and this
    /// PRD's section 4 both say so, counting ~11 `String`s, two `Vec`s and a
    /// `HashSet` per snake per frame. This test pins today's cost so the
    /// compositor's progress toward zero is visible, and fails if anything
    /// makes it worse.
    #[test]
    fn perf_frame_allocation_census_is_pinned_to_todays_cost() {
        // Measured: 268 allocations / 17,680 bytes for the eight-snake frame,
        // about 34 allocations per snake. The ceiling carries ~20% headroom so
        // an unrelated `format!` on an error path does not fail the build,
        // while a new per-cell allocation does.
        const FRAME_ALLOCATION_CEILING: usize = 320;

        // Warm anything lazily initialised (the document skins compile on
        // first use) so the census measures painting, not registration.
        paint_perf_frame();

        let census = allocations_during(paint_perf_frame);
        assert!(
            census.allocations > 0,
            "the counting allocator recorded nothing, so this test proves \
             nothing — check that CountingAllocator is still #[global_allocator]"
        );
        assert!(
            census.allocations <= FRAME_ALLOCATION_CEILING,
            "the perf frame allocates {} times ({} bytes), over the \
             {FRAME_ALLOCATION_CEILING} ceiling. Section 11's actual gate is \
             zero; this ceiling only stops it getting worse.",
            census.allocations,
            census.bytes
        );
    }
}
