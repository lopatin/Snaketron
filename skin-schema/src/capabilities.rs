//! Machine-readable SkinDoc v2 compiler/renderer contract.
//!
//! Agents and the factory consume the committed JSON generated from this
//! function. Keeping the values sourced from validator constants means prompts
//! do not carry a second, stale copy of limits the runtime has already changed.

use crate::v2::{EvalSite, TextureKindV2};

fn inputs(site: EvalSite) -> Vec<&'static str> {
    site.allowed_inputs()
        .iter()
        .copied()
        .map(crate::expr::Input::name)
        .collect()
}

fn functions(site: EvalSite) -> Vec<&'static str> {
    crate::expr::Func::ALL
        .iter()
        .copied()
        .filter(|function| *function != crate::expr::Func::Noise || site.allows_noise())
        .map(crate::expr::Func::name)
        .collect()
}

fn kind_name(kind: TextureKindV2) -> &'static str {
    match kind {
        TextureKindV2::Coat => "coat",
        TextureKindV2::Sheet => "sheet",
        TextureKindV2::Overlay => "overlay",
    }
}

/// Current SkinDoc v2 capability manifest, derived from runtime constants.
pub fn capabilities_v2() -> serde_json::Value {
    use crate::v2::*;

    let image_kinds = [
        (
            TextureKindV2::Coat,
            serde_json::json!({
                "body_columns": "required",
                "frame_rows": "forbidden",
                "intrinsic_seam_axes": []
            }),
        ),
        (
            TextureKindV2::Sheet,
            serde_json::json!({
                "body_columns": "required",
                "frame_rows": "required",
                "intrinsic_seam_axes": ["y"]
            }),
        ),
        (
            TextureKindV2::Overlay,
            serde_json::json!({
                "body_columns": "optional",
                "frame_rows": "forbidden",
                "intrinsic_seam_axes": []
            }),
        ),
    ]
    .into_iter()
    .map(|(kind, value)| (kind_name(kind).to_string(), value))
    .collect::<serde_json::Map<_, _>>();

    serde_json::json!({
        "manifest_version": 1,
        "schema_version": SCHEMA_VERSION_V2,
        "limits": {
            "max_flattened_layers": MAX_LAYERS,
            "max_ops_per_snake": MAX_OPS_PER_SNAKE,
            "max_texture_refs": MAX_TEXTURE_REFS,
            "max_texture_variants": MAX_TEXTURE_VARIANTS,
            "max_raster_overhang_px": MAX_RASTER_OVERHANG_PX,
            "raster_body_texels_per_cell": RASTER_BODY_TEXELS_PER_CELL,
            "max_group_depth": MAX_GROUP_DEPTH,
            "min_gradient_stops": MIN_GRADIENT_STOPS,
            "max_gradient_stops": MAX_GRADIENT_STOPS,
            "max_text_content_length": MAX_TEXT_CONTENT_LEN,
            "max_texture_dimension_px": MAX_TEXTURE_DIMENSION_PX,
            "max_texture_variant_bytes": MAX_TEXTURE_VARIANT_BYTES,
            "max_texture_decoded_bytes": MAX_TEXTURE_DECODED_BYTES,
            "max_skin_texture_decoded_bytes": MAX_SKIN_TEXTURE_DECODED_BYTES,
            "max_skin_texture_compressed_bytes": MAX_SKIN_TEXTURE_COMPRESSED_BYTES,
            "max_sprite_frame_rows": MAX_SPRITE_FRAME_ROWS,
            "max_sprite_frame_rate_fps": MAX_SPRITE_FRAME_RATE_FPS,
            "min_animation_period_ms": crate::MIN_ANIMATION_PERIOD_MS,
            "max_animation_period_ms": crate::MAX_ANIMATION_PERIOD_MS,
            "expression_animation_steps": crate::ANIMATION_STEPS
        },
        "expression_inputs_by_site": {
            "palette": inputs(EvalSite::Palette),
            "snake": inputs(EvalSite::Snake),
            "cell": inputs(EvalSite::Cell),
            "bounded": inputs(EvalSite::Bounded)
        },
        "expression_constants": ["pi", "tau"],
        "expression_functions": crate::expr::Func::ALL.map(crate::expr::Func::name),
        "expression_functions_by_site": {
            "palette": functions(EvalSite::Palette),
            "snake": functions(EvalSite::Snake),
            "cell": functions(EvalSite::Cell),
            "bounded": functions(EvalSite::Bounded)
        },
        "layer_types": ["group", "ribbon", "span", "head_disc", "head_ramp"],
        "source_types": ["solid", "gradient", "band", "image", "text"],
        "image_fits": ["clip", "stretch", "tile", "cutout"],
        "tile_phase_origins": ["head", "tail"],
        "image_kinds": image_kinds,
        "animation": {
            "expression_clock": "baked_ring",
            "sprite_clock": "continuous_period",
            "sprite_row_zero_is_resting": true,
            "reachable_row_formula": "min(max_sprite_frame_rows, ceil(period_ms / 1000 * max_sprite_frame_rate_fps))",
            "image_drift": "constant_cells_per_cycle"
        },
        "image_contract": {
            "descriptor_required_for_generated": true,
            "fallback_layer_required": true,
            "raster_overhang": {
                "units": "authored_bleed_pixels_per_side_around_unchanged_16x16_body_cell",
                "logical_body_cell": "16x16_texels",
                "stored_transverse_row": "scaled_bleed_apron + texels_per_cell + scaled_bleed_apron",
                "visible_clip": "bounded_transverse_expansion_with_unchanged_longitudinal_caps",
                "cross_snake_compositing": "bounded_bleed_may_overlap_occupied_cells_and_follows_normal_snake_draw_order"
            },
            "variant_url_template": "/api/textures/variants/{variant_content_ref}.png",
            "repair_backend": "local_simple_lama",
            "repair_methods": ["tx_t_inpaint", "roll_and_repair"],
            "max_repair_attempts_per_join": 1,
            "seam_axes_by_usage": {
                "sheet": ["y"],
                "tile": ["x"]
            }
        },
        "group_flags": {
            "boost_only": "propagated_to_children",
            "omit_on_single_cell": "propagated_to_children"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_manifest_is_exactly_the_generated_contract() {
        let committed: serde_json::Value =
            serde_json::from_str(include_str!("../capabilities-v2.json")).expect("valid JSON");
        assert_eq!(committed, capabilities_v2());
    }

    #[test]
    fn inputs_are_taken_from_the_validator_sites() {
        let manifest = capabilities_v2();
        assert_eq!(
            manifest["expression_inputs_by_site"]["snake"],
            serde_json::json!(["time", "len", "boost", "seed"])
        );
        assert_eq!(
            manifest["limits"]["max_sprite_frame_rows"],
            crate::v2::MAX_SPRITE_FRAME_ROWS
        );
        for name in manifest["expression_functions"]
            .as_array()
            .expect("function vocabulary")
        {
            assert!(
                crate::expr::Func::ALL
                    .iter()
                    .any(|function| function.name() == name.as_str().expect("function name"))
            );
        }
        assert!(
            manifest["expression_functions_by_site"]["cell"]
                .as_array()
                .expect("cell functions")
                .contains(&serde_json::json!("noise"))
        );
        for site in ["palette", "snake", "bounded"] {
            assert!(
                !manifest["expression_functions_by_site"][site]
                    .as_array()
                    .expect("site functions")
                    .contains(&serde_json::json!("noise"))
            );
        }
        assert_eq!(
            manifest["image_kinds"]["coat"]["intrinsic_seam_axes"],
            serde_json::json!([])
        );
        assert_eq!(
            manifest["image_kinds"]["sheet"]["intrinsic_seam_axes"],
            serde_json::json!(["y"])
        );
        assert_eq!(
            manifest["image_contract"]["seam_axes_by_usage"]["tile"],
            serde_json::json!(["x"])
        );
        assert_eq!(
            manifest["image_contract"]["repair_methods"],
            serde_json::json!(["tx_t_inpaint", "roll_and_repair"])
        );
    }
}
