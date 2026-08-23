import copy
import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/validate_package.py"
SPEC = importlib.util.spec_from_file_location("validate_author_skin", SCRIPT)
assert SPEC and SPEC.loader
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


class PackageValidationTests(unittest.TestCase):
    def test_repository_package_is_valid(self):
        self.assertEqual(validator.validate_package(), [])

    def test_skin_design_guidelines_are_required_and_optimizer_locked(self):
        boundary = validator.read_json(validator.PACKAGE / "optimization-boundary.json")
        guideline_path = "references/design-guidelines.md"
        self.assertIn(guideline_path, boundary["locked_paths"])
        self.assertNotEqual(guideline_path, boundary["editable_path"])

        skill = (validator.PACKAGE / "SKILL.md").read_text(encoding="utf-8")
        self.assertIn("[Skin Design Guidelines](references/design-guidelines.md)", skill)
        self.assertIn("completely and apply", skill)
        validation = (validator.PACKAGE / "references/validation.md").read_text(encoding="utf-8")
        self.assertIn("`design_guidelines` object", validation)

        guidelines = (validator.PACKAGE / guideline_path).read_text(encoding="utf-8")
        self.assertEqual(guidelines.count(validator.DESIGN_GUIDELINE_START), 1)
        self.assertEqual(guidelines.count(validator.DESIGN_GUIDELINE_END), 1)
        self.assertEqual(guidelines.count(validator.PROTOTYPE_IMAGE_RULES_START), 1)
        self.assertEqual(guidelines.count(validator.PROTOTYPE_IMAGE_RULES_END), 1)
        self.assertLess(
            guidelines.index(validator.DESIGN_GUIDELINE_START),
            guidelines.index(validator.PROTOTYPE_IMAGE_RULES_START),
        )
        self.assertLess(
            guidelines.index(validator.PROTOTYPE_IMAGE_RULES_END),
            guidelines.index(validator.DESIGN_GUIDELINE_END),
        )

    def test_locked_guidelines_preserve_renderer_truth(self):
        guidelines = (validator.PACKAGE / "references/design-guidelines.md").read_text(encoding="utf-8")
        locked = guidelines.split(validator.DESIGN_GUIDELINE_START, 1)[1].split(validator.DESIGN_GUIDELINE_END, 1)[0]
        for term in (
            "DEFAULT_SNAKE_LENGTH",
            "GameArena.tsx",
            "one-cell-wide path that continuously",
            "5\u201315 CSS px",
            "canonical texel density of a sprite-sheet cell",
            "coats and overlays use 64 texels per cell",
            "compressed head, turn, and tail points",
            "Manhattan arc",
            "not a visual cell plate",
            "TextureDescriptorV2.raster_overhang_px",
            "0 through 4 authored bleed pixels per transverse side",
            "16×16 logical body cell",
            "does not relax the longitudinal head and tail caps",
            "neighboring snake",
            "not a freeform canvas",
            "light_field_dark_core",
            "dark_field_light_disc_dark_core",
        ):
            self.assertIn(term, locked)

    def test_geometry_authority_handoff_is_required_in_both_references(self):
        references = (
            validator.PACKAGE / "references/design-guidelines.md",
            validator.PACKAGE / "references/prototypes.md",
        )
        for path in references:
            text = path.read_text(encoding="utf-8")
            for term in (
                "prototype_geometry",
                "prototype_geometry_guide",
                "artifact_refs",
                "authoring_inputs",
                "exact inline guide bytes",
                "contract_sha256",
                "guide_sha256",
                "prototype_geometry_sha256",
                "prototype_guide_sha256",
                "invalid_input",
            ):
                self.assertIn(term, text, f"{path}: missing {term}")

        prototypes = (validator.PACKAGE / "references/prototypes.md").read_text(encoding="utf-8")
        for term in (
            "source_image_sha256",
            "geometry_projection",
            "prototype-body-mask-v1",
            "only authoring input",
        ):
            self.assertIn(term, prototypes, f"prototype reference missing {term}")

    def test_prototype_manifest_rejects_each_missing_authority_hash(self):
        manifest = validator.read_json(validator.PACKAGE / "fixtures/layers/prototype-manifest.json")
        for authority in sorted(validator.PROTOTYPE_AUTHORITY_KEYS):
            with self.subTest(authority=authority):
                missing = copy.deepcopy(manifest)
                missing.pop(authority)
                errors = []
                validator.validate_manifest(missing, "probe", errors)
                self.assertTrue(
                    any(f"{authority} is required and must be a SHA-256 digest" in error for error in errors),
                    errors,
                )

    def test_prototype_manifest_schema_cannot_make_authority_optional_or_nullable(self):
        schema = validator.read_json(validator.PACKAGE / "schemas/prototype-manifest.schema.json")
        for authority in sorted(validator.PROTOTYPE_AUTHORITY_KEYS):
            with self.subTest(authority=authority):
                relaxed = copy.deepcopy(schema)
                relaxed["required"].remove(authority)
                relaxed["properties"][authority]["type"] = ["string", "null"]
                errors = []
                validator.validate_prototype_manifest_schema(relaxed, errors)
                self.assertTrue(
                    any("must require every field and authority hash" in error for error in errors),
                    errors,
                )
                self.assertTrue(
                    any(f"must require a non-null SHA-256: {authority}" in error for error in errors),
                    errors,
                )

    def test_prototype_manifest_requires_exact_projection_provenance(self):
        manifest = validator.read_json(validator.PACKAGE / "fixtures/layers/prototype-manifest.json")
        probes = (
            ("source_image_sha256", None, "source_image_sha256 is required"),
            ("source_image_sha256", "not-a-hash", "source_image_sha256 is required"),
            ("geometry_projection", None, "geometry_projection must be exactly"),
            ("geometry_projection", "legacy-mask", "geometry_projection must be exactly"),
        )
        for field, replacement, expected in probes:
            with self.subTest(field=field, replacement=replacement):
                changed = copy.deepcopy(manifest)
                if replacement is None:
                    changed.pop(field)
                else:
                    changed[field] = replacement
                errors = []
                validator.validate_manifest(changed, "probe", errors)
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_prototype_manifest_schema_cannot_relax_projection_provenance(self):
        schema = validator.read_json(validator.PACKAGE / "schemas/prototype-manifest.schema.json")

        relaxed_source = copy.deepcopy(schema)
        relaxed_source["required"].remove("source_image_sha256")
        relaxed_source["properties"]["source_image_sha256"]["pattern"] = "^[a-f0-9]{64}$"
        errors = []
        validator.validate_prototype_manifest_schema(relaxed_source, errors)
        self.assertTrue(any("must require every field" in error for error in errors), errors)
        self.assertTrue(any("prefixed source image SHA-256" in error for error in errors), errors)

        relaxed_projection = copy.deepcopy(schema)
        relaxed_projection["required"].remove("geometry_projection")
        relaxed_projection["properties"]["geometry_projection"].pop("const")
        errors = []
        validator.validate_prototype_manifest_schema(relaxed_projection, errors)
        self.assertTrue(any("must require every field" in error for error in errors), errors)
        self.assertTrue(any("pin geometry_projection" in error for error in errors), errors)

    def test_canonical_prototype_prompts_and_hashes_bind_continuous_geometry(self):
        expected = validator.canonical_prototype_authorities()
        for route in sorted(validator.ROUTES):
            manifest = validator.read_json(validator.PACKAGE / f"fixtures/{route}/prototype-manifest.json")
            for authority, digest in expected.items():
                self.assertEqual(manifest[authority], digest, f"{route}: {authority}")
            self.assertTrue(validator.is_hash(manifest["source_image_sha256"]), route)
            self.assertEqual(
                manifest["geometry_projection"],
                validator.PROTOTYPE_GEOMETRY_PROJECTION,
                route,
            )
            self.assertIn(validator.PROTOTYPE_PROJECTION_PROMPT, manifest["prompt"], route)
            for term in validator.PROTOTYPE_PROMPT_TERMS:
                self.assertIn(term, manifest["prompt"], f"{route}: {term}")

    def test_plan_requires_exact_bounded_design_guideline_evidence(self):
        fixture = validator.PACKAGE / "fixtures/layers"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = validator.read_json(fixture / "implementation-plan.json")

        missing = copy.deepcopy(plan)
        missing.pop("design_guidelines")
        errors = []
        validator.validate_plan(missing, manifest, approval, "probe", errors)
        self.assertTrue(
            any("design_guidelines must be an object" in error for error in errors),
            errors,
        )

        malformed = copy.deepcopy(plan)
        malformed["design_guidelines"]["unexpected"] = "not allowed"
        malformed["design_guidelines"]["artistic_direction"] = " "
        malformed["design_guidelines"]["body_strategy"] = "x" * 321
        malformed["design_guidelines"]["structure"] = "mixed"
        malformed["design_guidelines"]["head_zone"] = "white_head_core"
        errors = []
        validator.validate_plan(malformed, manifest, approval, "probe", errors)
        for expected in (
            "design_guidelines fields drifted",
            "artistic_direction must be a non-empty string",
            "body_strategy exceeds 320 characters",
            "design_guidelines.structure is invalid",
            "design_guidelines.head_zone is invalid",
        ):
            self.assertTrue(any(expected in error for error in errors), errors)

    def test_plan_schema_cannot_relax_the_guideline_boundary(self):
        schema = copy.deepcopy(validator.read_json(validator.PACKAGE / "schemas/implementation-plan.schema.json"))
        schema["$defs"]["designGuidelines"]["additionalProperties"] = True
        schema["$defs"]["designGuidelines"]["properties"]["body_strategy"]["maxLength"] = 321
        errors = []
        validator.validate_implementation_plan_schema(schema, errors)
        self.assertTrue(any("forbid extra fields" in error for error in errors), errors)
        self.assertTrue(any("bound drifted: body_strategy" in error for error in errors), errors)

    def test_safety_ip_invariants_live_inside_the_locked_contract(self):
        contract = (validator.PACKAGE / "references/contract.md").read_text(encoding="utf-8")
        locked = contract.split("<!-- FACTORY_LOCKED:START -->", 1)[1].split("<!-- FACTORY_LOCKED:END -->", 1)[0]
        for term in (
            "protected marks",
            "public-figure likeness",
            "unsafe content",
            "unlicensed references",
            "blocking `safety_ip`",
            "non-waivable",
        ):
            self.assertIn(term, locked)

    def test_band_lane_invariant_is_locked_and_schema_visible(self):
        contract = (validator.PACKAGE / "references/contract.md").read_text(encoding="utf-8")
        layers = (validator.PACKAGE / "references/layers-effects.md").read_text(encoding="utf-8")
        validation = (validator.PACKAGE / "references/validation.md").read_text(encoding="utf-8")
        schema = validator.read_json(validator.PACKAGE / "schemas/implementation-plan.schema.json")

        for text in (contract, layers, validation):
            self.assertIn(validator.BAND_LANE_INVARIANT, text)
        self.assertIn(validator.BAND_LANE_SAFE_EXAMPLE, contract)
        self.assertIn(validator.BAND_LANE_SAFE_EXAMPLE, layers)
        for field in ("layer_plan", "animation_plan"):
            self.assertIn(
                validator.BAND_LANE_INVARIANT,
                schema["properties"][field]["description"],
            )
        self.assertIn(
            validator.BAND_LANE_SAFE_EXAMPLE,
            schema["properties"]["animation_plan"]["description"],
        )

        relaxed = copy.deepcopy(schema)
        relaxed["properties"]["animation_plan"]["description"] = "Describe motion."
        errors = []
        validator.validate_implementation_plan_schema(relaxed, errors)
        self.assertTrue(any("animation_plan must preserve" in error for error in errors), errors)

    def test_approval_must_bind_the_manifest_and_plan_hash(self):
        fixture = validator.PACKAGE / "fixtures/layers"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = validator.read_json(fixture / "implementation-plan.json")
        approval["artifact_sha256"] = "sha256:" + "f" * 64
        errors = []
        validator.validate_plan(plan, manifest, approval, "probe", errors)
        self.assertTrue(any("same image" in error for error in errors), errors)

    def test_sheet_requires_y_wrap(self):
        fixture = validator.PACKAGE / "fixtures/sprite-sheet"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = validator.read_json(fixture / "implementation-plan.json")
        plan = copy.deepcopy(plan)
        plan["required_wrap_axes"] = ["x"]
        errors = []
        validator.validate_plan(plan, manifest, approval, "probe", errors)
        self.assertTrue(any("derived from kind and fit" in error for error in errors), errors)

    def test_nonconstant_image_drift_is_rejected_until_capability_changes(self):
        fixture = validator.PACKAGE / "fixtures/texture"
        plan = validator.read_json(fixture / "implementation-plan.json")
        document = validator.read_json(fixture / "skin.skin.json")
        document = copy.deepcopy(document)
        image = next(layer for layer in document["layers"] if layer.get("source", {}).get("type") == "image")
        image["source"]["drift_cells"] = "2 * time"
        errors = []
        validator.validate_document(document, plan, "probe", errors)
        self.assertTrue(any("drift must stay constant" in error for error in errors), errors)

    def test_sprite_x_and_y_are_independent_metadata(self):
        fixture = validator.PACKAGE / "fixtures/sprite-sheet"
        plan = validator.read_json(fixture / "implementation-plan.json")
        document = validator.read_json(fixture / "skin.skin.json")
        asset = plan["asset_plan"][0]
        descriptor = document["textures"][0]["descriptor"]
        self.assertEqual(descriptor["body_columns"], asset["natural_length_cells"])
        self.assertEqual(descriptor["frame_rows"], asset["frames"])
        self.assertNotEqual(descriptor["body_columns"], descriptor["frame_rows"])

    def test_asset_requests_cannot_call_an_editor_as_a_generator(self):
        request = validator.read_json(validator.PACKAGE / "templates/asset-request.json")
        request["capability_role"] = "image_editor"
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertTrue(any("generate needs image_generator" in error for error in errors), errors)

    def test_inpaint_repairs_bind_an_exact_mask(self):
        request = validator.read_json(validator.PACKAGE / "templates/asset-request.json")
        request["operation"] = "edit"
        request["capability_role"] = "image_editor"
        request["input_artifacts"] = ["sha256:" + "a" * 64]
        request["repair"] = {"method": "tx_t_inpaint", "mask_artifact": None}
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertTrue(any("needs a mask artifact" in error for error in errors), errors)

        request["repair"]["mask_artifact"] = "sha256:" + "b" * 64
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertEqual(errors, [])

    def test_tiled_assets_require_x_wrap(self):
        fixture = validator.PACKAGE / "fixtures/hybrid"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = validator.read_json(fixture / "implementation-plan.json")
        plan = copy.deepcopy(plan)
        plan["required_wrap_axes"] = ["y"]
        errors = []
        validator.validate_plan(plan, manifest, approval, "probe", errors)
        self.assertTrue(any("derived from kind and fit" in error for error in errors), errors)

    def test_asset_plan_uses_the_current_forge_texel_density(self):
        fixture = validator.PACKAGE / "fixtures/texture"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = validator.read_json(fixture / "implementation-plan.json")
        plan = copy.deepcopy(plan)
        plan["asset_plan"][0]["texels_per_cell"] = 16
        errors = []
        validator.validate_plan(plan, manifest, approval, "probe", errors)
        self.assertTrue(any("current forge requires 64" in error for error in errors), errors)

    def test_asset_plan_is_bounded_before_generation(self):
        fixture = validator.PACKAGE / "fixtures/texture"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = copy.deepcopy(validator.read_json(fixture / "implementation-plan.json"))
        plan["asset_plan"][0]["natural_length_cells"] = 33
        errors = []
        validator.validate_plan(plan, manifest, approval, "probe", errors)
        self.assertTrue(
            any("width exceeds the current capability bound" in error for error in errors),
            errors,
        )

    def test_worker_draft_uses_one_exact_pending_sentinel(self):
        fixture = validator.PACKAGE / "fixtures/worker-drafts"
        errors = []
        validator.validate_worker_draft(
            validator.read_json(fixture / "skin-draft.valid.json"),
            validator.read_json(fixture / "implementation-plan.json"),
            validator.read_json(fixture / "tool-requests.json"),
            "probe",
            errors,
        )
        self.assertEqual(errors, [])

    def test_worker_draft_rejects_fabricated_asset_evidence(self):
        fixture = validator.PACKAGE / "fixtures/worker-drafts"
        errors = []
        validator.validate_worker_draft(
            validator.read_json(fixture / "skin-draft.fabricated.invalid.json"),
            validator.read_json(fixture / "implementation-plan.json"),
            validator.read_json(fixture / "tool-requests.json"),
            "probe",
            errors,
        )
        self.assertTrue(any("must use pending:asset:0" in error for error in errors), errors)
        self.assertTrue(any("cannot fabricate a descriptor" in error for error in errors), errors)

    def test_pending_sentinel_cannot_reach_final_gates(self):
        fixture = validator.PACKAGE / "fixtures/worker-drafts"
        errors = []
        validator.validate_worker_draft(
            validator.read_json(fixture / "skin-draft.valid.json"),
            validator.read_json(fixture / "implementation-plan.json"),
            validator.read_json(fixture / "tool-requests.json"),
            "probe",
            errors,
            allow_pending=False,
        )
        self.assertTrue(any("survived exact binding" in error for error in errors), errors)

    def test_input_authority_modes_are_explicit_and_closed(self):
        workflow = validator.PACKAGE / "fixtures/modifier-workflow"
        plan = validator.read_json(workflow / "implementation-plan.json")
        errors = []
        validator.validate_plan(
            plan,
            {"image_sha256": "sha256:" + "8" * 64},
            {},
            "probe",
            errors,
        )
        self.assertEqual(errors, [])

        false_approval = copy.deepcopy(plan)
        false_approval["input_authority"]["human_approval_decision_id"] = "invented"
        errors = []
        validator.validate_plan(
            false_approval,
            {"image_sha256": "sha256:" + "8" * 64},
            {},
            "probe",
            errors,
        )
        self.assertTrue(any("draft cannot claim human approval" in error for error in errors), errors)

    def test_modifier_components_are_distinct_and_span_bounded(self):
        plan = validator.read_json(
            validator.PACKAGE / "fixtures/modifier-workflow/implementation-plan.json"
        )
        modifiers = plan["modifier_plan"]
        self.assertEqual([item["component_key"] for item in modifiers], ["T1", "T2", "B1", "H"])
        self.assertEqual(len({item["texture_name"] for item in modifiers}), 4)
        self.assertEqual(len({item["image_layer_name"] for item in modifiers}), 4)

        too_long = copy.deepcopy(plan)
        too_long["modifier_plan"][3]["span_limit_value"] = 7
        errors = []
        validator.validate_plan(
            too_long,
            {"image_sha256": "sha256:" + "8" * 64},
            {},
            "probe",
            errors,
        )
        self.assertTrue(any("head span exceeds cell 6" in error for error in errors), errors)

        too_much_tail = copy.deepcopy(plan)
        too_much_tail["modifier_plan"][0]["span_limit_value"] = 0.6
        errors = []
        validator.validate_plan(
            too_much_tail,
            {"image_sha256": "sha256:" + "8" * 64},
            {},
            "probe",
            errors,
        )
        self.assertTrue(any("tail span exceeds half" in error for error in errors), errors)

    def test_plan_can_keep_more_than_four_modest_assets_separate(self):
        plan_schema = validator.read_json(
            validator.PACKAGE / "schemas/implementation-plan.schema.json"
        )
        asset_schema = validator.read_json(
            validator.PACKAGE / "schemas/asset-request.schema.json"
        )
        self.assertEqual(plan_schema["properties"]["asset_plan"]["maxItems"], 8)
        self.assertEqual(plan_schema["properties"]["modifier_plan"]["maxItems"], 8)
        self.assertEqual(plan_schema["$defs"]["modifier"]["properties"]["asset_index"]["maximum"], 7)
        self.assertEqual(asset_schema["properties"]["asset_index"]["maximum"], 7)

        plan = validator.read_json(
            validator.PACKAGE / "fixtures/modifier-workflow/implementation-plan.json"
        )
        fifth_asset = copy.deepcopy(plan["asset_plan"][3])
        fifth_asset["prompt"] = "Keep an additional modest cheek accent independently anchored."
        plan["asset_plan"].append(fifth_asset)
        fifth_modifier = copy.deepcopy(plan["modifier_plan"][3])
        fifth_modifier.update(
            {
                "asset_index": 4,
                "logical_key": "cheek_accent",
                "component_key": "H2",
                "texture_name": "modifier_h2",
                "image_layer_name": "H2 Image",
                "fallback_layer_name": "H2 Fallback",
            }
        )
        plan["modifier_plan"].append(fifth_modifier)
        errors = []
        validator.validate_plan(
            plan,
            {"image_sha256": "sha256:" + "8" * 64},
            {},
            "five-asset-probe",
            errors,
        )
        self.assertEqual(errors, [])

    def test_video_contract_binds_ordered_endpoints_and_exact_cadence(self):
        requests = validator.read_json(
            validator.PACKAGE / "fixtures/modifier-workflow/media-requests.json"
        )
        request = copy.deepcopy(next(item for item in requests if item["operation"] == "generate_video"))
        request["input_artifacts"].reverse()
        request["video"]["derived_frame_rows"] = 47
        request["video"]["effective_frame_row_cap"] = 84
        errors = []
        validator.validate_media_request(request, "probe", errors)
        for expected in (
            "ordered start/end frame hashes",
            "ceil(period*fps/1000)",
            "effective row cap is wrong",
        ):
            self.assertTrue(any(expected in error for error in errors), errors)

    def test_pixverse_transition_prompt_preserves_flat_component_contract(self):
        requests = validator.read_json(
            validator.PACKAGE / "fixtures/modifier-workflow/media-requests.json"
        )
        request = copy.deepcopy(next(item for item in requests if item["operation"] == "generate_video"))
        errors = []
        validator.validate_media_request(request, "probe", errors)
        self.assertEqual(errors, [])

        request["prompt"] = request["prompt"].replace("[Context]", "[Scene]")
        request["prompt"] = request["prompt"].replace("background completely static", "background")
        errors = []
        validator.validate_media_request(request, "probe", errors)
        self.assertTrue(any("five literal sections" in error for error in errors), errors)
        self.assertTrue(any("background completely static" in error for error in errors), errors)

    def test_video_extraction_binds_exact_source_video(self):
        requests = validator.read_json(
            validator.PACKAGE / "fixtures/modifier-workflow/media-requests.json"
        )
        request = copy.deepcopy(next(item for item in requests if item["operation"] == "extract_video_frames"))
        request["input_artifacts"] = ["sha256:" + "f" * 64]
        errors = []
        validator.validate_media_request(request, "probe", errors)
        self.assertTrue(any("exact source video hash" in error for error in errors), errors)

    def test_video_modifier_binds_exact_extracted_sheet(self):
        plan = copy.deepcopy(
            validator.read_json(
                validator.PACKAGE / "fixtures/modifier-workflow/implementation-plan.json"
            )
        )
        modifier = next(item for item in plan["modifier_plan"] if item["source_mode"] == "video_frames")
        modifier["video"]["extracted_sheet_sha256"] = "sha256:" + "f" * 64
        errors = []
        validator.validate_plan(
            plan,
            {"image_sha256": "sha256:" + "8" * 64},
            {},
            "probe",
            errors,
        )
        self.assertTrue(any("exact extracted video sheet bytes" in error for error in errors), errors)

    def test_extraction_fails_closed_before_crop(self):
        request = copy.deepcopy(
            validator.read_json(validator.PACKAGE / "templates/media-operation-request.json")
        )
        request["extraction"]["fail_on_edge_contact"] = False
        request["extraction"]["matte_policy"] = "best_effort"
        errors = []
        validator.validate_media_request(request, "probe", errors)
        self.assertTrue(any("fail closed before cropping" in error for error in errors), errors)

    def test_raster_overhang_uses_fixed_16_grid_and_no_gutters(self):
        request = copy.deepcopy(validator.read_json(validator.PACKAGE / "templates/asset-request.json"))
        request["grid"]["row_texels"] = 23
        request["height_px"] = request["grid"]["frame_rows"] * 23
        request["edge_contract"]["no_frame_or_repeat_gutters"] = False
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertTrue(any("row_texels must scale" in error for error in errors), errors)
        self.assertTrue(any("gutters are forbidden" in error for error in errors), errors)

    def test_modifier_reuse_binds_manifest_and_lineage(self):
        requests = validator.read_json(
            validator.PACKAGE / "fixtures/modifier-workflow/media-requests.json"
        )
        request = copy.deepcopy(next(item for item in requests if item["operation"] == "reuse_modifier"))
        request["input_artifacts"] = ["sha256:" + "f" * 64]
        request["reuse"]["require_authorized_lineage"] = False
        errors = []
        validator.validate_media_request(request, "probe", errors)
        self.assertTrue(any("one exact modifier manifest" in error for error in errors), errors)
        self.assertTrue(any("enforce lineage scope" in error for error in errors), errors)

    def test_modifier_identity_binds_exact_bytes_and_authorized_lineage(self):
        manifest = validator.read_json(
            validator.PACKAGE / "templates/modifier-manifest.json"
        )
        errors = []
        validator.validate_modifier_manifest(manifest, "probe", errors)
        self.assertEqual(errors, [])

        mismatched = copy.deepcopy(manifest)
        mismatched["modifier_id"] = "modifier:not_authorized:sha256:" + "a" * 64
        errors = []
        validator.validate_modifier_manifest(mismatched, "probe", errors)
        self.assertTrue(any("exact content hash" in error for error in errors), errors)
        self.assertTrue(any("lineage is not authorized" in error for error in errors), errors)

    def test_modifier_alpha_contract_requires_matching_outer_edge_proof(self):
        manifest = copy.deepcopy(
            validator.read_json(validator.PACKAGE / "templates/modifier-manifest.json")
        )
        manifest["verification"]["outer_frame_edge"] = "not_applicable_opaque_fill"
        errors = []
        validator.validate_modifier_manifest(manifest, "probe", errors)
        self.assertTrue(any("outer frame edge is clear" in error for error in errors), errors)

        manifest["verification"]["alpha_matte"] = "opaque_not_applicable"
        errors = []
        validator.validate_modifier_manifest(manifest, "probe", errors)
        self.assertEqual(errors, [])

    def test_opaque_edge_exemption_requires_opaque_asset_bytes(self):
        request = copy.deepcopy(validator.read_json(validator.PACKAGE / "templates/asset-request.json"))
        request["transparency"] = "allowed"
        request["edge_contract"]["transverse_policy"] = "not_applicable_opaque_fill"
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertTrue(any("opaque edge exemption requires opaque bytes" in error for error in errors), errors)

    def test_modifier_workflow_binds_every_plan_media_and_manifest_record(self):
        directory = validator.PACKAGE / "fixtures/modifier-workflow"
        plan = validator.read_json(directory / "implementation-plan.json")
        requests = validator.read_json(directory / "media-requests.json")
        manifests = {
            component: (
                validator.sha256_file(directory / f"{component}.modifier-manifest.json"),
                validator.read_json(directory / f"{component}.modifier-manifest.json"),
            )
            for component in ("T1", "T2", "B1", "H")
        }
        errors = []
        validator.validate_modifier_workflow_links(plan, requests, manifests, "probe", errors)
        self.assertEqual(errors, [])

        plan["modifier_plan"][0]["source_object_sha256"] = "sha256:" + "f" * 64
        errors = []
        validator.validate_modifier_workflow_links(plan, requests, manifests, "probe", errors)
        self.assertTrue(any("manifest content differs" in error for error in errors), errors)

    def test_modifier_reference_is_optimizer_locked(self):
        boundary = validator.read_json(validator.PACKAGE / "optimization-boundary.json")
        self.assertIn("references/modifiers-video.md", boundary["locked_paths"])
        text = (validator.PACKAGE / "references/modifiers-video.md").read_text(encoding="utf-8")
        for term in (
            "reserved empty source arena",
            "transparent RGBA",
            "exact mask/matte",
            "actual final-frame-to-row-zero",
            "authorized_lineage_ids",
            "raster_overhang_px",
            "T1/T2/B1/H",
        ):
            self.assertIn(term, text)

    def test_optimizer_cannot_edit_outside_marked_playbook_body(self):
        playbook = (validator.PACKAGE / "references/playbook.md").read_text(encoding="utf-8")
        changed_body = playbook.replace("Choose the cheapest faithful route", "Choose a faithful route")
        self.assertEqual(validator.validate_playbook_candidate(changed_body), [])
        changed_prefix = playbook.replace(
            "The marked section is the only content",
            "Everything is editable",
        )
        errors = validator.validate_playbook_candidate(changed_prefix)
        self.assertTrue(any("locked playbook prefix" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
