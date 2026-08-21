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
        self.assertTrue(
            any("derived from kind and fit" in error for error in errors), errors
        )

    def test_nonconstant_image_drift_is_rejected_until_capability_changes(self):
        fixture = validator.PACKAGE / "fixtures/texture"
        plan = validator.read_json(fixture / "implementation-plan.json")
        document = validator.read_json(fixture / "skin.skin.json")
        document = copy.deepcopy(document)
        image = next(
            layer
            for layer in document["layers"]
            if layer.get("source", {}).get("type") == "image"
        )
        image["source"]["drift_cells"] = "2 * time"
        errors = []
        validator.validate_document(document, plan, "probe", errors)
        self.assertTrue(
            any("drift must stay constant" in error for error in errors), errors
        )

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
        request = validator.read_json(
            validator.PACKAGE / "templates/asset-request.json"
        )
        request["capability_role"] = "image_editor"
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertTrue(
            any("generate needs image_generator" in error for error in errors), errors
        )

    def test_inpaint_repairs_bind_an_exact_mask(self):
        request = validator.read_json(
            validator.PACKAGE / "templates/asset-request.json"
        )
        request["operation"] = "edit"
        request["capability_role"] = "image_editor"
        request["input_artifacts"] = ["sha256:" + "a" * 64]
        request["repair"] = {"method": "tx_t_inpaint", "mask_artifact": None}
        errors = []
        validator.validate_asset_request(request, "probe", errors)
        self.assertTrue(
            any("needs a mask artifact" in error for error in errors), errors
        )

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
        self.assertTrue(
            any("derived from kind and fit" in error for error in errors), errors
        )

    def test_asset_plan_uses_the_current_forge_texel_density(self):
        fixture = validator.PACKAGE / "fixtures/texture"
        manifest = validator.read_json(fixture / "prototype-manifest.json")
        approval = validator.read_json(fixture / "approval.json")
        plan = validator.read_json(fixture / "implementation-plan.json")
        plan = copy.deepcopy(plan)
        plan["asset_plan"][0]["texels_per_cell"] = 16
        errors = []
        validator.validate_plan(plan, manifest, approval, "probe", errors)
        self.assertTrue(
            any("current forge requires 64" in error for error in errors), errors
        )

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
        self.assertTrue(
            any("must use pending:asset:0" in error for error in errors), errors
        )
        self.assertTrue(
            any("cannot fabricate a descriptor" in error for error in errors), errors
        )

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
        self.assertTrue(
            any("survived exact binding" in error for error in errors), errors
        )

    def test_optimizer_cannot_edit_outside_marked_playbook_body(self):
        playbook = (validator.PACKAGE / "references/playbook.md").read_text(
            encoding="utf-8"
        )
        changed_body = playbook.replace(
            "Choose the cheapest faithful route", "Choose a faithful route"
        )
        self.assertEqual(validator.validate_playbook_candidate(changed_body), [])
        changed_prefix = playbook.replace(
            "The marked section is the only content",
            "Everything is editable",
        )
        errors = validator.validate_playbook_candidate(changed_prefix)
        self.assertTrue(
            any("locked playbook prefix" in error for error in errors), errors
        )


if __name__ == "__main__":
    unittest.main()
