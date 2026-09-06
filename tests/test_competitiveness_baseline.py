import unittest

from scripts.competitiveness_baseline import (
    REFERENCE_COMMIT,
    build_baseline,
    render_markdown,
)


class CompetitivenessBaselineTests(unittest.TestCase):
    def test_baseline_is_conservative_and_has_all_delivery_areas(self):
        report = build_baseline()

        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["reference"]["commit"], REFERENCE_COMMIT)
        self.assertEqual(report["decision"]["current_assessment"], "production_candidate_not_yet_parity")
        capability_ids = {item["id"] for item in report["capabilities"]}
        self.assertEqual(
            capability_ids,
            {
                "tenant_isolation",
                "session_reliability",
                "channel_breadth",
                "release_evidence",
                "real_im_acceptance",
                "real_runtime_acceptance",
                "performance_acceptance",
                "disaster_recovery",
                "test_system",
                "operations_observability",
            },
        )

    def test_real_environment_claims_are_not_inferred_from_source_paths(self):
        report = build_baseline()
        external_status = {item["id"]: item["local_external_evidence"] for item in report["capabilities"]}

        self.assertEqual(external_status["real_im_acceptance"], "not_run")
        self.assertEqual(external_status["real_runtime_acceptance"], "not_run")
        self.assertEqual(external_status["performance_acceptance"], "not_run")
        self.assertEqual(external_status["disaster_recovery"], "not_run")

    def test_markdown_contains_decision_and_matrix(self):
        markdown = render_markdown(build_baseline())

        self.assertIn("# Competitiveness Baseline", markdown)
        self.assertIn("production_candidate_not_yet_parity", markdown)
        self.assertIn("real IM acceptance", markdown)
        self.assertIn("`not_run`", markdown)


if __name__ == "__main__":
    unittest.main()
