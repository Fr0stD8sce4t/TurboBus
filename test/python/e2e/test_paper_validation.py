from __future__ import annotations

from pathlib import Path
import sys
import unittest


BENCHMARKS = Path(__file__).resolve().parents[3] / "benchmarks"
sys.path.insert(0, str(BENCHMARKS))

import paper_validation  # noqa: E402


class PaperValidationEvidenceTest(unittest.TestCase):
    def test_unified_report_rejects_completed_bytes_without_receipt_evidence(self) -> None:
        metric = {
            "report_schema": paper_validation.PHASE6_REPORT_SCHEMA,
            "workload": "model-loading",
            "policy": "daemon-default",
            "job_id": "job-1",
            "session_id": "session-1",
            "workload_kind": "model_weights",
            "cpu_buffer_id": "cpu-buffer",
            "gpu_buffer_id": "gpu-buffer",
            "receipt_ids": "receipt-1",
            "decision_ids": "decision-1",
            "topology_snapshot_ids": "topology-1",
            "ticket_ids": "ticket-1",
            "transfer_bytes": 64,
            "bytes_completed": 64,
            "direct_bytes": 64,
            "relay_bytes": 0,
            "direct_chunks": 1,
            "relay_chunks": 0,
            "transfer_ms": 1.0,
            "performance_ms": 1.0,
            "fallback_reason": "none",
            "executed": False,
            "verified": False,
            "verified_bytes": 0,
            "content_match": False,
            "correctness_status": "incomplete",
        }

        errors = paper_validation.unified_report_validation_errors([metric])

        self.assertIn("missing_execution_evidence", errors)
        self.assertIn("missing_verification_evidence", errors)
        self.assertIn("verified_bytes_mismatch", errors)
        self.assertIn("missing_content_match", errors)
        self.assertIn("invalid_correctness_status", errors)

    def test_correctness_status_requires_executed_and_verified_bytes(self) -> None:
        self.assertEqual(
            paper_validation.correctness_status(
                64,
                64,
                executed=True,
                verified=True,
                verified_bytes=64,
                content_match=True,
            ),
            "complete",
        )
        self.assertEqual(
            paper_validation.correctness_status(
                64,
                64,
                executed=True,
                verified=True,
                verified_bytes=32,
                content_match=True,
            ),
            "incomplete",
        )


if __name__ == "__main__":
    unittest.main()
