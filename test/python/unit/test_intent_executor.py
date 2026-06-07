from __future__ import annotations

import unittest

from turbobus.intent_executor import (
    _merge_mixed_completion_evidence,
    _relay_only_completion_evidence,
)


class IntentExecutorEvidenceTest(unittest.TestCase):
    def test_mixed_completion_promotes_worker_startup_evidence(self) -> None:
        direct = {
            "verified_bytes": 32,
            "content_match": True,
            "direct_chunks": 2,
            "resource_evidence": {"path": "direct"},
        }
        worker_startup = {
            "startup_source": "worker_process",
            "topology_snapshot_id": "topology-1",
            "require_authenticated_peers": False,
        }
        worker = {
            "verified_bytes": 32,
            "content_match": True,
            "relay_chunks": 2,
            "worker_startup": worker_startup,
            "resource_evidence": {"path": "relay"},
        }

        evidence = _merge_mixed_completion_evidence(
            direct,
            worker,
            expected_bytes=64,
            direct_bytes=32,
            relay_bytes=32,
        )

        self.assertEqual(evidence["worker_startup"], worker_startup)
        self.assertEqual(
            evidence["relay_completion_evidence"]["worker_startup"],
            worker_startup,
        )

    def test_relay_only_completion_promotes_worker_startup_evidence(self) -> None:
        worker_startup = {
            "startup_source": "worker_process",
            "topology_snapshot_id": "topology-1",
            "require_authenticated_peers": False,
        }
        worker = {
            "verified_bytes": 64,
            "content_match": True,
            "worker_startup": worker_startup,
        }

        evidence = _relay_only_completion_evidence(worker, expected_bytes=64)

        self.assertEqual(evidence["worker_startup"], worker_startup)
        self.assertEqual(
            evidence["relay_completion_evidence"]["worker_startup"],
            worker_startup,
        )


if __name__ == "__main__":
    unittest.main()
