from __future__ import annotations

import unittest

from turbobus.worker import WorkerTransferRequest, WorkerTransferResult, WorkerTransferState
from turbobus.worker.lifecycle import validate_worker_completion_bytes
from test.python.integration.test_worker_helper import ticket_authorization_payload


class WorkerAuthorizationBoundaryTest(unittest.TestCase):
    def test_rejects_ticket_buffer_mismatch(self) -> None:
        payload = ticket_authorization_payload(source_buffer_id="other-cpu")

        with self.assertRaisesRegex(ValueError, "source buffer"):
            WorkerTransferRequest.from_execution_ticket_payload(payload)

    def test_rejects_stale_ticket_generation(self) -> None:
        payload = ticket_authorization_payload(plan_generation=2)
        payload["plan_generation"] = 1

        with self.assertRaisesRegex(ValueError, "plan_generation is stale"):
            WorkerTransferRequest.from_execution_ticket_payload(payload)

    def test_rejects_range_mismatch_with_ticket_plan(self) -> None:
        payload = ticket_authorization_payload()
        payload["ticket"]["ranges"] = (
            {"src_offset": 0, "dst_offset": 0, "bytes": 8},
        )

        with self.assertRaisesRegex(ValueError, "range bytes"):
            WorkerTransferRequest.from_execution_ticket_payload(payload)

    def test_rejects_completed_bytes_that_do_not_match_plan(self) -> None:
        request = WorkerTransferRequest.from_execution_ticket_payload(
            ticket_authorization_payload()
        )
        result = WorkerTransferResult(
            transfer_id=request.transfer_id,
            state=WorkerTransferState.COMPLETE,
            bytes_completed=8,
        )

        checked = validate_worker_completion_bytes(request, result)

        self.assertEqual(checked.state, WorkerTransferState.FAILED)
        self.assertIn("daemon-planned bytes", checked.error)
        self.assertEqual(checked.bytes_completed, 8)


if __name__ == "__main__":
    unittest.main()
