from __future__ import annotations

from pathlib import Path
import unittest

import turbobus
from turbobus.schema import RequestType


class PackageBoundaryTest(unittest.TestCase):
    def test_public_package_exports_runtime_session_not_legacy_client(self) -> None:
        self.assertIn("TurboBusRuntimeSession", turbobus.__all__)
        self.assertNotIn("TurboBusClient", turbobus.__all__)
        self.assertFalse(hasattr(turbobus, "TurboBusClient"))
        self.assertFalse(Path(turbobus.__file__).with_name("api.py").exists())

    def test_daemon_protocol_does_not_expose_legacy_transfer_requests(self) -> None:
        names = {item.name for item in RequestType}

        self.assertIn("SUBMIT_TRANSFER_INTENT", names)
        self.assertIn("WAIT_TRANSFER_RECEIPT", names)
        self.assertNotIn("PLAN_TRANSFER", names)
        self.assertNotIn("RESERVE_TRANSFER", names)


if __name__ == "__main__":
    unittest.main()
