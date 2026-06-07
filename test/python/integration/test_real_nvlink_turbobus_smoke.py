from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[3]


def real_smoke_enabled() -> bool:
    return os.environ.get("TURBOBUS_REAL_NVLINK_SMOKE") == "1"


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


@unittest.skipUnless(
    real_smoke_enabled(),
    "set TURBOBUS_REAL_NVLINK_SMOKE=1 to run real GPU/NVLink smoke",
)
class RealNvlinkTurboBusSmokeTest(unittest.TestCase):
    def test_model_loading_benchmark_uses_relay_path(self) -> None:
        target_gpu = env_int("TURBOBUS_TARGET_GPU", 5)
        mib = env_int("TURBOBUS_SMOKE_MIB", 1024)
        chunk_mib = env_int("TURBOBUS_CHUNK_MIB", 16)
        bucket_mib = env_int("TURBOBUS_BUCKET_MIB", chunk_mib)
        bucket_count = max(1, mib // bucket_mib)

        with tempfile.TemporaryDirectory(prefix="turbobus-real-nvlink-") as tmpdir:
            json_output = Path(tmpdir) / "model_loading.json"
            summary_output = Path(tmpdir) / "model_loading.txt"
            command = [
                sys.executable,
                "benchmarks/model_loading.py",
                "--start-services",
                "--target-gpu",
                str(target_gpu),
                "--bucket-count",
                str(bucket_count),
                "--bucket-bytes",
                str(bucket_mib * 1024 * 1024),
                "--chunk-bytes",
                str(chunk_mib * 1024 * 1024),
                "--iterations",
                "1",
                "--warmup",
                "0",
                "--json-output",
                str(json_output),
                "--summary-output",
                str(summary_output),
                "--no-copy-summary",
                "--daemon-max-inflight-chunks",
                "128",
                "--profile-bytes",
                str(min(mib * 1024 * 1024, 256 * 1024 * 1024)),
            ]
            completed = subprocess.run(
                command,
                cwd=REPO_ROOT,
                env={**os.environ, "TURBOBUS_BENCHMARK_TRACE": "1"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                completed.returncode,
                0,
                completed.stdout + "\n" + completed.stderr,
            )
            data = json.loads(json_output.read_text(encoding="utf-8"))

        summary = data["summary"]
        self.assertEqual(summary["bytes_completed"], summary["bytes"])
        self.assertTrue(summary["executed"])
        self.assertTrue(summary["verified"])
        self.assertTrue(summary["content_match"])
        self.assertGreater(summary["relay_bytes"], 0)
        self.assertGreater(summary["relay_chunks"], 0)
        print(
            "\nreal_nvlink_model_loading "
            f"target_gpu={target_gpu} "
            f"bytes={summary['bytes']} "
            f"direct_bytes={summary['direct_bytes']} "
            f"relay_bytes={summary['relay_bytes']} "
            f"direct_chunks={summary['direct_chunks']} "
            f"relay_chunks={summary['relay_chunks']} "
            f"median_gib_s={summary['median_gib_per_second']:.2f}"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
