from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


class BuildLiveEvidenceTests(unittest.TestCase):
    def test_performance_bundle_keeps_shape_and_redacts_before_signing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw.json"
            public_key = root / "manifest-public.pem"
            output = root / "signed"
            raw.write_text(
                json.dumps(
                    {
                        "apiVersion": "performance.overmesh.io/v1",
                        "campaign": {
                            "environment": (
                                "https://example.vault.azure.net/"
                                "subscriptions/e74f6a12-1dd5-4652-96a0-f49007c59990"
                            ),
                            "operatorHome": "/Users/alice/.azcopy/jobs/123.log",
                            "operatorIp": "203.0.113.42",
                            "operatorIpV6": "2001:db8::1",
                            "operatorEmail": "alice@example.test",
                            "authorization": "Authorization: token-value",
                            "credential": "Bearer token-value",
                            "directUrl": (
                                "https://example.invalid/blob"
                                "?sv=2026-01-01&se=2026-08-20&sig=secret"
                            ),
                            "jobId": "azcopy-job-123",
                        },
                    }
                ),
                encoding="utf-8",
            )
            public_key.write_text("PUBLIC KEY\n", encoding="utf-8")
            subprocess.run(
                [
                    "python3",
                    "harness/environments/azure/build-live-evidence.py",
                    "--raw-bundle",
                    str(raw),
                    "--output-directory",
                    str(output),
                    "--bundle-name",
                    "performance.json",
                    "--public-key",
                    str(public_key),
                ],
                check=True,
            )
            canonical = json.loads(
                (output / "performance.json").read_text(encoding="utf-8")
            )
            self.assertNotIn("gates", canonical)
            self.assertNotIn("example.vault.azure.net", json.dumps(canonical))
            self.assertNotIn(
                "e74f6a12-1dd5-4652-96a0-f49007c59990",
                json.dumps(canonical),
            )
            serialized = json.dumps(canonical)
            for forbidden in (
                "/Users/",
                "203.0.113.42",
                "2001:db8::1",
                "alice@example.test",
                "token-value",
                "sig=secret",
                ".azcopy",
                "azcopy-job-123",
                "jobId",
            ):
                self.assertNotIn(forbidden, serialized)
            self.assertEqual(
                canonical["redaction"]["canonicalForm"],
                "redacted-before-signing",
            )
            self.assertEqual(
                (output / public_key.name).read_text(encoding="utf-8"),
                "PUBLIC KEY\n",
            )


if __name__ == "__main__":
    unittest.main()
