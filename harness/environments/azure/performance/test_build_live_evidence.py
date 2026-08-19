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
                            )
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
