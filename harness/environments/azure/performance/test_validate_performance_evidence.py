from __future__ import annotations

import unittest

from validate_performance_evidence import FORBIDDEN


class ValidatePerformanceEvidenceTests(unittest.TestCase):
    def test_canonical_scan_rejects_infrastructure_identifiers(self) -> None:
        rejected = [
            "e74f6a12-1dd5-4652-96a0-f49007c59990",
            "/subscriptions/sub-redacted/resourceGroups/example",
            "example.azurefd.net",
            "example.vault.azure.net",
            "example.blob.core.windows.net",
            "\x1b[31mred",
        ]
        for value in rejected:
            with self.subTest(value=value):
                self.assertTrue(
                    any(pattern.search(value) for pattern in FORBIDDEN)
                )

    def test_canonical_scan_accepts_deterministic_pseudonyms(self) -> None:
        accepted = "sub-4f2a9c1e8b7d3056 fd-6b3e05d7c4a19f28 storage-a"
        self.assertFalse(any(pattern.search(accepted) for pattern in FORBIDDEN))


if __name__ == "__main__":
    unittest.main()
