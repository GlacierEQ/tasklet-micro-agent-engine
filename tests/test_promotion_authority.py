from __future__ import annotations
import hashlib, json, tempfile, unittest
from dataclasses import asdict
from pathlib import Path
from src.promotion_authority import (
    LOCAL_OPERATOR_SECRET, PromotionAuthority, verify_bound_grant,
)
class PromotionAuthTests(unittest.TestCase):
    def test_issue_verify(self):
        a = PromotionAuthority(LOCAL_OPERATOR_SECRET, ttl_s=60)
        g = a.issue("GlacierEQ/x", "abc", "def", now=1000.0)
        ok, r = a.verify(g, now=1001.0)
        self.assertTrue(ok)
    def test_expired(self):
        a = PromotionAuthority(b"test-secret", ttl_s=10)
        g = a.issue("GlacierEQ/x", "abc", "def", now=1000.0)
        ok, r = a.verify(g, now=2000.0)
        self.assertFalse(ok)
        self.assertEqual(r, "GRANT_EXPIRED")
    def test_verify_bound_grant_round_trip_on_injected_now(self):
        # No committed grant file: mint a fresh grant and verify it against
        # a proof receipt at an injected `now`, so the suite never depends
        # on wall-clock time or a committed expiry (the expired-grant
        # time-bomb: machine/promotion_authority.json, not_after 2026-09-09).
        with tempfile.TemporaryDirectory() as tmp:
            proof_path = Path(tmp) / "proof_receipt.json"
            proof = {
                "schema": "glaciereq.proof-receipt.v1",
                "repository": "GlacierEQ/x",
                "source_sha": "abc123",
            }
            proof_path.write_text(json.dumps(proof, sort_keys=True))
            digest = hashlib.sha256(proof_path.read_bytes()).hexdigest()
            authority = PromotionAuthority(LOCAL_OPERATOR_SECRET, ttl_s=60)
            grant = authority.issue("GlacierEQ/x", "abc123", digest, now=1000.0)
            ok, reason = verify_bound_grant(
                asdict(grant), proof_path, secret=LOCAL_OPERATOR_SECRET, now=1001.0
            )
            self.assertTrue(ok, reason)
            ok, reason = verify_bound_grant(
                asdict(grant), proof_path, secret=LOCAL_OPERATOR_SECRET, now=2000.0
            )
            self.assertFalse(ok)
            self.assertEqual(reason, "GRANT_EXPIRED")
if __name__ == "__main__":
    unittest.main()
