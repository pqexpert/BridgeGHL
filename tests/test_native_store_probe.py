import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from scripts import bind_native_providers as provider
from scripts.probe_native_store import probe

class StoreProbeTests(unittest.TestCase):
    def test_verified_scope_installs_only_isolated_store_binding(self):
        with tempfile.TemporaryDirectory() as root:
            base=Path(root)
            fixture=SimpleNamespace(ProbeError=provider.ProbeError,persist=provider.persist,
                shopify=lambda cfg:({"authentication":"verified","store_identity":"verified",
                    "granted_scopes":["read_products"],"write_execution":"not_attempted"},
                    {"shop_domain":"fixture.myshopify.com","client_id":"fixture","client_secret":"test-only"}))
            result=probe({},fixture,base)
            self.assertEqual(result["status"],"VERIFIED")
            self.assertEqual(result["store_writes"],0)
            self.assertEqual([p.name for p in base.iterdir()],["shopify"])
            self.assertEqual((base/"shopify/credentials.json").stat().st_mode & 0o777,0o600)

    def test_invalid_grant_has_sanitized_reason_and_no_binding(self):
        def denied(config):raise provider.ProbeError("http_400_shop_not_permitted")
        with tempfile.TemporaryDirectory() as root:
            result=probe({},SimpleNamespace(ProbeError=provider.ProbeError,shopify=denied),Path(root))
            self.assertEqual(result["reason"],"http_400_shop_not_permitted")
            self.assertEqual(result["binding"],"not_changed")
            self.assertEqual(list(Path(root).iterdir()),[])
