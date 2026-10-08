"""Local checks for the app's mutation boundary. Real data semantics: scripts/verify.py."""
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("demo_app", Path(__file__).parents[1] / "src/app/app.py")
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class AppTests(unittest.TestCase):
    def setUp(self):
        self.client = demo.app.test_client()

    def test_delete_rejects_invalid_keys_before_sql(self):
        with patch.object(demo, "configuration", return_value={"seed_rows": 1000}), patch.object(demo, "sql") as sql:
            for value in ["", "0", "1001", "1; DROP TABLE x", "42,", None]:
                response = self.client.post("/api/delete", json={"ids": value}, headers={"X-Demo-Action": "1"})
                self.assertEqual(response.status_code, 400)
            sql.assert_not_called()

    def test_mutation_requires_same_origin_json_action(self):
        self.assertEqual(self.client.post("/api/delete", json={"ids": "42"}).status_code, 400)
        response = self.client.post("/api/delete", json={"ids": "42"},
                                    headers={"X-Demo-Action": "1", "Origin": "https://unrelated.example"})
        self.assertEqual(response.status_code, 403)

    def test_active_run_prevents_source_delete(self):
        with patch.object(demo, "configuration", return_value={"seed_rows": 1000}), \
             patch.object(demo, "active_runs", return_value=[object()]), patch.object(demo, "sql") as sql:
            response = self.client.post("/api/delete", json={"ids": "42"}, headers={"X-Demo-Action": "1"})
            self.assertEqual(response.status_code, 409)
            sql.assert_not_called()

    def test_platform_proxy_preserves_same_origin_actions(self):
        config = {"seed_rows": 1000, "namespace": "`afeng`.`cdf_delete_demo`"}
        headers = {"X-Demo-Action": "1", "X-Forwarded-Host": "demo.azure.databricksapps.com",
                   "X-Forwarded-Proto": "https", "Origin": "https://demo.azure.databricksapps.com"}
        with patch.object(demo, "configuration", return_value=config), \
             patch.object(demo, "active_runs", return_value=[]), patch.object(demo, "sql") as sql:
            response = self.client.post("/api/delete", json={"ids": "42"}, headers=headers)
            self.assertEqual(response.status_code, 200)
            sql.assert_called_once()
            headers["Origin"] = "https://unrelated.example"
            response = self.client.post("/api/delete", json={"ids": "42"}, headers=headers)
            self.assertEqual(response.status_code, 403)
            self.assertEqual(sql.call_count, 1)

    def test_delete_binds_keys_and_only_changes_source(self):
        config = {"seed_rows": 1000, "namespace": "`afeng`.`cdf_delete_demo`"}
        with patch.object(demo, "configuration", return_value=config), \
             patch.object(demo, "active_runs", return_value=[]), patch.object(demo, "sql") as sql:
            response = self.client.post("/api/delete", json={"ids": "43,42,42"}, headers={"X-Demo-Action": "1"})
            self.assertEqual(response.status_code, 200)
            self.assertIn("source_orders", sql.call_args.args[0])
            self.assertEqual(sql.call_args.args[1][0].value, "42,43")
            self.assertEqual(sql.call_count, 1)


if __name__ == "__main__":
    unittest.main()
