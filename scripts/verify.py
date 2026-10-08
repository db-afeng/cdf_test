#!/usr/bin/env python3
"""Integration check against deployed synthetic data; leaves the demo restored.

Requires the app's requirements.txt installed locally. Uses the same control-centre
backend with the deployment user's CLI profile, then exercises the real SDP jobs.
"""
import argparse
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="azure-fe-east")
    parser.add_argument("--target", default="dev")
    parser.add_argument("--var", action="append", default=[])
    parser.add_argument("--deployed-app", action="store_true",
                        help="Use the live app endpoints to also verify its service-principal permissions.")
    args = parser.parse_args()
    flags = ["--profile", args.profile, "--target", args.target]
    for variable in args.var:
        flags += ["--var", variable]
    config = json.loads(subprocess.check_output(
        ["databricks", "bundle", "validate", "--strict", "--output", "json", *flags], cwd=ROOT, text=True))
    summary = json.loads(subprocess.check_output(
        ["databricks", "bundle", "summary", "--output", "json", *flags], cwd=ROOT, text=True))
    for env, key in [("RESET_JOB_ID", "reset_demo"), ("RUN_A_JOB_ID", "run_a"),
                     ("RUN_B_JOB_ID", "run_b"), ("RUN_BOTH_JOB_ID", "run_both")]:
        os.environ[env] = str(summary["resources"]["jobs"][key]["id"])
    os.environ["DATABRICKS_WAREHOUSE_ID"] = config["variables"]["warehouse_id"]["value"]
    os.environ["LOCAL_DATABRICKS_PROFILE"] = args.profile
    # The SDK invokes CLI OAuth with --host; retain the selected profile when
    # several profiles point at the same workspace.
    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    spec = importlib.util.spec_from_file_location("demo_app", ROOT / "src/app/app.py")
    demo = importlib.util.module_from_spec(spec)
    sys.modules["demo_app"] = demo
    spec.loader.exec_module(demo)
    test_client = demo.app.test_client()
    headers = {"X-Demo-Action": "1"}
    observations = []
    app_url = None
    if args.deployed_app:
        app_name = config["variables"]["app_name"]["value"]
        app_url = demo.client().apps.get(app_name).url.rstrip("/")

    def request(method, path, body=None):
        if app_url:
            response = requests.request(method, app_url + path, json=body, timeout=110,
                                        allow_redirects=False,
                                        headers={**demo.client().config.authenticate(),
                                                 **headers, "Origin": app_url})
            if "application/json" not in response.headers.get("Content-Type", ""):
                raise RuntimeError(f"App returned HTTP {response.status_code} instead of JSON")
            data = response.json()
        else:
            response = test_client.open(path, method=method, json=body, headers=headers)
            data = response.get_json()
        if response.status_code != 200:
            raise RuntimeError(data)
        return data

    def run(action):
        run_id = request("POST", "/api/run", {"action": action})["run_id"]
        deadline = time.monotonic() + 1800
        while time.monotonic() < deadline:
            state = request("GET", f"/api/run/{run_id}")
            if state["state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if state["result"] != "SUCCESS":
                    raise RuntimeError(state)
                return
            time.sleep(15)
        raise TimeoutError(f"Run {run_id} did not finish")

    def observe(stage):
        state = request("GET", "/api/state?ids=42")
        observations.append({"stage": stage, **state["metrics"]})
        print(stage, json.dumps(state["metrics"]), flush=True)
        return state["metrics"]

    # Deliberately processes A before B to verify consumer isolation, not only final convergence.
    run("reset")
    baseline = observe("baseline")
    n = baseline["source_rows"]
    assert (baseline["a_rows"], baseline["b_rows"]) == (n, 2 * n)
    try:
        request("POST", "/api/delete", {"ids": "42"})
        deleted = observe("deleted, neither consumer run")
        assert (deleted["source_rows"], deleted["a_rows"], deleted["b_rows"]) == (n - 1, n, 2 * n)
        assert deleted["source_round_events"] == 1
        run("a")
        a = observe("only A caught up")
        assert (a["a_rows"], a["b_rows"]) == (n - 1, 2 * n)
        assert a["a_total_events"] - baseline["a_total_events"] == 1
        assert a["b_total_events"] == baseline["b_total_events"]
        run("b")
        b = observe("both caught up")
        assert b["b_rows"] == 2 * (n - 1)
        assert b["b_total_events"] - baseline["b_total_events"] == 1
        run("both")
        noop = observe("no new source changes")
        assert noop["a_total_events"] == b["a_total_events"]
        assert noop["b_total_events"] == b["b_total_events"]
    finally:
        run("reset")
    restored = observe("reset restored baseline")
    assert (restored["source_rows"], restored["a_rows"], restored["b_rows"]) == (n, n, 2 * n)
    assert restored["a_total_events"] == baseline["a_total_events"] + 2
    assert restored["b_total_events"] == baseline["b_total_events"] + 2
    output = ROOT / ("test-results/app-integration.json" if app_url else "test-results/integration.json")
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(observations, indent=2) + "\n")
    print("PASS: deletion, independent catch-up, no-op and incremental reset. Results:", output)


if __name__ == "__main__":
    main()
