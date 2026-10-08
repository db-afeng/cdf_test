#!/usr/bin/env python3
"""Deploy the entire bundle, initialize it, grant app access, then start the app.

Uses the CLI only. No token copying or Python dependency installation is needed.
"""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="azure-fe-east")
    parser.add_argument("--target", default="dev")
    parser.add_argument("--var", action="append", default=[], metavar="KEY=VALUE")
    args = parser.parse_args()

    def cli(*command, capture=False):
        result = subprocess.run(["databricks", *command, "--profile", args.profile],
                                cwd=ROOT, check=True, text=True,
                                stdout=subprocess.PIPE if capture else None)
        return json.loads(result.stdout) if capture else None

    flags = ["--target", args.target]
    for variable in args.var:
        flags += ["--var", variable]
    config = cli("bundle", "validate", "--strict", "--output", "json", *flags, capture=True)
    values = {key: variable["value"] for key, variable in config["variables"].items()}
    catalog, schema = values["catalog"], values["schema"]

    cli("bundle", "deploy", *flags)
    deployed_app = cli("apps", "get", values["app_name"], "--output", "json", capture=True)
    principal = deployed_app["service_principal_client_id"]

    def grant(securable, name, privileges):
        # Add permissions without replacing anyone else's grants.
        payload = {"changes": [{"principal": principal, "add": privileges}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as file:
            json.dump(payload, file)
            file.flush()
            cli("grants", "update", securable, name, "--json", "@" + file.name,
                "--omit-permissions-in-response")

    # Done after app creation to avoid a schema -> app -> pipeline -> schema dependency cycle.
    grant("catalog", catalog, ["USE_CATALOG"])
    grant("schema", f"{catalog}.{schema}", ["USE_SCHEMA", "SELECT"])
    cli("bundle", "run", "reset_demo", *flags)
    grant("table", f"{catalog}.{schema}.source_orders", ["MODIFY"])
    cli("bundle", "run", "control_centre", *flags)
    deployed_app = cli("apps", "get", values["app_name"], "--output", "json", capture=True)
    print("\nControl centre:", deployed_app.get("url", "See Databricks Apps"))
    print("Demo schema:", f"{catalog}.{schema}")


if __name__ == "__main__":
    main()
