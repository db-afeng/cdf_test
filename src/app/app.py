"""Small control centre. The app queries SQL and starts Jobs; Spark stays in SDP."""
import json
import logging
import os
import re
import threading
import time
from functools import lru_cache
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from flask import Flask, jsonify, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
# Databricks Apps terminates HTTPS and forwards the public host to the container.
# Trust the single platform proxy so same-origin validation uses that public host.
app.wsgi_app = ProxyFix(app.wsgi_app, x_host=1, x_proto=1)
logging.basicConfig(level=logging.INFO)
mutation_lock = threading.Lock()


@lru_cache(maxsize=1)
def client():
    profile = os.getenv("LOCAL_DATABRICKS_PROFILE")
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


@lru_cache(maxsize=1)
def configuration():
    jobs = {key: int(os.environ[env]) for key, env in {
        "reset": "RESET_JOB_ID", "a": "RUN_A_JOB_ID", "b": "RUN_B_JOB_ID",
        "both": "RUN_BOTH_JOB_ID",
    }.items()}
    # Read bundle-resolved parameters from the reset job, avoiding duplicated app settings.
    job = client().jobs.get(jobs["reset"])
    params = next(t.notebook_task.base_parameters for t in job.settings.tasks if t.notebook_task)
    catalog, schema = params["catalog"], params["schema"]
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", x) for x in (catalog, schema)):
        raise ValueError("Invalid demo namespace")
    return {"jobs": jobs, "catalog": catalog, "schema": schema,
            "namespace": f"`{catalog}`.`{schema}`", "seed_rows": int(params["seed_rows"])}


def sql(statement, parameters=None):
    api = client().statement_execution
    result = api.execute_statement(
        statement=statement, warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        wait_timeout="10s", parameters=parameters or [], row_limit=500,
    )
    deadline = time.monotonic() + 75
    while result.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            api.cancel_execution(result.statement_id)
            raise TimeoutError("SQL warehouse is taking too long. Retry after it starts.")
        time.sleep(0.5)
        result = api.get_statement(result.statement_id)
    if result.status.state != StatementState.SUCCEEDED:
        error = result.status.error
        raise RuntimeError(error.message if error else str(result.status.state))
    if result.manifest and result.manifest.truncated:
        raise RuntimeError("SQL result was truncated")
    if not result.result or not result.result.data_array:
        return []
    columns = [c.name for c in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in result.result.data_array]


def parse_ids(value, seed_rows):
    if not isinstance(value, str):
        raise ValueError("Enter comma-separated order IDs")
    tokens = value.split(",")
    if not 1 <= len(tokens) <= 100 or any(not re.fullmatch(r"\s*\d+\s*", t) for t in tokens):
        raise ValueError("Enter up to 100 numeric IDs, separated by commas")
    ids = sorted(set(int(t) for t in tokens))
    if any(i < 1 or i > seed_rows for i in ids):
        raise ValueError(f"Order IDs must be between 1 and {seed_rows}")
    return ids


def active_runs():
    return [r for jid in configuration()["jobs"].values()
            for r in client().jobs.list_runs(job_id=jid, active_only=True, limit=5)]


@app.before_request
def require_json_action():
    # A custom header and JSON content type prevent cross-origin form submissions.
    if request.method == "POST":
        if not request.is_json or request.headers.get("X-Demo-Action") != "1":
            return jsonify(error="JSON demo action required"), 400
        origin = request.headers.get("Origin")
        if origin and origin.split("://", 1)[-1] != request.host:
            app.logger.warning("Rejected action origin %s for public host %s", origin, request.host)
            return jsonify(error="Cross-origin action rejected"), 403


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/state")
def state():
    cfg = configuration()
    ns = cfg["namespace"]
    focused = parse_ids(request.args.get("ids", "42"), cfg["seed_rows"])
    focus_sql = ",".join(str(i) for i in sorted(set(range(1, 16)) | set(focused)))
    baseline = sql(f"SELECT * FROM {ns}.demo_baseline")[0]
    version = int(baseline["baseline_version"])
    source_name = f"{cfg['catalog']}.{cfg['schema']}.source_orders"
    # These are control-centre observations, not the consumers' processing queries.
    parts = [f"""
      SELECT 'metrics' section, to_json(named_struct(
        'source_rows', (SELECT count(*) FROM {ns}.source_orders),
        'a_rows', (SELECT count(*) FROM {ns}.orders_enriched),
        'b_rows', (SELECT count(*) FROM {ns}.order_lines),
        'a_total_events', (SELECT count(*) FROM {ns}.engineer_a_cdf),
        'b_total_events', (SELECT count(*) FROM {ns}.engineer_b_cdf),
        'a_round_events', (SELECT count(*) FROM {ns}.engineer_a_cdf WHERE _commit_version > {version}),
        'b_round_events', (SELECT count(*) FROM {ns}.engineer_b_cdf WHERE _commit_version > {version}),
        'source_round_events', (SELECT count(*) FROM table_changes('{source_name}', {version})
                                 WHERE _commit_version > {version})
      )) payload
    """, f"""
      SELECT 'records' section, to_json(named_struct(
        'order_id', ids.order_id, 'customer', coalesce(s.customer, a.customer),
        'region', coalesce(s.region, a.region), 'amount', coalesce(s.amount, a.amount),
        'source_present', s.order_id IS NOT NULL,
        'a_present', a.order_id IS NOT NULL,
        'b_lines', coalesce(b.line_count, 0)
      )) payload
      FROM (SELECT explode(array({focus_sql})) AS order_id) ids
      LEFT JOIN {ns}.source_orders s ON ids.order_id = s.order_id
      LEFT JOIN {ns}.orders_enriched a ON ids.order_id = a.order_id
      LEFT JOIN (SELECT order_id, count(*) line_count FROM {ns}.order_lines
                 WHERE order_id IN ({focus_sql}) GROUP BY order_id) b ON ids.order_id = b.order_id
    """]
    for engineer in ("a", "b"):
        parts.append(f"""
          SELECT 'ledger_{engineer}' section, to_json(named_struct(
            'version', _commit_version, 'change', _change_type,
            'events', event_count, 'sample_ids', sample_ids
          )) payload FROM (
            SELECT _commit_version, _change_type, count(*) event_count,
                   slice(sort_array(collect_list(order_id)), 1, 10) sample_ids
            FROM {ns}.engineer_{engineer}_cdf GROUP BY _commit_version, _change_type
            ORDER BY _commit_version DESC LIMIT 12
          )
        """)
    result = {"metrics": {}, "records": [], "ledger_a": [], "ledger_b": []}
    for row in sql(" UNION ALL ".join(parts)):
        payload = json.loads(row["payload"])
        if row["section"] == "metrics":
            result["metrics"] = payload
        else:
            result[row["section"]].append(payload)
    result["records"].sort(key=lambda r: r["order_id"])
    for engineer in ("a", "b"):
        result[f"ledger_{engineer}"].sort(key=lambda r: r["version"], reverse=True)
    runs = active_runs()
    result.update(namespace=source_name.rsplit(".", 1)[0], seed_rows=cfg["seed_rows"],
                  baseline=baseline, focused=focused, busy=bool(runs),
                  observed_at=datetime.now(timezone.utc).isoformat(),
                  runs=[{"run_id": r.run_id, "url": r.run_page_url,
                         "state": r.state.life_cycle_state.value} for r in runs])
    return jsonify(result)


@app.post("/api/delete")
def delete():
    cfg = configuration()
    ids = parse_ids((request.get_json() or {}).get("ids", ""), cfg["seed_rows"])
    with mutation_lock:
        if active_runs():
            return jsonify(error="A job is running. Wait for it to finish before deleting orders."), 409
        ns = cfg["namespace"]
        params = [StatementParameterListItem(name="ids", value=",".join(map(str, ids)), type="STRING")]
        predicate = "order_id IN (SELECT CAST(value AS BIGINT) FROM explode(split(:ids, ',')) AS t(value))"
        sql(f"DELETE FROM {ns}.source_orders WHERE {predicate}", params)
    return jsonify(message=f"Deleted orders {', '.join(map(str, ids))} from the source. Run a pipeline to update its output.")


@app.post("/api/run")
def run():
    action = (request.get_json() or {}).get("action")
    if action not in ("a", "b", "both", "reset"):
        raise ValueError("Unknown run action")
    with mutation_lock:
        if active_runs():
            return jsonify(error="A job is running. Wait for it to finish before starting another."), 409
        response = client().jobs.run_now(job_id=configuration()["jobs"][action]).response
    return jsonify(run_id=response.run_id, action=action)


@app.get("/api/run/<int:run_id>")
def run_status(run_id):
    run = client().jobs.get_run(run_id)
    if run.job_id not in configuration()["jobs"].values():
        return jsonify(error="Run does not belong to this demo"), 404
    tasks = [{"name": t.task_key, "state": t.state.life_cycle_state.value,
              "result": t.state.result_state.value if t.state.result_state else None,
              "message": t.state.state_message} for t in run.tasks or []]
    return jsonify(run_id=run_id, state=run.state.life_cycle_state.value,
                   result=run.state.result_state.value if run.state.result_state else None,
                   message=run.state.state_message, tasks=tasks, url=run.run_page_url)


@app.errorhandler(Exception)
def handle_error(error):
    from werkzeug.exceptions import HTTPException
    if isinstance(error, HTTPException):
        return jsonify(error=error.description), error.code
    if isinstance(error, ValueError):
        return jsonify(error=str(error)), 400
    app.logger.exception("Demo action failed")
    return jsonify(error=str(error)), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")), threaded=True)
