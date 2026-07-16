from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE_ROOT = "/Users/shailus2002@gmail.com/retail-lakehouse-files"
SERVERLESS_JOB_FILES = [
    REPO_ROOT / "deploy" / "databricks_daily_job_serverless.json",
    REPO_ROOT / "deploy" / "databricks_backfill_job_serverless.json",
]
DASHBOARD_FILE = REPO_ROOT / "dashboards" / "retail_lakehouse_dashboard.lvdash.json"
DASHBOARD_NAME = "Retail Lakehouse Dashboard"
DEFAULT_WAREHOUSE_ID = "542b132076a46f4c"


def run_databricks(args: list[str], *, output_json: bool = False) -> Any:
    command = ["databricks", *args]
    profile = os.environ.get("DATABRICKS_PROFILE")
    if profile:
        command.extend(["--profile", profile])
    if output_json:
        command.extend(["-o", "json"])

    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    if output_json:
        return json.loads(result.stdout or "{}")
    if result.stdout:
        print(result.stdout.strip())
    return result.stdout


def workspace_root() -> str:
    return os.environ.get("DATABRICKS_WORKSPACE_ROOT", DEFAULT_WORKSPACE_ROOT).rstrip("/")


def upload_workspace_files() -> None:
    root = workspace_root()
    run_databricks(["workspace", "mkdirs", root])
    for directory in ["src", "config", "databricks", "deploy", "dashboards"]:
        source = REPO_ROOT / directory
        if source.exists():
            target = f"{root}/{directory}"
            print(f"Uploading {source.relative_to(REPO_ROOT)} -> {target}")
            run_databricks(["workspace", "import-dir", str(source), target, "--overwrite"])


def find_job_id(job_name: str) -> int | None:
    jobs = run_databricks(["jobs", "list", "--name", job_name, "--limit", "25"], output_json=True)
    for job in jobs if isinstance(jobs, list) else jobs.get("jobs", []):
        settings = job.get("settings", {})
        if job.get("job_id") and (settings.get("name") == job_name or job.get("name") == job_name):
            return int(job["job_id"])
    return None


def deploy_job(settings_path: Path) -> None:
    settings = json.loads(settings_path.read_text())
    job_name = settings["name"]
    job_id = find_job_id(job_name)

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        if job_id is None:
            json.dump(settings, tmp, indent=2)
            tmp_path = tmp.name
            print(f"Creating Databricks job: {job_name}")
            run_databricks(["jobs", "create", "--json", f"@{tmp_path}"], output_json=True)
        else:
            json.dump({"job_id": job_id, "new_settings": settings}, tmp, indent=2)
            tmp_path = tmp.name
            print(f"Resetting Databricks job: {job_name} ({job_id})")
            run_databricks(["jobs", "reset", "--json", f"@{tmp_path}"])

    Path(tmp_path).unlink(missing_ok=True)


def find_dashboard_id(display_name: str) -> str | None:
    dashboards = run_databricks(["lakeview", "list"], output_json=True)
    for dashboard in dashboards if isinstance(dashboards, list) else dashboards.get("dashboards", []):
        if dashboard.get("display_name") == display_name and dashboard.get("dashboard_id"):
            return str(dashboard["dashboard_id"])
    return None


def dashboard_payload() -> dict[str, Any]:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID)
    return {
        "display_name": DASHBOARD_NAME,
        "warehouse_id": warehouse_id,
        "serialized_dashboard": DASHBOARD_FILE.read_text(),
    }


def deploy_dashboard() -> None:
    if not DASHBOARD_FILE.exists():
        print("Dashboard definition not found; skipping dashboard deploy.")
        return

    dashboard_id = find_dashboard_id(DASHBOARD_NAME)
    payload = dashboard_payload()
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        json.dump(payload, tmp, indent=2)
        tmp_path = tmp.name

    if dashboard_id is None:
        print(f"Creating Lakeview dashboard: {DASHBOARD_NAME}")
        created = run_databricks(["lakeview", "create", "--json", f"@{tmp_path}"], output_json=True)
        dashboard_id = created["dashboard_id"]
    else:
        print(f"Updating Lakeview dashboard: {DASHBOARD_NAME} ({dashboard_id})")
        run_databricks(["lakeview", "update", dashboard_id, "--json", f"@{tmp_path}"], output_json=True)

    Path(tmp_path).unlink(missing_ok=True)
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID)
    print(f"Publishing Lakeview dashboard: {DASHBOARD_NAME} ({dashboard_id})")
    run_databricks(["lakeview", "publish", dashboard_id, "--warehouse-id", warehouse_id], output_json=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy repo files, jobs, and dashboard to Databricks.")
    parser.add_argument("--skip-dashboard", action="store_true", help="Only deploy workspace files and jobs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    upload_workspace_files()
    for settings_path in SERVERLESS_JOB_FILES:
        deploy_job(settings_path)
    if not args.skip_dashboard:
        deploy_dashboard()


if __name__ == "__main__":
    main()
