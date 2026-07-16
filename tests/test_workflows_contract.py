from pathlib import Path

import yaml


WORKFLOW_FILES = {
    "ci.yml",
    "deploy_databricks.yml",
    "pipeline.yml",
    "backfill.yml",
}


def test_expected_github_actions_workflows_exist() -> None:
    workflow_dir = Path(".github/workflows")
    actual = {path.name for path in workflow_dir.glob("*.yml")}

    assert WORKFLOW_FILES.issubset(actual)


def test_workflows_are_valid_yaml_and_define_jobs() -> None:
    for workflow_name in WORKFLOW_FILES:
        workflow_path = Path(".github/workflows") / workflow_name
        workflow = yaml.safe_load(workflow_path.read_text())

        assert workflow["name"]
        assert workflow["jobs"]
