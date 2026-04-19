"""
lambda_function/dbt_runner.py
Runs dbt run + dbt test via subprocess after Glue ETL completes.
Called as the final Step Functions state "RunDbt".
Uses dbt CLI installed in Lambda layer or as a pip package.

Alternative pattern used here: invoke dbt via AWS CodeBuild
(more reliable than running dbt in Lambda due to memory/timeout).
"""
import boto3
import json
import logging
import os

log = logging.getLogger()
log.setLevel(logging.INFO)

cb  = boto3.client("codebuild")
sns = boto3.client("sns")

DBT_PROJECT_NAME = os.environ.get("CODEBUILD_PROJECT", "olist-dbt-run")
SNS_ARN          = os.environ.get("SNS_ARN", "")


def handler(event, context):
    run_id = event.get("run_id", "unknown")
    log.info(json.dumps({"event": "dbt_trigger", "run_id": run_id}))

    try:
        resp = cb.start_build(
            projectName=DBT_PROJECT_NAME,
            environmentVariablesOverride=[
                {"name": "RUN_ID", "value": run_id, "type": "PLAINTEXT"}
            ]
        )
        build_id = resp["build"]["id"]
        log.info(json.dumps({"event": "dbt_build_started", "build_id": build_id}))
        return {"status": "STARTED", "build_id": build_id, "run_id": run_id}

    except Exception as exc:
        log.error(json.dumps({"event": "dbt_trigger_failed", "error": str(exc)}))
        if SNS_ARN:
            sns.publish(
                TopicArn=SNS_ARN,
                Subject=f"🚨 Olist dbt trigger failed — run {run_id}",
                Message=str(exc),
            )
        raise
