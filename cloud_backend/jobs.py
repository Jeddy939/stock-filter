"""Dispatch long-running fetch and filter work to Cloud Run Jobs."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from google.cloud import run_v2


def dispatch(job_type: str, payload: dict[str, Any], job_id: str | None = None) -> dict[str, Any]:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "moneymaker-aedf7")
    region = os.environ.get("MONEYMAKER_RUN_REGION", "australia-southeast1")
    job_name = os.environ.get(
        "MONEYMAKER_FETCH_JOB" if job_type == "fetch" else "MONEYMAKER_FILTER_JOB",
        f"moneymaker-{job_type}",
    )
    job_id = job_id or str(uuid.uuid4())
    full_name = f"projects/{project}/locations/{region}/jobs/{job_name}"
    client = run_v2.JobsClient()
    override = run_v2.RunJobRequest.Overrides(
        container_overrides=[
            run_v2.RunJobRequest.Overrides.ContainerOverride(
                env=[
                    run_v2.EnvVar(name="MONEYMAKER_JOB_ID", value=job_id),
                    run_v2.EnvVar(name="MONEYMAKER_JOB_TYPE", value=job_type),
                    run_v2.EnvVar(name="MONEYMAKER_JOB_PAYLOAD", value=json.dumps(payload)),
                ],
            )
        ]
    )
    operation = client.run_job(request=run_v2.RunJobRequest(name=full_name, overrides=override))
    return {
        "job_id": job_id,
        "job_type": job_type,
        "cloud_run_job": full_name,
        "operation": getattr(operation, "operation", None),
        "queued_at_utc": datetime.now(timezone.utc).isoformat(),
    }
