"""Select the Cloud Run service or job process from environment configuration."""

from __future__ import annotations

import os
import sys


if os.environ.get("MONEYMAKER_JOB_TYPE"):
    os.execv(sys.executable, [sys.executable, "-m", "firebase.worker"])
else:
    os.execv(sys.executable, [sys.executable, "-m", "uvicorn", "cloud_backend.app:app", "--host", "0.0.0.0", "--port", os.environ.get("PORT", "8080")])
