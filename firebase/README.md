# MoneyMaker Firebase backend

This directory contains the online migration and deployment layer. The local
SQLite files remain the rollback source until the migration has been verified.

## Local prerequisites

Install and authenticate the Firebase and Google Cloud CLIs:

```powershell
npm install -g firebase-tools
firebase login
firebase use moneymaker-aedf7
gcloud auth login
gcloud config set project moneymaker-aedf7
```

The project must be on the Blaze plan before Cloud Run or SQL Connect can be
used. Configure a budget alert before deploying.

## Database migration

Create the SQL Connect PostgreSQL database, apply
`firebase/migrations/001_schema.sql`, then set the database URL without
committing it:

```powershell
$env:MONEYMAKER_DATABASE_URL = "postgresql://..."
py firebase\apply_schema.py
py firebase\migrate_sqlite_to_postgres.py `
  --market asx `
  --cache stock_cache.sqlite `
  --ratings-db ratings\central_stock_ratings.sqlite
py firebase\migrate_sqlite_to_postgres.py `
  --market us `
  --cache stock_cache_us.sqlite `
  --ratings-db ratings\central_stock_ratings.sqlite
```

Use `--dry-run` first. The importer is chunked and resumable; it opens source
SQLite files read-only and uses stable market/source keys for upserts.

## Deploying

The intended deployment sequence is:

1. Build the Python service with Cloud Build.
2. Deploy the API service as `moneymaker-api` in `australia-southeast1`.
3. Deploy the fetch, screening, and analysis Cloud Run Jobs.
4. Deploy Firebase Hosting with `firebase deploy --only hosting`.
5. Add Cloud Scheduler jobs for incremental price updates and outcome refreshes.

The first deployment can be performed with Cloud Build, so Docker does not
need to be installed locally:

```powershell
gcloud services enable run.googleapis.com artifactregistry.googleapis.com `
  secretmanager.googleapis.com cloudscheduler.googleapis.com
gcloud artifacts repositories create moneymaker `
  --repository-format=docker --location=australia-southeast1
gcloud storage buckets create gs://moneymaker-aedf7-cache `
  --location=australia-southeast1
gcloud builds submit --tag `
  australia-southeast1-docker.pkg.dev/moneymaker-aedf7/moneymaker/moneymaker:latest .
```

Create the `moneymaker-database-url` Secret Manager secret, then deploy the
service and two long-running jobs:

```powershell
$image = "australia-southeast1-docker.pkg.dev/moneymaker-aedf7/moneymaker/moneymaker:latest"
gcloud run deploy moneymaker-api --image $image --region australia-southeast1 `
  --allow-unauthenticated --max 1 `
  --set-env-vars "MONEYMAKER_REQUIRE_AUTH=true,MONEYMAKER_CLOUD_MODE=true,GOOGLE_CLOUD_PROJECT=moneymaker-aedf7,MONEYMAKER_CACHE_BUCKET=moneymaker-aedf7-cache" `
  --set-secrets MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest
gcloud run jobs deploy moneymaker-fetch --image $image --region australia-southeast1 `
  --command python --args -m,firebase.worker `
  --set-env-vars MONEYMAKER_JOB_TYPE=fetch,MONEYMAKER_CACHE_BUCKET=moneymaker-aedf7-cache `
  --set-secrets MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest
gcloud run jobs deploy moneymaker-filter --image $image --region australia-southeast1 `
  --command python --args -m,firebase.worker `
  --set-env-vars MONEYMAKER_JOB_TYPE=filter,MONEYMAKER_CACHE_BUCKET=moneymaker-aedf7-cache `
  --set-secrets MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest
firebase deploy --only hosting
```

The API service needs permission to invoke the two jobs. The deploy account or
service account must have Cloud Run Invoker and the appropriate service-account
permissions. The exact service account is selected during the first Cloud Run
deployment and should be granted only the roles needed by this project.

The service and jobs must receive database credentials through Secret Manager.
No database URL, service-account key, Yahoo credential, or Firebase private key
belongs in this repository.

After creating a Firebase Web app, set its public configuration on the Cloud
Run service. The API key and app ID are public browser configuration, not the
database credential:

```text
FIREBASE_API_KEY=...
FIREBASE_APP_ID=...
FIREBASE_AUTH_DOMAIN=moneymaker-aedf7.firebaseapp.com
FIREBASE_STORAGE_BUCKET=...
MONEYMAKER_REQUIRE_AUTH=true
```

Enable Anonymous Authentication for the first private deployment, or replace
the browser bootstrap with email/Google sign-in before inviting other users.

## Compatibility and cutover

The current local UI remains available through `START_MONEYMAKER.bat` while
the online service is being tested. Google Sheets remains disabled only after
the online rating path has passed the rating count and duplicate-event checks.
