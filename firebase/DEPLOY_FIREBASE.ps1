param(
    [switch]$ProvisionSql,
    [switch]$BuildOnly,
    [switch]$DeployOnly,
    [switch]$ApplySchema,
    [switch]$ScheduleUpdates,
    [switch]$MigrateCaches,
    [switch]$CreateDatabaseSecret
)

$ErrorActionPreference = "Stop"
$Project = "moneymaker-aedf7"
$Region = "australia-southeast1"
$Image = "$Region-docker.pkg.dev/$Project/moneymaker/moneymaker:latest"
$Bucket = "$Project-cache"
$ApiKey = "AIzaSyA4tXcCkEv26i83WlM8k_dv-EubkjRCFRM"
$AppId = "1:137012961005:web:4e50719b24c3bb382c76e4"
$AppCheckSiteKey = if ($env:FIREBASE_APPCHECK_SITE_KEY) { $env:FIREBASE_APPCHECK_SITE_KEY } else { "" }
$RequireAppCheck = if ($env:MONEYMAKER_REQUIRE_APP_CHECK) { $env:MONEYMAKER_REQUIRE_APP_CHECK } else { "false" }
$UseDataConnect = if ($env:MONEYMAKER_USE_DATA_CONNECT) { $env:MONEYMAKER_USE_DATA_CONNECT } else { "true" }
$UseTaskQueue = if ($env:MONEYMAKER_USE_TASK_QUEUE) { $env:MONEYMAKER_USE_TASK_QUEUE } else { "true" }
$Gcloud = if (Get-Command gcloud.cmd -ErrorAction SilentlyContinue) { "gcloud.cmd" } else { "gcloud" }

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found. Install Google Cloud CLI and Firebase CLI first."
    }
}

function Invoke-Gcloud([string[]]$Arguments) {
    & $Gcloud @Arguments
    if ($LASTEXITCODE -ne 0) { throw "gcloud command failed: $Gcloud $($Arguments -join ' ')" }
}

function Test-Gcloud([string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    & $Gcloud @Arguments 2>&1 | Out-Null
    $code = $LASTEXITCODE
    $ErrorActionPreference = $previous
    return $code
}

Require-Command "gcloud"
Require-Command "firebase"

Invoke-Gcloud @("config", "set", "project", $Project)

$billing = (& $Gcloud billing projects describe $Project --format="value(billingAccountName)" 2>$null).Trim()
if ([string]::IsNullOrWhiteSpace($billing)) {
    throw "Billing is not enabled for $Project. Enable billing before provisioning Cloud SQL or Cloud Run."
}

if ($ProvisionSql) {
    Write-Host "Provisioning Firebase SQL Connect / Cloud SQL..." -ForegroundColor Cyan
    & firebase dataconnect:sql:setup --service moneymaker --location $Region
    if ($LASTEXITCODE -ne 0) { throw "Firebase SQL setup failed." }
}

if (-not $DeployOnly) {
    Invoke-Gcloud @("services", "enable", "run.googleapis.com", "artifactregistry.googleapis.com", "cloudbuild.googleapis.com", "secretmanager.googleapis.com", "cloudscheduler.googleapis.com", "storage.googleapis.com")
    if ((Test-Gcloud @("artifacts", "repositories", "describe", "moneymaker", "--location", $Region)) -ne 0) {
        Invoke-Gcloud @("artifacts", "repositories", "create", "moneymaker", "--repository-format=docker", "--location=$Region")
    }
    if ((Test-Gcloud @("storage", "buckets", "describe", "gs://$Bucket")) -ne 0) {
        Invoke-Gcloud @("storage", "buckets", "create", "gs://$Bucket", "--location=$Region")
    }
}

if (-not $DeployOnly -or $BuildOnly) {
    Write-Host "Building container image..." -ForegroundColor Cyan
    Invoke-Gcloud @("builds", "submit", "--tag", $Image, ".")
}

if ($BuildOnly) {
    Write-Host "Build complete: $Image" -ForegroundColor Green
    exit 0
}

if ($ApplySchema) {
    if (-not $env:MONEYMAKER_DATABASE_URL) {
        Write-Host "Reading the local/public database URL from Secret Manager version 2..." -ForegroundColor Cyan
        $env:MONEYMAKER_DATABASE_URL = (& $Gcloud secrets versions access 2 --secret=moneymaker-database-url --project=$Project).Trim()
        if ([string]::IsNullOrWhiteSpace($env:MONEYMAKER_DATABASE_URL)) {
            throw "Could not read Secret Manager version 2. Set MONEYMAKER_DATABASE_URL manually to the public PostgreSQL URL. Do not use the Cloud Run socket URL."
        }
    }
    Write-Host "Applying PostgreSQL schema and multi-user tables..." -ForegroundColor Cyan
    python firebase\apply_schema.py
}

$previous = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
$secret = (& $Gcloud secrets describe moneymaker-database-url --format="value(name)" 2>&1).Trim()
$ErrorActionPreference = $previous
if ([string]::IsNullOrWhiteSpace($secret)) {
    if ($CreateDatabaseSecret) {
        $databaseUrl = Read-Host "Paste the PostgreSQL connection URL (input is sent directly to Secret Manager)"
        if ([string]::IsNullOrWhiteSpace($databaseUrl)) { throw "A database URL is required." }
        $databaseUrl | & $Gcloud secrets create moneymaker-database-url --replication-policy=automatic --data-file=-
        if ($LASTEXITCODE -ne 0) { throw "Could not create the database URL secret." }
    } else {
        Write-Host "Create the database URL secret before deploying, or rerun with -CreateDatabaseSecret:" -ForegroundColor Yellow
        Write-Host '  .\firebase\DEPLOY_FIREBASE.ps1 -DeployOnly -CreateDatabaseSecret'
        throw "Missing Secret Manager secret moneymaker-database-url."
    }
}

$envVars = "MONEYMAKER_REQUIRE_AUTH=true,MONEYMAKER_CLOUD_MODE=true,GOOGLE_CLOUD_PROJECT=$Project,MONEYMAKER_CACHE_BUCKET=$Bucket,FIREBASE_API_KEY=$ApiKey,FIREBASE_APP_ID=$AppId,FIREBASE_AUTH_DOMAIN=$Project.firebaseapp.com,FIREBASE_STORAGE_BUCKET=$Bucket,MONEYMAKER_STORAGE_BUCKET=$Bucket,FIREBASE_APPCHECK_SITE_KEY=$AppCheckSiteKey,MONEYMAKER_REQUIRE_APP_CHECK=$RequireAppCheck,MONEYMAKER_USE_DATA_CONNECT=$UseDataConnect,MONEYMAKER_DATACONNECT_SERVICE=moneymaker,MONEYMAKER_DATACONNECT_LOCATION=$Region,MONEYMAKER_USE_TASK_QUEUE=$UseTaskQueue"
$ApiUrl = "https://moneymaker-api-137012961005.australia-southeast1.run.app"
$envVars += ",MONEYMAKER_SCHEDULER_AUDIENCE=$ApiUrl,MONEYMAKER_SCHEDULER_SERVICE_ACCOUNT=moneymaker-scheduler@$Project.iam.gserviceaccount.com"

Write-Host "Deploying API service..." -ForegroundColor Cyan
Invoke-Gcloud @("run", "deploy", "moneymaker-api", "--image", $Image, "--region", $Region, "--allow-unauthenticated", "--max", "1", "--set-env-vars", $envVars, "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")

Write-Host "Deploying Cloud Run Jobs..." -ForegroundColor Cyan
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-fetch", "--image", $Image, "--region", $Region, "--command", "python", "--args=-m,firebase.worker", "--task-timeout=14400s", "--max-retries=0", "--set-env-vars", "MONEYMAKER_JOB_TYPE=fetch,MONEYMAKER_CACHE_BUCKET=$Bucket,GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-filter", "--image", $Image, "--region", $Region, "--command", "python", "--args=-m,firebase.worker", "--memory=4Gi", "--task-timeout=3600s", "--max-retries=0", "--set-env-vars", "MONEYMAKER_JOB_TYPE=filter,MONEYMAKER_CACHE_BUCKET=$Bucket,GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-import", "--image", $Image, "--region", $Region, "--command", "python", "--args=-m,firebase.worker", "--memory=4Gi", "--task-timeout=14400s", "--max-retries=0", "--set-env-vars", "MONEYMAKER_JOB_TYPE=import-sqlite,MONEYMAKER_CACHE_BUCKET=$Bucket,FIREBASE_STORAGE_BUCKET=$Bucket,MONEYMAKER_STORAGE_BUCKET=$Bucket,GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-export", "--image", $Image, "--region", $Region, "--command", "python", "--args=-m,firebase.worker", "--memory=1Gi", "--task-timeout=3600s", "--max-retries=0", "--set-env-vars", "MONEYMAKER_JOB_TYPE=export-ratings,MONEYMAKER_CACHE_BUCKET=$Bucket,FIREBASE_STORAGE_BUCKET=$Bucket,MONEYMAKER_STORAGE_BUCKET=$Bucket,GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-weekly-backfill", "--image", $Image, "--region", $Region, "--command", "python", "--args=-m,firebase.backfill_weekly", "--memory=2Gi", "--task-timeout=14400s", "--max-retries=0", "--set-cloudsql-instances", "$Project`:$Region`:moneymaker-db", "--set-env-vars", "GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-weekly-metrics-backfill", "--image", $Image, "--region", $Region, "--command", "python", "--args=-m,firebase.backfill_weekly_metrics", "--memory=2Gi", "--task-timeout=14400s", "--max-retries=0", "--set-cloudsql-instances", "$Project`:$Region`:moneymaker-db", "--set-env-vars", "GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")

if ($ScheduleUpdates) {
    Write-Host "Configuring daily Brisbane fetch schedules..." -ForegroundColor Cyan
    $schedulerEmail = "moneymaker-scheduler@$Project.iam.gserviceaccount.com"
    if ((Test-Gcloud @("iam", "service-accounts", "describe", $schedulerEmail)) -ne 0) {
        Invoke-Gcloud @("iam", "service-accounts", "create", "moneymaker-scheduler", "--display-name=MoneyMaker daily data scheduler")
    }
    $projectNumber = (& $Gcloud projects describe $Project --format="value(projectNumber)").Trim()
    $schedulerAgent = "service-$projectNumber@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
    Invoke-Gcloud @("iam", "service-accounts", "add-iam-policy-binding", $schedulerEmail,
        "--member=serviceAccount:$schedulerAgent", "--role=roles/iam.serviceAccountUser")
    Invoke-Gcloud @("run", "services", "add-iam-policy-binding", "moneymaker-api", "--region=$Region",
        "--member=serviceAccount:$schedulerEmail", "--role=roles/run.invoker")

    $asxBody = '{"market":"asx"}'
    $usBody = '{"market":"us"}'
    $asxArgs = @("scheduler", "jobs", "update", "http", "moneymaker-daily-fetch-asx", "--location=$Region",
        "--schedule=0 0 * * *", "--time-zone=Australia/Brisbane", "--uri=$ApiUrl/api/scheduled-fetch",
        "--http-method=POST", "--update-headers=Content-Type=application/json", "--message-body=$asxBody",
        "--oidc-service-account-email=$schedulerEmail", "--oidc-token-audience=$ApiUrl")
    $usArgs = @("scheduler", "jobs", "update", "http", "moneymaker-daily-fetch-us", "--location=$Region",
        "--schedule=30 0 * * *", "--time-zone=Australia/Brisbane", "--uri=$ApiUrl/api/scheduled-fetch",
        "--http-method=POST", "--update-headers=Content-Type=application/json", "--message-body=$usBody",
        "--oidc-service-account-email=$schedulerEmail", "--oidc-token-audience=$ApiUrl")
    foreach ($job in @(@($asxArgs, "moneymaker-daily-fetch-asx"), @($usArgs, "moneymaker-daily-fetch-us"))) {
        $args = $job[0]
        $name = $job[1]
        if ((Test-Gcloud @("scheduler", "jobs", "describe", $name, "--location=$Region")) -eq 0) {
            Invoke-Gcloud $args
        } else {
            $args[2] = "create"
            Invoke-Gcloud $args
        }
    }
}

if ($MigrateCaches) {
    if (-not $env:MONEYMAKER_DATABASE_URL) {
        throw "Set MONEYMAKER_DATABASE_URL in this PowerShell session before migrating caches."
    }
    Write-Host "Applying PostgreSQL schema..." -ForegroundColor Cyan
    py firebase\apply_schema.py
    Write-Host "Importing Australian cache..." -ForegroundColor Cyan
    py firebase\migrate_sqlite_to_postgres.py --market asx --cache stock_cache.sqlite --ratings-db ratings\central_stock_ratings.sqlite --price-since (Get-Date).AddDays(-14).ToString("yyyy-MM-dd") --full-tickers
    Write-Host "Importing US cache..." -ForegroundColor Cyan
    py firebase\migrate_sqlite_to_postgres.py --market us --cache stock_cache_us.sqlite --ratings-db ratings\central_stock_ratings.sqlite --price-since (Get-Date).AddDays(-14).ToString("yyyy-MM-dd") --full-tickers
}

Write-Host "Deploying Firebase Hosting and security rules..." -ForegroundColor Cyan
& firebase deploy --project $Project --only hosting,firestore:rules
if ($LASTEXITCODE -ne 0) { throw "Firebase deployment failed." }

Write-Host "Deployment complete: https://$Project.web.app" -ForegroundColor Green
