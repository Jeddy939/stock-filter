param(
    [switch]$ProvisionSql,
    [switch]$BuildOnly,
    [switch]$DeployOnly,
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

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found. Install Google Cloud CLI and Firebase CLI first."
    }
}

function Invoke-Gcloud([string[]]$Arguments) {
    & gcloud @Arguments
    if ($LASTEXITCODE -ne 0) { throw "gcloud command failed: gcloud $($Arguments -join ' ')" }
}

Require-Command "gcloud"
Require-Command "firebase"

Invoke-Gcloud @("config", "set", "project", $Project)

$billing = (& gcloud beta billing projects describe $Project --format="value(billingAccountName)" 2>$null).Trim()
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
    & gcloud artifacts repositories describe moneymaker --location $Region 2>$null
    if ($LASTEXITCODE -ne 0) {
        Invoke-Gcloud @("artifacts", "repositories", "create", "moneymaker", "--repository-format=docker", "--location=$Region")
    }
    & gcloud storage buckets describe "gs://$Bucket" 2>$null
    if ($LASTEXITCODE -ne 0) {
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

$secret = (& gcloud secrets describe moneymaker-database-url --format="value(name)" 2>$null).Trim()
if ([string]::IsNullOrWhiteSpace($secret)) {
    if ($CreateDatabaseSecret) {
        $databaseUrl = Read-Host "Paste the PostgreSQL connection URL (input is sent directly to Secret Manager)"
        if ([string]::IsNullOrWhiteSpace($databaseUrl)) { throw "A database URL is required." }
        $databaseUrl | & gcloud secrets create moneymaker-database-url --replication-policy=automatic --data-file=-
        if ($LASTEXITCODE -ne 0) { throw "Could not create the database URL secret." }
    } else {
        Write-Host "Create the database URL secret before deploying, or rerun with -CreateDatabaseSecret:" -ForegroundColor Yellow
        Write-Host '  .\firebase\DEPLOY_FIREBASE.ps1 -DeployOnly -CreateDatabaseSecret'
        throw "Missing Secret Manager secret moneymaker-database-url."
    }
}

$envVars = "MONEYMAKER_REQUIRE_AUTH=true,MONEYMAKER_CLOUD_MODE=true,GOOGLE_CLOUD_PROJECT=$Project,MONEYMAKER_CACHE_BUCKET=$Bucket,FIREBASE_API_KEY=$ApiKey,FIREBASE_APP_ID=$AppId,FIREBASE_AUTH_DOMAIN=$Project.firebaseapp.com,FIREBASE_STORAGE_BUCKET=$Project.firebasestorage.app"

Write-Host "Deploying API service..." -ForegroundColor Cyan
Invoke-Gcloud @("run", "deploy", "moneymaker-api", "--image", $Image, "--region", $Region, "--allow-unauthenticated", "--max", "1", "--set-env-vars", $envVars, "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")

Write-Host "Deploying Cloud Run Jobs..." -ForegroundColor Cyan
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-fetch", "--image", $Image, "--region", $Region, "--command", "python", "--args", "-m,firebase.worker", "--set-env-vars", "MONEYMAKER_JOB_TYPE=fetch,MONEYMAKER_CACHE_BUCKET=$Bucket,GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")
Invoke-Gcloud @("run", "jobs", "deploy", "moneymaker-filter", "--image", $Image, "--region", $Region, "--command", "python", "--args", "-m,firebase.worker", "--set-env-vars", "MONEYMAKER_JOB_TYPE=filter,MONEYMAKER_CACHE_BUCKET=$Bucket,GOOGLE_CLOUD_PROJECT=$Project", "--set-secrets", "MONEYMAKER_DATABASE_URL=moneymaker-database-url:latest")

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
