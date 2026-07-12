param(
    [switch]$Functions,
    [switch]$Hosting,
    [switch]$DataConnect,
    [switch]$All
)

$ErrorActionPreference = "Stop"

$Project = "moneymaker-aedf7"
$Region = "australia-southeast1"
$FunctionName = "api"
$CloudSqlInstance = "$Project`:$Region`:moneymaker-db"
$DatabaseSecret = "moneymaker-database-url"
$ApiUrl = "https://api-137012961005.australia-southeast1.run.app"
$ApiKey = "AIzaSyA4tXcCkEv26i83WlM8k_dv-EubkjRCFRM"
$AppId = "1:137012961005:web:4e50719b24c3bb382c76e4"
$AppCheckSiteKey = if ($env:FIREBASE_APPCHECK_SITE_KEY) { $env:FIREBASE_APPCHECK_SITE_KEY } else { "" }
$RequireAppCheck = if ($env:MONEYMAKER_REQUIRE_APP_CHECK) { $env:MONEYMAKER_REQUIRE_APP_CHECK } else { "false" }
$Firebase = if (Get-Command firebase.cmd -ErrorAction SilentlyContinue) { "firebase.cmd" } else { "firebase" }
$Gcloud = if (Get-Command gcloud.cmd -ErrorAction SilentlyContinue) { "gcloud.cmd" } else { "gcloud" }

if (-not ($Functions -or $Hosting -or $DataConnect -or $All)) {
    $Functions = $true
    $Hosting = $true
}
if ($All) {
    $Functions = $true
    $Hosting = $true
    $DataConnect = $true
}

function Invoke-Checked([string]$Command, [string[]]$Arguments) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $Command $($Arguments -join ' ')"
    }
}

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    if ($DataConnect) {
        Invoke-Checked $Firebase @("dataconnect:sql:diff", "--project", $Project)
        Write-Host "Review the SQL diff before running dataconnect:sql:migrate." -ForegroundColor Yellow
        Invoke-Checked $Firebase @("deploy", "--only", "dataconnect", "--project", $Project)
    }

    if ($Functions) {
        Invoke-Checked $Firebase @("functions:artifacts:setpolicy", "--location", $Region, "--days", "14", "--force", "--project", $Project)
        Invoke-Checked $Firebase @("deploy", "--only", "functions", "--project", $Project)
        Invoke-Checked $Gcloud @(
            "run", "services", "update", $FunctionName,
            "--region", $Region,
            "--project", $Project,
            "--add-cloudsql-instances", $CloudSqlInstance,
            "--set-env-vars", "MONEYMAKER_REQUIRE_AUTH=true,MONEYMAKER_CLOUD_MODE=true,GOOGLE_CLOUD_PROJECT=$Project,MONEYMAKER_RUN_REGION=$Region,MONEYMAKER_FETCH_JOB=moneymaker-fetch,MONEYMAKER_FILTER_JOB=moneymaker-filter,MONEYMAKER_SCHEDULER_AUDIENCE=$ApiUrl,MONEYMAKER_SCHEDULER_SERVICE_ACCOUNT=moneymaker-scheduler@$Project.iam.gserviceaccount.com,FIREBASE_API_KEY=$ApiKey,FIREBASE_APP_ID=$AppId,FIREBASE_AUTH_DOMAIN=$Project.firebaseapp.com,FIREBASE_STORAGE_BUCKET=$Project.firebasestorage.app,FIREBASE_APPCHECK_SITE_KEY=$AppCheckSiteKey,MONEYMAKER_REQUIRE_APP_CHECK=$RequireAppCheck",
            "--set-secrets", "MONEYMAKER_DATABASE_URL=$DatabaseSecret`:latest"
        )
    }

    if ($Hosting) {
        Invoke-Checked $Firebase @("deploy", "--only", "hosting", "--project", $Project)
    }
} finally {
    Pop-Location
}
