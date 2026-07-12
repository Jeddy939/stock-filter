param(
    [switch]$Functions,
    [switch]$Hosting,
    [switch]$DataConnect,
    [switch]$AllowDataConnectMigrations,
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
$UseDataConnect = if ($env:MONEYMAKER_USE_DATA_CONNECT) { $env:MONEYMAKER_USE_DATA_CONNECT } else { "false" }
$UseTaskQueue = if ($env:MONEYMAKER_USE_TASK_QUEUE) { $env:MONEYMAKER_USE_TASK_QUEUE } else { "true" }
$DeployTaskFunctions = if ($env:MONEYMAKER_DEPLOY_TASK_FUNCTIONS) { $env:MONEYMAKER_DEPLOY_TASK_FUNCTIONS } else { "true" }
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

function Invoke-Captured([string]$Command, [string[]]$Arguments) {
    $output = & $Command @Arguments 2>&1
    $code = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }
    if ($code -ne 0) {
        throw "Command failed: $Command $($Arguments -join ' ')"
    }
    return ($output -join [Environment]::NewLine)
}

function Invoke-CapturedResult([string]$Command, [string[]]$Arguments) {
    $output = & $Command @Arguments 2>&1
    $code = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }
    return [pscustomobject]@{
        Code = $code
        Text = ($output -join [Environment]::NewLine)
    }
}

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    if ($DataConnect) {
        $dryRunOutput = Invoke-Captured $Firebase @("deploy", "--only", "dataconnect", "--project", $Project, "--dry-run", "--non-interactive")
        $hasRequiredMigration = $dryRunOutput -match "PostgreSQL schema is incompatible"
        if ($hasRequiredMigration -and -not $AllowDataConnectMigrations) {
            throw "Data Connect dry-run found required SQL migrations against the brownfield database. Review the output and rerun with -AllowDataConnectMigrations only if those SQL changes are intentional."
        }
        $deployResult = Invoke-CapturedResult $Firebase @("deploy", "--only", "dataconnect", "--project", $Project, "--non-interactive")
        if ($deployResult.Code -ne 0) {
            $isCompatibleBrownfieldWarning =
                $dryRunOutput -match "Database schema .* is compatible with SQL Connect Schema" -and
                $deployResult.Text -match "Database schema .* is compatible with SQL Connect Schema" -and
                $deployResult.Text -match "Deployed connector" -and
                $deployResult.Text -match "database schema is incompatible"
            if (-not $isCompatibleBrownfieldWarning) {
                throw "Command failed: $Firebase deploy --only dataconnect --project $Project --non-interactive"
            }
            Write-Warning "Data Connect connector deployed and compatible schema validation passed. Firebase CLI still exited nonzero because the brownfield PostgreSQL schema has extra app-owned tables; this script treats that known strict-mode warning as non-fatal."
        }
    }

    if ($Functions) {
        $env:MONEYMAKER_DEPLOY_TASK_FUNCTIONS = $DeployTaskFunctions
        Invoke-Checked $Firebase @("functions:artifacts:setpolicy", "--location", $Region, "--days", "14", "--force", "--project", $Project)
        Invoke-Checked $Firebase @("deploy", "--only", "functions", "--project", $Project)
        $functionEnvVars = "MONEYMAKER_REQUIRE_AUTH=true,MONEYMAKER_CLOUD_MODE=true,GOOGLE_CLOUD_PROJECT=$Project,MONEYMAKER_RUN_REGION=$Region,MONEYMAKER_FETCH_JOB=moneymaker-fetch,MONEYMAKER_FILTER_JOB=moneymaker-filter,MONEYMAKER_SCHEDULER_AUDIENCE=$ApiUrl,MONEYMAKER_SCHEDULER_SERVICE_ACCOUNT=moneymaker-scheduler@$Project.iam.gserviceaccount.com,FIREBASE_API_KEY=$ApiKey,FIREBASE_APP_ID=$AppId,FIREBASE_AUTH_DOMAIN=$Project.firebaseapp.com,FIREBASE_STORAGE_BUCKET=$Project.firebasestorage.app,FIREBASE_APPCHECK_SITE_KEY=$AppCheckSiteKey,MONEYMAKER_REQUIRE_APP_CHECK=$RequireAppCheck,MONEYMAKER_USE_DATA_CONNECT=$UseDataConnect,MONEYMAKER_DATACONNECT_SERVICE=moneymaker,MONEYMAKER_DATACONNECT_LOCATION=$Region,MONEYMAKER_USE_TASK_QUEUE=$UseTaskQueue"
        Invoke-Checked $Gcloud @(
            "run", "services", "update", $FunctionName,
            "--region", $Region,
            "--project", $Project,
            "--add-cloudsql-instances", $CloudSqlInstance,
            "--set-env-vars", $functionEnvVars,
            "--set-secrets", "MONEYMAKER_DATABASE_URL=$DatabaseSecret`:latest"
        )
        foreach ($scheduledService in @(
            "scheduledrefreshasx",
            "scheduledrefreshus",
            "scheduleddefaultscanasx",
            "scheduleddefaultscanus"
        )) {
            Invoke-Checked $Gcloud @(
                "run", "services", "update", $scheduledService,
                "--region", $Region,
                "--project", $Project,
                "--add-cloudsql-instances", $CloudSqlInstance,
                "--set-env-vars", $functionEnvVars,
                "--set-secrets", "MONEYMAKER_DATABASE_URL=$DatabaseSecret`:latest"
            )
        }
        if (@("1", "true", "yes") -contains $DeployTaskFunctions.ToLower()) {
            Invoke-Checked $Gcloud @(
                "run", "services", "update", "refreshtickerbatch",
                "--region", $Region,
                "--project", $Project,
                "--add-cloudsql-instances", $CloudSqlInstance,
                "--set-env-vars", $functionEnvVars,
                "--set-secrets", "MONEYMAKER_DATABASE_URL=$DatabaseSecret`:latest"
            )
        }
    }

    if ($Hosting) {
        Invoke-Checked $Firebase @("deploy", "--only", "hosting", "--project", $Project)
    }
} finally {
    Pop-Location
}
