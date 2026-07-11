$ErrorActionPreference = "Stop"

$projectId = "moneymaker-aedf7"
$billingAccount = "billingAccounts/01AA82-6F4E02-356E01"
$confirmation = Read-Host "Type LINK BILLING to link My Billing Account 1 to $projectId"
if ($confirmation -cne "LINK BILLING") {
    Write-Host "No changes made."
    exit 1
}

$configPath = Join-Path $env:USERPROFILE ".config\configstore\firebase-tools.json"
if (-not (Test-Path -LiteralPath $configPath)) {
    throw "Firebase CLI credentials were not found. Run FIREBASE_LOGIN.bat first."
}
$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
$token = [string]$config.tokens.access_token
if ([string]::IsNullOrWhiteSpace($token)) {
    throw "Firebase CLI access token is missing. Run FIREBASE_LOGIN.bat first."
}

$headers = @{ Authorization = "Bearer $token" }
$body = @{ billingAccountName = $billingAccount } | ConvertTo-Json
$uri = "https://cloudbilling.googleapis.com/v1/projects/$projectId/billingInfo"
Invoke-RestMethod -Uri $uri -Method Put -Headers $headers -ContentType "application/json" -Body $body | Out-Null

$state = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers
Write-Host "Billing enabled: $($state.billingEnabled)"
Write-Host "Billing account: $($state.billingAccountName)"
