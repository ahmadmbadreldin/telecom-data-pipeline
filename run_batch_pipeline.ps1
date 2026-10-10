param(
    [string]$RunId = (Get-Date -Format 'yyyyMMdd_HHmmss')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($RunId -notmatch '^[A-Za-z0-9_-]{1,80}$') {
    throw 'RunId must contain only letters, digits, _ or - (max 80 characters).'
}

# Run from the folder containing docker-compose.yml.
$projectRoot = (Get-Location).Path
if (-not (Test-Path (Join-Path $projectRoot 'docker-compose.yml'))) {
    throw 'Open PowerShell in the project root (next to docker-compose.yml).'
}

# Never overwrite or reuse an existing run.
$runDir = Join-Path $projectRoot "data/processed/runs/$RunId"
if (Test-Path $runDir) {
    throw "Run folder already exists: $runDir. Choose a different RunId."
}

$steps = @(
    @{ Name = 'Reference tables'; Clean = 'clean_reference_tables.py'; Verify = 'verify_reference_tables.py' },
    @{ Name = 'Network reference'; Clean = 'clean_network_reference.py'; Verify = 'verify_network_reference.py' },
    @{ Name = 'Customers'; Clean = 'clean_customers.py'; Verify = 'verify_customers.py' },
    @{ Name = 'Incident impact'; Clean = 'clean_incident_impact.py'; Verify = 'verify_incident_impact.py' },
    @{ Name = 'Customer feedback'; Clean = 'clean_customer_feedback.py'; Verify = 'verify_customer_feedback.py' }
)

function Invoke-SparkScript {
    param([string]$Script)
    $scriptPath = "/opt/telecom/src/telecom_pipeline/processing/silver/$Script"
    Write-Host "`n>>> Running $Script (RunId=$RunId)" -ForegroundColor Cyan
    & docker compose --profile jobs run --rm `
        -e "TELECOM_RUN_ID=$RunId" `
        spark-client `
        /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        $scriptPath
    if ($LASTEXITCODE -ne 0) {
        throw "FAILED: $Script (exit code $LASTEXITCODE). RunId=$RunId is NOT verified."
    }
    Write-Host "PASSED: $Script" -ForegroundColor Green
}

Write-Host "Full batch run: $RunId" -ForegroundColor Yellow
Write-Host 'All 11 datasets will be rebuilt from raw/batch CSV files.'
Write-Host 'Outputs will be isolated in data/processed/runs/<RunId>.'
foreach ($step in $steps) {
    Write-Host "`n========== $($step.Name) ==========" -ForegroundColor Yellow
    Invoke-SparkScript -Script $step.Clean
    Invoke-SparkScript -Script $step.Verify
}
Write-Host "`nSUCCESS: Full batch and all five verification scripts passed. RunId=$RunId" -ForegroundColor Green
Write-Host "Verified outputs: $runDir"
Write-Host 'Next: publish this verified run to Azure Data Lake (not done by this script).'
