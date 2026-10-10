param([Parameter(Mandatory=$true)][string]$RunId)
$ErrorActionPreference = 'Stop'
if ($RunId -notmatch '^[A-Za-z0-9_-]{1,80}$') { throw 'Invalid RunId' }
$root = $PSScriptRoot
$base = ' /opt/telecom/src/telecom_pipeline/processing/silver/'
$scripts = @('verify_reference_tables.py','verify_network_reference.py','verify_customers.py','verify_incident_impact.py','verify_customer_feedback.py')
foreach ($name in $scripts) {
    Write-Host "Verifying $name ..."
    docker compose --profile jobs run --rm -e "TELECOM_RUN_ID=$RunId" spark-client /opt/spark/bin/spark-submit --master spark://spark-master:7077 "/opt/telecom/src/telecom_pipeline/processing/silver/$name"
    if ($LASTEXITCODE -ne 0) { throw "Verification failed: $name" }
}
python (Join-Path $root 'src/telecom_pipeline/ingestion/upload_to_azure.py') mark-verified --run-id $RunId
if ($LASTEXITCODE -ne 0) { throw 'Cannot mark verification manifest' }
Write-Host '[SUCCESS] Run verified and manifest saved.'
