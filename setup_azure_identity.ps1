$ErrorActionPreference = 'Stop'
$repo = $PSScriptRoot
$secretsDir = Join-Path $repo '.secrets'
$secretsFile = Join-Path $secretsDir 'azure-sp.json'
if (Test-Path $secretsFile) { throw "Credentials already exist: $secretsFile. Refusing to rotate/overwrite." }
if (-not (Get-Command az -ErrorAction SilentlyContinue)) { throw 'Azure CLI (az) is required.' }
$subscription = az account show --query id -o tsv
if ($LASTEXITCODE -ne 0 -or -not $subscription) { throw 'One-time admin az login required to provision the service principal.' }
$tenant = az account show --query tenantId -o tsv
if ($LASTEXITCODE -ne 0 -or -not $tenant) { throw 'Could not get tenant ID.' }
$scope = "/subscriptions/$subscription/resourceGroups/rg-telecom/providers/Microsoft.Storage/storageAccounts/telecomproject/blobServices/default/containers/telecom-data"
Write-Host "Creating service principal (one-time) ..."
# WARNING: The Azure CLI outputs the secret. Capture it without echoing.
$spJson = az ad sp create-for-rbac --name 'sp-telecom-batch-uploader' --skip-assignment --years 1 -o json
if ($LASTEXITCODE -ne 0) { throw 'Failed creating SP. Check Entra app registration permissions.' }
$sp = $spJson | ConvertFrom-Json
if (-not $sp.appId -or -not $sp.password) { throw 'SP create returned incomplete credentials.' }
New-Item -ItemType Directory -Path $secretsDir -Force | Out-Null
# Windows ACL: prevent inheritance and grant full control to current Windows user only.
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
& icacls $secretsDir /inheritance:r /grant:r "${currentUser}:(OI)(CI)F" | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not protect secrets folder ACL; secret not stored. Securely rotate the new SP credential.' }
$payload = [ordered]@{
    tenant_id = $tenant
    client_id = $sp.appId
    client_secret = $sp.password
    storage_account = 'telecomproject'
    container = 'telecom-data'
}
$payload | ConvertTo-Json | Set-Content -Path $secretsFile -Encoding UTF8 -NoNewline
$objectId = az ad sp show --id $sp.appId --query id -o tsv
if ($LASTEXITCODE -ne 0 -or -not $objectId) { throw 'SP created and secret safely stored, but object ID lookup failed; assign RBAC manually.' }
Write-Host "Granting Storage Blob Data Contributor ONLY at container scope..."
az role assignment create --assignee-object-id $objectId --assignee-principal-type ServicePrincipal --role 'Storage Blob Data Contributor' --scope $scope -o none
if ($LASTEXITCODE -ne 0) { throw 'SP and secret created; RBAC failed. Need roleAssignments/write (Owner or RBAC Administrator). Do NOT recreate SP. Assign RBAC to existing app.' }
Write-Host "[OK] Credential file saved at $secretsFile (never commit to git)."
Write-Host '[OK] Daily uploads do not require az login.'
Write-Host 'Note: Azure RBAC propagation can take several minutes.'
