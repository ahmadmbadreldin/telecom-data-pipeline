#!/usr/bin/env python3
"""Upload a verified immutable full-batch Silver snapshot to Azure Blob/ADLS Gen2.

Authentication: certificate stored outside repository. No interactive login.
Usage:
  python upload_to_azure.py mark-verified --run-id batch_run_001
  python upload_to_azure.py check-auth
  python upload_to_azure.py upload --run-id batch_run_001
"""
import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

TABLES = (
    'areas', 'towers', 'plans', 'services', 'incident_types', 'complaint_types',
    'customers', 'network_incidents_reference', 'bridge_incident_customer',
    'complaints', 'customer_experience'
)
RUN_RE = re.compile(r'^[A-Za-z0-9_-]{1,80}$')

def root_path():
    # Script may live at repo_root/src/telecom_pipeline/ingestion/
    return Path(__file__).resolve().parents[3]

def digest(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for piece in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(piece)
    return h.hexdigest()

def snapshot(run_id, data_root):
    silver = data_root / 'processed' / 'runs' / run_id / 'silver'
    if not silver.is_dir():
        raise ValueError(f'Missing Silver output directory: {silver}')
    entries = []
    for table in TABLES:
        folder = silver / table
        parts = sorted(folder.glob('part-*.parquet'))
        if not parts:
            raise ValueError(f'Missing Parquet data: {folder}')
        for p in parts:
            entries.append({'path': p.relative_to(silver).as_posix(),
                            'size': p.stat().st_size, 'sha256': digest(p)})
    return entries

def manifest_path(run_id, data_root):
    return data_root / 'processed' / 'runs' / run_id / 'verified_manifest.json'

def make_manifest(run_id, data_root):
    """Only invoke AFTER all 5 verifier processes exit with code zero."""
    p = manifest_path(run_id, data_root)
    if p.exists():
        raise ValueError(f'Manifest already exists; refusing to overwrite: {p}')
    content = {'run_id': run_id, 'verified_at_utc': datetime.now(timezone.utc).isoformat(),
               'verification_scripts': [
                   'verify_reference_tables.py', 'verify_network_reference.py',
                   'verify_customers.py', 'verify_incident_impact.py',
                   'verify_customer_feedback.py'], 'files': snapshot(run_id, data_root)}
    p.write_text(json.dumps(content, indent=2) + '\n', encoding='utf-8')
    print(f'[OK] Verification manifest created: {p} ({len(content["files"])} parquet files)')

TENANT_ID = 'ff4a48d6-4b5e-4fd3-8266-7eafc3e6e23e'
CLIENT_ID = 'fe6ceb94-cfc4-442b-a35d-a583c4594729'
STORAGE_ACCOUNT = 'telecomproject'
CONTAINER = 'telecom-data'

def azure_container(certificate):
    from azure.identity import CertificateCredential
    from azure.storage.blob import BlobServiceClient
    if not certificate.is_file():
        raise FileNotFoundError(f'Certificate not found: {certificate}')
    credential = CertificateCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID,
        certificate_path=str(certificate)
    )
    client = BlobServiceClient(
        account_url=f'https://{STORAGE_ACCOUNT}.blob.core.windows.net',
        credential=credential,
    )
    return client.get_container_client(CONTAINER)

def check_auth(certificate):
    container = azure_container(certificate)
    # This checks both certificate authentication and container data-plane RBAC.
    container.get_container_properties()
    print('[SUCCESS] Certificate authentication and Azure container access verified.')

def upload(run_id, data_root, certificate):
    from azure.storage.blob import ContentSettings
    from azure.core.exceptions import ResourceNotFoundError

    p = manifest_path(run_id, data_root)
    if not p.is_file():
        raise ValueError('Verification manifest missing. Run verify_and_mark.ps1 first.')
    expected = json.loads(p.read_text(encoding='utf-8'))
    if expected.get('run_id') != run_id or not expected.get('verification_scripts'):
        raise ValueError('Invalid verification manifest')
    current = snapshot(run_id, data_root)
    if current != expected.get('files'):
        raise ValueError('Silver files changed since verification. Re-run verification using a NEW run ID.')
    prefix = f'batch/runs/{run_id}/silver/'
    container = azure_container(certificate)
    # Fail closed if an older run with this ID has an uploaded completion marker.
    done = container.get_blob_client(f'batch/runs/{run_id}/_UPLOAD_COMPLETE.json')
    if done.exists():
        print('[OK] This run already has an upload completion marker. Checking only local manifest is not sufficient to overwrite it.')
        raise ValueError('Run already published: choose a new RUN_ID. No remote objects were modified.')

    silver = data_root / 'processed' / 'runs' / run_id / 'silver'
    for idx, item in enumerate(current, 1):
        name = prefix + item['path']
        blob = container.get_blob_client(name)
        try:
            props = blob.get_blob_properties()
        except ResourceNotFoundError:
            props = None
        if props is not None:
            remote_hash = (props.metadata or {}).get('sha256')
            if props.size != item['size'] or remote_hash != item['sha256']:
                raise ValueError(f'Existing blob differs: {name}. Refusing to overwrite.')
            print(f'[{idx}/{len(current)}] SKIP verified existing {name}')
            continue
        local_path = silver / item['path']
        with local_path.open('rb') as stream:
            blob.upload_blob(stream, overwrite=False,
                             metadata={'sha256': item['sha256']},
                             content_settings=ContentSettings(content_type='application/octet-stream'),
                             max_concurrency=2)
        props = blob.get_blob_properties()
        if props.size != item['size'] or (props.metadata or {}).get('sha256') != item['sha256']:
            raise RuntimeError(f'Post-upload verification failed: {name}')
        print(f'[{idx}/{len(current)}] UPLOADED {name}')

    # Check exact remote listing and metadata for every file before marking complete.
    actual_names = {b.name for b in container.list_blobs(name_starts_with=prefix)}
    expected_names = {prefix + x['path'] for x in current}
    if actual_names != expected_names:
        raise RuntimeError(f'Unexpected Azure files: missing={expected_names-actual_names}, extra={actual_names-expected_names}')
    for item in current:
        prop = container.get_blob_client(prefix + item['path']).get_blob_properties()
        if prop.size != item['size'] or (prop.metadata or {}).get('sha256') != item['sha256']:
            raise RuntimeError(f'Azure verification failed: {item["path"]}')

    # Final marker publishes the full run only after verification succeeds.
    done_doc = json.dumps({'run_id': run_id, 'file_count': len(current),
                           'verified_at_utc': expected['verified_at_utc'],
                           'uploaded_at_utc': datetime.now(timezone.utc).isoformat(),
                           'files': current}, indent=2).encode('utf-8')
    done.upload_blob(done_doc, overwrite=False,
                     content_settings=ContentSettings(content_type='application/json'))
    print(f'[SUCCESS] Published {len(current)} parquet files to {CONTAINER}/batch/runs/{run_id}/')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['mark-verified', 'check-auth', 'upload'])
    parser.add_argument('--run-id', required=False)
    parser.add_argument('--data-root', type=Path, default=None)
    parser.add_argument('--certificate', type=Path, default=Path(os.environ.get('TELECOM_AZURE_CERT_PATH', str(Path.home() / 'AppData/Roaming/TelecomPipeline/secrets/azure-uploader.pem'))))
    args = parser.parse_args()
    if args.action != 'check-auth' and (not args.run_id or not RUN_RE.fullmatch(args.run_id)):
        parser.error('Valid --run-id required')
    data_root = args.data_root or Path(os.environ.get('TELECOM_DATA_ROOT', str(root_path() / 'data')))
    if args.action == 'mark-verified':
        make_manifest(args.run_id, data_root)
    elif args.action == 'check-auth':
        check_auth(args.certificate)
    else:
        upload(args.run_id, data_root, args.certificate)

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        sys.exit(1)
