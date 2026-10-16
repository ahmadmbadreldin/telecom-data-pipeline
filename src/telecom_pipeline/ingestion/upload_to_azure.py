#!/usr/bin/env python3
"""Upload a verified immutable Silver pipeline-group snapshot to Azure Blob/ADLS Gen2.

Authentication: certificate stored outside repository. No interactive login.
Usage:
  python upload_to_azure.py mark-verified --run-id customer_feedback_001 --group customer_feedback
  python upload_to_azure.py check-auth
  python upload_to_azure.py upload --run-id customer_feedback_001 --group customer_feedback
"""
import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

GROUPS = {
    "network_reference": (
        "areas",
        "towers",
    ),
    "reference_tables": (
        "plans",
        "services",
        "incident_types",
        "complaint_types",
    ),
    "customers": (
        "customers",
    ),
    "incident_impact": (
        "network_incidents_reference",
        "bridge_incident_customer",
    ),
    "customer_feedback": (
        "complaints",
        "customer_experience",
    ),
}

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


def snapshot(run_id, data_root, group):
    if group not in GROUPS:
        raise ValueError(
            f'Unknown group: {group}. '
            f'Valid groups: {", ".join(GROUPS)}'
        )

    silver = data_root / 'processed' / 'runs' / run_id / 'silver'

    if not silver.is_dir():
        raise ValueError(f'Missing Silver output directory: {silver}')

    entries = []

    for table in GROUPS[group]:
        folder = silver / table
        parts = sorted(folder.glob('part-*.parquet'))

        if not parts:
            raise ValueError(f'Missing Parquet data: {folder}')

        for p in parts:
            entries.append({
                'path': p.relative_to(silver).as_posix(),
                'size': p.stat().st_size,
                'sha256': digest(p),
            })

    return entries


def manifest_path(run_id, data_root):
    return data_root / 'processed' / 'runs' / run_id / 'verified_manifest.json'


def make_manifest(run_id, data_root, group):
    """Create a verification manifest for one verified pipeline group."""

    if group not in GROUPS:
        raise ValueError(
            f'Unknown group: {group}. '
            f'Valid groups: {", ".join(GROUPS)}'
        )

    p = manifest_path(run_id, data_root)

    if p.exists():
        raise ValueError(
            f'Manifest already exists; refusing to overwrite: {p}'
        )

    verification_script = f'verify_{group}.py'

    content = {
        'run_id': run_id,
        'group': group,
        'verified_at_utc': datetime.now(timezone.utc).isoformat(),
        'verification_scripts': [
            verification_script
        ],
        'files': snapshot(run_id, data_root, group),
    }

    p.write_text(
        json.dumps(content, indent=2) + '\n',
        encoding='utf-8'
    )

    print(
        f'[OK] Verification manifest created: {p} '
        f'({len(content["files"])} parquet files, group={group})'
    )


TENANT_ID = 'ff4a48d6-4b5e-4fd3-8266-7eafc3e6e23e'
CLIENT_ID = 'fe6ceb94-cfc4-442b-a35d-a583c4594729'
STORAGE_ACCOUNT = 'telecomproject'
CONTAINER = 'telecom-data'


def azure_container(certificate):
    from azure.identity import CertificateCredential
    from azure.storage.blob import BlobServiceClient

    if not certificate.is_file():
        raise FileNotFoundError(f'Certificate not found: {certificate}')

    certificate_password = os.environ.get(
        'TELECOM_AZURE_CERT_PASSWORD'
    )

    if not certificate_password:
        raise ValueError(
            'TELECOM_AZURE_CERT_PASSWORD environment variable is not set'
        )

    credential = CertificateCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        certificate_path=str(certificate),
        password=certificate_password,
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

    # NOTE:
    # upload() is intentionally left on the previous interface for now.
    # It will be updated in the next step to accept and validate --group.
    current = snapshot(run_id, data_root, expected.get('group'))

    if current != expected.get('files'):
        raise ValueError(
            'Silver files changed since verification. '
            'Re-run verification using a NEW run ID.'
        )

    prefix = f'batch/runs/{run_id}/silver/'
    container = azure_container(certificate)

    # Fail closed if an older run with this ID has an uploaded completion marker.
    done = container.get_blob_client(
        f'batch/runs/{run_id}/_UPLOAD_COMPLETE.json'
    )

    if done.exists():
        print(
            '[OK] This run already has an upload completion marker. '
            'Checking only local manifest is not sufficient to overwrite it.'
        )
        raise ValueError(
            'Run already published: choose a new RUN_ID. '
            'No remote objects were modified.'
        )

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
                raise ValueError(
                    f'Existing blob differs: {name}. Refusing to overwrite.'
                )

            print(f'[{idx}/{len(current)}] SKIP verified existing {name}')
            continue

        local_path = silver / item['path']

        with local_path.open('rb') as stream:
            blob.upload_blob(
                stream,
                overwrite=False,
                metadata={'sha256': item['sha256']},
                content_settings=ContentSettings(
                    content_type='application/octet-stream'
                ),
                max_concurrency=2
            )

        props = blob.get_blob_properties()

        if (
            props.size != item['size']
            or (props.metadata or {}).get('sha256') != item['sha256']
        ):
            raise RuntimeError(f'Post-upload verification failed: {name}')

        print(f'[{idx}/{len(current)}] UPLOADED {name}')

    actual_names = {
    b.name
    for b in container.list_blobs(name_starts_with=prefix)
    if b.name.endswith(".parquet")
    }
    expected_names = {
        prefix + x['path'] for x in current
    }

    if actual_names != expected_names:
        raise RuntimeError(
            f'Unexpected Azure files: '
            f'missing={expected_names - actual_names}, '
            f'extra={actual_names - expected_names}'
        )

    for item in current:
        prop = container.get_blob_client(
            prefix + item['path']
        ).get_blob_properties()

        if (
            prop.size != item['size']
            or (prop.metadata or {}).get('sha256') != item['sha256']
        ):
            raise RuntimeError(
                f'Azure verification failed: {item["path"]}'
            )

    done_doc = json.dumps({
        'run_id': run_id,
        'group': expected.get('group'),
        'file_count': len(current),
        'verified_at_utc': expected['verified_at_utc'],
        'uploaded_at_utc': datetime.now(timezone.utc).isoformat(),
        'files': current
    }, indent=2).encode('utf-8')

    done.upload_blob(
        done_doc,
        overwrite=False,
        content_settings=ContentSettings(
            content_type='application/json'
        )
    )

    print(
        f'[SUCCESS] Published {len(current)} parquet files '
        f'to {CONTAINER}/batch/runs/{run_id}/'
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'action',
        choices=['mark-verified', 'check-auth', 'upload']
    )
    parser.add_argument('--run-id', required=False)
    parser.add_argument('--group', choices=GROUPS.keys(), required=False)
    parser.add_argument('--data-root', type=Path, default=None)
    parser.add_argument(
        '--certificate',
        type=Path,
        default=Path(
            os.environ.get(
                'TELECOM_AZURE_CERT_PATH',
                str(
                    Path.home()
                    / 'AppData/Roaming/TelecomPipeline/secrets/azure-uploader.pfx'
                )
            )
        )
    )

    args = parser.parse_args()

    if args.action != 'check-auth':
        if not args.run_id or not RUN_RE.fullmatch(args.run_id):
            parser.error('Valid --run-id required')

        if not args.group:
            parser.error('--group required')

    data_root = args.data_root or Path(
        os.environ.get(
            'TELECOM_DATA_ROOT',
            str(root_path() / 'data')
        )
    )

    if args.action == 'mark-verified':
        make_manifest(args.run_id, data_root, args.group)
    elif args.action == 'check-auth':
        check_auth(args.certificate)
    else:
        # upload() reads and validates the group recorded in the manifest.
        upload(args.run_id, data_root, args.certificate)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        sys.exit(1)
