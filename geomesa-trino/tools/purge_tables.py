"""
Drop all spatial tables without stopping the stack or re-ingesting.

Use this to clear the datastore before benchmarking a single dataset in isolation.
The stack keeps running; run any ingest target afterwards.

Usage:
  python tools/purge_tables.py
  make purge-trino
"""
import sys

import boto3
from botocore.config import Config
from pyiceberg.exceptions import NoSuchTableError

sys.path.insert(0, ".")
from common import local_rest_catalog

_BASE_TABLES = ["observations", "regions", "tdrive", "geolife", "ais"]
# base tables + observations_2geom (ingest_synthetic.py --twogeom).
ALL_TABLES = _BASE_TABLES + ["observations_2geom"]

_CATALOG = local_rest_catalog()

_S3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    config=Config(s3={"addressing_style": "path"}),
    region_name="us-east-1",
)

_BUCKET = "warehouse"


def _delete_prefix(prefix):
    deleted = 0
    paginator = _S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            _S3.delete_objects(
                Bucket=_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
            deleted += len(objects)
    return deleted


def _drop_table(table):
    """Drop the catalog entry (if any) and remove any S3 objects under its
    prefix. Returns True when either side actually had work to do, so callers
    can distinguish a real purge from a no-op on a missing table."""
    dropped = False
    try:
        _CATALOG.drop_table(f"spatial.{table}", purge_requested=False)
        dropped = True
    except NoSuchTableError:
        pass
    if _delete_prefix(f"spatial/{table}/") > 0:
        dropped = True
    return dropped


def main():
    try:
        _S3.head_bucket(Bucket=_BUCKET)
    except Exception as exc:
        print(f"Cannot reach MinIO at localhost:9000 — is the stack running?\n{exc}")
        sys.exit(1)

    for table in ALL_TABLES:
        if _drop_table(table):
            print(f"Dropped iceberg.spatial.{table}")

    print()
    print("All spatial tables dropped. Ingest targets:")
    print("  make ingest-demo-data  # synthetic observations + regions")
    print("  make ingest-tdrive     # T-Drive Beijing taxi (~2M rows)")
    print("  make ingest-geolife    # GeoLife GPS trajectories (~25M rows)")
    print("  make ingest-ais        # Marine AIS vessel tracks (Zone 17, Jan 2017)")


if __name__ == "__main__":
    main()
