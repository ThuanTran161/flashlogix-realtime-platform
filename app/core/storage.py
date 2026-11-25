from typing import Optional
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from loguru import logger
from app.core.config import settings


def get_s3_client():
    """Creates a boto3 S3 client configured for the local MinIO Lakehouse."""
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ROOT_USER,
        aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
        config=Config(
            signature_version="s3v4",
            connect_timeout=2,
            read_timeout=15,
            retries={"max_attempts": 2},
        ),
        region_name="us-east-1",
    )


def ensure_lakehouse_bucket(bucket_name: Optional[str] = None) -> bool:
    """Ensures the target S3/MinIO Lakehouse bucket exists."""
    target_bucket = bucket_name or settings.MINIO_BUCKET
    client = get_s3_client()
    try:
        client.head_bucket(Bucket=target_bucket)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket", "NotFound"):
            client.create_bucket(Bucket=target_bucket)
            logger.info(f"[LAKEHOUSE] Created MinIO bucket '{target_bucket}'.")
            return True
        logger.warning(f"[LAKEHOUSE] Could not verify bucket '{target_bucket}': {exc}")
        return False
    except Exception as exc:
        logger.warning(f"[LAKEHOUSE] MinIO unavailable during bucket check: {exc}")
        return False
