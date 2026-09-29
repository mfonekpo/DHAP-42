from utils.logging_config import logger
import re
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

from etl.extract import read_manifest
from etl.transform import transform_data

def load_to_minio(
    frame,
    *,
    bucket_name: str,
    dataset: str,
    aws_conn_id: str = "minio_s3",
    s3_hook=None,
):
    """Upload one Parquet file per month and return a small result summary.

    Input must have passed validate_contract and transform_data. Repeating the
    same full CSV overwrites the same keys. Uploads are not atomic as a group,
    and this function does not delete partitions absent from a later CSV.
    The optional s3_hook supports local tests without contacting MinIO.
    """


    if not isinstance(bucket_name, str) or not bucket_name.strip():
        raise ValueError("bucket_name must be a nonempty string")
    if not isinstance(dataset, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", dataset) is None:
        raise ValueError("dataset must contain only letters, digits, underscores or hyphens")
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError("frame must be a nonempty pandas DataFrame")
    if frame.columns.has_duplicates:
        raise ValueError("The dataset contains duplicate column names")
    if not {"timestamp", "message_month"}.issubset(frame.columns):
        raise ValueError("timestamp and message_month are required; run transformation first")

    months = frame["message_month"]
    if months.isna().any():
        raise ValueError("message_month cannot contain missing values")
    valid_months = months.map(
        lambda value: isinstance(value, str)
        and re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", value) is not None
    )
    if not valid_months.all():
        raise ValueError("message_month must use YYYY-MM with a valid month")

    timestamps = frame["timestamp"]
    if not is_datetime64_any_dtype(timestamps.dtype) or timestamps.isna().any():
        raise ValueError("timestamp must contain validated, non-null datetimes")
    if not months.eq(timestamps.dt.strftime("%Y-%m")).all():
        raise ValueError("message_month must match each row's timestamp")

    if s3_hook is None:

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # Check the existing bucket before doing any serialization work.
    s3_hook.get_conn().head_bucket(Bucket=bucket_name)

    with TemporaryDirectory(prefix="dhap42-parquet-") as temporary_dir:
        uploads = []
        for month, group in frame.groupby("message_month", sort=True, observed=True):
            relative_path = Path(f"message_month={month}") / "part-00000.parquet"
            local_path = Path(temporary_dir) / relative_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Hive partition discovery restores message_month from the directory.
            group.drop(columns="message_month").to_parquet(
                local_path,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            uploads.append((local_path, f"{dataset}/{relative_path.as_posix()}"))

        # Finish serializing every group before uploading the first object.
        for local_path, key in uploads:
            s3_hook.load_file(
                filename=str(local_path),
                key=key,
                bucket_name=bucket_name,
                replace=True,
            )

    summary = {
        "bucket": bucket_name,
        "prefix": f"{dataset}/",
        "rows": len(frame),
        "files": len(uploads),
    }
    logger.info(
        "Uploaded %d rows in %d Parquet files to s3://%s/%s",
        summary["rows"], summary["files"], summary["bucket"], summary["prefix"],
    )
    return summary


if __name__ == "__main__":

    # Your manifest and schema paths are relative to the project root.
    project_root = Path(__file__).resolve().parents[1]
    os.chdir(project_root)

    manifest = read_manifest()

    # Your current transform_data() already calls extraction and validation.
    transformed = transform_data()

    result = load_to_minio(
        transformed,
        bucket_name=os.environ["MINIO_BUCKET"],
        dataset=manifest["dataset"],
        aws_conn_id="minio_s3",
    )

    print(result)