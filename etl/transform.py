from utils.logging_config import logger
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from etl.extract import read_data
from etl.validate import validate_contract


def transform_data():
    """Return a copy with message_month (YYYY-MM) derived from timestamp.

    Call validate_contract first. Preserve the existing timestamp timezone,
    row order, index and source values; do not deduplicate email threads.
    """

    extracted = read_data()
    validated = validate_contract(extracted, "schema_contract.yaml")
    dataframe = validated

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a pandas Datadataframe")
    if dataframe.columns.has_duplicates:
        raise ValueError("The dataset contains duplicate column names")
    if "timestamp" not in dataframe.columns:
        raise ValueError("The timestamp column is required for partitioning")
    if "message_month" in dataframe.columns:
        raise ValueError("message_month already exists; refusing to overwrite it")

    timestamps = dataframe["timestamp"]
    if not is_datetime64_any_dtype(timestamps.dtype):
        raise ValueError("timestamp must be a datetime column; run validation first")
    if timestamps.isna().any():
        raise ValueError("timestamp cannot contain missing values for partitioning")

    transformed = dataframe.copy(deep=True)
    transformed["message_month"] = timestamps.dt.strftime("%Y-%m").astype("string")

    logger.info(
        "Transformed %d rows into %d monthly groups",
        len(transformed),
        transformed["message_month"].nunique(),
    )
    return transformed