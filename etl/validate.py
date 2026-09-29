from utils.logging_config import logger
import re
from pathlib import Path
import yaml
from etl.extract import read_data
import pandas as pd


class ContractValidationError(ValueError):
    """Raised when the contract or dataset fails validation."""


def _read_contract():
    contract_file_path = Path("schema_contract.yaml").resolve()
    if not contract_file_path.exists():
        logger.error(f"schema_contract file not found at {contract_file_path}")
        raise ContractValidationError(f"schema_contract file not found at {contract_file_path}")
    with open(contract_file_path, "r", encoding="utf-8") as f:
        schema_contract = yaml.safe_load(f)

    # check schema_contract keys
    if not isinstance(schema_contract, dict):
        logger.error("schema_contract file is not a dictionary")
        raise ContractValidationError("schema_contract file is not a dictionary")

    if type(schema_contract.get("allow_extra_columns")) is not bool:
        logger.error("allow_extra_columns must be true or false")
        raise ContractValidationError("allow_extra_columns must be true or false")


    columns = schema_contract.get("columns")
    if not isinstance(columns, list) or not columns:
        logger.error("The contract must define a nonempty columns list")
        raise ContractValidationError("The contract must define a nonempty columns list")

    seen = set()
    for column in columns:
        if not isinstance(column, dict):
            logger.error("Each column rule must be a YAML mapping")
            raise ContractValidationError("Each column rule must be a YAML mapping")
        name = column.get("name")
        if not isinstance(name, str) or not name.strip():
            logger.error("Each column rule needs a nonempty name")
            raise ContractValidationError("Each column rule needs a nonempty name")
        if name in seen:
            logger.error(f"Duplicate column detected: {name}")
            raise ContractValidationError(f"Duplicate column rule: {name}")
        seen.add(name)
        if column.get("type") not in ("integer", "string", "datetime"):
            logger.error(f"Unsupported type for column '{name}'")
            raise ContractValidationError(f"Unsupported type for column '{name}'")
        if type(column.get("nullable")) is not bool:
            logger.error(f"nullable for '{name}' must be true or false")
            raise ContractValidationError(f"nullable for '{name}' must be true or false")

    return schema_contract

def _parse_integer(value):
    # Reject decimals, scientific notation, surrounding spaces and underscores.
    if re.fullmatch(r"[+-]?[0-9]+", value) is None:
        logger.error("Expected an integer")
        raise ValueError("Expected an integer")
    number = int(value)
    if not -(2**63) <= number < 2**63:
        logger.error("Integer is outside the signed 64-bit range")
        raise ValueError("Integer is outside the signed 64-bit range")
    return number


def validate_contract(frame, contract_path: str | Path):
    """Return a typed copy of the raw-text DataFrame, or raise on failure.

    All declared columns are required. Nulls and blank/whitespace-only strings
    are missing values; literal strings such as 'NA' are not. Datetimes must
    use ISO 8601: all naive, or all timezone-aware with the same UTC offset.
    Naive timestamps remain naive; this function does not assume UTC.
    """

    contract = _read_contract()
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")
    if frame.columns.has_duplicates:
        raise ContractValidationError("The dataset contains duplicate column names")
    if not all(isinstance(name, str) for name in frame.columns):
        raise ContractValidationError("Dataset column names must be strings")

    expected = {column["name"] for column in contract["columns"]}
    actual = set(frame.columns)
    missing_columns = sorted(expected - actual)
    extra_columns = sorted(actual - expected)
    if missing_columns:
        raise ContractValidationError(f"Missing required columns: {missing_columns}")
    if extra_columns and not contract["allow_extra_columns"]:
        raise ContractValidationError(f"Unexpected columns: {extra_columns}")
    if frame.empty:
        raise ContractValidationError("The dataset contains no data rows")

    validated = frame.copy(deep=True)
    for column in contract["columns"]:
        name = column["name"]
        values = validated[name]

        # The extractor supplies raw strings. Do not hide prior type inference.
        if not values.dropna().map(lambda value: isinstance(value, str)).all():
            raise ContractValidationError(f"Column '{name}' must arrive as raw CSV text")
        values = values.astype("string")
        missing = values.isna() | values.str.strip().eq("").fillna(False)
        if missing.any() and not column["nullable"]:
            raise ContractValidationError(
                f"Column '{name}' has {int(missing.sum())} missing required value(s)"
            )
        values = values.mask(missing, pd.NA)

        if column["type"] == "integer":
            try:
                numbers = [
                    pd.NA if pd.isna(value) else _parse_integer(value)
                    for value in values
                ]
                validated[name] = pd.array(numbers, dtype="Int64")
            except (ValueError, TypeError, OverflowError):
                raise ContractValidationError(
                    f"Column '{name}' must contain signed 64-bit integers"
                ) from None

        elif column["type"] == "datetime":
            try:
                parsed = pd.to_datetime(values, format="ISO8601", errors="raise")
            except (ValueError, TypeError, OverflowError):
                # Avoid exposing email data through parser error messages.
                raise ContractValidationError(
                    f"Column '{name}' must contain ISO 8601 datetimes "
                    "that are all naive or share the same UTC offset"
                ) from None
            # Pandas accepts the literal 'NaT'; reject it unless the input was missing.
            if (parsed.isna() & ~missing).any():
                raise ContractValidationError(f"Column '{name}' has invalid datetime values")
            validated[name] = parsed

        else:
            validated[name] = values

    logger.info("Validated %d rows against %s", len(validated), Path(contract_path).name)
    return validated


if __name__ == "__main__":
    validate_contract(read_data(), "schema_contract.yaml")