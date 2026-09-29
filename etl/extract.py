import pandas as pd
from pathlib import Path
from utils.logging_config import logger
import yaml



def read_manifest():
    manifest_file_path = Path("manifest.yaml").resolve()
    if not manifest_file_path.exists():
        logger.error(f"Manifest file not found at {manifest_file_path}")
        raise FileNotFoundError(f"Manifest file not found at {manifest_file_path}")
    with open(manifest_file_path, "r", encoding="utf-8") as f:
        manifest = yaml.safe_load(f)

    # check manifest keys
    if not isinstance(manifest, dict):
        logger.error("Manifest file is not a dictionary")
        raise TypeError("Manifest file is not a dictionary")

    file_format = manifest.get("source", {}).get("format")
    file_path = manifest.get("source", {}).get("file_path")

    # check if format matches file format
    if file_format not in Path(file_path).suffix:
        logger.error(f"File format {file_format} does not match file extension {Path(file_path).suffix}")
        raise ValueError(f"File format {file_format} does not match file extension {Path(file_path).suffix}")

    # check if file exist in manifest.yaml file path
    if not Path(file_path).is_file():
        logger.error(f"File {file_path} does not exist")
        raise FileNotFoundError(f"File {file_path} does not exist")


    return manifest


def read_data():
    manifest = read_manifest()
    datafile = manifest.get("source", {}).get("file_path")
    encoding = manifest.get("source", {}).get("encoding")
    df = pd.read_csv(
        datafile,
        dtype=str,
        encoding=encoding,
        keep_default_na=False,
        skip_blank_lines=False,
        index_col=False,
        on_bad_lines="error"
    )
    if df.empty:
        logger.error("Dataframe is empty")
        raise ValueError("Dataframe is empty")
    logger.info(
        "Extracted %d rows and %d columns from %s",
        len(df),
        len(df.columns),
        Path(manifest.get("source").get("file_path")).name,
    )
    return df


# if __name__ == "__main__":
#     read_data()