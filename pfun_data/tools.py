import sys
import json
import pandas as pd
import zipfile
import concurrent.futures
from pathlib import Path
import argparse
from typing import Optional
import logging
import logging.config
import datetime

# Setup log file path using UTC date
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
tstamp = datetime.datetime.now(
    datetime.timezone.utc
).strftime('%Y-%m-%d-%H_%M')
log_filename = log_dir / f"{tstamp}.log"

logging.config.dictConfig(
    {
        "version": 1,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"}
        },
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
            "file": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "formatter": "standard",
                "filename": str(log_filename),
                "mode": "a",
                "encoding": "utf-8",
            }
        },
        "loggers": {
            "": {
                "handlers": ["default", "file"],
                "level": "INFO",
                "propagate": True
            }
        },
    }
)


def _extract_single_zipitem(zip_path, info_item, output_path, skip_existing: bool = True):
    try:
        full_output_filepath = Path(output_path).joinpath(info_item.filename)
        # skip if flag is True AND file exists AND filesize is the same as expected uncompressed.
        if all([full_output_filepath.exists(),
                skip_existing is True,
                full_output_filepath.stat().st_size == info_item.file_size]):
            logging.info("* Skipping existing file '%s' !", full_output_filepath)
            return True
        logging.info("Extracting '%s'...", info_item.filename)
        with zipfile.ZipFile(zip_path, "r") as zipref:
            zipref.extract(info_item, path=output_path)
    except KeyboardInterrupt:
        logging.warning("Extraction interrupted by user.")
        sys.exit(0)


def extractFromZip(zip_path: Path | str, output_path: Path | str, skip_existing: bool = True):
    """Extract all files from a zipfile."""
    infolist = []
    with zipfile.ZipFile(zip_path, "r") as zipref:
        infolist = [zitem for zitem in zipref.infolist()]
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(_extract_single_zipitem, zip_path,
                            info_item, output_path, skip_existing)
            for info_item in infolist
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logging.error(
                    f"Error extracting zipfile: {future.exception()}")
    logging.info("...done extracting zip.")


def _convert_single_csv_to_parquet(
    csv_file: Path, parquet_path: Path, skip_existing: bool = True
) -> None:
    """Helper function to convert a single CSV file to a Parquet file."""
    try:
        parquet_file = parquet_path.joinpath(csv_file.parent.name, csv_file.stem + ".parquet")
        if all([skip_existing is True, parquet_file.exists()]):
            logging.warning("* Skipping existing file: '%s' !", str(parquet_file))
            return
        logging.info("Creating new database file: '%s'...", str(parquet_file))
        df = pd.read_csv(csv_file)
        df.to_parquet(parquet_file)
    except KeyboardInterrupt:
        logging.warning("Conversion interrupted by user.")
        sys.exit(0)


def convertCsvToParquet(
    csv_path: Path | str, parquet_path: Path | str, skip_existing: bool = True
):
    """Convert CSV files to Parquet files.

    Arguments:
    ----------

    csv_path : input path to directory containing CSV files.

    parquet_path : output path to directory containing .parquet files.

    """
    logging.info("Converting CSVs to Parquet...")
    csv_path = Path(csv_path)
    parquet_path = Path(parquet_path)
    parquet_path.mkdir(parents=False, exist_ok=True)

    # Find all CSV files to convert
    csv_files = [
        root.joinpath(file)
        for root, _, files in csv_path.walk()
        for file in files
        if file.endswith(".csv")
    ]

    # Use a process pool to convert files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                _convert_single_csv_to_parquet, csv_file, parquet_path,
                skip_existing=skip_existing
            )
            for csv_file in csv_files
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logging.error(
                    "Error converting a CSV file to Parquet: %s",
                    str(future.exception())
                )


class Csv2ParquetPipeline:
    def __init__(
        self, zip_path: str | Path, csv_path: str | Path, parquet_path: str | Path,
        skip_existing: bool = True
    ):
        self.zip_path = zip_path
        self.csv_path = csv_path
        self.parquet_path = parquet_path
        #: Flag to indicate if existing files should be skipped (rather than overwritten)
        self.skip_existing = skip_existing

    def __call__(self, parquet_path: Optional[str | Path] = None):
        """Execute the csv2parquet pipeline in order."""

        if parquet_path is not None:
            self.parquet_path = parquet_path

        # Extract ZIP -> CSVs
        self.extract_csvs_from_zip()

        # Convert CSVs to Parquet
        self.convert_csvs_to_parquet()

        return self

    def extract_csvs_from_zip(self):
        return extractFromZip(self.zip_path, self.csv_path, skip_existing=self.skip_existing)

    def convert_csvs_to_parquet(self):
        return convertCsvToParquet(self.csv_path, self.parquet_path, skip_existing=self.skip_existing)


def unzipCsv2Parquet(zip_path, csv_path, parquet_path, skip_existing=True):
    pipeline = Csv2ParquetPipeline(
        zip_path, csv_path, parquet_path, skip_existing=skip_existing)
    pipeline()


def main():
    parser = argparse.ArgumentParser(
        description="Extract CSVs from a ZIP file and convert them to Parquet format."
    )
    parser.add_argument("--zip-path", required=True,
                        help="Path to the input ZIP file.")
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to the directory to extract CSV files into.",
    )
    parser.add_argument(
        "--parquet-path",
        required=True,
        help="Path to the directory to save Parquet files.",
    )
    parser.add_argument(
        "--skip-existing",
        required=False,
        help="Skip existing CSV files.",
        action="store_true",
        default=True
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_false",
        required=False,
        dest="skip_existing",
        help="Set --skip-existing flag to False.",
    )

    parsed_args = parser.parse_args()
    parsed_args_fmt = json.dumps(vars(parsed_args), indent=3)
    input(
        f"\n=== Press any key to confirm (\033[91mor CTRL+C to exit early\033[0m). ====\n\n{parsed_args_fmt}"
    )
    unzipCsv2Parquet(
        parsed_args.zip_path, parsed_args.csv_path, parsed_args.parquet_path,
        skip_existing=parsed_args.skip_existing
    )


if __name__ == "__main__":
    main()
