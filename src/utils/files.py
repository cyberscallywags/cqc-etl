"""File conversion/manipulation functions"""

# Imports
import re
import zipfile
from typing import Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from loguru import logger
from tqdm import tqdm
import polars as pl



class RawPipeline:
    """
    Replace ratings and locations Excel/ODS sheets with Parquet using Polars.
    Return file registry with new file paths.
    """
    def __init__(self, folder_path: Path, schema: dict[str, dict[str, Any]]):
        self.folder_path = folder_path
        self.schema = schema
        self.file_registry = {}

    def run(self):
        """Run pipeline"""
        self.attach_filepaths()
        self.build_registry()
        self.process_files()
        return self.file_registry

    # STEP 1 - Get filepaths
    def attach_filepaths(self):
        """Add filepaths to schema"""
        for cfg in self.schema.values():
            cfg["filepath"] = self.match_file(cfg["match_term"], cfg["prefix"])

    def match_file(self, match_term: str, prefix: str) -> Optional[Path]:
        """Match file in folder using match term or prefix if Parquet."""
        return next(
            (
                fp
                for fp in self.folder_path.iterdir()
                if (
                    (fp.suffix == ".parquet" and fp.name.startswith(prefix))
                    or (fp.suffix != ".parquet" and match_term in fp.name.lower())
                )
            ), None,
        )

    # STEP 2 - Build registry
    def build_registry(self):
        """Build file registry."""
        for name, cfg in self.schema.items():
            fp = cfg["filepath"]
            if fp is None:
                self.file_registry[name] = None
                continue
            if fp.suffix == ".parquet":
                self.file_registry[name] = self.folder_path / fp.name
            else:
                self.file_registry[name] = (
                    self.folder_path / f"{cfg['prefix']}{fp.stem}.parquet"
                )

    # STEP 3 - Convert files
    def process_files(self):
        """Process ODS/XLSX files in parallel."""
        excel_files = [
            f for f in self.folder_path.iterdir()
            if f.suffix in [".xlsx", ".ods"]
        ]

        with ThreadPoolExecutor(max_workers=2) as ex:
            ex.map(self.process_single_file, excel_files)

    def process_single_file(self, fp: Path):
        """Process single file (multiple sheet/column configs) in parallel"""
        tasks = [
            {"name": name, **cfg}
            for name, cfg in self.schema.items()
            if cfg["filepath"] == fp
        ]

        if not tasks:
            return

        with ThreadPoolExecutor(max_workers=4) as ex:
            results = ex.map(lambda t: self.write_parquet(fp, t), tasks)

        if all(results):
            fp.unlink()

    def write_parquet(self, fp: Path, task: dict) -> bool:
        """Read file with Polars and save as Parquet."""
        try:
            df = pl.read_excel(fp, sheet_name=task["sheet"], columns=task["columns"])
            out_file = self.folder_path / f"{task['prefix']}{fp.stem}.parquet" # pylint: disable=W0101
            df.write_parquet(out_file)
            return True
        except Exception: # pylint: disable=W0718
            return False


def load_file(fp: str | Path) -> pl.DataFrame:
    """
    Load a file into a Polars DataFrame.
    Supports CSV (.csv) and Parquet (.parquet) formats.
    Automatically detects header row for CSVs with metadata preamble.
    """
    fp = Path(fp)
    ext = fp.suffix.lower()

    if ext == ".csv":
        # Find number of rows to skip
        # Detect first row where all fields look like strings
        skip = 0
        with fp.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                fields = [x.strip() for x in line.split(",")]

                # Must have multiple columns
                if len(fields) < 2:
                    continue

                # All fields must contain at least one alphabetic character
                if all(re.search(r"[A-Za-z]", field) for field in fields):
                    skip = i
                    break

        try:
            # Try reading normally
            return pl.read_csv(fp, skip_rows=skip)

        except pl.exceptions.ComputeError:
            # Fallback to loose schema
            logger.warning("Read attempt failed. Falling back to loose schema...")
            sample = pl.read_csv(
                fp, skip_rows=skip, n_rows=500, infer_schema_length=10000,
            )

            numeric_types = {pl.Int64, pl.Float64, pl.Int32, pl.Float32}
            schema_overrides = {
                col: pl.Utf8
                for col, dtype in sample.schema.items()
                if dtype not in numeric_types
            }

            return pl.read_csv(
                fp, skip_rows=skip, schema_overrides=schema_overrides,
            )

    elif ext == ".parquet":
        return pl.read_parquet(fp)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Use .csv or .parquet.")


def excel_to_parquet(folder_path, sheet_map: dict[str, tuple[str, ...]]):
    """Convert Excel/ODS sheets to Parquet using Polars."""
    folder_path = Path(folder_path)
    excel_files = list(folder_path.glob("*.xlsx")) + list(folder_path.glob("*.ods"))

    if not excel_files:
        logger.info("No Excel/ODS files found.")
        return

    def match_sheets(fp: Path):
        """Return the sheet tuple for the first matching pattern in the filename."""
        name = fp.name.lower()
        for pattern, sheets in sheet_map.items():
            if pattern.lower() in name:
                return (sheets,) if isinstance(sheets, str) else sheets
        return ()

    def read_sheet(file_path, sheet):
        return sheet, pl.read_excel(file_path, sheet_name=sheet)

    def process_file(fp):
        logger.info(f"Processing {fp.name}...")

        file_sheets = list(match_sheets(fp))
        if not file_sheets:
            logger.info(f"No matching sheets found in {fp.name}, skipping.")
            return

        # Read sheets in parallel
        with ThreadPoolExecutor(max_workers=2) as ex:
            results = list(ex.map(read_sheet, [fp] * len(file_sheets), file_sheets))

        # Write parquet files
        success = True
        for sheet, df in results:
            try:
                out_file = folder_path / f"{sheet}_{fp.stem}.parquet"
                df.write_parquet(out_file)
                logger.success(f"Saved {out_file.name}")
            except (OSError, pl.exceptions.ComputeError) as e:
                success = False
                logger.error(f"Failed writing sheet '{sheet}': {e}")

        if success:
            fp.unlink()
            logger.info(f"Deleted original file: {fp.name}")
        else:
            logger.warning(f"Not deleting {fp.name} due to errors.")

    # Parallelise files
    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(process_file, excel_files))


def excel2parquet_naive(folder_path, sheets: tuple[str, ...]):
    """Convert Excel/ODS sheets to Parquet using Polars."""
    folder_path = Path(folder_path)
    excel_files = list(folder_path.glob("*.xlsx")) + list(folder_path.glob("*.ods"))

    if not excel_files:
        logger.info("No Excel/ODS files found.")
        return

    def read_sheet(file_path, sheet):
        return sheet, pl.read_excel(file_path, sheet_name=sheet)

    for fp in excel_files:
        logger.info(f"Processing {fp.name}...")

        # Parallel sheet reads
        with ThreadPoolExecutor() as ex:
            results = list(ex.map(read_sheet, [fp] * len(sheets), sheets))

        # Write flat parquet files
        success = True
        for sheet, df in results:
            try:
                out_file = folder_path / f"{sheet}_{fp.stem}.parquet"
                df.write_parquet(out_file)
                logger.success(f"Saved {out_file.name}")
            except (OSError, pl.exceptions.ComputeError) as e:
                success = False
                logger.error(f"Failed writing sheet '{sheet}': {e}")

        # Delete original file if all good
        if success:
            fp.unlink()
            logger.info(f"Deleted original file: {fp.name}")
        else:
            logger.warning(f"Not deleting {fp.name} due to errors.")


def excel2parquet(folder_path, sheets: tuple[str, ...]):
    """Convert Excel/ODS sheets to Parquet using Polars."""
    folder_path = Path(folder_path)
    excel_files = list(folder_path.glob("*.xlsx")) + list(folder_path.glob("*.ods"))

    if not excel_files:
        logger.info("No Excel/ODS files found.")
        return

    def trim_sheets(path, sheets):
        with zipfile.ZipFile(path) as z:
            if path.suffix == ".ods":
                try:
                    content = z.read("content.xml").decode("utf-8")
                except KeyError:
                    return ()
                return (s for s in sheets if f'table:name="{s}"' in content)

            if path.suffix == ".xlsx":
                try:
                    wb = z.read("xl/workbook.xml").decode("utf-8")
                except KeyError:
                    return ()
                return (s for s in sheets if f'name="{s}"' in wb)

            return ()

    def read_sheet(file_path, sheet):
        return sheet, pl.read_excel(file_path, sheet_name=sheet)

    def process_file(fp):
        logger.info(f"Processing {fp.name}...")

        file_sheets = list(trim_sheets(fp, sheets))
        if not file_sheets:
            logger.info(f"No matching sheets found in {fp.name}, skipping.")
            return

        # Read sheets in parallel
        with ThreadPoolExecutor(max_workers=2) as ex:
            results = list(ex.map(read_sheet, [fp] * len(file_sheets), file_sheets))

        # Write parquet files
        success = True
        for sheet, df in results:
            try:
                out_file = folder_path / f"{sheet}_{fp.stem}.parquet"
                df.write_parquet(out_file)
                logger.success(f"Saved {out_file.name}")
            except (OSError, pl.exceptions.ComputeError) as e:
                success = False
                logger.error(f"Failed writing sheet '{sheet}': {e}")

        if success:
            fp.unlink()
            logger.info(f"Deleted original file: {fp.name}")
        else:
            logger.warning(f"Not deleting {fp.name} due to errors.")

    # Parallelise files
    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(process_file, excel_files))


def csv2parquet(folder_path, max_workers=8):
    """Convert all CSV files in a folder to Parquet using Polars."""
    folder_path = Path(folder_path)
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        logger.info("No CSV files found.")
        return

    logger.info(f"Found {len(csv_files)} CSV files to process.")

    def convert_csv(fp: Path):
        """Worker: read CSV and write Parquet."""
        try:
            df = load_file(fp)
            out_file = fp.with_suffix(".parquet")
            df.write_parquet(out_file)
            return fp, True
        except (OSError, pl.exceptions.ComputeError) as e:
            logger.error(f"Failed converting {fp.name}: {e}")
            return fp, False

    # Bounded thread pool + progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(convert_csv, fp): fp for fp in csv_files}

        for fut in tqdm(as_completed(futures), total=len(futures), desc="Converting CSVs"):
            fp, ok = fut.result()
            if ok:
                fp.unlink()
                logger.success(f"Converted and deleted {fp.name}")
            else:
                logger.warning(f"Not deleting {fp.name} due to errors.")


def to_parquet(folder_path, sheets: tuple[str, ...]):
    """Convert CSV or Excel/ODS sheets to Parquet using Polars."""
    folder_path = Path(folder_path)

    # Detect file types
    csv_files = list(folder_path.glob("*.csv"))
    excel_files = (
        list(folder_path.glob("*.xlsx")) +
        list(folder_path.glob("*.ods"))
    )

    if not csv_files and not excel_files:
        logger.info("No CSV or Excel/ODS files found.")
        return

    # Process Excel/ODS first
    if excel_files:
        logger.info(f"Found {len(excel_files)} Excel/ODS files.")
        excel2parquet_naive(folder_path, sheets)

    # Process CSV files
    if csv_files:
        logger.info(f"Found {len(csv_files)} CSV files.")
        csv2parquet(folder_path)
