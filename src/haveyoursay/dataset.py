"""Export datasets from the database and extract attachment text."""

import json
import logging
import platform
import sqlite3
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

from haveyoursay import __version__
from haveyoursay.utils import (connect, extract_text, git_commit, schema_version,
                               sha256_file, utc_now)

logger = logging.getLogger(__name__)

# dataset name -> (view, sort order of the exported rows)
DATASETS = {
    "initiatives": ("initiatives_view", "id"),
    "initiative_topics": ("initiative_topics_view", "initiative_id, code"),
    "initiative_policy_areas": ("initiative_policy_areas_view", "initiative_id, code"),
    "publications": ("publications_view", "initiative_id, id"),
    "publication_attachments": ("publication_attachments_view", "publication_id, id"),
    "feedback": ("feedback_view", "id"),
    "feedback_attachments": ("feedback_attachments_view", "feedback_id, id"),
}

MERGE_PREFIX = {
    "initiatives": "initiative",
    "publications": "publication",
    "publication_attachments": "publication_attachment",
    "feedback": "feedback",
    "feedback_attachments": "feedback_attachment",
}

BOOLEAN_COLUMNS = {"is_major", "is_evaluation", "is_grouped_cfe", "is_original", "published",
                   "id_shared", "id_collision", "fetch_failed"}
INTEGER_COLUMNS = {"id", "initiative_id", "publication_id", "feedback_id", "group_id",
                   "total_feedback", "feedback_period", "pages", "size"}


def coerce_types(df):
    """Nullable booleans and integers, so that NULLs do not turn 0/1 into 0.0/1.0."""
    for column in df.columns:
        if column in BOOLEAN_COLUMNS:
            df[column] = df[column].map({0: False, 1: True, False: False, True: True}).astype("boolean")
        elif column in INTEGER_COLUMNS:
            numeric = pd.to_numeric(df[column], errors="coerce")
            if numeric.notna().sum() == df[column].notna().sum() and (numeric.dropna() % 1 == 0).all():
                df[column] = numeric.astype("Int64")
    return df


def selected_datasets(only=None, attachments=False, long_tables=False):
    only = set(only or ["initiative", "publication", "feedback"])
    names = []
    if "initiative" in only:
        names.append("initiatives")
        if long_tables:
            names += ["initiative_topics", "initiative_policy_areas"]
    if "publication" in only:
        names.append("publications")
        if attachments:
            names.append("publication_attachments")
    if "feedback" in only:
        names.append("feedback")
        if attachments:
            names.append("feedback_attachments")
    return names


def read_dataset(conn, name, include_data=False):
    view, order = DATASETS[name]
    logger.info(f"Reading {name}")
    df = pd.read_sql(f"SELECT * FROM {view} ORDER BY {order}", conn)
    if not include_data and "data" in df.columns:
        df = df.drop(columns=["data"])
    return coerce_types(df)


def write_dataset(df, path, fmt="csv"):
    path = Path(path)
    if not path.suffix:
        path = path.with_suffix(f".{fmt}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "json":
        df.to_json(path, orient="records")
    else:
        raise ValueError(f"Invalid format: {fmt}")
    logger.info(f"Written {path}")
    return path


def _records(df):
    """DataFrame rows as JSON-safe dicts (NULL as null, not NaN)."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def export_datasets(db_path, directory=".", only=None, attachments=False, long_tables=False,
                    merge=False, fmt="csv", include_data=False, arguments=None):
    """Export the selected datasets plus manifest.json and SHA256SUMS. The
    database is opened read-only and is not modified."""
    db_path = Path(db_path)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    names = selected_datasets(only, attachments, long_tables)

    logger.info("Computing database checksum")
    db_sha256 = sha256_file(db_path)
    with connect(db_path, readonly=True) as conn:
        version = schema_version(conn)
        frames = {name: read_dataset(conn, name, include_data) for name in names}
        runs = _records(pd.read_sql("SELECT * FROM runs ORDER BY id", conn))

    written = {}
    if merge:
        merged = merge_datasets(frames)
        path = write_dataset(merged, directory / "haveyoursay", fmt)
        written[path.name] = {"rows": len(merged), "columns": list(merged.columns), "sha256": sha256_file(path)}
    else:
        for name, df in frames.items():
            path = write_dataset(df, directory / name, fmt)
            written[path.name] = {"rows": len(df), "columns": list(df.columns), "sha256": sha256_file(path)}

    manifest = {
        "tool": "haveyoursay",
        "version": __version__,
        "git_commit": git_commit(),
        "created_at": utc_now(),
        "arguments": arguments,
        "database": {"path": str(db_path.resolve()), "sha256": db_sha256,
                     "size_bytes": db_path.stat().st_size, "schema_version": version},
        "runs": runs,
        "environment": {"python": platform.python_version(), "pandas": pd.__version__,
                        "sqlite": sqlite3.sqlite_version},
        "files": written,
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    (directory / "SHA256SUMS").write_text(
        "".join(f"{info['sha256']}  {name}\n" for name, info in written.items()))
    logger.info(f"Manifest written to {directory / 'manifest.json'}")
    return manifest


def merge_datasets(frames):
    """Left-join the datasets into one wide table. Attachment rows and
    feedback rows of the same publication form a cross product; see README."""
    frames = {name: df for name, df in frames.items() if name in MERGE_PREFIX and df is not None}
    if "publication_attachments" in frames and "feedback" in frames:
        logger.warning("The merged dataset contains one row per combination of publication "
                       "attachment and feedback. Deduplicate by feedback_id before computing "
                       "feedback-level statistics or use the separate datasets instead.")
    prefixes = tuple(MERGE_PREFIX.values())
    renamed = {}
    for name in MERGE_PREFIX:
        if name not in frames:
            continue
        df = frames[name]
        renamed[name] = df.rename(columns={
            col: f"{MERGE_PREFIX[name]}_{col}" for col in df.columns
            if not col.startswith(prefixes)})
    merged = None
    for name, df in renamed.items():
        if merged is None:
            merged = df
            continue
        key = {"publications": "initiative_id", "publication_attachments": "publication_id",
               "feedback": "publication_id", "feedback_attachments": "feedback_id"}[name]
        logger.info(f"Merging {name} on {key}")
        merged = merged.merge(df, on=key, how="left", suffixes=("", f"_{MERGE_PREFIX[name]}"))
    return merged


def create_attachments_text_dataset(input_directory=".", output_directory=".", types=None,
                                    parallel=1, pdf_library="pdfplumber", fmt="csv"):
    """Extract text from downloaded attachments into one dataset. Rows are
    keyed by attachment id and type; join to the attachment datasets."""
    input_directory = Path(input_directory)
    output_directory = Path(output_directory)
    types = list(types or ["publication", "feedback"])
    root = input_directory / "data" / "attachments"
    if not root.is_dir():
        raise FileNotFoundError(f"Directory {root} does not exist")
    if "publication" in types and "feedback" in types:
        dataset_type, search_root = "all", root
    elif "publication" in types:
        dataset_type, search_root = "publication", root / "publications"
    else:
        dataset_type, search_root = "feedback", root / "feedback"
    logger.info(f"Creating ({dataset_type}) text dataset")

    files = [p for p in search_root.rglob("*")
             if p.is_file() and p.suffix.lower() in (".txt", ".doc", ".docx", ".pdf")]
    logger.info(f"Found {len(files)} files")

    def extraction_pipeline(path):
        attachment_id = path.parent.name
        attachment_type = path.parent.parent.name.rstrip("s")
        try:
            return attachment_id, attachment_type, extract_text(path, pdf_library=pdf_library), None
        except Exception as e:
            return attachment_id, attachment_type, None, f"Error reading text from {path}: {e}"

    if parallel > 1:
        logger.warning("Error log messages are only written to the log after all items "
                       "have been processed when using parallel processing.")
    results = Parallel(n_jobs=max(parallel, 1))(
        delayed(extraction_pipeline)(path)
        for path in tqdm(files, desc="Extracting text from files"))
    for _, _, _, error in results:
        if error:
            logger.error(error)
    rows = [(i, t, text) for i, t, text, error in results if error is None]
    df = pd.DataFrame(rows, columns=["id", "type", "text"])
    name = f"{dataset_type}_attachments_text" if dataset_type != "all" else "attachments_text"
    return write_dataset(df, output_directory / name, fmt)
