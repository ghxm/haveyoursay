"""Write a censored copy of exported datasets for public release.

Usage: python scripts/censor_datasets.py <input_dir> <output_dir>

Reads the CSV files and manifest.json that `haveyoursay dataset` wrote to
<input_dir>, applies the rules below, and writes the censored CSVs, an
updated manifest.json (with a "censoring" section) and SHA256SUMS to
<output_dir>. The raw JSON column is never carried over.
"""

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

CITIZEN_TYPES = ["EU_CITIZEN", "NON_EU_CITIZEN"]

RULES = {
    "feedback.csv": {
        "drop_columns": ["feedback", "surname", "first_name"],
        "blank_columns": {"organization": {"where_column": "user_type", "in": CITIZEN_TYPES}},
    },
    "feedback_attachments.csv": {
        "drop_columns": ["filename", "title"],
    },
    "*": {
        "drop_columns": ["data"],
    },
}

RULE_NOTES = {
    "feedback.feedback": "free text written by submitters (personal data, third-party copyright)",
    "feedback.surname": "submitter name",
    "feedback.first_name": "submitter name",
    "feedback.organization": "blanked for citizen submissions, where it can hold a personal name; "
                             "kept for organizational actor types",
    "feedback_attachments.filename": "submitter-chosen file name, can contain names",
    "feedback_attachments.title": "submitter-chosen title, can contain names",
    "*.data": "raw portal JSON incl. Commission staff contact details",
}


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def censor_file(src, dst):
    """Apply the rules for one CSV. Returns a dict describing what was done."""
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    rules = {**RULES.get("*", {}), **RULES.get(src.name, {})}
    drop = [c for c in RULES.get("*", {}).get("drop_columns", []) + RULES.get(src.name, {}).get("drop_columns", [])
            if c in df.columns]
    df = df.drop(columns=drop)
    blanked = {}
    for column, rule in rules.get("blank_columns", {}).items():
        if column in df.columns and rule["where_column"] in df.columns:
            mask = df[rule["where_column"]].isin(rule["in"]) & (df[column] != "")
            df.loc[mask, column] = ""
            blanked[column] = int(mask.sum())
    df.to_csv(dst, index=False)
    return {"rows": len(df), "columns": list(df.columns), "dropped_columns": drop,
            "blanked_cells": blanked, "source_sha256": sha256_file(src), "sha256": sha256_file(dst)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args(argv)
    src_dir, out_dir = args.input_dir, args.output_dir
    if not src_dir.is_dir():
        sys.exit(f"Input directory not found: {src_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    files = {}
    for src in sorted(src_dir.glob("*.csv")):
        files[src.name] = censor_file(src, out_dir / src.name)
        print(f"{src.name}: {files[src.name]['rows']} rows, dropped {files[src.name]['dropped_columns']}, "
              f"blanked {files[src.name]['blanked_cells']}")

    manifest_path = src_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
    manifest["files"] = {name: {"rows": info["rows"], "columns": info["columns"], "sha256": info["sha256"]}
                         for name, info in files.items()}
    manifest["censoring"] = {
        "script": "scripts/censor_datasets.py",
        "rules": RULES,
        "rule_notes": RULE_NOTES,
        "source_manifest_sha256": sha256_file(manifest_path) if manifest_path.is_file() else None,
        "applied": {name: {"dropped_columns": info["dropped_columns"], "blanked_cells": info["blanked_cells"],
                           "source_sha256": info["source_sha256"]} for name, info in files.items()},
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    (out_dir / "SHA256SUMS").write_text("".join(f"{info['sha256']}  {name}\n" for name, info in files.items()))
    print(f"Censored datasets written to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
