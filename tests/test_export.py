import csv
import hashlib
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from haveyoursay import SCHEMA_VERSION
from haveyoursay.dataset import export_datasets, selected_datasets
from haveyoursay.utils import SchemaError, connect

CODEBOOK = Path(__file__).parent.parent / "docs" / "codebook.csv"


def test_export_does_not_modify_database(db, tmp_path):
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    export_datasets(db, tmp_path / "out", attachments=True, long_tables=True)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before


def test_exported_columns_match_codebook(export):
    out, manifest = export
    with open(CODEBOOK, newline="") as f:
        rows = list(csv.DictReader(f))
    for name, info in manifest["files"].items():
        dataset = name[:-4]
        documented = [r["column"] for r in rows if r["dataset"] == dataset and r["column"] != "data"]
        assert info["columns"] == documented, f"{dataset}: columns differ from docs/codebook.csv"


def test_shared_and_colliding_ids(export):
    out, _ = export
    pub = pd.read_csv(out / "publications.csv")
    shared = pub[pub.id == 2001]
    assert len(shared) == 2 and shared.id_shared.all() and not shared.id_collision.any()
    collision = pub[pub.id == 2003]
    assert len(collision) == 2 and collision.id_collision.all() and not collision.id_shared.any()
    assert set(collision.type) == {"PROP_REG", "OPC_LAUNCHED"}
    assert not pub[pub.id == 2002].id_shared.iloc[0]


def test_booleans_and_integers(export):
    out, _ = export
    ini = pd.read_csv(out / "initiatives.csv", dtype={"is_major": "boolean"})
    assert ini.set_index("id").is_major.to_dict() == {1001: True, 1002: False, 1003: pd.NA} or \
        ini.set_index("id").is_major.isna().loc[1003]
    assert ini.id.dtype == "int64"
    pa = pd.read_csv(out / "publication_attachments.csv")
    assert pa.pages.dropna().apply(float.is_integer).all()
    raw = (out / "publication_attachments.csv").read_text()
    assert ",4," in raw and "4.0" not in raw  # pages written as integers
    assert "fetch_failed" in ini.columns and not ini.fetch_failed.any()


def test_text_roundtrip(export, fixtures):
    out, _ = export
    _, feedback = fixtures
    fb = pd.read_csv(out / "feedback.csv")
    assert fb.loc[fb.id == 5001, "feedback"].iloc[0] == feedback[2001][0]["feedback"]
    ini = pd.read_csv(out / "initiatives.csv")
    assert ini.loc[ini.id == 1003, "short_title"].iloc[0] == "Third initiative\nwith a line break"


def test_row_order_and_manifest(export):
    out, manifest = export
    pub = pd.read_csv(out / "publications.csv")
    assert list(zip(pub.initiative_id, pub.id)) == sorted(zip(pub.initiative_id, pub.id))
    assert manifest["database"]["schema_version"] == SCHEMA_VERSION
    assert manifest["runs"][0]["status"] == "completed"
    sums = {name: digest for digest, name in (line.split("  ") for line in (out / "SHA256SUMS").read_text().splitlines())}
    for name, info in manifest["files"].items():
        assert sums[name] == info["sha256"] == hashlib.sha256((out / name).read_bytes()).hexdigest()


def test_long_tables_and_attachment_links(export):
    out, _ = export
    topics = pd.read_csv(out / "initiative_topics.csv")
    assert topics[topics.initiative_id == 1001].code.tolist() == ["BUSINESS", "SINGMARK"]
    assert 1003 not in set(topics.initiative_id)
    fa = pd.read_csv(out / "feedback_attachments.csv")
    assert fa.iloc[0].feedback_id == 5002 and fa.iloc[0].publication_id == 2001
    pa = pd.read_csv(out / "publication_attachments.csv")
    assert (pa.id == 3001).sum() == 1  # shared publication's attachment exported once


def test_selection_and_merge(db, tmp_path):
    assert selected_datasets(["feedback"], attachments=True) == ["feedback", "feedback_attachments"]
    manifest = export_datasets(db, tmp_path / "merged", attachments=False, merge=True)
    assert list(manifest["files"]) == ["haveyoursay.csv"]
    merged = pd.read_csv(tmp_path / "merged" / "haveyoursay.csv")
    assert {"initiative_id", "publication_id", "feedback_id"} <= set(merged.columns)
    # 5 initiative-publication rows; 2001 carries 3 items under each of its 2 initiatives, 2003 one item under each of its 2
    assert len(merged) == 9


def test_include_data_column(db, tmp_path):
    manifest = export_datasets(db, tmp_path / "raw", only=["initiative"], include_data=True)
    assert manifest["files"]["initiatives.csv"]["columns"][-1] == "data"


def test_missing_database(tmp_path):
    with pytest.raises(FileNotFoundError):
        export_datasets(tmp_path / "missing.sqlite", tmp_path / "out")
    assert not (tmp_path / "missing.sqlite").exists()


def test_schema_mismatch(tmp_path):
    old = tmp_path / "old.sqlite"
    conn = sqlite3.connect(old)
    conn.execute("CREATE TABLE initiatives (id INTEGER, data TEXT, timestamp TEXT)")
    conn.commit()
    conn.close()
    with pytest.raises(SchemaError):
        with connect(old, readonly=True):
            pass
    with pytest.raises(SchemaError):
        with connect(old):
            pass
