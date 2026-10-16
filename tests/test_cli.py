import subprocess
import sys
from pathlib import Path

import pandas as pd

from haveyoursay import __version__
from haveyoursay.cli import main

ROOT = Path(__file__).parent.parent


CONSOLE_SCRIPT = Path(sys.executable).parent / "haveyoursay"


def test_console_script_and_shim():
    for cmd in ([str(CONSOLE_SCRIPT), "--version"], [sys.executable, str(ROOT / "haveyoursay.py"), "--version"]):
        out = subprocess.run(cmd, capture_output=True, text=True, check=True)
        assert out.stdout.strip() == f"haveyoursay {__version__}"
    for mode in ("collect", "download", "dataset"):
        subprocess.run([str(CONSOLE_SCRIPT), mode, "--help"], capture_output=True, text=True, check=True)


def test_dataset_command_creates_directory(db, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "new" / "dir"
    code = main(["--db", str(db), "--log-file", str(tmp_path / "run.log"),
                 "dataset", "--directory", str(out), "--attachments"])
    assert code == 0
    assert (out / "manifest.json").is_file() and (out / "feedback_attachments.csv").is_file()
    log = (tmp_path / "run.log").read_text()
    assert f"haveyoursay {__version__}" in log and "Arguments:" in log


def test_missing_database_exit_code(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    code = main(["--db", str(tmp_path / "none.sqlite"), "--log-file", str(tmp_path / "run.log"),
                 "dataset", "--directory", str(tmp_path / "out")])
    assert code == 2
    assert not (tmp_path / "none.sqlite").exists()


def test_censor_script(export, tmp_path):
    out, manifest = export
    public = tmp_path / "public"
    script = ROOT / "scripts" / "censor_datasets.py"
    subprocess.run([sys.executable, str(script), str(out), str(public)], check=True, capture_output=True)
    fb = pd.read_csv(public / "feedback.csv", dtype=str, keep_default_na=False)
    assert not {"feedback", "surname", "first_name"} & set(fb.columns)
    assert fb.set_index("id").organization.to_dict() == {
        "5001": "", "5002": "Example Industry Association", "5003": "", "5004": "Example NGO", "5005": "Example Company"}
    fa = pd.read_csv(public / "feedback_attachments.csv")
    assert not {"filename", "title"} & set(fa.columns) and {"pages", "size", "feedback_id"} <= set(fa.columns)
    pa = pd.read_csv(public / "publication_attachments.csv")
    assert "filename" in pa.columns  # Commission documents keep their names
    import json
    m = json.loads((public / "manifest.json").read_text())
    assert m["censoring"]["applied"]["feedback.csv"]["blanked_cells"] == {"organization": 1}
    assert m["files"]["feedback.csv"]["columns"] == list(fb.columns)
    assert m["database"]["sha256"] == manifest["database"]["sha256"]
    sums = (public / "SHA256SUMS").read_text()
    assert "feedback.csv" in sums and "manifest" not in sums


def test_shim_hands_over_to_package_when_imported():
    out = subprocess.run([sys.executable, "-c", "import haveyoursay, haveyoursay.cli; print(haveyoursay.__file__)"],
                         cwd=ROOT, capture_output=True, text=True, check=True)
    assert out.stdout.strip().endswith(str(Path("src") / "haveyoursay" / "__init__.py"))
