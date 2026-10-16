import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from helpers import build_fixture_db, load_fixtures  # noqa: E402

from haveyoursay.dataset import export_datasets  # noqa: E402


@pytest.fixture
def db(tmp_path):
    return build_fixture_db(tmp_path / "fixture.sqlite")


@pytest.fixture
def export(db, tmp_path):
    out = tmp_path / "out"
    manifest = export_datasets(db, out, attachments=True, long_tables=True)
    return out, manifest


@pytest.fixture
def fixtures():
    return load_fixtures()
