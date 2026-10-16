"""Build a small database from the JSON fixtures without network access."""

import json
from pathlib import Path

from haveyoursay.collect import store_feedback, store_initiative
from haveyoursay.utils import connect

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixtures():
    initiatives = json.loads((FIXTURES / "initiatives.json").read_text())
    feedback = json.loads((FIXTURES / "feedback.json").read_text())
    return ({int(k): v for k, v in initiatives.items()},
            {int(k): v for k, v in feedback.items()})


def build_fixture_db(path, now="2026-01-01 00:00:00"):
    """Create a schema-2 database holding the fixture initiatives and feedback,
    using the same storage functions as a live collection."""
    initiatives, feedback = load_fixtures()
    with connect(path) as conn:
        conn.execute(
            "INSERT INTO runs (mode, tool_version, arguments, api_base, started_at, finished_at, status) "
            "VALUES ('collect', 'fixture', '{}', 'fixture', ?, ?, 'completed')", (now, now))
        for initiative_id, data in initiatives.items():
            store_initiative(conn, initiative_id, data, now)
        for publication_id, items in feedback.items():
            store_feedback(conn, publication_id, items, now)
            conn.execute(
                "INSERT INTO publications_fetched (publication_id, feedback_fetched_at, items_served, pages) "
                "VALUES (?, ?, ?, 1)", (publication_id, now, len(items)))
    return Path(path)
