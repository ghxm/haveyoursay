import json

import pytest

from haveyoursay import FEEDBACK_URL, INITIATIVE_URL, SEARCH_URL
from haveyoursay import collect
from haveyoursay.utils import connect


@pytest.fixture
def fake_portal(monkeypatch, fixtures):
    """Serve the fixture JSON through collect.fetch_json; records every URL."""
    initiatives, feedback = fixtures
    state = {"initiatives": dict(initiatives), "feedback": dict(feedback), "calls": []}

    def fetch_json(url, wait=0.0, timeout=60):
        state["calls"].append(url)
        if url.startswith(SEARCH_URL.split("?")[0]):
            return {"initiativeResultDtoPage": {"content": [{"id": i} for i in state["initiatives"]],
                                                "totalPages": 1}}
        if url.startswith(INITIATIVE_URL.split("{")[0]):
            return state["initiatives"][int(url.rsplit("/", 1)[1])]
        if url.startswith(FEEDBACK_URL.split("?")[0]):
            query = dict(part.split("=") for part in url.split("?")[1].split("&"))
            items = state["feedback"].get(int(query["publicationId"]), [])
            return {"content": items, "totalPages": 1 if items else 0}
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(collect, "fetch_json", fetch_json)
    return state


def test_first_collection(tmp_path, fake_portal, monkeypatch):
    db = tmp_path / "c.sqlite"
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-01-01 00:00:00")
    with connect(db) as conn:
        ini = collect.collect_initiatives(conn, wait=0)
        fb = collect.collect_feedback(conn, wait=0)
        assert ini["found"] == 3 and ini["fetched"] == 3 and ini["new"] == 3 and not ini["failed"]
        assert fb["found"] == 3 and fb["fetched"] == 3 and fb["items"] == 5
        rows = conn.execute("SELECT id, first_seen, last_seen, last_changed FROM initiatives").fetchall()
        assert all(r[1] == r[2] == r[3] for r in rows)
        assert conn.execute("SELECT COUNT(*) FROM publications_fetched").fetchone()[0] == 3
    # three distinct publication ids: 2001 is shared, 2003 collides, each requested once
    feedback_calls = [u for u in fake_portal["calls"] if "allFeedback" in u]
    assert len(feedback_calls) == 3


def test_default_run_skips_fetched(tmp_path, fake_portal):
    db = tmp_path / "c.sqlite"
    with connect(db) as conn:
        collect.collect_initiatives(conn, wait=0)
        collect.collect_feedback(conn, wait=0)
    fake_portal["calls"].clear()
    with connect(db) as conn:
        ini = collect.collect_initiatives(conn, wait=0)
        fb = collect.collect_feedback(conn, wait=0)
    assert ini["fetched"] == 0 and fb["fetched"] == 0 and fb["skipped"] == 3
    assert all("allFeedback" not in u and "groupInitiatives" not in u for u in fake_portal["calls"])


def test_refresh_updates_timestamps(tmp_path, fake_portal, monkeypatch):
    db = tmp_path / "c.sqlite"
    times = iter(["2026-01-01 00:00:00"] * 50)
    monkeypatch.setattr(collect, "utc_now", lambda: next(times))
    with connect(db) as conn:
        collect.collect_initiatives(conn, wait=0)
        collect.collect_feedback(conn, wait=0)
    # change one initiative and one feedback item on the portal, remove another feedback item
    fake_portal["initiatives"][1001]["shortTitle"] = "First initiative (renamed)"
    fake_portal["feedback"][2001][0]["feedback"] = "Edited feedback"
    removed = fake_portal["feedback"][2001].pop(2)
    times = iter(["2026-02-01 00:00:00"] * 50)
    with connect(db) as conn:
        ini = collect.collect_initiatives(conn, wait=0, refresh=True)
        fb = collect.collect_feedback(conn, wait=0, refresh=True)
        assert ini["changed"] == 1 and ini["unchanged"] == 2 and ini["fetched"] == 3
        assert fb["changed"] == 1 and fb["unchanged"] == 3 and fb["new"] == 0 and fb["skipped"] == 0
        t = dict((r[0], r[1:]) for r in conn.execute(
            "SELECT id, first_seen, last_seen, last_changed FROM initiatives"))
        assert t[1001] == ("2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-02-01 00:00:00")
        assert t[1002] == ("2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-01-01 00:00:00")
        f = dict((r[0], r[1:]) for r in conn.execute(
            "SELECT id, first_seen, last_seen, last_changed, data FROM feedback"))
        assert f[5001][:3] == ("2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-02-01 00:00:00")
        assert json.loads(f[5001][3])["feedback"] == "Edited feedback"
        assert f[5002][:3] == ("2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-01-01 00:00:00")
        # the removed item is kept with its old last_seen, so its disappearance is visible
        assert f[removed["id"]][1] == "2026-01-01 00:00:00"
        assert conn.execute("SELECT items_served FROM publications_fetched WHERE publication_id = 2001"
                            ).fetchone()[0] == 2


def test_failed_fetch_is_reported(tmp_path, fake_portal, monkeypatch):
    db = tmp_path / "c.sqlite"
    original = collect.fetch_json

    def flaky(url, wait=0.0, timeout=60):
        if url.endswith("/1002"):
            raise ConnectionResetError("boom")
        return original(url, wait, timeout)

    monkeypatch.setattr(collect, "fetch_json", flaky)
    with connect(db) as conn:
        ini = collect.collect_initiatives(conn, wait=0)
        assert ini["failed"] == [1002] and ini["fetched"] == 2
        assert conn.execute("SELECT fetch_failed FROM initiatives_view WHERE id = 1002").fetchone()[0] == 1
        # a later default run requests only the failed one
        monkeypatch.setattr(collect, "fetch_json", original)
        fake_portal["calls"].clear()
        ini = collect.collect_initiatives(conn, wait=0)
        assert ini["fetched"] == 1 and not ini["failed"]
        assert [u for u in fake_portal["calls"] if "groupInitiatives" in u] == [INITIATIVE_URL.format(id=1002)]


def test_explicit_ids(tmp_path, fake_portal):
    db = tmp_path / "c.sqlite"
    with connect(db) as conn:
        ini = collect.collect_initiatives(conn, wait=0, initiative_ids=[1003, 1003])
        fb = collect.collect_feedback(conn, wait=0, initiative_ids=[1003])
    assert ini["found"] == 1 and ini["fetched"] == 1
    assert fb["found"] == 1 and fb["items"] == 1  # publication 2003 served under 1003
    assert not any("searchInitiatives" in u for u in fake_portal["calls"])
