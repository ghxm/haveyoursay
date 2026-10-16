"""Collect initiatives, publications and feedback from the portal."""

import json
import logging

from tqdm import tqdm

from haveyoursay import FEEDBACK_URL, INITIATIVE_URL, SEARCH_URL
from haveyoursay.utils import fetch_json, utc_now

logger = logging.getLogger(__name__)


def _page_content(data):
    """Initiative search results, tolerant of the response layouts the
    portal has used over time."""
    if "initiativeResultDtoPage" in data and "content" in data["initiativeResultDtoPage"]:
        return data["initiativeResultDtoPage"]["content"]
    if "content" in data:
        return data["content"]
    if "_embedded" in data and "initiativeResultDtoes" in data["_embedded"]:
        return data["_embedded"]["initiativeResultDtoes"]
    raise ValueError("Unrecognized initiative search response structure")


def _total_pages(data):
    if "initiativeResultDtoPage" in data and "totalPages" in data["initiativeResultDtoPage"]:
        return int(data["initiativeResultDtoPage"]["totalPages"])
    return int(data["totalPages"])


def search_initiative_ids(wait=0.5):
    """All initiative ids from the portal-wide search, in search order."""
    ids, page, total_pages = [], 0, None
    while total_pages is None or page < total_pages:
        logger.info(f"Initiative search page {page}")
        data = fetch_json(SEARCH_URL.format(page=page), wait=wait)
        ids += [int(item["id"]) for item in _page_content(data)]
        if total_pages is None:
            total_pages = _total_pages(data)
        page += 1
    return list(dict.fromkeys(ids))


def store_initiative(conn, initiative_id, data, now):
    """Insert or update one initiative. Returns 'new', 'changed' or 'unchanged'."""
    row = conn.execute("SELECT data FROM initiatives WHERE id = ?", (initiative_id,)).fetchone()
    payload = json.dumps(data)
    if row is None:
        conn.execute(
            "INSERT INTO initiatives (id, data, first_seen, last_seen, last_changed) "
            "VALUES (?, ?, ?, ?, ?)", (initiative_id, payload, now, now, now))
        return "new"
    if row[0] is None:
        conn.execute(
            "UPDATE initiatives SET data = ?, first_seen = COALESCE(first_seen, ?), "
            "last_seen = ?, last_changed = ? WHERE id = ?",
            (payload, now, now, now, initiative_id))
        return "new"
    if json.loads(row[0]) == data:
        conn.execute("UPDATE initiatives SET last_seen = ? WHERE id = ?", (now, initiative_id))
        return "unchanged"
    conn.execute(
        "UPDATE initiatives SET data = ?, last_seen = ?, last_changed = ? WHERE id = ?",
        (payload, now, now, initiative_id))
    return "changed"


def collect_initiatives(conn, wait=0.5, refresh=False, initiative_ids=None):
    """Fetch initiative metadata. By default only initiatives without stored
    data are requested; with refresh=True every initiative is re-requested
    and its last_seen and last_changed timestamps are maintained. Explicitly
    given ids are always requested."""
    now = utc_now()
    stats = {"found": 0, "fetched": 0, "new": 0, "changed": 0, "unchanged": 0, "failed": []}

    if initiative_ids:
        ids = list(dict.fromkeys(int(i) for i in initiative_ids))
        logger.info(f"Using specified initiative ids: {ids}")
    else:
        logger.info("Getting initiative search results")
        ids = search_initiative_ids(wait=wait)
    stats["found"] = len(ids)
    logger.info(f"Found {len(ids)} initiatives")

    for initiative_id in ids:
        conn.execute("INSERT OR IGNORE INTO initiatives (id, first_seen) VALUES (?, ?)",
                     (initiative_id, now))
    conn.commit()

    if initiative_ids or refresh:
        to_fetch = ids
    else:
        to_fetch = [row[0] for row in conn.execute(
            "SELECT id FROM initiatives WHERE data IS NULL ORDER BY id")]
    logger.info(f"Requesting {len(to_fetch)} initiatives")

    for initiative_id in tqdm(to_fetch, desc="Requesting initiative data"):
        try:
            data = fetch_json(INITIATIVE_URL.format(id=initiative_id), wait=wait)
        except Exception as e:
            logger.error(f"Error getting initiative {initiative_id}: {e}")
            stats["failed"].append(initiative_id)
            continue
        if not data:
            logger.error(f"Empty response for initiative {initiative_id}")
            stats["failed"].append(initiative_id)
            continue
        status = store_initiative(conn, initiative_id, data, utc_now())
        conn.commit()
        stats[status] += 1
        stats["fetched"] += 1

    collisions = [row[0] for row in conn.execute(
        "SELECT DISTINCT id FROM publications_view WHERE id_collision = 1 ORDER BY id")]
    if collisions:
        logger.warning(f"{len(collisions)} publication ids are served with differing content "
                       f"under several initiatives (id_collision = 1): {collisions}")
    return stats


def get_feedback_by_publication_id(publication_id, wait=0.5):
    """All feedback items of a publication and the number of pages served.
    Raises on any failure so that no partial result is stored."""
    feedback, page, total_pages = [], 0, None
    while total_pages is None or page < total_pages:
        data = fetch_json(FEEDBACK_URL.format(publication_id=publication_id, page=page), wait=wait)
        if total_pages is None:
            total_pages = int(data["totalPages"])
        if "content" in data:
            feedback += data["content"]
        elif "_embedded" in data and "feedback" in data["_embedded"]:
            feedback += data["_embedded"]["feedback"]
        else:
            raise ValueError(f"Unrecognized feedback response structure for publication "
                             f"{publication_id} (page {page})")
        page += 1
    return feedback, total_pages or 0


def store_feedback(conn, publication_id, items, now):
    """Insert or update the feedback items of one publication.
    Returns counts of new, changed and unchanged items."""
    counts = {"new": 0, "changed": 0, "unchanged": 0}
    for item in items:
        feedback_id = int(item["id"])
        row = conn.execute("SELECT publication_id, data FROM feedback WHERE id = ?",
                           (feedback_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO feedback (id, publication_id, data, first_seen, last_seen, last_changed) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (feedback_id, publication_id, json.dumps(item), now, now, now))
            counts["new"] += 1
        elif row[0] == publication_id and json.loads(row[1]) == item:
            conn.execute("UPDATE feedback SET last_seen = ? WHERE id = ?", (now, feedback_id))
            counts["unchanged"] += 1
        else:
            conn.execute(
                "UPDATE feedback SET publication_id = ?, data = ?, last_seen = ?, last_changed = ? "
                "WHERE id = ?", (publication_id, json.dumps(item), now, now, feedback_id))
            counts["changed"] += 1
    return counts


def collect_feedback(conn, wait=0.5, refresh=False, initiative_ids=None):
    """Fetch the feedback of every publication. By default publications whose
    feedback was fetched before are skipped; with refresh=True all are
    re-requested. Each publication is stored in one transaction."""
    stats = {"found": 0, "fetched": 0, "skipped": 0, "items": 0,
             "new": 0, "changed": 0, "unchanged": 0, "failed": []}

    if initiative_ids:
        placeholders = ",".join("?" for _ in initiative_ids)
        rows = conn.execute(
            f"SELECT DISTINCT id FROM publications_view WHERE initiative_id IN ({placeholders}) "
            "ORDER BY id", [int(i) for i in initiative_ids]).fetchall()
    else:
        rows = conn.execute("SELECT DISTINCT id FROM publications_view ORDER BY id").fetchall()
    publications = [row[0] for row in rows if row[0] is not None]
    stats["found"] = len(publications)
    logger.info(f"Found {len(publications)} publications")

    if not refresh:
        already = {row[0] for row in conn.execute("SELECT publication_id FROM publications_fetched")}
        before = len(publications)
        publications = [p for p in publications if p not in already]
        stats["skipped"] = before - len(publications)
        logger.info(f"Skipping {stats['skipped']} publications fetched before")

    for publication_id in tqdm(publications, desc="Requesting feedback data"):
        try:
            items, pages = get_feedback_by_publication_id(publication_id, wait=wait)
        except Exception as e:
            logger.error(f"Error getting feedback for publication {publication_id}: {e}")
            stats["failed"].append(publication_id)
            continue
        now = utc_now()
        try:
            with conn:
                counts = store_feedback(conn, publication_id, items, now)
                conn.execute(
                    "INSERT INTO publications_fetched (publication_id, feedback_fetched_at, items_served, pages) "
                    "VALUES (?, ?, ?, ?) ON CONFLICT(publication_id) DO UPDATE SET "
                    "feedback_fetched_at = excluded.feedback_fetched_at, "
                    "items_served = excluded.items_served, pages = excluded.pages",
                    (publication_id, now, len(items), pages))
        except Exception as e:
            logger.error(f"Error storing feedback for publication {publication_id}: {e}")
            stats["failed"].append(publication_id)
            continue
        stats["fetched"] += 1
        stats["items"] += len(items)
        for key in counts:
            stats[key] += counts[key]
    return stats
