"""Download publication and feedback attachments."""

import logging
import time
import urllib.parse
from pathlib import Path

from tqdm import tqdm

from haveyoursay import DOWNLOAD_URL
from haveyoursay.utils import download_attachment

logger = logging.getLogger(__name__)

MIN_FILE_SIZE = 3000  # files smaller than this are treated as failed downloads


def _where(language, publication_type):
    clauses, params = [], []
    for values, column in ((language, "language"), (publication_type, "publication_type")):
        if values:
            ors = []
            for value in values:
                ors.append(f"{column} LIKE ?" if "%" in value else f"{column} = ?")
                params.append(value)
            clauses.append("(" + " OR ".join(ors) + ")")
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _download_rows(rows, subdirectory, directory, force, wait):
    for attachment_id, document_id, filename in tqdm(rows, desc=f"Downloading {subdirectory} attachments"):
        if attachment_id is None or document_id is None or filename is None:
            continue
        path = Path(directory) / "data" / "attachments" / subdirectory / str(attachment_id) / filename
        url = DOWNLOAD_URL.format(document_id=urllib.parse.quote(str(document_id), safe=""))
        if path.is_file() and path.stat().st_size >= MIN_FILE_SIZE and not force:
            logger.info(f"Attachment already exists in {path}")
            continue
        logger.info(f"Downloading attachment from {url} to {path}")
        try:
            download_attachment(url, path)
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Error downloading attachment from {url}: {e}")


def download_publication_attachments(conn, directory=".", language=None, publication_type=None,
                                     force=False, wait=0.5):
    logger.info("Getting publication attachments...")
    where, params = _where(language, publication_type)
    rows = conn.execute(
        "SELECT DISTINCT id, document_id, filename FROM publication_attachments_view" + where,
        params).fetchall()
    logger.info(f"Found {len(rows)} publication attachments")
    _download_rows(rows, "publications", directory, force, wait)


def download_feedback_attachments(conn, directory=".", language=None, publication_type=None,
                                  force=False, wait=0.5):
    logger.info("Getting feedback attachments...")
    where, params = _where(language, publication_type)
    rows = conn.execute(
        "SELECT DISTINCT id, document_id, filename FROM ("
        "  SELECT fa.id, fa.document_id, fa.filename, fa.feedback_language AS language, "
        "         p.publication_type"
        "  FROM feedback_attachments_view AS fa"
        "  JOIN (SELECT DISTINCT id, type AS publication_type FROM publications_view) AS p"
        "    ON fa.publication_id = p.id)" + where,
        params).fetchall()
    logger.info(f"Found {len(rows)} feedback attachments")
    _download_rows(rows, "feedback", directory, force, wait)
