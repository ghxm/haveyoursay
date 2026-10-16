"""Database schema, views, HTTP access and text extraction helpers."""

import hashlib
import http.client
import json
import logging
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import backoff

from haveyoursay import SCHEMA_VERSION, __version__

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
USER_AGENT = f"haveyoursay/{__version__} (+https://github.com/ghxm/haveyoursay)"
DEFAULT_TIMEOUT = 60


def utc_now():
    """Current UTC time as 'YYYY-MM-DD HH:MM:SS'."""
    return datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)


def git_commit():
    """Commit hash of the source checkout the package runs from, or None."""
    root = Path(__file__).resolve().parents[2]
    if not (root / ".git").exists():
        return None
    try:
        out = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                             capture_output=True, text=True, check=True, timeout=10)
        return out.stdout.strip()
    except Exception:
        return None


def sha256_file(path, chunk_size=1 << 20):
    """SHA-256 hex digest of a file, read in chunks."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class SchemaError(RuntimeError):
    """The database was written by another schema version of the tool."""


@contextmanager
def connect(db_path, readonly=False):
    """Open the database and create the export views on the connection.

    Write connections create the tables when the file is new. Read-only
    connections refuse missing files. Both refuse databases of another
    schema version. The views are temporary views, so opening a database
    never modifies the file.
    """
    db_path = Path(db_path)
    if readonly:
        if not db_path.is_file():
            raise FileNotFoundError(f"Database not found: {db_path}")
        conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(str(db_path))
    try:
        if readonly:
            check_schema(conn)
        else:
            create_tables(conn)
        create_views(conn)
        yield conn
        if not readonly:
            conn.commit()
    finally:
        conn.close()


def schema_version(conn):
    return conn.execute("PRAGMA user_version").fetchone()[0]


def check_schema(conn):
    version = schema_version(conn)
    if version != SCHEMA_VERSION:
        raise SchemaError(
            f"Database schema version {version} does not match this tool "
            f"(schema version {SCHEMA_VERSION}). Collect into a new database.")


def create_tables(conn):
    """Create the tables of a new database, or verify an existing one."""
    has_tables = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").fetchone()[0] > 0
    if has_tables:
        check_schema(conn)
    conn.executescript(f"""
    CREATE TABLE IF NOT EXISTS initiatives (
        id INTEGER PRIMARY KEY,
        data TEXT,
        first_seen TEXT,
        last_seen TEXT,
        last_changed TEXT);
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY,
        publication_id INTEGER,
        data TEXT,
        first_seen TEXT,
        last_seen TEXT,
        last_changed TEXT);
    CREATE INDEX IF NOT EXISTS feedback_publication_id ON feedback (publication_id);
    CREATE TABLE IF NOT EXISTS publications_fetched (
        publication_id INTEGER PRIMARY KEY,
        feedback_fetched_at TEXT,
        items_served INTEGER,
        pages INTEGER);
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mode TEXT,
        tool_version TEXT,
        git_commit TEXT,
        arguments TEXT,
        api_base TEXT,
        started_at TEXT,
        finished_at TEXT,
        status TEXT,
        initiatives_found INTEGER,
        initiatives_fetched INTEGER,
        initiatives_failed INTEGER,
        publications_found INTEGER,
        publications_fetched INTEGER,
        publications_failed INTEGER,
        feedback_items INTEGER);
    PRAGMA user_version = {SCHEMA_VERSION};
    """)
    conn.commit()


# The export views are projections of the stored JSON. They are created as
# temporary views on each connection and therefore never written to the file.
# Publication author and survey contact fields (authorName, authorSurname,
# authorMail, authorPhoneNumber, surveyContactMail) are deliberately not
# exposed; they remain in the raw data column.
VIEWS = {
    "initiatives_view": """
    SELECT
        initiatives.id AS id,
        json_extract(data, '$.reference') AS reference,
        json_extract(data, '$.unit') AS unit,
        json_extract(data, '$.dg') AS dg,
        json_extract(data, '$.committee') AS committee,
        json_extract(data, '$.expertGroup') AS expert_group,
        json_extract(data, '$.dossierSummary') AS dossier_summary,
        json_extract(data, '$.shortTitle') AS short_title,
        json_extract(data, '$.createdDate') AS created_date,
        json_extract(data, '$.publishedDate') AS published_date,
        json_extract(data, '$.modifiedDate') AS modified_date,
        json_extract(data, '$.initiativeStatus') AS initiative_status,
        json_extract(data, '$.foreseenActType') AS foreseen_act_type,
        json_extract(data, '$.receivingFeedbackStatus') AS receiving_feedback_status,
        json_extract(data, '$.stage') AS stage,
        json_extract(data, '$.isMajor') AS is_major,
        json_extract(data, '$.isEvaluation') AS is_evaluation,
        json_extract(data, '$.isGroupedCfe') AS is_grouped_cfe,
        (SELECT group_concat(json_extract(t.value, '$.label'), '; ')
            FROM json_each(initiatives.data, '$.topics') AS t) AS topics,
        (SELECT group_concat(json_extract(t.value, '$.code'), '; ')
            FROM json_each(initiatives.data, '$.topics') AS t) AS topic_codes,
        (SELECT group_concat(json_extract(p.value, '$.label'), '; ')
            FROM json_each(initiatives.data, '$.policyAreas') AS p) AS policy_areas,
        (SELECT group_concat(json_extract(p.value, '$.code'), '; ')
            FROM json_each(initiatives.data, '$.policyAreas') AS p) AS policy_area_codes,
        (data IS NULL) AS fetch_failed,
        first_seen,
        last_seen,
        last_changed,
        data
    FROM initiatives""",

    "initiative_topics_view": """
    SELECT
        initiatives.id AS initiative_id,
        json_extract(t.value, '$.code') AS code,
        json_extract(t.value, '$.label') AS label
    FROM initiatives, json_each(initiatives.data, '$.topics') AS t
    WHERE json_type(initiatives.data, '$.topics') = 'array'""",

    "initiative_policy_areas_view": """
    SELECT
        initiatives.id AS initiative_id,
        json_extract(p.value, '$.code') AS code,
        json_extract(p.value, '$.label') AS label
    FROM initiatives, json_each(initiatives.data, '$.policyAreas') AS p
    WHERE json_type(initiatives.data, '$.policyAreas') = 'array'""",

    # id_shared: the same publication (same type, reference, reference number
    # and title) is served under several initiatives. id_collision: the
    # portal serves different publications under one id.
    "publications_view": """
    WITH pubs AS (
        SELECT
            json_extract(j.value, '$.id') AS id,
            initiatives.id AS initiative_id,
            json_extract(j.value, '$.type') AS type,
            json_extract(j.value, '$.receivingFeedbackStatus') AS receiving_feedback_status,
            json_extract(j.value, '$.reference') AS reference,
            json_extract(j.value, '$.referenceNumber') AS reference_number,
            json_extract(j.value, '$.groupId') AS group_id,
            json_extract(j.value, '$.title') AS title,
            json_extract(j.value, '$.frontEndStage') AS front_end_stage,
            json_extract(j.value, '$.totalFeedback') AS total_feedback,
            json_extract(j.value, '$.feedbackPeriod') AS feedback_period,
            json_extract(j.value, '$.publishedDate') AS published_date,
            json_extract(j.value, '$.createdDate') AS created_date,
            json_extract(j.value, '$.modifiedDate') AS modified_date,
            json_extract(j.value, '$.adoptionDate') AS adoption_date,
            json_extract(j.value, '$.endDate') AS end_date,
            j.value AS data
        FROM initiatives, json_each(initiatives.data, '$.publications') AS j
        WHERE json_type(initiatives.data, '$.publications') = 'array'),
    stats AS (
        SELECT
            id,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT COALESCE(type, '') || '|' || COALESCE(reference, '') || '|'
                  || COALESCE(reference_number, '') || '|' || COALESCE(title, '')) AS n_variants
        FROM pubs GROUP BY id)
    SELECT
        pubs.id, pubs.initiative_id, pubs.type, pubs.receiving_feedback_status,
        pubs.reference, pubs.reference_number, pubs.group_id, pubs.title,
        pubs.front_end_stage, pubs.total_feedback, pubs.feedback_period,
        pubs.published_date, pubs.created_date, pubs.modified_date,
        pubs.adoption_date, pubs.end_date,
        (stats.n_rows > 1 AND stats.n_variants = 1) AS id_shared,
        (stats.n_variants > 1) AS id_collision,
        pubs.data
    FROM pubs JOIN stats ON pubs.id = stats.id""",

    "publication_attachments_view": """
    SELECT DISTINCT
        json_extract(a.value, '$.id') AS id,
        json_extract(a.value, '$.documentId') AS document_id,
        json_extract(a.value, '$.reference') AS reference,
        json_extract(a.value, '$.type') AS type,
        json_extract(a.value, '$.workType') AS work_type,
        json_extract(a.value, '$.category') AS category,
        json_extract(a.value, '$.title') AS title,
        publications_view.type AS publication_type,
        json_extract(a.value, '$.date') AS date,
        json_extract(a.value, '$.createdDate') AS created_date,
        json_extract(a.value, '$.modifiedDate') AS modified_date,
        COALESCE(json_extract(a.value, '$.ersFileName'), json_extract(a.value, '$.filename')) AS filename,
        json_extract(a.value, '$.language') AS language,
        json_extract(a.value, '$.pages') AS pages,
        json_extract(a.value, '$.size') AS size,
        json_extract(a.value, '$.isOriginal') AS is_original,
        json_extract(a.value, '$.published') AS published,
        publications_view.id AS publication_id,
        a.value AS data
    FROM publications_view, json_each(publications_view.data, '$.attachments') AS a
    WHERE json_type(publications_view.data, '$.attachments') = 'array'""",

    "feedback_view": """
    SELECT
        feedback.id AS id,
        feedback.publication_id AS publication_id,
        json_extract(data, '$.tr_number') AS tr_number,
        json_extract(data, '$.language') AS language,
        json_extract(data, '$.country') AS country,
        json_extract(data, '$.organization') AS organization,
        json_extract(data, '$.surname') AS surname,
        json_extract(data, '$.firstName') AS first_name,
        json_extract(data, '$.status') AS status,
        json_extract(data, '$.publicationStatus') AS publication_status,
        json_extract(data, '$.feedback') AS feedback,
        json_extract(data, '$.dateFeedback') AS date_feedback,
        json_extract(data, '$.publication') AS publication,
        json_extract(data, '$.userType') AS user_type,
        json_extract(data, '$.companySize') AS company_size,
        json_extract(data, '$.referenceInitiative') AS reference_initiative,
        first_seen,
        last_seen,
        last_changed,
        data
    FROM feedback""",

    "feedback_attachments_view": """
    SELECT
        json_extract(a.value, '$.id') AS id,
        json_extract(a.value, '$.documentId') AS document_id,
        json_extract(a.value, '$.ersFileName') AS filename,
        json_extract(a.value, '$.title') AS title,
        json_extract(a.value, '$.category') AS category,
        json_extract(a.value, '$.pages') AS pages,
        json_extract(a.value, '$.size') AS size,
        json_extract(feedback.data, '$.language') AS feedback_language,
        json_extract(feedback.data, '$.status') AS feedback_status,
        feedback.id AS feedback_id,
        feedback.publication_id AS publication_id,
        a.value AS data
    FROM feedback, json_each(feedback.data, '$.attachments') AS a
    WHERE json_type(feedback.data, '$.attachments') = 'array'""",
}


def create_views(conn):
    for name, body in VIEWS.items():
        conn.execute(f"CREATE TEMP VIEW IF NOT EXISTS {name} AS {body}")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def fatal_code(e):
    """Give up on client errors except 408 (timeout) and 429 (rate limit);
    retry server errors (5xx), timeouts and network-level errors."""
    code = getattr(e, "code", None)
    if code is not None:
        return 400 <= int(code) < 500 and int(code) not in (408, 429)
    return False


RETRY_ERRORS = (urllib.error.URLError, ConnectionResetError, TimeoutError,
                socket.timeout, http.client.HTTPException)


@backoff.on_exception(backoff.expo, RETRY_ERRORS, giveup=fatal_code, max_time=60 * 15)
def url_open(url, headers=None, timeout=DEFAULT_TIMEOUT):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    return urllib.request.urlopen(request, timeout=timeout)


@backoff.on_exception(backoff.expo, RETRY_ERRORS, giveup=fatal_code, max_time=60 * 15)
def fetch_json(url, wait=0.0, timeout=DEFAULT_TIMEOUT):
    """GET a JSON document, then pause for `wait` seconds. Retried as a whole,
    so a connection that drops while the body is being read is retried too."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
    time.sleep(wait)
    return json.loads(body.decode("utf-8"))


def download_attachment(url, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with url_open(url) as response, open(path, "wb") as f:
        f.write(response.read())


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def get_file_type(file):
    first_eight_bytes = file.read(8)
    file.seek(0)
    if first_eight_bytes[:4] == b"%PDF":
        return "pdf"
    if first_eight_bytes[:4] == b"PK\x03\x04":
        return "docx"
    if first_eight_bytes == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1":
        return "doc"
    return None


def pdf_to_text(file, library="pdfplumber"):
    library = library.lower()
    if library == "pdfminer.six":
        try:
            from pdfminer.high_level import extract_text as pdfminer_extract
        except ImportError:
            raise ImportError("pdfminer.six is required for --pdf-library pdfminer.six "
                              "(pip install 'haveyoursay[pdf]')")
        return pdfminer_extract(file)
    if library == "pdfplumber":
        import pdfplumber
        with pdfplumber.open(file) as pdf:
            return "".join(page.extract_text() or "" for page in pdf.pages)
    if library == "pymupdf":
        try:
            import fitz
        except ImportError:
            raise ImportError("PyMuPDF is required for --pdf-library pymupdf "
                              "(pip install 'haveyoursay[pdf]')")
        with fitz.open(stream=file.read(), filetype="pdf") as pdf:
            return "".join(page.get_text() for page in pdf)
    raise ValueError(f"PDF library {library} not supported")


def docx_to_text(file):
    import docx
    document = docx.Document(file)
    return "".join(paragraph.text + "\n" for paragraph in document.paragraphs)


def extract_text(path, filetype=None, pdf_library="pdfplumber"):
    path = Path(path)
    with open(path, "rb") as file:
        if not filetype:
            suffix = path.suffix.lower().lstrip(".")
            if suffix in ("pdf", "docx"):
                filetype = suffix
            else:
                filetype = get_file_type(file) or suffix
        if not filetype:
            raise ValueError("Filetype not recognized")
        if filetype == "pdf":
            return pdf_to_text(file, library=pdf_library)
        if filetype == "docx":
            return docx_to_text(file)
        if filetype == "txt":
            return file.read().decode("utf-8", errors="replace")
        raise ValueError(f"Filetype {filetype} not supported")
