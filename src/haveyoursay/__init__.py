"""haveyoursay: collect data from the European Commission's Have Your Say portal."""

__version__ = "0.9.0"

# Bumped whenever the database layout changes. Databases written by another
# schema version are refused instead of silently misread.
SCHEMA_VERSION = 2

API_BASE = "https://ec.europa.eu/info/law/better-regulation"
SEARCH_URL = API_BASE + "/brpapi/searchInitiatives?page={page}&size=100&language=EN"
INITIATIVE_URL = API_BASE + "/brpapi/groupInitiatives/{id}"
FEEDBACK_URL = API_BASE + "/api/allFeedback?publicationId={publication_id}&page={page}&size=100"
DOWNLOAD_URL = API_BASE + "/api/download/{document_id}"
