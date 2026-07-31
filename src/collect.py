from src.utils import db_decorator, url_open
import json
from tqdm import tqdm
import time
import logging

logger = logging.getLogger(__name__)

@db_decorator
def collect_initiatives(c, update=False, wait = 0.5, initiative_ids=None):
    page = 0
    initiatives = []
    initiative_ids = list(dict.fromkeys(initiative_ids or []))

    if initiative_ids:
        logger.info(f"Using specified initiative IDs: {initiative_ids}")
        initiatives = [{'id': id} for id in initiative_ids]
        total_pages = 0
    else:
        logger.info("Getting initiative search results")
        total_pages = None

    while total_pages is None or page < total_pages:
        logger.info(f"Page: {page}")

        url = f'https://ec.europa.eu/info/law/better-regulation/brpapi/searchInitiatives?page={str(page)}&size=100&language=EN'

        try:
            response = url_open(url)
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Error getting initiative search results page {page}: {e}")
            raise

        data = json.loads(response.read().decode('utf-8'))

        try:
            # API response structure changed - now uses 'content' key
            # Try new structure first, fall back to old structure for compatibility
            if 'initiativeResultDtoPage' in data and 'content' in data['initiativeResultDtoPage']:
                initiatives += data['initiativeResultDtoPage']['content']
            elif 'content' in data:
                initiatives += data['content']
            elif '_embedded' in data and 'initiativeResultDtoes' in data['_embedded']:
                # Old API structure (for backward compatibility)
                initiatives += data['_embedded']['initiativeResultDtoes']
            else:
                logger.warning("Unrecognized API response structure")
                break
        except Exception as e:
            logger.error(f"Error parsing initiative data: {e}")
            break

        if total_pages is None:
            try:
                total_pages = int(data['initiativeResultDtoPage']['totalPages'])
            except Exception as e:
                logger.error(f"Error getting total pages: {e}")
                raise

        page += 1

    logger.info(f"Got {len(initiatives)} initiatives")

    logger.info("Writing initiative IDs to db")

    for initiative in initiatives:
        id = initiative['id']

        # write id to db
        c.execute("INSERT OR IGNORE INTO initiatives(id) VALUES(?)", (id,))

    if initiative_ids:
        placeholders = ','.join('?' for _ in initiative_ids)
        ids = c.execute(f"SELECT * FROM initiatives WHERE id IN ({placeholders})", initiative_ids).fetchall()
    else:
        ids = c.execute("SELECT * FROM initiatives").fetchall()

    if update:
        # keep only ids without data
        ids = [id for id in ids if id[1] is None]

    # Request initiative data and write to db
    for id_tuple in tqdm(ids, desc="Requesting initiative data and writing to db"):
        id = id_tuple[0]
        url = f'https://ec.europa.eu/info/law/better-regulation/brpapi/groupInitiatives/{id}'

        try:
            response = url_open(url)
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Error getting initiative {id}: {e}")
            continue

        data = json.loads(response.read().decode('utf-8'))

        if len(data) > 0:
            try:
                c.execute("UPDATE initiatives SET data = ?, timestamp=datetime('now') WHERE id = ?", (json.dumps(data), id))
            except Exception as e:
                logger.error(f"Error writing initiative {id} to db: {e}")
                continue

@db_decorator
def collect_feedback(c, update=False, wait = 0.5, initiative_ids=None):

    logger.info("Getting publications...")

    # get all publication ids from db view
    if initiative_ids:
        placeholders = ','.join('?' for _ in initiative_ids)
        publications = c.execute(f"SELECT id FROM publications_view WHERE initiative_id IN ({placeholders})", initiative_ids).fetchall()
    else:
        publications = c.execute("SELECT id FROM publications_view").fetchall()

    logger.info(f"Found {len(publications)} publications")

    if update:
        # keep only publications without data for feedback
        collected_publication_ids = {row[0] for row in c.execute("SELECT DISTINCT publication_id FROM feedback")}
        publications = [publication for publication in publications if publication[0] not in collected_publication_ids]

    for publication in tqdm(publications, desc="Requesting feedback data and writing to db"):
        publication_id = publication[0]

        try:
            id_feedback = get_feedback_by_publication_id(publication_id, wait=wait)
        except Exception as e:
            logger.error(f"Error getting feedback for publication {publication_id}: {e}")
            continue

        try:
            # Start a transaction
            c.execute("BEGIN TRANSACTION")

            # Insert all feedbacks
            for feedback in id_feedback:
                c.execute("INSERT OR REPLACE INTO feedback (id, publication_id, data) VALUES (?,?,?)",
                          (feedback['id'], publication_id, json.dumps(feedback)))

            # Commit the transaction
            c.execute("COMMIT")
        except Exception as e:
            # If there's an error, rollback the transaction
            c.execute("ROLLBACK")
            logger.error(f"An error occurred when inserting feedback for publication {publication_id}: {e}")

def get_feedback_by_publication_id(publication_id, wait = 0.5):

    feedback = []

    page = 0

    logger.info(f"Getting feedback for publication {publication_id}")
    
    total_pages = None

    while total_pages is None or page < total_pages:
        logger.info(f"Page: {page}")
        url = f'https://ec.europa.eu/info/law/better-regulation/api/allFeedback?publicationId={str(publication_id)}&page={str(page)}&size=100'

        # raise on any failure so that no partial feedback is stored for the publication
        try:
            response = url_open(url)
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Could not get response for {publication_id} (page {page}): {e}")
            raise

        try:
            data = json.loads(response.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Error reading data from {publication_id} (page {page}): {e}")
            raise

        if total_pages is None:
            try:
                total_pages = int(data['totalPages'])
            except Exception as e:
                logger.error(f"Error getting total pages for {publication_id}: {e}")
                raise

        # API response structure changed - now uses 'content' key
        if 'content' in data:
            feedback += data['content']
        elif '_embedded' in data and 'feedback' in data['_embedded']:
            # Old API structure (for backward compatibility)
            feedback += data['_embedded']['feedback']
        else:
            logger.error(f"Unrecognized API response structure for {publication_id} (page {page})")
            raise ValueError(f"Unrecognized API response structure for publication {publication_id} (page {page})")

        page += 1

    logger.info(f"Got {len(feedback)} feedbacks")

    return feedback
