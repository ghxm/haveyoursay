from src.utils import db_decorator, url_open
import json
from tqdm import tqdm
import time
import logging

logger = logging.getLogger(__name__)

@db_decorator
def collect_initiatives(c, update=False, wait = 0.5, verbose=True):
    page = 0
    initiatives = []

    if verbose:
        print("Getting initiative search results")
    logger.info("Getting initiative search results")

    total_pages = None

    while total_pages is None or page<=total_pages:
        if verbose:
            print("\tPage: " + str(page))
        logger.info(f"Page: {page}")

        url = f'https://ec.europa.eu/info/law/better-regulation/brpapi/searchInitiatives?page={str(page)}&size=100&language=EN'

        try:
            response = url_open(url)
            time.sleep(wait)
        except:
            break

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
                if verbose:
                    print("Warning: Unrecognized API response structure")
                logger.warning("Unrecognized API response structure")
                break
        except Exception as e:
            if verbose:
                print(f"Error parsing initiative data: {e}")
            logger.error(f"Error parsing initiative data: {e}")
            break
            
        if total_pages is None:
            try:
                if 'initiativeResultDtoPage' in data:
                    total_pages = int(data['initiativeResultDtoPage']['totalPages'])
            except Exception as e:
                if verbose:
                    print(f"Warning: Error getting total pages: {e}. User will need to abort manually.")
                logger.warning(f"Error getting total pages: {e}")
                

        page += 1

    if verbose:
        print("Got " + str(len(initiatives)) + " initiatives")
    logger.info(f"Got {len(initiatives)} initiatives")

    if verbose:
        print("Writing initiative IDs to db")
    logger.info("Writing initiative IDs to db")

    for initiative in initiatives:
        id = initiative['id']

        # write id to db
        c.execute("INSERT OR IGNORE INTO initiatives(id) VALUES(?)", (id,))

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
            if verbose:
                print("\tError getting initiative " + str(id) + ": " + str(e))
            logger.error(f"Error getting initiative {id}: {e}")
            continue

        data = json.loads(response.read().decode('utf-8'))

        if len(data) > 0:
            try:
                c.execute("UPDATE initiatives SET data = ?, timestamp=datetime('now') WHERE id = ?", (json.dumps(data), id))
            except Exception as e:
                if verbose:
                    print("\tError writing initiative " + str(id) + " to db: " + str(e))
                logger.error(f"Error writing initiative {id} to db: {e}")
                continue

@db_decorator
def collect_feedback(c, update=False, wait = 0.5, verbose=True):

    if verbose:
        print("Getting publications...")
    logger.info("Getting publications...")

    # get all publication ids from db view
    publications = c.execute("SELECT * FROM publications_view").fetchall()

    if verbose:
        print("Found " + str(len(publications)) + " publications")
    logger.info(f"Found {len(publications)} publications")

    if update:

        feedback = c.execute("SELECT id FROM feedback").fetchall()

        # keep only publications without data for feedback
        publications = [publication for publication in publications if publication[0] not in [fe[0] for fe in feedback]]

    for publication in tqdm(publications, desc="Requesting feedback data and writing to db"):

        try:
            id_feedback = get_feedback_by_id(id = publication[1], wait=wait, verbose=verbose)
        except Exception as e:
            if verbose:
                print("Error getting feedback for publication " + str(publication[1]) + ": " + str(e))
            logger.error(f"Error getting feedback for publication {publication[1]}: {e}")
            continue

        try:
            # Start a transaction
            c.execute("BEGIN TRANSACTION")

            # Insert all feedbacks
            for feedback in id_feedback:
                c.execute("INSERT OR REPLACE INTO feedback (id, publication_id, data) VALUES (?,?,?)",
                          (feedback['id'], publication[1], json.dumps(feedback)))

            # Commit the transaction
            c.execute("COMMIT")
        except Exception as e:
            # If there's an error, rollback the transaction
            c.execute("ROLLBACK")
            if verbose:
                print("An error occurred when inserting feedback for publication " + str(publication[1]) + ": " + str(e))
            logger.error(f"An error occurred when inserting feedback for publication {publication[1]}: {e}")

def get_feedback_by_id(id, wait = 0.5, verbose=True):

    feedback = []

    page = 0

    if verbose:
        print("Getting feedback for publication " + str(id))
    logger.info(f"Getting feedback for publication {id}")
    
    total_pages = None

    while total_pages is None or page<=total_pages:
        if verbose:
            print("\tPage: " + str(page))
        logger.info(f"Page: {page}")
        url = f'https://ec.europa.eu/info/law/better-regulation/api/allFeedback?publicationId={str(id)}&page={str(page)}&size=100'

        try:
            response = url_open(url)
            time.sleep(wait)
        except Exception as e:
            if verbose:
                print("Could not get response for " + str(id) + " (" + str(e) + ")")
            logger.error(f"Could not get response for {id}: {e}")
            break
            
        try:
            data = json.loads(response.read().decode('utf-8'))
        except Exception as e:
            if verbose:
                print("Error reading data from " + str(id) + " (" + str(e) + ")")
            logger.error(f"Error reading data from {id}: {e}")
            break
            
        if total_pages is None:
            try:
                if 'initiativeResultDtoPage' in data:
                    total_pages = int(data['initiativeResultDtoPage']['totalPages'])
            except Exception as e:
                if verbose:
                    print(f"Warning: Error getting total pages: {e}. User will need to abort manually.")
                logger.warning(f"Error getting total pages: {e}")

        try:
            # API response structure changed - now uses 'content' key
            if 'content' in data:
                feedback += data['content']
            elif '_embedded' in data and 'feedback' in data['_embedded']:
                # Old API structure (for backward compatibility)
                feedback += data['_embedded']['feedback']
            else:
                break
        except Exception as e:
            if verbose:
                print(f"Error parsing feedback data: {e}")
            logger.error(f"Error parsing feedback data: {e}")
            break

        page += 1

    if verbose:
        print("Got " + str(len(feedback)) + " feedbacks")
    logger.info(f"Got {len(feedback)} feedbacks")

    return feedback
