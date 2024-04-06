from src.utils import db_decorator, download_attachment
from tqdm import tqdm
import logging
import time
import os

logger = logging.getLogger(__name__)

@db_decorator
def download_publication_attachments(c, directory='', language=None, publication_type=None, force=False, wait=0, verbose=True):

        if verbose:
            print("Getting publications attachments...")
        logger.info("Getting publications attachments...")

        if directory is not None and len(directory)>0:
            if not os.path.exists(directory):
                os.makedirs(directory)

            if not directory.endswith('/'):
                directory = directory + '/'


        # Prepare the SQL query
        sql_query = "SELECT id, document_id, filename FROM publication_attachments_view"
        params = []

        # Add conditions based on the parameters
        if (language and len(language)>0) or (publication_type and len(publication_type)>0):
            language_conditions = []
            publication_type_conditions = []
            if language:
                for lang in language:
                    if '%' in lang:
                        language_conditions.append("language LIKE ?")
                    else:
                        language_conditions.append("language = ?")
                    params.append(lang)
            if publication_type:
                for pub_type in publication_type:
                    if '%' in pub_type:
                        publication_type_conditions.append("publication_type LIKE ?")
                    else:
                        publication_type_conditions.append("publication_type = ?")
                    params.append(pub_type)

            # add 'true' to conditions to make the SQL query construction easier
            if len(publication_type_conditions)==0:
                publication_type_conditions = ["true"]
            if len(language_conditions)==0:
                language_conditions = ["true"]

            sql_query += " WHERE " + " AND ".join(["(" + " OR ".join(language_conditions) + ")", "(" + " OR ".join(publication_type_conditions) + ")"])

        publication_attachments = c.execute(sql_query, params).fetchall()

        if verbose:
            print("Found " + str(len(publication_attachments)) + " publication attachments")
        logger.info(f"Found {len(publication_attachments)} publication attachments")

        for publication_attachment in tqdm(publication_attachments, desc="Downloading publication attachments"):

            id = publication_attachment[0]
            document_id = publication_attachment[1]
            filename = publication_attachment[2]


            if any([d is None for d in [id, document_id, filename]]):
                continue


            path = f"{directory}data/attachments/publications/{id}/{filename}"
            attachment_url = f'https://ec.europa.eu/info/law/better-regulation/api/download/{document_id}'

            attachment_url = attachment_url.replace(" ", "%20").encode('utf-8').decode('utf-8')

            if verbose:
                print(f"Downloading attachment from {attachment_url} to {path}")
            logger.info(f"Downloading attachment from {attachment_url} to {path}")

            # check if already downloaded
            if not os.path.isfile(path) or os.path.getsize(path) < 3000 or force:

                # download attachment
                try:
                    download_attachment(attachment_url, path)
                    time.sleep(wait)
                    if verbose:
                        print(f"Attachment downloaded to {path}")
                    logger.info(f"Attachment downloaded to {path}")
                except Exception as e:
                    if verbose:
                        print("Error downloading attachment from {}: {}".format(attachment_url, str(e)))
                    logger.error(f"Error downloading attachment from {attachment_url}: {e}")
            else:
                if verbose:
                    print(f"Attachment already exists in {path}")
                logger.info(f"Attachment already exists in {path}")


@db_decorator
def download_feedback_attachments(c, directory='', language=None, publication_type=None, force=False, wait=0, verbose=True):

        if verbose:
            print("Getting feedback attachments...")
        logger.info("Getting feedback attachments...")

        if directory is not None and len(directory)>0:
            if not os.path.exists(directory):
                os.makedirs(directory)

            if not directory.endswith('/'):
                directory = directory + '/'

        # Prepare the SQL query
        sql_query = "SELECT id, document_id, filename FROM feedback_attachments_view"
        params = []

        # Add conditions based on the parameters
        if (language and len(language)>0) or (publication_type and len(publication_type)>0):
            language_conditions = []
            publication_type_conditions = []
            if language:
                for lang in language:
                    if '%' in lang:
                        language_conditions.append("feedback_language LIKE ?")
                    else:
                        language_conditions.append("feedback_language = ?")
                    params.append(lang)
            if publication_type:
                for pub_type in publication_type:
                    if '%' in pub_type:
                        publication_type_conditions.append("publication_type LIKE ?")
                    else:
                        publication_type_conditions.append("publication_type = ?")
                    params.append(pub_type)

            # add 'true' to conditions to make the SQL query construction easier
            if len(publication_type_conditions) == 0:
                publication_type_conditions = ["true"]
            if len(language_conditions) == 0:
                language_conditions = ["true"]

            sql_query += " WHERE " + " AND ".join(["( " + " OR ".join(language_conditions) + ")", "(" + " OR ".join(publication_type_conditions) + ")"])

        feedback_attachments = c.execute(sql_query, params).fetchall()

        if verbose:
            print("Found " + str(len(feedback_attachments)) + " feedback attachments")
        logger.info(f"Found {len(feedback_attachments)} feedback attachments")

        for feedback_attachment in tqdm(feedback_attachments, desc="Downloading feedback attachments"):

            id = feedback_attachment[0]
            document_id = feedback_attachment[1]
            filename = feedback_attachment[2]

            if any([d is None for d in [id, document_id, filename]]):
                continue

            path = f"{directory}data/attachments/feedback/{id}/{filename}"
            attachment_url = f'https://ec.europa.eu/info/law/better-regulation/api/download/{document_id}'

            attachment_url = attachment_url.replace(" ", "%20").encode('utf-8').decode('utf-8')

            if verbose:
                print(f"Downloading attachment from {attachment_url} to {path}")
            logger.info(f"Downloading attachment from {attachment_url} to {path}")

            # check if already downloaded
            if not os.path.isfile(path) or os.path.getsize(path) < 3000 or force:

                # download attachment
                try:
                    download_attachment(attachment_url, path)
                    time.sleep(wait)
                    if verbose:
                        print(f"Attachment downloaded to {path}")
                    logger.info(f"Attachment downloaded to {path}")
                except Exception as e:
                    if verbose:
                        print("Error downloading attachment from {}: {}".format(attachment_url, str(e)))
                    logger.error(f"Error downloading attachment from {attachment_url}: {e}")
            else:
                if verbose:
                    print(f"Attachment already exists in {path}")
                logger.info(f"Attachment already exists in {path}")