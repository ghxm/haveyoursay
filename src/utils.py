import backoff
from urllib.request import urlopen
import urllib
import urllib.error
from pathlib import Path
import random
import time
import sqlite3
import docx

def db_decorator(func):
    def wrapper(db_path, *args, **kwargs):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        result = func(c, *args, **kwargs)

        conn.commit()
        conn.close()

        return result
    return wrapper

@db_decorator
def create_tables(c):
    # create initiatives table if it doesn't exist
    c.execute('''Create TABLE if not exists initiatives (
    	id integer NOT NULL,
    	data text,
    	timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
    	PRIMARY KEY (id));''')

    c.execute("""
    CREATE TRIGGER IF NOT EXISTS update_timestamp_after_data_change_initiatives
    AFTER UPDATE OF data ON initiatives
    FOR EACH ROW
    BEGIN
       UPDATE initiatives SET timestamp = CURRENT_TIMESTAMP WHERE id = OLD.id;
    END;
    """)

    # create publications view if it doesn't exist
    c.execute('''CREATE VIEW IF NOT EXISTS publications_view AS
    SELECT
        json_extract(json_each.value, '$.id') AS id,
        initiatives.id AS initiative_id,
        json_extract(json_each.value, '$.type') AS type,
        json_extract(json_each.value, '$.receivingFeedbackStatus') AS receiving_feedback_status,
        json_extract(json_each.value, '$.reference') AS reference,
        json_extract(json_each.value, '$.title') AS title,
        json_each.value AS data
    FROM
        initiatives,
        json_each(initiatives.data, '$.publications');''')

    # create publication_attachments view if it doesn't exist
    c.execute('''CREATE VIEW IF NOT EXISTS publication_attachments_view AS
    SELECT DISTINCT
        json_extract(json_each.value, '$.id') AS id,
        json_extract(json_each.value, '$.documentId') AS document_id,
        json_extract(json_each.value, '$.reference') AS reference,
        json_extract(json_each.value, '$.type') AS type,
        json_extract(json_each.value, '$.workType') AS work_type,
        publications_view.type AS publication_type,
        json_extract(json_each.value, '$.date') AS date,
        json_extract(json_each.value, '$.createdDate') AS created_date,
        json_extract(json_each.value, '$.modifiedDate') AS modified_date,
        COALESCE(json_extract(json_each.value, '$.ersFileName'), json_extract(json_each.value, '$.filename')) AS filename,
        json_extract(json_each.value, '$.language') AS language,
        json_extract(json_each.value, '$.isOriginal') AS is_original,
        json_extract(json_each.value, '$.published') AS published,
        publications_view.id AS publication_id,
        json_each.value AS data
    FROM
        publications_view,
        json_each(publications_view.data, '$.attachments');''')


    # create feedback table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS feedback(
        id integer NOT NULL,
        publication_id integer,
        data text,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(id));''')

    # create feedback_attachments view if it doesn't exist
    c.execute('''CREATE VIEW IF NOT EXISTS feedback_attachments_view AS
    SELECT DISTINCT
        json_extract(attachment_json.value, '$.id') AS id,
        json_extract(attachment_json.value, '$.documentId') AS document_id,
        json_extract(attachment_json.value, '$.ersFileName') AS filename,
        json_extract(feedback.data, '$.language') AS feedback_language,
        json_extract(feedback.data, '$.status') AS feedback_status,
        feedback.id AS feedback_id,
        attachment_json.value AS data,
        publications_view.type AS publication_type
    FROM
        feedback
    JOIN
        publications_view ON feedback.publication_id = publications_view.id,
        json_each(feedback.data, '$.attachments') as attachment_json;''')



    c.execute("""
    CREATE TRIGGER IF NOT EXISTS update_timestamp_after_data_change_feedback
    AFTER UPDATE OF data ON feedback
    FOR EACH ROW
    BEGIN
       UPDATE feedback SET timestamp = CURRENT_TIMESTAMP WHERE id = OLD.id;
    END;
    """)



def random_sleep(low, high):
    time.sleep(random.randint(low,high))

def fatal_code(e):

    if hasattr(e, 'code'):
        return 400 <= int(e.code) <= 500 and int(e.code) not in [408, 401, 409, 405]
    # check if it is a ConnectionResetError
    elif isinstance(e, ConnectionResetError):
        return False
    else:
        return True

@backoff.on_exception(backoff.expo, (urllib.error.HTTPError, ConnectionResetError), giveup=fatal_code, max_time=60*15)
def url_open(url, headers=[]):
    time.sleep(0.01)
    opener = urllib.request.build_opener()
    opener.addheaders = headers
    try:
        return opener.open(url)
    except (urllib.error.URLError, ConnectionResetError) as e:
        # in case of an internal server error, wait 3 minutes and try again
        if hasattr(e, 'code') and int(e.code) == 500:
            time.sleep(3*60)
            return opener.open(url)
        else:
            raise e

def download_attachment(url, filename):

    # create the directory if it doesn't exist
    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    with open(filename, 'wb') as f:
        f.write(url_open(url).read())

# generates a random header for urllib
# def random_header():
#     agents = [
#         ('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.3'),
#         ('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/43.4'),
#         ('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36')
#     ]
#     return [random.choice(agents)] + [("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"),
#                                        ("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8")]


def get_file_type(file):
    first_eight_bytes = file.read(8)
    file.seek(0)
    if first_eight_bytes[:4] == b'%PDF':
        return 'pdf'
    elif first_eight_bytes[:4] == b'PK\x03\x04':
        return 'docx'
    elif first_eight_bytes == b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1':
        return 'doc'
    else:
        return None

def pdf_to_text(filepath, library='pdfplumber'):
    if library.lower() == 'pdfminer.six':
        try:
            from pdfminer.high_level import extract_text
        except ImportError:
            raise ImportError("The 'pdfminer.six' module is required to extract text from PDFs. Install it with 'pip install pdfminer.six'")

        text = extract_text(filepath)

    # elif library.lower() == 'pypdf':
    #
    #     try:
    #         from pypdf import PdfReader
    #     except ImportError:
    #         raise ImportError("The 'pypdf' module is required to extract text from PDFs. Install it with 'pip install pypdf'")
    #
    #     with PdfReader(filepath) as pdf:
    #         text = ''
    #         for page in pdf.pages:
    #             text += page.extract_text()

    elif library.lower() == 'pdfplumber':

        try:
            import pdfplumber
        except ImportError:
            raise ImportError("The 'pdfplumber' module is required to extract text from PDFs. Install it with 'pip install pdfplumber'")

        with pdfplumber.open(filepath) as pdf:
            text = ''
            for page in pdf.pages:
                text += page.extract_text()

    elif library.lower() == 'pymupdf':
        try:
            import fitz
        except ImportError:
            raise ImportError("The 'fitz' module is required to extract text from PDFs. Install it with 'pip install PyMuPDF'")

        with fitz.open(filepath) as pdf:
            text = ''
            for page in pdf:
                text += page.get_text()
    else:
        raise ValueError(f"Library {library} not supported")

    return text


def docx_to_text(filepath):
    docx_ = docx.Document(filepath)

    # extract text
    text = ''

    for paragraph in docx_.paragraphs:
        text += paragraph.text + '\n'

    return text


def extract_text(path, filetype=None, pdf_library='pdfplumber'):

    # open file
    with open(path, 'rb') as file:

        if not filetype:
            if path.endswith('.pdf'):
                filetype = 'pdf'
            elif path.endswith('.docx'):
                filetype = 'docx'
            else:
                filetype = get_file_type(path)

            if not filetype:
                # get from path
                filetype = path.split('.')[-1]

        if not filetype:
            raise ValueError("Filetype not recognized")

        # extract text
        if filetype == 'pdf':
            return pdf_to_text(file, library=pdf_library)
        elif filetype == 'docx':
            return docx_to_text(file)
        else:
            raise ValueError(f"Filetype {filetype} not supported")



