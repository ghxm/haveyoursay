from src.utils import db_decorator, extract_text
import pandas as pd
from tqdm import tqdm
import logging
import warnings
import csv
import os
from joblib import Parallel, delayed

logger = logging.getLogger(__name__)


def write_dataset(df, filepath, format='csv', index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\'):
    # if filepath has no extension, add the format as extension
    filename = filepath.split('/')[-1]
    if '.' not in filename:
        filepath = f'{filepath}.{format}'

    if format == 'csv':
        df.to_csv(filepath, index=index, quoting=quoting, escapechar=escapechar)
    elif format == 'json':
        df.to_json(filepath, orient='records')
    else:
        raise ValueError(f'Invalid format: {format}')

@db_decorator
def create_dataset(c, type, json = False, attachments=False, data=False, directory=None, verbose=False):

    if verbose:
        print(f"Creating {type}{' attachements' if attachments else ''} dataset")
    logger.info(f"Creating {type}{' attachements' if attachments else ''} dataset")

    con = c.connection


    if type == 'initiative':
        if attachments:
            if verbose:
                print('Initiatives do not have attachments')
            logger.error('Initiatives do not have attachments')

        dataset = pd.read_sql("""
            SELECT 
                id,
                timestamp,
                json_extract(data, '$.reference') as reference,
                json_extract(data, '$.unit') as unit,
                json_extract(data, '$.dg') as dg,
                json_extract(data, '$.committee') as committee,
                json_extract(data, '$.expertGroup') as expert_group,
                json_extract(data, '$.dossierSummary') as dossier_summary,
                json_extract(data, '$.shortTitle') as short_title,
                json_extract(data, '$.publishedDate') as published_date,
                json_extract(data, '$.modifiedDate') as modified_date,
                json_extract(data, '$.initiativeStatus') as initiative_status,
                json_extract(data, '$.foreseenActType') as foreseen_act_type,
                json_extract(data, '$.receivingFeedbackStatus') as receiving_feedback_status,
                json_extract(data, '$.stage') as stage,
                json_extract(data, '$.isMajor') as is_major,
                json_extract(data, '$.isEvaluation') as is_evaluation,
                json_extract(data, '$.isGroupedCfe') as is_grouped_cfe
            FROM initiatives
            """, con)

    elif type == 'publication':
        if attachments:
            dataset = pd.read_sql("SELECT * FROM publication_attachments_view", con)
        else:
            dataset = pd.read_sql("SELECT * FROM publications_view", con)

    elif type == 'feedback':
        if attachments:
            dataset = pd.read_sql("SELECT * FROM feedback_attachments_view", con)

            # remove column publication_type
            dataset = dataset.drop(columns=['publication_type'])
        else:
            dataset = pd.read_sql("""
            SELECT
                id,
                publication_id,
                timestamp,
                json_extract(data, '$.tr_number') as tr_number,
                json_extract(data, '$.language') as language,
                json_extract(data, '$.country') as country,
                json_extract(data, '$.organization') as organization,
                json_extract(data, '$.surname') as surname,
                json_extract(data, '$.firstName') as first_name,
                json_extract(data, '$.status') as status,
                json_extract(data, '$.feedback') as feedback,
                json_extract(data, '$.dateFeedback') as date_feedback,
                json_extract(data, '$.status') as status,
                json_extract(data, '$.publication') as publication,
                json_extract(data, '$.userType') as user_type,
                json_extract(data, '$.companySize') as company_size,
                json_extract(data, '$.referenceInitiative') as reference_initiative
            FROM feedback
            """, con)

    else:
        logger.error(f'Invalid dataset type: {type}')
        raise ValueError(f'Invalid dataset type: {type}')

    if not data:
        # drop all columns ending with data
        dataset = dataset.loc[:, ~dataset.columns.str.endswith('data')]

    if directory is None:
        return dataset
    else:

        if directory == '':
            directory = './'
        elif not directory.endswith('/'):
            directory = directory + '/'

        if verbose:
            print(f"Writing {type} {'attachments' if attachments else ''} dataset to {directory}")
        logger.info(f"Writing {type} {'attachments' if attachments else ''} dataset to {directory}")

        filepath = f"{directory}{type if (type=='feedback' or type.endswith('s') or attachments) else type + 's'}{'_attachments' if attachments else ''}"
        write_dataset(dataset, filepath, format='csv' if not json else 'json')
        if verbose:
            print(f"Dataset written to {filepath+'.'+'csv' if not json else 'json'}")
        logger.info(f"Dataset written to {filepath+'.'+'csv' if not json else 'json'}")

def merge_datasets(datasets, json = False, directory=None, verbose=False):
    # remove all datasets that are None
    datasets = {key: dataset for key, dataset in datasets.items() if dataset is not None}

    if verbose:
        print('Merging datasets')
    logger.info('Merging datasets')

    merged_dataset = None

    dataset_merging_order = ['initiative', 'publication', 'publication_attachment', 'feedback', 'feedback_attachment']

    # make sure all datasets have a type as prefix
    for key, dataset in datasets.items():
        for col in dataset.columns:
            # Check if the column name already starts with one of the prefixes in dataset_merging_order
            if not any(col.startswith(prefix) for prefix in dataset_merging_order):
                # If not, add the prefix
                dataset = dataset.rename(columns={col: f'{key}_{col}'})
        datasets[key] = dataset

    # sort datasets dict by order of merging
    datasets = {key: datasets[key] for key in dataset_merging_order if key in datasets}

    for key, dataset in datasets.items():
        if merged_dataset is None:
            # first dataset
            merged_dataset = dataset
        else:
            if key == 'publication':
                # publications can only be merged to initiatives
                if verbose:
                    print('Merging publications to initiatives')
                logger.info('Merging publications to initiatives')
                merged_dataset = merged_dataset.merge(dataset, on='initiative_id', how='left', suffixes=('', '_publication'))
            elif key == 'publication_attachment':
                if verbose:
                    print('Merging publication attachments to publications')
                logger.info('Merging publication attachments to publications')
                # publication attachments can only be merged to publications
                merged_dataset = merged_dataset.merge(dataset, on='publication_id', how='left', suffixes=('', '_publication_attachment'))
            elif key == 'feedback':
                if verbose:
                    print('Merging feedback to publications')
                logger.info('Merging feedback to publications')
                # feedback can only be merged to publications
                merged_dataset = merged_dataset.merge(dataset, on='publication_id', how='left', suffixes=('', '_feedback'))
            elif key == 'feedback_attachment':
                if verbose:
                    print('Merging feedback attachments to feedback')
                logger.info('Merging feedback attachments to feedback')
                # feedback attachments can only be merged to feedback
                merged_dataset = merged_dataset.merge(dataset, on='feedback_id', how='left', suffixes=('', '_feedback_attachment'))
            else:
                if verbose:
                    warnings.warn(f"Dataset {key} cannot be merged.")
                logger.error(f"Dataset {key} cannot be merged.")

    if verbose:
        print('Writing merged dataset to disk')
    logger.info('Writing merged dataset to disk')

    if directory is None:
        directory = './'
    elif directory == '':
        directory = './'
    elif not directory.endswith('/'):
        directory = directory + '/'

    filepath = f"{directory}haveyoursay"
    write_dataset(merged_dataset, filepath, format='csv' if not json else 'json')

    if verbose:
        print(f"Merged dataset written to {filepath+'.'+'csv' if not json else 'json'}")
    logger.info(f"Merged dataset written to {filepath+'.'+'csv' if not json else 'json'}")



def create_attachments_text_dataset(input_directory=None, output_directory=None, types=None, parallel=1, pdf_library='pdfplumber', json=False, verbose=False):

    if input_directory is None:
        input_directory = './'
    elif input_directory == '':
        input_directory = './'
    elif not input_directory.endswith('/'):
        input_directory = input_directory + '/'

    if not types:
        types = ['publication', 'feedback']

    dataset_type = 'all'

    if 'publication' in types and 'feedback' in types:
        attachment_path = f'{input_directory}data/attachments'
    elif 'publication' in types:
        attachment_path = f'{input_directory}data/attachments/publications'
        dataset_type = 'publication'
    elif 'feedback' in types:
        attachment_path = f'{input_directory}data/attachments/feedback'
        dataset_type = 'feedback'

    if verbose:
        print(f"Creating ({dataset_type}) text dataset")
    logger.info(f"Creating ({dataset_type}) text dataset")

    # check if directory exists
    if not os.path.exists(f'{input_directory}data/attachments'):
        if verbose:
            print(f"Directory {input_directory}data/attachments does not exist")
        logger.error(f"Directory {input_directory}data/attachments does not exist")
        raise FileNotFoundError(f"Directory {input_directory}data/attachments does not exist")

    # get all files in directory (recursively)
    text_files = []
    for root, dirs, files in os.walk(attachment_path):
        for file in files:
            if file.endswith('.txt') or file.endswith('.doc') or file.endswith('.docx') or file.endswith('pdf'):
                    text_files.append((root, file))

    if verbose:
        print(f'Found {len(text_files)} text files')

    logger.info(f'Found {len(text_files)} text files')

    # read all text files
    texts = []

    def extraction_pipeline (path, file):

        filepath = os.path.join(path, file)

        id = path.split('/')[-1]
        type = path.split('/')[-2]

        # remove 's' from type if it is plural
        if type.endswith('s'):
            type = type[:-1]

        text = None
        error_log_msg = None

        try:
            text =  extract_text(filepath, pdf_library=pdf_library)
        except Exception as e:
            if verbose:
                print(f'Error reading text from {filepath}: {e}')
            error_log_msg = f'Error reading text from {filepath}: {e}'

        return (id, type, text, error_log_msg)

    n_jobs = 1

    if parallel > 1:
        n_jobs = parallel

        if verbose:
            print(f'Using {n_jobs} parallel jobs')
        logger.info(f'Using {n_jobs} parallel jobs')

        logger.warning('Error log messages are only written to the log after all items have been processed when using parallel processing.')

    texts = Parallel(n_jobs=n_jobs, verbose=0)(delayed(extraction_pipeline)(path, file) for path, file in tqdm(text_files, desc='Extracting text from files', total=len(text_files)))

    # extract error log messages and log them
    error_log = [text[3] for text in texts if text[3] is not None]
    texts = [text[:3] for text in texts if text[3] is None]

    for error in error_log:
            logger.error(error)

    text_dataset = pd.DataFrame(texts, columns=['id', 'type', 'text'])

    if output_directory is None:
        output_directory = './'
    elif output_directory == '':
        output_directory = './'
    elif not output_directory.endswith('/'):
        output_directory = output_directory + '/'

    dataset_filepath = f'{output_directory}{dataset_type + "_" if dataset_type != "all" else ""}attachments_text'
    if verbose:
        print(f'Writing text dataset to {dataset_filepath + ".csv" if not json else ".json"}')
    logger.info(f'Writing text dataset to {dataset_filepath + ".csv" if not json else ".json"}')

    write_dataset(text_dataset, dataset_filepath, format='csv' if not json else 'json')

    if verbose:
        print(f'Text dataset written to {dataset_filepath + ".csv" if not json else ".json"}')
    logger.info(f'Text dataset written to {dataset_filepath + ".csv" if not json else ".json"}')





