from src.utils import db_decorator
import pandas as pd
import logging
import warnings
import csv

logger = logging.getLogger(__name__)

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

        filepath = f"{directory}{type if (type=='feedback' or type.endswith('s') or attachments) else type + 's'}{'_attachments' if attachments else ''}.csv"
        dataset.to_csv(filepath, index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        if verbose:
            print(f"Dataset written to {filepath}")
        logger.info(f"Dataset written to {filepath}")

def merge_datasets(datasets, directory=None, verbose=False):
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

    filepath = f"{directory}haveyoursay.csv"
    merged_dataset.to_csv(filepath, index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')

    if verbose:
        print(f"Merged dataset written to {filepath}")
    logger.info(f"Merged dataset written to {filepath}")
