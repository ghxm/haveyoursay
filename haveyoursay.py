import argparse
from src import utils, collect as cl, download as dl, dataset as ds
import logging
from datetime import datetime

def collect(args):
    print('Collecting data')
    cl.collect_initiatives(args.db, update=args.update, wait=args.wait, verbose=args.verbose)
    cl.collect_feedback(args.db, update=args.update, wait=args.wait, verbose=args.verbose)

def download(args):
    print('Downloading attachments')

    if not args.only or (args.only and 'publication' in args.only):
        dl.download_publication_attachments(args.db, directory=args.directory, language=args.language, publication_type=args.publication_type, force=args.force, verbose=args.verbose)
    if not args.only or (args.only and 'feedback' in args.only):
        dl.download_feedback_attachments(args.db, directory=args.directory, language=args.language, publication_type=args.publication_type, force=args.force, verbose=args.verbose)

def dataset(args):
    print('Creating datasets')

    if args.dataset_type == 'meta':

        # set the directory argument to None (such that the datasets are returned rather than written) if we want to merge the datasets
        if args.merge:
            directory_arg = None
        else:
            directory_arg = args.directory

        datasets = {}

        if not args.only or (args.only and 'initiative' in args.only):
            datasets['initiative'] = ds.create_dataset(args.db, 'initiative', attachments=False, directory=directory_arg, json=args.json, verbose=args.verbose)

        if not args.only or (args.only and 'publication' in args.only):
            datasets['publication'] = ds.create_dataset(args.db, 'publication', attachments=False, directory=directory_arg, json=args.json, verbose=args.verbose)
            if args.attachments:
                datasets['publication_attachment'] = ds.create_dataset(args.db, 'publication', attachments=True, directory=directory_arg, json=args.json, verbose=args.verbose)

        if not args.only or (args.only and 'feedback' in args.only):
            datasets['feedback'] = ds.create_dataset(args.db, 'feedback', attachments=False, directory=directory_arg, json=args.json, verbose=args.verbose)
            if args.attachments:
                datasets['feedback_attachment'] = ds.create_dataset(args.db, 'feedback', attachments=True, directory=directory_arg,  json=args.json, verbose=args.verbose)

        if args.merge:
            ds.merge_datasets(datasets, directory=args.directory, json=args.json, verbose=args.verbose)

    elif args.dataset_type == 'text':

        if 'publications' in args.only:
            raise ValueError("'publications' is not a valid value for the --only argument. Use 'publication' instead.")

        if args.only and 'publication' not in args.only and 'feedback' not in args.only:
            raise ValueError('The text dataset can only be created for publications and feedback (--only).')

        ds.create_attachments_text_dataset(input_directory=args.input_directory, output_directory=args.directory, types=args.only, parallel=args.parallel, json=args.json, pdf_library=args.pdf_library, verbose=args.verbose)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect data from the European Commission Have Your Say website and assemble it into a dataset.')
    subparsers = parser.add_subparsers(dest='mode', required=True, help='Mode to run the script in. Can be either "collect" (data), "download" (attachments) or (create) "dataset".')

    # create the parser for the "collect" command
    parser_collect = subparsers.add_parser('collect', help='Collect metadata from the European Commission Have Your Say website.')
    parser_collect.add_argument('-w', '--wait', type=float, default=0.5, help='Seconds to wait inbetween requests. Default is 0.5 seconds.')
    parser_collect.add_argument('-u', '--update', default=False, action='store_true', help='Only request data not already in the database. Default is False.')
    parser_collect.set_defaults(func=collect)

    parser_download = subparsers.add_parser('download', help='Download publication and feedback attachments from the European Commission Have Your Say website.')
    parser_download.add_argument('-d', '--directory', type=str, default='./', help='Directory to save attachments to. Defaults to current working directory.')
    parser_download.add_argument('-w', '--wait', type=float, default=0, help='Seconds to wait inbetween requests. Default is 0 seconds.')
    parser_download.add_argument('-o', '--only', nargs='+', default=None, help='Only download attachments for the specified type(s) of documents. Possible values are "publication" (attachments) or "feedback" (attachments). Default is None (will download all attachments).')
    parser_download.add_argument('-f', '--force', action="store_true", help='Force download of all attachments, even if they already exist. By default, only non-existing files will be downloaded.')
    parser_download.add_argument('--publication-type', nargs='+', default=None,
                                 help='Filter publications by type before downloading. SQL wildcards can be used. Default is None.')
    parser_download.add_argument('--language', nargs='+', default=None,
                                    help='Filter attachments by language before downloading. Default is None.')
    parser_download.set_defaults(func=download)

    # create the parser for the "dataset" command
    parser_dataset = subparsers.add_parser('dataset', help='Create datasets from the collected data. By default, this will create distinct (meta-)datasets for initiatives, publications and feedback.')
    parser_dataset.add_argument(dest='dataset_type', default = 'meta', choices=['meta', 'text'], help='Type of dataset to create. Can be either "meta"data (overview of all initiatives, publication, attachments) or extracted attachment "text". Default is "meta".')
    parser_dataset.add_argument('-i', '--input-directory', type=str, default='./', help='Input directory for text files (relevant for dataset type "text" only). Defaults to current working directory.')
    parser_dataset.add_argument('-d', '--directory', type=str, default='./', help='Output directory for the dataset. Defaults to current working directory.')
    parser_dataset.add_argument('-a', '--attachments', action='store_true', help='Include attachment datasets. Default is False.')
    parser_dataset.add_argument('-o', '--only', nargs='+', default=None, help='Only create datasets for the specified type(s) of documents. Possible values are "initiative", "publication" or "feedback". Default is None (will create all datasets).')
    parser_dataset.add_argument('-m', '--merge', action='store_true', help='Merge all datasets into a single dataset. Default is False.')
    parser_dataset.add_argument('-p', '--parallel', type=int, default=1, help='(text datasets only) Run in parallel with -p <n> jobs. Default is 1 (sequential processing).')
    parser_dataset.add_argument('--json', action='store_true', help='Output datasets as JSON files. Default is False (csv output).')
    parser_dataset.add_argument('--include-data', action='store_true', help='Include the \'data\' (contains raw JSON) column in meta dataset. Default is False.')
    parser_dataset.add_argument('--pdf-library', type=str, default='pdfplumber', choices=['pdfplumber', 'pdfminer.six', 'pymupdf'], help='Library to use for extracting text from PDFs. Default is pdfplumber.')

    parser_dataset.set_defaults(func=dataset)

    # common flag for both modes
    parser.add_argument('-d', '--db', type=str, default='haveyoursay.db', help='Path to the SQLite database file.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output. Default is False.')

    args = parser.parse_args()

    # set up logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(f'haveyoursay_{args.mode}_{datetime.now().strftime("%Y%m%d%H%M")}.log')
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    if args.verbose:
        print('Setting up database tables/views...')
    logger.info('Setting up database tables/views...')

    utils.create_tables(args.db)

    logger.info(f'Arguments: {args}')

    if args.mode in ['collect', 'download', 'dataset']:
        args.func(args)
    else:
        # print help and exit
        parser.print_help()