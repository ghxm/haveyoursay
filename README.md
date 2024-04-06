# haveyoursay

This is a simple tool to retrieve and store data from the European Union's public consultation platform "[Have your say](https://ec.europa.eu/info/law/better-regulation/)". Data is stored in an SQLite database. The tool can `collect` initiative and feedback data, `download` attached files and create CSV `dataset`s of the collected data.

## Data structure

The "Have Your Say" API provides data on initiatives with publications nested within them. Publications can have feedback and attached files. Feedback can also have attached files. The data structure is as follows:

- Initiatives *contain*
  - Publications *contain*
    - Publication attachments
    - Feedback *contain*
      - Feedback attachments

The tool re-creates this structure in an SQLite database but stores only the necessary raw JSON responses and extracts the relevant information on-the-fly when needed.


## Installation

1. Clone the repository and navigate to the project folder

```bash
git clone https://github.com/ghxm/haveyoursay.git
cd haveyoursay
```

2. Install the dependencies

```bash
pip install -r requirements.txt
```

## Usage

The tool provides a CLI interface with three modes of operation: `collect`, `download`, and `dataset`.

```bash
python haveyoursay.py [common options] <mode> [mode options]
```

The common options are:

- `--db` or `-d`: Path to the SQLite database file. Default is `haveyoursay.db`.
- `--verbose` or `-v`: Enable verbose output.

Replace `<mode>` with one of the following options:  

- `collect`: Collects data from the European Commission Have Your Say website. This should be run first. Use `--update` to only request data not already in the database and `--wait` to specify seconds to wait in between requests.
- `download`: Downloads publication and feedback attachments from the collected data. Use `--directory` to specify the output directory for the attachments. Use `--only` to specify the type(s) of documents to download (default is both publication and feedback attachments). Attachments can be further filtered by `--publication-type` and `--language` to reduce the number of files to download.
- `dataset`: Creates datasets from the collected data and output them as csv files. Use --directory to specify the output directory for the dataset, --attachments to include attachment datasets, --only to specify the type(s) of documents to create datasets for, and --merge to merge all datasets into a single dataset. 

See the help message for more information:

```bash
python haveyoursay.py <mode> --help
```

The tool will automatically create the necessary tables in the database if they do not exist and document all runs in a logfile.

## Project structure

The project is structured as follows:

- `haveyoursay.py` - the main script that contains the CLI interface
- `src/` - folder containing the modules with the main functionality
  - `collect.py` - the data collection module
  - `download.py` - the attachment download module
  - `dataset.py` - the dataset creation module
  - `utils.py` - utility functions

