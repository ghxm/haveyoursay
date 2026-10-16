# haveyoursay

A command line tool that collects the data behind the European Commission's
public consultation portal "[Have your say](https://ec.europa.eu/info/law/better-regulation/have-your-say_en)"
and turns it into research datasets. It retrieves all initiatives with their
publications, feedback items and attachment metadata through the portal's own
JSON interface, stores the raw responses in a SQLite database, and exports
documented CSV datasets from it. Attachments can be downloaded and their text
extracted into a further dataset.

The tool is described in a research note (reference to follow). Column
documentation is in [docs/codebook.md](docs/codebook.md).

## Installation

Python 3.9 or newer. SQLite with JSON support, which the Python builds for
Windows, macOS and Linux ship with.

```bash
uv tool install git+https://github.com/ghxm/haveyoursay@v0.9.0
# or
pip install git+https://github.com/ghxm/haveyoursay@v0.9.0
```

From a clone, `pip install .` installs the `haveyoursay` command, and
`python haveyoursay.py` runs the same interface without installing. The
optional PDF backends for text extraction come with `pip install ".[pdf]"`.

## Ten minutes to a first dataset

```bash
haveyoursay --db selected.db collect --initiative-id 14626 14691
haveyoursay --db selected.db dataset --directory ./selected --attachments --long-tables
```

The first command fetches two initiatives, their publications and all feedback
items served for them (a few dozen publications and items, about two minutes
at the default pause of half a second between requests). The second writes
`initiatives.csv`, `initiative_topics.csv`, `initiative_policy_areas.csv`,
`publications.csv`, `publication_attachments.csv`, `feedback.csv`,
`feedback_attachments.csv`, plus `manifest.json` (tool version, database
checksum, run history, row counts) and `SHA256SUMS` to `./selected/`. Leave
out `--initiative-id` to collect the whole portal, which takes about half a
day and produces a database of a few gigabytes.

## Data structure

The portal organizes its content as initiatives that contain publications
(the documented steps of an initiative, such as a call for evidence, a public
consultation, an adopted proposal or a draft act), each with Commission
documents attached and with feedback items submitted on it, which can carry
attachments of their own. The database stores the raw JSON of every initiative
record (publications and their attachment metadata are nested inside) and of
every feedback item. The datasets are projections of that raw data, defined
as SQL views, so any field the interface serves can be added later without
collecting again.

Two properties of the portal matter for every analysis:

- **Publications are shared and ids collide.** The portal serves some
  publications under several initiatives (a call for evidence that covers a
  package), with the same `id` in each. The `publications` dataset keeps one
  row per initiative-publication pair and flags these rows with `id_shared`.
  A few ids are served with different content under different initiatives;
  those rows are flagged `id_collision` and must be treated as distinct
  publications. See the codebook before deduplicating or joining.
- **Served items are not reported totals.** `feedback` contains what the
  portal serves as individual items. `publications.total_feedback` is the
  number the Commission reports, which includes campaign and questionnaire
  submissions the portal aggregates. For campaign-heavy consultations the two
  differ by orders of magnitude, and some closed consultations report a total
  but serve nothing.

Dates are kept as served (`YYYY/MM/DD HH:MM:SS`, no time zone). Every record
carries `first_seen`, `last_seen` and `last_changed` timestamps (UTC) that the
tool maintains across runs.

## Usage

```bash
haveyoursay [--db PATH] [--verbose] [--log-file PATH] <mode> [options]
```

`--db` selects the SQLite file (default `haveyoursay.db`). Every run writes a
log file (default `haveyoursay_<mode>_<timestamp>.log` in the current
directory) and, for `collect`, a row in the database's `runs` table with the
tool version, commit, arguments and counts.

### collect

```bash
haveyoursay --db hys.db collect [--wait 0.5] [--refresh] [--initiative-id ID ...]
```

Fetches the initiative search, every initiative record not yet in the
database, and the feedback of every publication not yet fetched. Rerunning
the command resumes an interrupted collection and adds initiatives and
publications that are new on the portal. It never re-requests what is
already stored; use `--refresh` for that. `--update` is accepted as an alias
of this default behavior.

`--refresh` re-requests every initiative and every publication's feedback.
Records that the portal still serves get a new `last_seen`; records whose
content differs from the stored one are overwritten and get a new
`last_changed`; records the portal no longer serves keep their old
`last_seen`, so their disappearance is visible. Earlier content is not kept.

Requests pause for `--wait` seconds after each response (default 0.5), time
out after 60 seconds, identify the tool in the `User-Agent` header, and are
retried with exponential backoff on server errors (5xx), 408, 429, timeouts
and network errors for up to 15 minutes. Each initiative is committed as soon
as it is stored; each publication's feedback is stored in one transaction and
never partially. The run ends with a summary of found, fetched and failed
ids, and the exit code is 1 when anything failed; the next default run
requests exactly the failed records. Initiatives that appeared in the search
but could not be retrieved are exported with `fetch_failed = True`.

### download

```bash
haveyoursay --db hys.db download [--directory DIR] [--only publication|feedback]
    [--publication-type TYPE ...] [--language LANG ...] [--force] [--wait 0.5]
```

Downloads the attached files into `DIR/data/attachments/{publications,feedback}/<attachment id>/`.
Filters accept SQL wildcards (`%`). Existing files are skipped unless
`--force` is given.

### dataset

```bash
haveyoursay --db hys.db dataset [meta] [--directory DIR] [--attachments] [--long-tables]
    [--only initiative|publication|feedback ...] [--merge] [--json] [--include-data]
haveyoursay dataset text --input-directory DIR [--directory DIR] [--only publication|feedback]
    [--parallel N] [--pdf-library pdfplumber|pdfminer.six|pymupdf]
```

`dataset` (or `dataset meta`) exports the datasets listed in the codebook.
The database is opened read-only and is not modified. `--attachments` adds
the two attachment datasets, `--long-tables` adds one row per initiative and
topic or policy area, `--merge` joins everything into one wide file,
`--json` writes JSON instead of CSV, and `--include-data` adds the raw JSON
column. The raw column contains contact details of Commission staff and the
complete submitter data; do not publish it.

`dataset text` extracts text from downloaded PDF and Word files into one
dataset keyed by attachment id and type; join it to `publication_attachments`
or `feedback_attachments` on `id`.

The merged dataset contains one row per combination of publication attachment
and feedback item of the same publication, because both are joined to the
publication. Deduplicate by `feedback_id` before computing feedback-level
statistics, or use the separate datasets.

## CSV format

UTF-8, comma separated, header row, standard quoting (fields with commas,
quotes or line breaks are quoted, quotes are doubled, no escape character),
empty fields for missing values, booleans as `True`/`False`, integers without
decimals, rows sorted by their keys. `pandas.read_csv`, R's `read.csv`,
`readr::read_csv` and `data.table::fread` read the files with their defaults.
Titles and free text can contain line breaks and surrounding spaces as served.

## Personal data and public release

The feedback dataset contains what submitters chose to publish on the portal,
including names, organization names and the text of their contributions. For
a public release of the collected data, `scripts/censor_datasets.py` writes a
copy that drops the free text, submitter names and submitter-chosen file
names and titles, blanks the organization of citizen submissions, and never
includes the raw column; its `manifest.json` records the rules applied.

```bash
python scripts/censor_datasets.py ./datasets/full ./datasets/public
```

## Reproducibility

Each export writes `manifest.json` with the tool version and commit, the
database path, size and SHA-256, the schema version, the `runs` table, the
Python, pandas and SQLite versions, and the row count, columns and SHA-256 of
every file, plus a `SHA256SUMS` file. Databases written by another schema
version are refused rather than misread.

## Project structure

- `src/haveyoursay/` the package: `cli.py` (command line), `collect.py`,
  `download.py`, `dataset.py`, `utils.py` (schema, views, HTTP, text extraction)
- `haveyoursay.py` compatibility entry point for `python haveyoursay.py`
- `scripts/censor_datasets.py` public-release copy; `scripts/build_codebook.py`
  renders `docs/codebook.md` from `docs/codebook.csv`
- `docs/` codebook
- `tests/` test suite with a synthetic fixture (no network); run `pytest`

## Citation

See [CITATION.cff](CITATION.cff). A research note describing the tool and
the data is in preparation.

## License

MIT, see [LICENSE](LICENSE). The collected content belongs to the European
Commission and to the submitters; see the Commission's reuse policy
(Decision 2011/833/EU) and the note on personal data above.
