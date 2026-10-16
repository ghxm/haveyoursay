# Changelog

## 0.9.0 (unreleased)

Breaking changes

- The tool is now an installable package with a `haveyoursay` console
  command; `python haveyoursay.py` from a checkout still works. Modules
  moved from `src/` to `src/haveyoursay/`.
- Database schema version 2. Databases written by earlier versions are
  refused; collect into a new file. Rows carry `first_seen`, `last_seen`
  and `last_changed` instead of `timestamp`; new tables `runs` and
  `publications_fetched`.
- `collect` fetches what is missing by default (`--update` is now an alias
  of the default); the new `--refresh` re-requests everything and maintains
  the timestamps.
- CSV output uses standard quoting (no escape character); booleans are
  written as `True`/`False` and integers without decimals; rows are sorted.
- `feedback_attachments` gains `publication_id` and loses
  `publication_type`; `publications` gains `id_shared` and `id_collision`;
  `initiatives` gains `fetch_failed` and the three timestamps.
- `download --wait` defaults to 0.5 seconds.
- Dependencies are declared in `pyproject.toml` (`requirements.txt`
  removed); `pdfminer.six` and `pymupdf` are an optional extra `pdf`.

Added

- `dataset` opens the database read-only and never modifies it; the export
  views are temporary.
- `manifest.json` and `SHA256SUMS` next to every export, with tool version,
  commit, database checksum, run history and per-file counts.
- `--long-tables` exports `initiative_topics` and `initiative_policy_areas`.
- `scripts/censor_datasets.py` writes a copy for public release without
  submitter names, free text and submitter-chosen file names.
- `docs/codebook.md` documents every column, the vocabularies and the
  portal's quirks (shared and colliding publication ids, reported versus
  served feedback, dates without time zone).
- Request timeout, `User-Agent` header, per-initiative commits, an
  end-of-run summary with failed ids and a non-zero exit code on failures,
  a warning for colliding publication ids.
- `--log-file`, `--version`; `dataset --directory` is created if missing;
  paths work on Windows.
- Test suite with a synthetic fixture and GitHub Actions CI on Python 3.9,
  3.13 and 3.14.

Fixed

- `pip install .` produced a broken install (#6).
- `dataset --directory` failed when the directory did not exist (#7).
- Submitter names were exported with no way to leave them out (#8).
- `download` had no default pause between requests (#9).
- Paths built with `split("/")` broke on Windows (#10).
- `--update` re-requested every publication with zero feedback (#5).
- Dataset columns were undocumented (#12).
- Publications served under several initiatives were fetched several times.
