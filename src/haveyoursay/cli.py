"""Command line interface."""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from haveyoursay import API_BASE, __version__
from haveyoursay import collect as cl
from haveyoursay import dataset as ds
from haveyoursay import download as dl
from haveyoursay.utils import connect, git_commit, utc_now

logger = logging.getLogger("haveyoursay")


def _arguments_json(args):
    return json.dumps({k: v for k, v in vars(args).items() if k != "func"}, default=str)


def _start_run(conn, mode, args):
    cursor = conn.execute(
        "INSERT INTO runs (mode, tool_version, git_commit, arguments, api_base, started_at, status) "
        "VALUES (?, ?, ?, ?, ?, ?, 'running')",
        (mode, __version__, git_commit(), _arguments_json(args), API_BASE, utc_now()))
    conn.commit()
    return cursor.lastrowid


def _finish_run(conn, run_id, status, initiatives=None, feedback=None):
    initiatives = initiatives or {}
    feedback = feedback or {}
    conn.execute(
        "UPDATE runs SET finished_at = ?, status = ?, initiatives_found = ?, initiatives_fetched = ?, "
        "initiatives_failed = ?, publications_found = ?, publications_fetched = ?, "
        "publications_failed = ?, feedback_items = ? WHERE id = ?",
        (utc_now(), status, initiatives.get("found"), initiatives.get("fetched"),
         len(initiatives.get("failed", [])), feedback.get("found"), feedback.get("fetched"),
         len(feedback.get("failed", [])), feedback.get("items"), run_id))
    conn.commit()


def run_collect(args):
    if args.update:
        logger.info("--update is the default behavior (fetch what is missing); flag kept for compatibility")
    with connect(args.db) as conn:
        run_id = _start_run(conn, "collect", args)
        try:
            initiatives = cl.collect_initiatives(conn, wait=args.wait, refresh=args.refresh,
                                                 initiative_ids=args.initiative_id)
            feedback = cl.collect_feedback(conn, wait=args.wait, refresh=args.refresh,
                                           initiative_ids=args.initiative_id)
        except BaseException:
            _finish_run(conn, run_id, "interrupted")
            raise
        failed = len(initiatives["failed"]) + len(feedback["failed"])
        _finish_run(conn, run_id, "completed_with_errors" if failed else "completed",
                    initiatives, feedback)
    logger.info(f"Initiatives: {initiatives['found']} found, {initiatives['fetched']} fetched "
                f"({initiatives['new']} new, {initiatives['changed']} changed, "
                f"{initiatives['unchanged']} unchanged), {len(initiatives['failed'])} failed")
    logger.info(f"Publications: {feedback['found']} found, {feedback['fetched']} fetched, "
                f"{feedback['skipped']} skipped, {len(feedback['failed'])} failed; "
                f"{feedback['items']} feedback items served ({feedback['new']} new, "
                f"{feedback['changed']} changed, {feedback['unchanged']} unchanged)")
    if initiatives["failed"]:
        logger.error(f"Failed initiatives: {initiatives['failed']}")
    if feedback["failed"]:
        logger.error(f"Failed publications: {feedback['failed']}")
    return 1 if failed else 0


def run_download(args):
    with connect(args.db, readonly=True) as conn:
        if not args.only or "publication" in args.only:
            dl.download_publication_attachments(conn, directory=args.directory, language=args.language,
                                                publication_type=args.publication_type,
                                                force=args.force, wait=args.wait)
        if not args.only or "feedback" in args.only:
            dl.download_feedback_attachments(conn, directory=args.directory, language=args.language,
                                             publication_type=args.publication_type,
                                             force=args.force, wait=args.wait)
    return 0


def run_dataset(args):
    fmt = "json" if args.json else "csv"
    if args.dataset_type == "meta":
        ds.export_datasets(args.db, directory=args.directory, only=args.only,
                           attachments=args.attachments, long_tables=args.long_tables,
                           merge=args.merge, fmt=fmt, include_data=args.include_data,
                           arguments=json.loads(_arguments_json(args)))
    else:
        if args.only and "publication" not in args.only and "feedback" not in args.only:
            raise ValueError("The text dataset can only be created for publications and feedback (--only).")
        ds.create_attachments_text_dataset(input_directory=args.input_directory,
                                           output_directory=args.directory, types=args.only,
                                           parallel=args.parallel, pdf_library=args.pdf_library, fmt=fmt)
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog="haveyoursay",
        description="Collect data from the European Commission's Have Your Say portal and "
                    "assemble it into datasets.")
    parser.add_argument("--version", action="version", version=f"haveyoursay {__version__}")
    parser.add_argument("-d", "--db", default="haveyoursay.db", help="Path to the SQLite database file.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show progress information on the console. Warnings and errors are always shown.")
    parser.add_argument("--log-file", default=None,
                        help="Log file path. Default: haveyoursay_<mode>_<timestamp>.log in the current directory.")
    subparsers = parser.add_subparsers(dest="mode", required=True,
                                       help='"collect" data, "download" attachments or create a "dataset".')

    p = subparsers.add_parser("collect", help="Collect initiative, publication and feedback metadata.")
    p.add_argument("-w", "--wait", type=float, default=0.5, help="Seconds to pause after each request. Default 0.5.")
    p.add_argument("-u", "--update", action="store_true",
                   help="Fetch only what is missing from the database. This is the default behavior.")
    p.add_argument("--refresh", action="store_true",
                   help="Re-request every initiative and every publication's feedback, updating "
                        "last_seen and last_changed timestamps.")
    p.add_argument("--initiative-id", type=int, nargs="+", default=None,
                   help="Only collect the specified initiative ids and their feedback.")
    p.set_defaults(func=run_collect)

    p = subparsers.add_parser("download", help="Download publication and feedback attachments.")
    p.add_argument("-d", "--directory", default=".", help="Directory to save attachments to. Default: current directory.")
    p.add_argument("-w", "--wait", type=float, default=0.5, help="Seconds to pause after each download. Default 0.5.")
    p.add_argument("-o", "--only", nargs="+", default=None, choices=["publication", "feedback"],
                   help="Only download attachments of the given type(s). Default: both.")
    p.add_argument("-f", "--force", action="store_true", help="Download files that already exist.")
    p.add_argument("--publication-type", nargs="+", default=None,
                   help="Filter by publication type before downloading. SQL wildcards (%%) allowed.")
    p.add_argument("--language", nargs="+", default=None,
                   help="Filter attachments by language before downloading. SQL wildcards (%%) allowed.")
    p.set_defaults(func=run_download)

    p = subparsers.add_parser("dataset", help="Create datasets from the collected data.")
    p.add_argument("dataset_type", nargs="?", default="meta", choices=["meta", "text"],
                   help='"meta" exports the collected metadata (default); "text" extracts text from downloaded attachments.')
    p.add_argument("-i", "--input-directory", default=".",
                   help="Directory holding data/attachments/ (text datasets only). Default: current directory.")
    p.add_argument("-d", "--directory", default=".", help="Output directory, created if missing. Default: current directory.")
    p.add_argument("-a", "--attachments", action="store_true", help="Also export the two attachment datasets.")
    p.add_argument("--long-tables", action="store_true",
                   help="Also export initiative_topics and initiative_policy_areas (one row per initiative and code).")
    p.add_argument("-o", "--only", nargs="+", default=None, choices=["initiative", "publication", "feedback"],
                   help="Only export datasets of the given type(s). Default: all.")
    p.add_argument("-m", "--merge", action="store_true", help="Merge the exported datasets into one wide file.")
    p.add_argument("-p", "--parallel", type=int, default=1, help="(text datasets) Number of parallel jobs. Default 1.")
    p.add_argument("--json", action="store_true", help="Write JSON instead of CSV.")
    p.add_argument("--include-data", action="store_true",
                   help="Include the raw JSON 'data' column. It contains personal data of Commission staff and submitters.")
    p.add_argument("--pdf-library", default="pdfplumber", choices=["pdfplumber", "pdfminer.six", "pymupdf"],
                   help="Library for PDF text extraction. Default pdfplumber.")
    p.set_defaults(func=run_dataset)
    return parser


def setup_logging(args):
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    log_file = args.log_file or f"haveyoursay_{args.mode}_{datetime.now().strftime('%Y%m%d%H%M')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    root.addHandler(file_handler)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO if args.verbose else logging.WARNING)
    console.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(console)
    return log_file


def main(argv=None):
    args = build_parser().parse_args(argv)
    log_file = setup_logging(args)
    logger.info(f"haveyoursay {__version__} (commit {git_commit() or 'unknown'})")
    logger.info(f"Arguments: {_arguments_json(args)}")
    logger.info(f"Database: {Path(args.db).resolve()}")
    logger.info(f"Log file: {Path(log_file).resolve()}")
    try:
        return args.func(args)
    except FileNotFoundError as e:
        logger.error(str(e))
        return 2


if __name__ == "__main__":
    sys.exit(main())
