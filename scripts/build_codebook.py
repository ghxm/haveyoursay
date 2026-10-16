"""Render docs/codebook.md from docs/codebook_notes.md and docs/codebook.csv.

Usage: python scripts/build_codebook.py
"""

import csv
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"


def main():
    with open(DOCS / "codebook.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    out = [(DOCS / "codebook_notes.md").read_text().rstrip(), ""]
    for dataset in dict.fromkeys(r["dataset"] for r in rows):
        out += [f"### {dataset}", "", "| Column | Source | Type | Format | Censored | Description |", "|---|---|---|---|---|---|"]
        for r in rows:
            if r["dataset"] == dataset:
                cells = [r["column"], r["source"], r["type"], r["format"], r["censored"], r["description"]]
                out.append("| " + " | ".join(c.replace("|", "\\|") for c in cells) + " |")
        out.append("")
    (DOCS / "codebook.md").write_text("\n".join(out))
    print(f"Written {DOCS / 'codebook.md'} ({len(rows)} columns)")


if __name__ == "__main__":
    main()
