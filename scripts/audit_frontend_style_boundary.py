from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "consumers" / "frontend"

STYLE_BLOCK_RE = re.compile(r"<style\b[^>]*>.*?</style>", re.IGNORECASE | re.DOTALL)
STYLE_ATTR_RE = re.compile(r"\sstyle\s*=\s*([\"']).*?\1", re.IGNORECASE | re.DOTALL)
IMPORTANT_RE = re.compile(r"!important\b", re.IGNORECASE)
LINK_TAG_RE = re.compile(r"<link\b[^>]*>", re.IGNORECASE | re.DOTALL)
HREF_RE = re.compile(r"\bhref\s*=\s*([\"'])(.*?)\1", re.IGNORECASE | re.DOTALL)
REL_STYLESHEET_RE = re.compile(r"\brel\s*=\s*([\"'])stylesheet\1", re.IGNORECASE)


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def stylesheet_hrefs(text: str) -> list[str]:
    hrefs: list[str] = []
    for tag in LINK_TAG_RE.findall(text):
        if not REL_STYLESHEET_RE.search(tag):
            continue
        match = HREF_RE.search(tag)
        if match:
            hrefs.append(match.group(2))
    return hrefs


def build_report() -> dict[str, Any]:
    php_files = sorted(FRONTEND.rglob("*.php"))
    css_files = sorted(FRONTEND.rglob("*.css"))
    repo_css_names = {path.name for path in css_files}

    files: list[dict[str, Any]] = []
    referenced_stylesheets: set[str] = set()

    for path in php_files:
        text = path.read_text(encoding="utf-8")
        hrefs = stylesheet_hrefs(text)
        referenced_stylesheets.update(hrefs)
        files.append(
            {
                "path": rel(path),
                "embedded_style_blocks": len(STYLE_BLOCK_RE.findall(text)),
                "inline_style_attributes": len(STYLE_ATTR_RE.findall(text)),
                "important_declarations": len(IMPORTANT_RE.findall(text)),
                "stylesheet_hrefs": hrefs,
            }
        )

    embedded = [item for item in files if item["embedded_style_blocks"]]
    inline = [item for item in files if item["inline_style_attributes"]]
    important = [item for item in files if item["important_declarations"]]

    external_asset_dependencies: list[str] = []
    for href in sorted(referenced_stylesheets):
        clean = href.split("?", 1)[0]
        name = Path(clean).name
        if name.endswith(".css") and name not in repo_css_names:
            external_asset_dependencies.append(href)

    return {
        "schema_version": "1.0.0",
        "frontend_root": rel(FRONTEND),
        "php_files_scanned": len(php_files),
        "repo_css_files": [rel(path) for path in css_files],
        "embedded_style_blocks_total": sum(item["embedded_style_blocks"] for item in files),
        "inline_style_attributes_total": sum(item["inline_style_attributes"] for item in files),
        "important_declarations_total": sum(item["important_declarations"] for item in files),
        "embedded_style_files": embedded,
        "inline_style_files": inline,
        "important_files": important,
        "stylesheet_references": sorted(referenced_stylesheets),
        "stylesheet_dependencies_without_source_file_under_consumers_frontend": external_asset_dependencies,
        "files": files,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit the calculator frontend style boundary.")
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    args = parser.parse_args()

    report = build_report()
    payload = json.dumps(report, ensure_ascii=False, indent=2) + "\n"

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
