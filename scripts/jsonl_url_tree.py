from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from urllib.parse import unquote, urlparse

from treelib import Tree


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a URL segment tree from crawler JSONL and write a CSV."
    )
    parser.add_argument("--input", required=True, help="Input crawler JSONL path.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--segments",
        type=int,
        default=4,
        help="Number of segment columns to write. Default: 4.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    rows = read_jsonl(Path(args.input), args.segments)
    tree = build_tree(rows)
    write_csv(Path(args.output), sorted_rows(rows), args.segments)
    print(
        f"wrote {len(rows)} rows, tree_nodes={tree.size()}, output={args.output}",
        flush=True,
    )


def read_jsonl(path: Path, segment_count: int) -> list[dict[str, str]]:
    seen: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_number}: {exc}") from exc

            url = str(item.get("url") or "").strip()
            if not url:
                continue
            title = str(item.get("title") or "").strip()
            segments = split_url_segments(url, segment_count)

            if url not in seen:
                seen[url] = {
                    **{f"segment{i + 1}": segments[i] for i in range(segment_count)},
                    "url": url,
                    "title": title,
                    "_tree_segments": split_url_segments(url, None),
                }
            elif title and not seen[url].get("title"):
                seen[url]["title"] = title

    return list(seen.values())


def split_url_segments(url: str, segment_count: int | None) -> list[str]:
    parsed = urlparse(url)
    segments: list[str] = []
    if parsed.hostname:
        host = parsed.hostname.lower()
        if parsed.port:
            host = f"{host}:{parsed.port}"
        segments.append(host)

    path_segments = [
        unquote(part)
        for part in parsed.path.split("/")
        if part
    ]
    if parsed.query:
        if path_segments:
            path_segments[-1] = f"{path_segments[-1]}?{parsed.query}"
        else:
            path_segments.append(f"?{parsed.query}")

    segments.extend(path_segments)

    if segment_count is None:
        return segments or [""]

    return (segments + [""] * segment_count)[:segment_count]


def build_tree(rows: list[dict[str, str]]) -> Tree:
    tree = Tree()
    tree.create_node("urls", "urls")

    for row in rows:
        parent = "urls"
        current_path: list[str] = []
        for segment in row["_tree_segments"]:
            current_path.append(segment)
            node_id = "/".join(current_path)
            if not tree.contains(node_id):
                tree.create_node(segment or "(empty)", node_id, parent=parent)
            parent = node_id

        url_node_id = f"url:{row['url']}"
        if not tree.contains(url_node_id):
            title = row.get("title") or row["url"]
            tree.create_node(title, url_node_id, parent=parent)

    return tree


def sorted_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    def key(row: dict[str, str]) -> tuple[tuple[bool, str], ...]:
        segment_keys = []
        index = 1
        while f"segment{index}" in row:
            value = row[f"segment{index}"]
            segment_keys.append((value != "", value.lower()))
            index += 1
        return (*segment_keys, (row["url"] != "", row["url"].lower()))

    return sorted(rows, key=key)


def write_csv(path: Path, rows: list[dict[str, str]], segment_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [f"segment{i + 1}" for i in range(segment_count)] + ["url", "title"]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


if __name__ == "__main__":
    main()
