#!/usr/bin/env python3

import argparse
import json
import os
import re
import sqlite3
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_DIGEST = Path("outputs/latest.json")
DEFAULT_CONFIG = Path("screening_rules.toml")
DEFAULT_ZOTERO_ROOT = Path("/Users/tingsun/Documents/04_文献与阅读/zotero-文献")
DEFAULT_COLLECTION = "00_Inbox_RSS/00_Read Soon"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter weekly digest JSON by title/abstract keywords and import matches into Zotero."
    )
    parser.add_argument("--digest", type=Path, default=DEFAULT_DIGEST, help="Path to digest JSON.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to screening_rules.toml.")
    parser.add_argument(
        "--zotero-root",
        type=Path,
        default=DEFAULT_ZOTERO_ROOT,
        help="Path to Zotero data root containing zotero.sqlite.",
    )
    parser.add_argument(
        "--collection-path",
        default=None,
        help="Override target Zotero collection path, e.g. '00_Inbox_RSS/00_Read Soon'.",
    )
    parser.add_argument(
        "--library-type",
        default=os.environ.get("ZOTERO_LIBRARY_TYPE", "user"),
        choices=("user", "group"),
        help="Zotero Web API library type.",
    )
    parser.add_argument(
        "--library-id",
        default=os.environ.get("ZOTERO_LIBRARY_ID", ""),
        help="Zotero Web API user/group library ID.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("ZOTERO_API_KEY", ""),
        help="Zotero Web API key with write access.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Screen and dedupe without writing to Zotero.")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of matched articles to process.")
    parser.add_argument("--report-out", type=Path, default=None, help="Optional path to save a JSON report.")
    return parser.parse_args()


def load_toml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Screening config not found: {path}")
    return tomllib.loads(path.read_text(encoding="utf-8"))


def load_digest(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Digest JSON not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def open_db(zotero_root: Path) -> sqlite3.Connection:
    db_path = zotero_root / "zotero.sqlite"
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def split_collection_path(collection_path: str) -> list[str]:
    parts = [part.strip() for part in collection_path.split("/") if part.strip()]
    if not parts:
        raise ValueError("Collection path is empty.")
    return parts


def resolve_collection(conn: sqlite3.Connection, collection_path: str) -> dict:
    parent_id = None
    row = None
    for name in split_collection_path(collection_path):
        row = conn.execute(
            """
            SELECT collectionID, key, collectionName
            FROM collections
            WHERE collectionName = ?
              AND (
                (? IS NULL AND parentCollectionID IS NULL)
                OR parentCollectionID = ?
              )
            """,
            (name, parent_id, parent_id),
        ).fetchone()
        if row is None:
            raise ValueError(f"Collection not found: {collection_path}")
        parent_id = row["collectionID"]
    return {
        "collection_id": int(row["collectionID"]),
        "collection_key": row["key"],
        "collection_name": row["collectionName"],
        "collection_path": collection_path,
    }


def build_field_map(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT fieldID, fieldName FROM fieldsCombined").fetchall()
    return {row["fieldName"]: int(row["fieldID"]) for row in rows}


def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_title(text: str) -> str:
    text = normalize_text(text)
    return re.sub(r"[^a-z0-9]+", "", text)


def flatten_digest(digest: dict) -> list[dict]:
    items = []
    run_date = digest.get("run_date", "")
    for journal, journal_items in digest.get("journals", {}).items():
        for item in journal_items:
            flattened = dict(item)
            flattened["journal"] = item.get("journal") or journal
            flattened["run_date"] = run_date
            items.append(flattened)
    return items


def get_search_text(article: dict, search_fields: list[str], case_sensitive: bool) -> str:
    chunks = []
    for field in search_fields:
        value = article.get(field, "")
        if isinstance(value, list):
            chunks.extend(str(v) for v in value if v)
        elif value:
            chunks.append(str(value))
    text = "\n".join(chunks)
    return text if case_sensitive else text.lower()


def classify_article(article: dict, config: dict) -> dict | None:
    global_cfg = config.get("global", {})
    search_fields = global_cfg.get("search_fields", ["title", "abstract"])
    case_sensitive = bool(global_cfg.get("case_sensitive", False))
    search_text = get_search_text(article, search_fields, case_sensitive)
    journal_cmp = article.get("journal", "") if case_sensitive else article.get("journal", "").lower()

    matched_topics = []
    total_score = 0
    for topic_key, topic_cfg in config.get("topics", {}).items():
        topic_label = topic_cfg.get("label", topic_key)
        must_any = [term if case_sensitive else term.lower() for term in topic_cfg.get("must_any", [])]
        must_all = [term if case_sensitive else term.lower() for term in topic_cfg.get("must_all", [])]
        exclude = [term if case_sensitive else term.lower() for term in topic_cfg.get("exclude", [])]
        allowlist = topic_cfg.get("journal_allowlist", [])
        allowlist_cmp = [value if case_sensitive else value.lower() for value in allowlist]

        if allowlist_cmp and journal_cmp not in allowlist_cmp:
            continue
        if any(term and term in search_text for term in exclude):
            continue
        if must_all and not all(term in search_text for term in must_all):
            continue
        if must_any and not any(term in search_text for term in must_any):
            continue

        score = 0
        for term, weight in topic_cfg.get("priority_score", {}).items():
            candidate = term if case_sensitive else term.lower()
            if candidate in search_text:
                score += int(weight)
        matched_topics.append(
            {
                "topic_key": topic_key,
                "topic_label": topic_label,
                "score": score,
            }
        )
        total_score += score

    if not matched_topics and config.get("routing", {}).get("skip_if_no_topic_match", True):
        return None

    article = dict(article)
    article["matched_topics"] = matched_topics
    article["screening_score"] = total_score
    article["normalized_title"] = normalize_title(article.get("title", ""))
    return article


def dedupe_screened_articles(screened: list[dict], dedupe_by: list[str]) -> tuple[list[dict], int]:
    unique_articles = []
    seen_to_article = {}
    duplicates = 0

    for article in screened:
        keys = []
        for field in dedupe_by:
            value = article.get("normalized_title", "") if field == "normalized_title" else article.get(field, "")
            if value:
                keys.append(f"{field}:{value}")
        if not keys:
            keys = [f"fallback:{article.get('journal', '')}:{article.get('title', '')}"]

        existing = None
        for key in keys:
            if key in seen_to_article:
                existing = seen_to_article[key]
                break

        if existing is None:
            unique_articles.append(article)
            for key in keys:
                seen_to_article[key] = article
            continue

        duplicates += 1
        existing_topics = {topic["topic_key"] for topic in existing.get("matched_topics", [])}
        for topic in article.get("matched_topics", []):
            if topic["topic_key"] not in existing_topics:
                existing.setdefault("matched_topics", []).append(topic)
        existing["screening_score"] = max(existing.get("screening_score", 0), article.get("screening_score", 0))

    return unique_articles, duplicates


def find_existing_item(conn: sqlite3.Connection, field_map: dict[str, int], article: dict, target_collection_id: int) -> dict | None:
    candidates = []
    if article.get("doi"):
        candidates.append(("DOI", article["doi"]))
    if article.get("url"):
        candidates.append(("url", article["url"]))
    if article.get("title"):
        candidates.append(("title", article["title"]))

    for field_name, value in candidates:
        field_id = field_map.get(field_name)
        if field_id is None:
            continue
        row = conn.execute(
            """
            SELECT i.itemID, i.key,
                   EXISTS(
                     SELECT 1
                     FROM collectionItems ci
                     WHERE ci.itemID = i.itemID AND ci.collectionID = ?
                   ) AS in_target_collection,
                   EXISTS(
                     SELECT 1
                     FROM itemAttachments ia
                     WHERE ia.parentItemID = i.itemID AND ia.contentType = 'application/pdf'
                   ) AS has_pdf
            FROM items i
            JOIN itemData id ON id.itemID = i.itemID
            JOIN itemDataValues v ON v.valueID = id.valueID
            WHERE id.fieldID = ?
              AND lower(v.value) = lower(?)
              AND NOT EXISTS (
                SELECT 1 FROM itemAttachments ia2 WHERE ia2.itemID = i.itemID
              )
            LIMIT 1
            """,
            (target_collection_id, field_id, value),
        ).fetchone()
        if row:
            return {
                "item_id": int(row["itemID"]),
                "item_key": row["key"],
                "matched_field": field_name,
                "matched_value": value,
                "in_target_collection": bool(row["in_target_collection"]),
                "has_pdf": bool(row["has_pdf"]),
            }
    return None


def split_authors(article: dict) -> list[str]:
    author_list = [str(name).strip() for name in article.get("authors_list", []) if str(name).strip()]
    if author_list:
        return author_list
    authors = (article.get("authors") or "").strip()
    if not authors:
        return []
    if ";" in authors:
        return [part.strip() for part in authors.split(";") if part.strip()]
    if " and " in authors:
        return [part.strip() for part in authors.split(" and ") if part.strip()]
    return [authors]


def build_creators(article: dict) -> list[dict]:
    creators = []
    for author in split_authors(article):
        if "," in author:
            last, first = [part.strip() for part in author.split(",", 1)]
            if first and last:
                creators.append({"creatorType": "author", "firstName": first, "lastName": last})
                continue
        parts = author.split()
        if len(parts) >= 2:
            creators.append(
                {
                    "creatorType": "author",
                    "firstName": " ".join(parts[:-1]),
                    "lastName": parts[-1],
                }
            )
        else:
            creators.append({"creatorType": "author", "name": author})
    return creators


def build_tags(article: dict, config: dict) -> list[dict]:
    tags = [{"tag": tag} for tag in config.get("routing", {}).get("add_tags", [])]
    if config.get("routing", {}).get("add_week_tag", True) and article.get("run_date"):
        tags.append({"tag": f"week-{article['run_date']}"})
    for topic in article.get("matched_topics", []):
        tags.append({"tag": f"topic:{topic['topic_label']}"})

    deduped = []
    seen = set()
    for tag in tags:
        if tag["tag"] not in seen:
            deduped.append(tag)
            seen.add(tag["tag"])
    return deduped


def build_item_payload(article: dict, collection_key: str, config: dict) -> dict:
    topics = ", ".join(topic["topic_label"] for topic in article.get("matched_topics", []))
    extra_lines = []
    if topics:
        extra_lines.append(f"Journal Tracker Topics: {topics}")
    if article.get("screening_score"):
        extra_lines.append(f"Journal Tracker Score: {article['screening_score']}")
    if article.get("run_date"):
        extra_lines.append(f"Journal Tracker Week: {article['run_date']}")

    return {
        "itemType": "journalArticle",
        "title": article.get("title", ""),
        "creators": build_creators(article),
        "abstractNote": article.get("abstract", ""),
        "publicationTitle": article.get("journal", ""),
        "date": article.get("date", ""),
        "DOI": article.get("doi", ""),
        "url": article.get("url", ""),
        "collections": [collection_key],
        "tags": build_tags(article, config),
        "extra": "\n".join(extra_lines),
    }


def zotero_request(method: str, url: str, api_key: str, payload: dict | list | None = None) -> tuple[int, dict | None]:
    headers = {
        "Zotero-API-Key": api_key,
        "Zotero-API-Version": "3",
        "Content-Type": "application/json",
    }
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body) if body else None
        except json.JSONDecodeError:
            parsed = {"message": body}
        return exc.code, parsed


def create_item_via_api(article: dict, collection_key: str, config: dict, library_type: str, library_id: str, api_key: str) -> dict:
    base_url = f"https://api.zotero.org/{library_type}s/{library_id}"
    status, response = zotero_request("POST", f"{base_url}/items", api_key, [build_item_payload(article, collection_key, config)])
    if status != 200:
        message = response.get("message", response) if isinstance(response, dict) else response
        raise RuntimeError(f"Zotero API POST failed ({status}): {message}")

    success = (response or {}).get("successful", {})
    failed = (response or {}).get("failed", {})
    if "0" in success:
        item_ref = success["0"]
        if isinstance(item_ref, dict):
            return {"item_key": item_ref.get("key", "")}
        return {"item_key": str(item_ref)}
    if "0" in failed:
        failure = failed["0"]
        raise RuntimeError(f"Zotero API rejected item: {failure.get('message', failure)}")
    raise RuntimeError(f"Unexpected Zotero API response: {response}")


def write_report(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = load_toml(args.config)
    digest = load_digest(args.digest)
    collection_path = args.collection_path or config.get("routing", {}).get("zotero_collection_path", DEFAULT_COLLECTION)

    conn = open_db(args.zotero_root)
    field_map = build_field_map(conn)
    collection = resolve_collection(conn, collection_path)

    flattened = flatten_digest(digest)
    screened = []
    screened_out = 0
    for article in flattened:
        classified = classify_article(article, config)
        if classified is None:
            screened_out += 1
            continue
        screened.append(classified)

    dedupe_by = config.get("global", {}).get("dedupe_by", ["doi", "url", "normalized_title"])
    unique_articles, duplicate_count = dedupe_screened_articles(screened, dedupe_by)
    unique_articles.sort(key=lambda item: (-item.get("screening_score", 0), item.get("journal", ""), item.get("title", "")))
    if args.limit is not None:
        unique_articles = unique_articles[: args.limit]

    if not args.dry_run:
        missing = []
        if not args.library_id:
            missing.append("ZOTERO_LIBRARY_ID / --library-id")
        if not args.api_key:
            missing.append("ZOTERO_API_KEY / --api-key")
        if missing:
            raise SystemExit("Missing required Zotero API configuration: " + ", ".join(missing))

    results = []
    imported_count = 0
    skipped_existing = 0

    for article in unique_articles:
        existing = find_existing_item(conn, field_map, article, collection["collection_id"])
        result = {
            "title": article.get("title", ""),
            "journal": article.get("journal", ""),
            "doi": article.get("doi", ""),
            "url": article.get("url", ""),
            "topics": [topic["topic_label"] for topic in article.get("matched_topics", [])],
            "screening_score": article.get("screening_score", 0),
        }
        if existing:
            result["status"] = "skipped_existing_in_collection" if existing["in_target_collection"] else "skipped_existing_elsewhere"
            result["existing_item_key"] = existing["item_key"]
            result["matched_field"] = existing["matched_field"]
            result["has_pdf"] = existing["has_pdf"]
            skipped_existing += 1
            results.append(result)
            continue

        if args.dry_run:
            result["status"] = "dry_run_import"
            imported_count += 1
            results.append(result)
            continue

        try:
            created = create_item_via_api(
                article,
                collection["collection_key"],
                config,
                args.library_type,
                args.library_id,
                args.api_key,
            )
            result["status"] = "imported"
            result["item_key"] = created["item_key"]
            imported_count += 1
        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)
        results.append(result)

    summary = {
        "digest_path": str(args.digest.resolve()),
        "config_path": str(args.config.resolve()),
        "dry_run": args.dry_run,
        "run_date": digest.get("run_date", ""),
        "collection_path": collection["collection_path"],
        "collection_key": collection["collection_key"],
        "total_digest_items": len(flattened),
        "screened_out_count": screened_out,
        "matched_count": len(screened),
        "digest_duplicate_count": duplicate_count,
        "processed_count": len(unique_articles),
        "imported_count": imported_count,
        "skipped_existing_count": skipped_existing,
        "results": results,
    }

    if args.report_out:
        write_report(args.report_out, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
