"""Task 2 helpers: upsert the landed JSON pages into one raw_* table per endpoint."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import duckdb
import pandas as pd

from .config import DATA_DIR, DB_PATH, RUN_DIR


@dataclass(frozen=True)
class RawTable:
    name: str
    key_type: str
    key: Callable[[dict], object]                 # natural key -> raw.id
    updated: Callable[[dict], str | None]         # record timestamp -> raw.updated_at


# Endpoint folder under data/raw/<owner>__<repo>/ -> raw table.
RAW_TABLES = {
    # Commits have no updated_at; the committer date is what `since` filters on.
    "commits": RawTable("raw_commits", "VARCHAR", lambda r: r["sha"], lambda r: r["commit"]["committer"]["date"]),
    "pulls": RawTable("raw_pull_requests", "BIGINT", lambda r: r["number"], lambda r: r["updated_at"]),
    "reviews": RawTable("raw_pr_reviews", "BIGINT", lambda r: r["id"], lambda r: r.get("submitted_at")),
    "releases": RawTable("raw_releases", "BIGINT", lambda r: r["id"],
                         lambda r: r.get("published_at") or r["created_at"]),
}

# Landing folders whose pages hold each table's records. A pulls_graphql page (GraphQL mode)
# holds PRs, each with its first 100 reviews nested inside.
SOURCES = {
    "commits": ["commits"],
    "pulls": ["pulls", "pulls_graphql"],
    "reviews": ["reviews", "pulls_graphql"],
    "releases": ["releases"],
}


def landed_pages(endpoint: str):
    """(folder, path) for every landed page that holds records for `endpoint`."""
    for folder in SOURCES[endpoint]:
        for path in sorted(DATA_DIR.glob(f"*/{folder}/**/page_*.json")):
            yield folder, path


def page_records(folder: str, endpoint: str, page: dict) -> list[dict]:
    """The `endpoint` records inside one landed page from `folder`."""
    if folder != "pulls_graphql":
        return page["data"]
    prs = page["data"]["data"]["repository"]["pullRequests"]["nodes"]
    return prs if endpoint == "pulls" else [review for pr in prs for review in pr["reviews"]["nodes"]]


def connect() -> duckdb.DuckDBPyConnection:
    """Read-write connection used by Tasks 2 and 4-6. They share one mode on purpose: dbt-duckdb
    keeps its own read-write handle open after Task 3, and DuckDB refuses a second handle on
    the same file with a different configuration (e.g. read_only=True)."""
    con = duckdb.connect(str(DB_PATH))
    con.execute("set TimeZone = 'UTC'")  # GitHub timestamps are UTC; keep date_trunc in UTC too
    return con


def to_naive_utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    for spec in RAW_TABLES.values():
        con.execute(f"""
            create table if not exists {spec.name} (
                repo        varchar   not null,
                id          {spec.key_type} not null,
                updated_at  timestamp,
                fetched_at  timestamp not null,
                source_file varchar   not null,
                payload     json      not null,
                primary key (repo, id)
            )""")
    con.execute("""
        create table if not exists _load_state (
            repo       varchar   not null,
            endpoint   varchar   not null,
            watermark  timestamp,          -- newest updated_at loaded for this repo/endpoint
            row_count  bigint    not null,
            loaded_at  timestamp not null, -- when the load that wrote this row finished
            primary key (repo, endpoint)
        )""")


def read_landed(endpoint: str) -> tuple[pd.DataFrame, int]:
    """One row per record across every landed page of `endpoint`, plus the number of files read."""
    spec = RAW_TABLES[endpoint]
    rows, files = [], 0
    for folder, path in landed_pages(endpoint):
        rel = path.relative_to(RUN_DIR).as_posix()
        try:
            page = json.loads(path.read_text(encoding="utf-8"))
            fetched_at = to_naive_utc(page["fetched_at"])
            for rec in page_records(folder, endpoint, page):
                rows.append((page["repo"], spec.key(rec), to_naive_utc(spec.updated(rec)),
                             fetched_at, rel, json.dumps(rec)))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Cannot load landing file {rel}: {type(exc).__name__}: {exc}") from exc
        files += 1
    columns = ["repo", "id", "updated_at", "fetched_at", "source_file", "payload"]
    return pd.DataFrame(rows, columns=columns), files


def upsert(con: duckdb.DuckDBPyConnection, endpoint: str, frame: pd.DataFrame) -> dict:
    """INSERT OR REPLACE on (repo, id); the newest fetch of a record wins within the batch."""
    spec = RAW_TABLES[endpoint]
    count = f"select count(*) from {spec.name}"
    before = con.execute(count).fetchall()[0][0]
    con.register("incoming", frame)
    con.execute(f"""
        insert or replace into {spec.name}
        select repo, id::{spec.key_type}, updated_at::timestamp, fetched_at::timestamp, source_file, payload::json
        from incoming
        qualify row_number() over (partition by repo, id order by fetched_at desc, source_file desc) = 1
    """)
    con.unregister("incoming")
    after = con.execute(count).fetchall()[0][0]
    unique_keys = len(frame.drop_duplicates(["repo", "id"]))
    return {"table": spec.name, "records": len(frame), "unique_keys": unique_keys,
            "rows_before": before, "rows_after": after,
            "inserted": after - before, "updated": unique_keys - (after - before)}


def record_load_state(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Refresh _load_state from the raw tables once every landed file has been loaded."""
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for endpoint, spec in RAW_TABLES.items():
        con.execute(f"""
            insert or replace into _load_state
            select repo, ?, max(updated_at), count(*), ?
            from {spec.name}
            group by repo
        """, [endpoint, loaded_at])
    return con.execute("select * from _load_state order by repo, endpoint").df()
