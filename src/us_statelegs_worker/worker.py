"""Assemble the ``us_statelegs`` catalog and run the worker.

Run it:

    vgi-us-statelegs-worker                      # stdio: what DuckDB launches by default
    vgi-us-statelegs-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'us_statelegs' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-us-statelegs-worker');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .cache import CacheInfo, RefreshCache
from .common import (
    DEFAULT_KLARNER_URL,
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_OPENSTATES_API_URL,
    DEFAULT_PEOPLE_ARCHIVE_URL,
    DEFAULT_SHOR_MCCARTY_AGGREGATE_URL,
    DEFAULT_SHOR_MCCARTY_URL,
)
from .elections import ChamberIdeology, ElectionReturns, LegislatorIdeology
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .openstates_api import (
    DEFAULT_MIN_INTERVAL,
    OPENSTATES_SECRET,
    OsBills,
    OsCommittees,
    OsEvents,
    OsJurisdictions,
    OsPeople,
    OsSessions,
    OsVotes,
)
from .people import CommitteeMembers, Committees, LegislatorRoles, Legislators
from .reference import STATES_TABLE

FUNCTIONS: list[type[Any]] = [
    Legislators,
    LegislatorRoles,
    Committees,
    CommitteeMembers,
    ElectionReturns,
    LegislatorIdeology,
    ChamberIdeology,
    CacheInfo,
    RefreshCache,
    OsJurisdictions,
    OsSessions,
    OsPeople,
    OsBills,
    OsVotes,
    OsCommittees,
    OsEvents,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; return the first of each for the schema."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entries[0] for entries in by_name.values() if entries]


_SCHEMA_EXAMPLES = [json.loads(STATES_TABLE.tags["vgi.example_queries"])[0], *_stamp_example_queries(FUNCTIONS)]


class AttachOptions:
    """ATTACH options: upstream overrides (mirrors, fixtures), the cache, and API pacing."""

    cache_dir: Annotated[
        str | None,
        AttachOption(
            desc="Directory for downloaded datasets (people ~5 MB, Klarner ~110 MB + Parquet, Shor-McCarty ~7 MB). "
            "Default: the per-user cache directory, e.g. ~/Library/Caches/vgi-us-state-legislatures."
        ),
    ] = None
    max_age_days: Annotated[
        int,
        AttachOption(desc="Re-check the Open States people archive once the cached copy is this many days old."),
    ] = DEFAULT_MAX_AGE_DAYS
    offline: Annotated[
        bool,
        AttachOption(desc="Never download; use cached datasets only (a missing one fails)."),
    ] = False
    openstates_api_url: Annotated[
        str, AttachOption(desc="Base URL of the Open States API v3; override for a mirror or a test server.")
    ] = DEFAULT_OPENSTATES_API_URL
    openstates_min_interval: Annotated[
        float, AttachOption(desc="Minimum seconds between Open States API requests (free-tier rate limits).")
    ] = DEFAULT_MIN_INTERVAL
    people_archive_url: Annotated[str, AttachOption(desc="URL of the openstates/people repository tar.gz.")] = (
        DEFAULT_PEOPLE_ARCHIVE_URL
    )
    klarner_url: Annotated[
        str, AttachOption(desc="URL of the Klarner election returns tab file (Harvard Dataverse).")
    ] = DEFAULT_KLARNER_URL
    shor_mccarty_url: Annotated[
        str, AttachOption(desc="URL of the Shor-McCarty individual legislator tab file (Harvard Dataverse).")
    ] = DEFAULT_SHOR_MCCARTY_URL
    shor_mccarty_aggregate_url: Annotated[
        str, AttachOption(desc="URL of the Shor-McCarty chamber aggregate tab file (Harvard Dataverse).")
    ] = DEFAULT_SHOR_MCCARTY_AGGREGATE_URL


class UsStatelegsWorker(Worker):
    """The worker process: the ``us_statelegs`` catalog with one ``main`` schema."""

    AttachOptions = AttachOptions

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tables=[STATES_TABLE],
                tags={
                    "vgi.title": "US State Legislatures",
                    "domain": "government",
                    "vgi.keywords": keywords_array(
                        "state legislators, committees, election returns, ideology, bills, votes, open states"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_SCHEMA_EXAMPLES),
                },
            )
        ],
    )

    secret_types = [OPENSTATES_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    UsStatelegsWorker.main()


if __name__ == "__main__":
    main()
