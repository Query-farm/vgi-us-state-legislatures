"""Discovery metadata for the ``us_statelegs`` catalog.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these tags.
Catalog-level tags live in ``CATALOG_TAGS``; every function carries
``object_tags(...)`` with a category from ``SCHEMA_CATEGORIES``.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "us_statelegs"

SOURCE_URL = "https://github.com/Query-farm/vgi-us-state-legislatures"

OPENSTATES_DOCS = "https://docs.openstates.org/api-v3/"
OPENSTATES_SIGNUP = "https://open.pluralpolicy.com/accounts/signup/"
PEOPLE_REPO = "https://github.com/openstates/people"
KLARNER_DOI = "https://doi.org/10.7910/DVN/3WZFK9"
SHOR_MCCARTY_DOI = "https://doi.org/10.7910/DVN/SGOQ7G"
SHOR_MCCARTY_AGG_DOI = "https://doi.org/10.7910/DVN/T53FFK"

SCHEMA_CATEGORIES = [
    {
        "name": "reference",
        "title": "Reference",
        "description": "The states, districts and territories with legislatures, and their identifiers.",
    },
    {
        "name": "legislators",
        "title": "Legislators and Committees",
        "description": "Current and former state legislators, their terms, and committee rosters (keyless).",
    },
    {
        "name": "elections",
        "title": "Elections and Ideology",
        "description": "Historical state legislative election returns and legislator ideology scores.",
    },
    {
        "name": "openstates_api",
        "title": "Open States API",
        "description": "Live bills, votes, people, committees and events from the Open States API v3 (API key).",
    },
    {
        "name": "cache",
        "title": "Cache and Provenance",
        "description": "The local cache of downloaded datasets: vintage, size and refresh.",
    },
]

CHAMBER_DOC = (
    "`chamber` is 'upper' (state senate), 'lower' (house, assembly or house of delegates) or "
    "'legislature' for the unicameral Nebraska Legislature and the Council of the District of Columbia."
)

API_KEY_NOTE = (
    "Needs an Open States API key stored in a DuckDB secret of type `openstates` (field `API_KEY`); "
    f"keys are free at {OPENSTATES_SIGNUP}. Without the secret the call fails naming the secret type."
)

API_KEY_MD = (
    "Needs a free Open States API key "
    f"([register]({OPENSTATES_SIGNUP})) stored as a DuckDB secret:\n\n"
    "```sql\nCREATE SECRET (TYPE openstates, API_KEY 'your-key');\n```"
)


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def duckdb_type(t: pa.DataType) -> str:
    """Spell an Arrow type the way DuckDB prints it."""
    simple = {
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float64(): "DOUBLE",
        pa.string(): "VARCHAR",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
    }
    if t in simple:
        return simple[t]
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        inner = ", ".join(f"{t.field(i).name} {duckdb_type(t.field(i).type)}" for i in range(t.num_fields))
        return f"STRUCT({inner})"
    raise ValueError(f"no DuckDB spelling for {t}")


def _short(text: str) -> str:
    """Return the first clause of a column comment (agent tools bound tag size)."""
    for mark in (" (", "; "):
        cut = text.find(mark)
        if cut > 20:
            text = text[:cut]
    return text.rstrip(".") + "."


def result_columns(schema: pa.Schema) -> str:
    """Build the ``vgi.result_columns_schema`` JSON for a table function's output schema."""
    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type(f.type),
                "description": _short((f.metadata or {}).get(b"comment", b"").decode()),
            }
            for f in schema
        ]
    )


CATALOG_COMMENT = (
    "US state legislatures in SQL: current and former state legislators, their terms and committee "
    "rosters from the Open States people data, state legislative election returns since 1967, "
    "Shor-McCarty legislator ideology scores, and live bills, votes and events from the Open States API."
)

CATALOG_DOC_LLM = (
    "Data about the legislatures of the 50 US states, the District of Columbia and Puerto Rico. Keyless "
    "sources (downloaded on first use and cached): the Open States people repository (CC0) gives every "
    "current and former state legislator with Open States ocd-person ids, their terms (chamber, district, "
    "dates), party history and current committee memberships; Carl Klarner's State Legislative Election "
    "Returns (CC0) give candidate-level general and primary election results 1967-2016; the Shor-McCarty "
    "data (CC BY-NC-SA) give each legislator's NP-score ideology and chamber medians 1993-2022. With an "
    "Open States API key (secret type `openstates`) the os_* functions read live bills, roll-call votes, "
    "people, committees, events and legislative sessions. Join on state (USPS code, upper case), "
    "state_fips (two-digit `VARCHAR`), ocd-person ids and chamber ('upper', 'lower', or 'legislature' for "
    "unicameral Nebraska and DC). Districts are `VARCHAR` because many states name them ('Chittenden-1')."
)

CATALOG_DOC_MD = (
    "# US State Legislatures\n\n"
    "**Legislators, committees, elections, ideology, bills and votes for the 50 US states, DC and "
    "Puerto Rico, queryable from DuckDB.**\n\n"
    "Most of the catalog works without any account: the "
    f"[Open States people data]({PEOPLE_REPO}) (CC0), Carl Klarner's "
    f"[State Legislative Election Returns]({KLARNER_DOI}) (CC0) and the "
    f"[Shor-McCarty ideology data]({SHOR_MCCARTY_DOI}) (CC BY-NC-SA 4.0) are downloaded on first use "
    "into a per-user cache and reused. Live bills, roll-call votes and events come from the "
    f"[Open States API v3]({OPENSTATES_DOCS}) and need a free API key stored as a DuckDB secret:\n\n"
    "```sql\nCREATE SECRET (TYPE openstates, API_KEY 'your-key');\n```\n\n"
    "Identifiers follow Open States (`ocd-person/...`, `ocd-jurisdiction/...`), states are USPS codes "
    "with two-digit FIPS codes, and `chamber` is `upper`, `lower` or `legislature` (Nebraska's "
    "unicameral Legislature and the DC Council)."
)

SCHEMA_COMMENT = (
    "State legislators, terms, committees, election returns, ideology scores, and Open States API "
    "bills, votes and events."
)

SCHEMA_DOC_LLM = (
    "The `main` schema of the US state legislatures catalog. Keyless functions read cached open "
    "datasets and accept an optional `state` (USPS code) argument that limits what is parsed; a "
    "`WHERE state = 'XX'` filter is pushed down the same way. Functions prefixed `os_` call the "
    "Open States API v3 live and need a `openstates` secret holding an API key; their filters "
    "(state, session, chamber, classification, updated_since) are sent to the API. " + CHAMBER_DOC
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Everything about the 52 legislatures lives in this one schema. Keyless functions read cached open "
    "datasets; `os_*` functions call the Open States API and need an `openstates` secret.\n\n"
    "- **Reference**: the legislatures and their identifiers.\n"
    "- **Legislators and committees** from the Open States people data (keyless, CC0).\n"
    "- **Elections and ideology**: Klarner election returns 1967-2016 and Shor-McCarty scores.\n"
    "- **Open States API** (`os_*`): live bills, votes, people, committees, events; needs a key.\n"
    "- **Cache**: what has been downloaded, and a refresh."
)

AGENT_TEST_TASKS = [
    {
        "name": "unicameral_legislatures",
        "prompt": "Using the us_statelegs worker, which state-level legislatures are unicameral? "
        "Give their USPS codes in alphabetical order.",
    },
    {
        "name": "fips_for_state",
        "prompt": "Using the us_statelegs worker, what is the two-digit FIPS code of the state whose "
        "jurisdiction id is ocd-jurisdiction/country:us/state:ne/government?",
    },
    {
        "name": "lower_chamber_names",
        "prompt": "Using the us_statelegs worker, how many states call their lower chamber the House of Delegates?",
    },
    {
        "name": "nebraska_current_senators",
        "prompt": "Using the us_statelegs worker, how many people currently serve in the Nebraska "
        "Legislature according to the Open States people data?",
    },
    {
        "name": "both_chambers_ohio",
        "prompt": "Using the us_statelegs worker, how many Ohio legislators in the Open States people data have "
        "held seats in both the Ohio House and the Ohio Senate?",
    },
    {
        "name": "nebraska_agriculture_chair",
        "prompt": "Using the us_statelegs worker, who chairs the Nebraska Legislature's Agriculture committee, "
        "and how many members does that committee have?",
    },
    {
        "name": "vermont_addison_2016",
        "prompt": "Using the us_statelegs worker, which candidate received the most votes in the 2016 general "
        "election for the Vermont Senate's Addison district?",
    },
    {
        "name": "nebraska_ideology_2010",
        "prompt": "Using the us_statelegs worker, how many distinct legislators with a Shor-McCarty ideology "
        "score served in the Nebraska Legislature in 2010?",
    },
    {
        "name": "vermont_house_polarization_2010",
        "prompt": "Using the us_statelegs worker, what was the distance between the party medians (the "
        "Shor-McCarty polarization measure) in the Vermont House in 2010?",
    },
    {
        "name": "klarner_licence",
        "prompt": "Using the us_statelegs worker, under which licence is the Klarner state legislative "
        "election returns dataset distributed?",
    },
    {
        "name": "refresh_people_data",
        "prompt": "Using the us_statelegs worker, force a fresh download of the Open States people data and "
        "report whether it succeeded.",
    },
    {
        "name": "nebraska_sessions_api",
        "prompt": "Using the us_statelegs worker's Open States API functions (an openstates secret is "
        "configured), list Nebraska's legislative session identifiers and when Open States last updated "
        "Nebraska's bills.",
    },
    {
        "name": "nebraska_district_13_api",
        "prompt": "Using the us_statelegs worker's Open States API functions (an openstates secret is "
        "configured), who currently represents Nebraska legislative district 13?",
    },
    {
        "name": "nebraska_bills_votes_api",
        "prompt": "Using the us_statelegs worker's Open States API functions (an openstates secret is "
        "configured), how many roll-call votes were recorded on Nebraska bills of session 109 that were "
        "updated since 2026-01-01, and how many of those bills are there?",
    },
    {
        "name": "nebraska_committees_events_api",
        "prompt": "Using the us_statelegs worker's Open States API functions (an openstates secret is "
        "configured), list Nebraska's committees with their roster sizes, and count Nebraska's legislative "
        "events scheduled after 2026-01-01.",
    },
]

CATALOG_TAGS = {
    "vgi.title": "US State Legislatures",
    "vgi.subtitle": "State legislators, committees, election returns, ideology scores, bills and roll-call votes.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "state legislature, state legislators, open states, bills, roll call votes, committees, "
        "state senate, state house, election returns, ideology, shor mccarty, klarner"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
