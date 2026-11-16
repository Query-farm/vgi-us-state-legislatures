"""Reference data: the 52 legislatures covered (50 states, DC, Puerto Rico) and their identifiers.

This table is bundled (hand-written public facts: USPS and FIPS codes, Open Civic Data
identifiers, chamber names), so the ``states`` table needs no network.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import ClassVar

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from .meta import CHAMBER_DOC, column, object_tags

# usps, fips, name, legislature body, lower chamber name ("" = unicameral)
_GA = "General Assembly"
_LEG = "Legislature"
_HOUSE = "House of Representatives"
_ROWS: list[tuple[str, str, str, str, str]] = [
    ("AL", "01", "Alabama", _LEG, _HOUSE),
    ("AK", "02", "Alaska", _LEG, _HOUSE),
    ("AZ", "04", "Arizona", _LEG, _HOUSE),
    ("AR", "05", "Arkansas", _GA, _HOUSE),
    ("CA", "06", "California", _LEG, "Assembly"),
    ("CO", "08", "Colorado", _GA, _HOUSE),
    ("CT", "09", "Connecticut", _GA, _HOUSE),
    ("DE", "10", "Delaware", _GA, _HOUSE),
    ("DC", "11", "District of Columbia", "Council", ""),
    ("FL", "12", "Florida", _LEG, _HOUSE),
    ("GA", "13", "Georgia", _GA, _HOUSE),
    ("HI", "15", "Hawaii", _LEG, _HOUSE),
    ("ID", "16", "Idaho", _LEG, _HOUSE),
    ("IL", "17", "Illinois", _GA, _HOUSE),
    ("IN", "18", "Indiana", _GA, _HOUSE),
    ("IA", "19", "Iowa", _GA, _HOUSE),
    ("KS", "20", "Kansas", _LEG, _HOUSE),
    ("KY", "21", "Kentucky", _GA, _HOUSE),
    ("LA", "22", "Louisiana", _LEG, _HOUSE),
    ("ME", "23", "Maine", _LEG, _HOUSE),
    ("MD", "24", "Maryland", _GA, "House of Delegates"),
    ("MA", "25", "Massachusetts", "General Court", _HOUSE),
    ("MI", "26", "Michigan", _LEG, _HOUSE),
    ("MN", "27", "Minnesota", _LEG, _HOUSE),
    ("MS", "28", "Mississippi", _LEG, _HOUSE),
    ("MO", "29", "Missouri", _GA, _HOUSE),
    ("MT", "30", "Montana", _LEG, _HOUSE),
    ("NE", "31", "Nebraska", _LEG, ""),
    ("NV", "32", "Nevada", _LEG, "Assembly"),
    ("NH", "33", "New Hampshire", "General Court", _HOUSE),
    ("NJ", "34", "New Jersey", _LEG, "General Assembly"),
    ("NM", "35", "New Mexico", _LEG, _HOUSE),
    ("NY", "36", "New York", _LEG, "Assembly"),
    ("NC", "37", "North Carolina", _GA, _HOUSE),
    ("ND", "38", "North Dakota", "Legislative Assembly", _HOUSE),
    ("OH", "39", "Ohio", _GA, _HOUSE),
    ("OK", "40", "Oklahoma", _LEG, _HOUSE),
    ("OR", "41", "Oregon", "Legislative Assembly", _HOUSE),
    ("PA", "42", "Pennsylvania", _GA, _HOUSE),
    ("PR", "72", "Puerto Rico", "Legislative Assembly", _HOUSE),
    ("RI", "44", "Rhode Island", _GA, _HOUSE),
    ("SC", "45", "South Carolina", _GA, _HOUSE),
    ("SD", "46", "South Dakota", _LEG, _HOUSE),
    ("TN", "47", "Tennessee", _GA, _HOUSE),
    ("TX", "48", "Texas", _LEG, _HOUSE),
    ("UT", "49", "Utah", _LEG, _HOUSE),
    ("VT", "50", "Vermont", _GA, _HOUSE),
    ("VA", "51", "Virginia", _GA, "House of Delegates"),
    ("WA", "53", "Washington", _LEG, _HOUSE),
    ("WV", "54", "West Virginia", _LEG, "House of Delegates"),
    ("WI", "55", "Wisconsin", _LEG, "Assembly"),
    ("WY", "56", "Wyoming", _LEG, _HOUSE),
]

STATES: dict[str, dict[str, str]] = {r[0]: {"usps": r[0], "fips": r[1], "name": r[2]} for r in _ROWS}
FIPS_TO_USPS = {r[1]: r[0] for r in _ROWS}


def division_kind(usps: str) -> str:
    """Return the OCD division kind for a USPS code: 'state', 'district' (DC) or 'territory' (PR)."""
    return {"DC": "district", "PR": "territory"}.get(usps, "state")


def jurisdiction_id(usps: str) -> str:
    """Return the Open States / OCD jurisdiction id for a USPS code."""
    return f"ocd-jurisdiction/country:us/{division_kind(usps)}:{usps.lower()}/government"


def usps_from_jurisdiction(jid: str | None) -> str | None:
    """Return the USPS code from an ``ocd-jurisdiction/...`` id, or NULL if it is not one we cover."""
    if not jid:
        return None
    for part in jid.split("/"):
        kind, _, code = part.partition(":")
        if kind in ("state", "district", "territory") and code.upper() in STATES:
            return code.upper()
    return None


def is_unicameral(usps: str) -> bool:
    """Nebraska's Legislature and the DC Council are single-chamber."""
    return usps in ("NE", "DC")


def normalize_state(value: str | None) -> str | None:
    """Accept a USPS code (any case) or a two-digit FIPS code; return the upper-case USPS code.

    Args:
        value: The caller's state argument, or NULL.

    Returns:
        The USPS code, or NULL for NULL input.

    Raises:
        ValueError: ``value`` is not one of the 52 legislatures covered.

    """
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in STATES:
        return text
    if text.zfill(2) in FIPS_TO_USPS:
        return FIPS_TO_USPS[text.zfill(2)]
    raise ValueError(f"unknown state {value!r}: use a USPS code such as 'NE' (50 states, DC and PR)")


STATE_ARG_DOC = "Only this state: USPS code such as 'NE' (any case) or two-digit FIPS. NULL = all."

_SCHEMA = pa.schema(
    [
        column("state", pa.string(), "USPS code, upper case ('NE').", nullable=False),
        column("state_fips", pa.string(), "Two-digit state FIPS code, with leading zero ('31').", nullable=False),
        column("name", pa.string(), "State, district or territory name.", nullable=False),
        column(
            "jurisdiction_id",
            pa.string(),
            "Open States / Open Civic Data jurisdiction id (ocd-jurisdiction/country:us/state:ne/government).",
            nullable=False,
        ),
        column("division_id", pa.string(), "OCD division id (ocd-division/country:us/state:ne).", nullable=False),
        column(
            "legislature_name",
            pa.string(),
            "Official name of the legislative body ('Nebraska Legislature', 'Council of the District of Columbia').",
            nullable=False,
        ),
        column(
            "unicameral", pa.bool_(), "True for single-chamber bodies (Nebraska and the DC Council).", nullable=False
        ),
        column(
            "chambers",
            pa.list_(pa.string()),
            "Chamber codes used throughout this catalog: ['upper','lower'] or ['legislature'].",
            nullable=False,
        ),
        column("upper_chamber_name", pa.string(), "Name of the upper chamber ('Senate'); NULL when unicameral."),
        column(
            "lower_chamber_name",
            pa.string(),
            "Name of the lower chamber ('House of Representatives', 'Assembly', 'House of Delegates'); "
            "NULL when unicameral.",
        ),
    ]
)


def _legislature_name(usps: str, name: str, body: str) -> str:
    if usps == "DC":
        return "Council of the District of Columbia"
    if usps == "PR":
        return "Legislative Assembly of Puerto Rico"
    return f"{name} {body}"


def states_table() -> pa.Table:
    """Build the reference table."""
    rows = []
    for usps, fips, name, body, lower in _ROWS:
        uni = is_unicameral(usps)
        rows.append(
            {
                "state": usps,
                "state_fips": fips,
                "name": name,
                "jurisdiction_id": jurisdiction_id(usps),
                "division_id": f"ocd-division/country:us/{division_kind(usps)}:{usps.lower()}",
                "legislature_name": _legislature_name(usps, name, body),
                "unicameral": uni,
                "chambers": ["legislature"] if uni else ["upper", "lower"],
                "upper_chamber_name": None if uni else "Senate",
                "lower_chamber_name": None if uni else lower,
            }
        )
    return pa.Table.from_pylist(rows, schema=_SCHEMA)


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """The ``states`` scan takes no arguments."""


_STATES_EXAMPLES = [
    {
        "description": "Identifiers for Nebraska and North Carolina",
        "sql": "SELECT state, state_fips, jurisdiction_id FROM us_statelegs.main.states "
        "WHERE state IN ('NE', 'NC') ORDER BY state",
    },
    {
        "description": "How many legislatures use each name for their lower chamber",
        "sql": "SELECT lower_chamber_name, count(*) AS states FROM us_statelegs.main.states "
        "WHERE NOT unicameral GROUP BY ALL ORDER BY states DESC",
    },
]

STATES_TAGS = {
    **object_tags(
        title="State Legislatures Reference",
        category="reference",
        keywords="states, usps, fips, jurisdiction id, ocd, unicameral, nebraska, chamber names, dc, puerto rico",
        doc_llm=(
            "One row per legislature covered by this catalog: the 50 states, the District of Columbia and "
            "Puerto Rico (52 rows). Columns: state (USPS code), state_fips (two-digit `VARCHAR`), name, "
            "jurisdiction_id (the Open States ocd-jurisdiction id used by every os_* function), division_id, "
            "legislature_name, unicameral, chambers (list of chamber codes), upper_chamber_name, "
            "lower_chamber_name. Bundled, so it needs no network. Use it to map FIPS codes to USPS codes or "
            "to label chambers. " + CHAMBER_DOC
        ),
        doc_md=(
            "## states\n\n"
            "The 52 legislatures covered (50 states, DC, Puerto Rico) with USPS and FIPS codes, Open Civic "
            "Data ids and chamber names.\n\nNebraska's Legislature and the DC Council are unicameral: their "
            "only chamber is coded `legislature`."
        ),
    ),
    "domain": "government",
    "vgi.example_queries": json.dumps(_STATES_EXAMPLES),
    "vgi.executable_examples": json.dumps(
        [
            {
                "description": "The unicameral legislatures",
                "sql": "SELECT state, legislature_name FROM us_statelegs.main.states WHERE unicameral ORDER BY state",
                "expected_result": [["DC", "Council of the District of Columbia"], ["NE", "Nebraska Legislature"]],
            },
            {
                "description": "How the lower chambers are named",
                "sql": "SELECT lower_chamber_name, count(*) AS n FROM us_statelegs.main.states "
                "WHERE NOT unicameral GROUP BY ALL ORDER BY n DESC, lower_chamber_name",
                "expected_result": [
                    ["House of Representatives", 42],
                    ["Assembly", 4],
                    ["House of Delegates", 3],
                    ["General Assembly", 1],
                ],
            },
        ]
    ),
}


@bind_fixed_schema
@init_single_worker
class States(TableFunctionGenerator[NoArgs, None]):
    """Rows of the ``states`` table: the 52 legislatures with their identifiers and chamber names."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = _SCHEMA

    class Meta:
        """Scan function of the ``states`` table (not callable from SQL)."""

        name = "states_scan"
        description = "Rows of the states reference table"

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the bundled table."""
        out.emit(states_table().combine_chunks().to_batches()[0])
        out.finish()


STATES_TABLE = Table(
    name="states",
    function=States,
    comment="The 50 state legislatures plus DC and Puerto Rico, with USPS, FIPS and Open States identifiers.",
    tags=STATES_TAGS,
    constraints=(
        *(
            constraint_record(
                ["main"],
                "states",
                f"{f.name}_not_null",
                "NOT_NULL",
                [[f.name]],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
            for f in _SCHEMA
            if not f.nullable
        ),
        constraint_record(
            ["main"], "states", "pk", "PRIMARY_KEY", [["state"]], enforcement="ENFORCED", validation="ALL_ROWS"
        ),
    ),
    column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in _SCHEMA},
)
