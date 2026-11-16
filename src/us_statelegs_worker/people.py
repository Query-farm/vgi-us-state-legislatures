"""Keyless legislators, terms and committees from the Open States people repository (CC0).

The repository (https://github.com/openstates/people) is one YAML file per person and per
committee. Its tarball (about 5 MB) is downloaded on first use, cached, and re-checked once
it is ``MAX_AGE_DAYS`` old. Files under ``data/<state>/legislature/`` are sitting members;
``data/<state>/retired/`` holds former officials (only those with a legislative role are
returned); ``data/<state>/committees/`` holds current committees and their rosters.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import shutil
import tarfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, ClassVar, TypeVar

import pyarrow as pa
import yaml
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .common import Settings, clean_str, ensure_dataset, to_date
from .datasets import PEOPLE, PerStateSource
from .meta import CHAMBER_DOC, PEOPLE_REPO, column, object_tags, result_columns
from .reference import STATE_ARG_DOC, STATES, jurisdiction_id

LEGISLATIVE_TYPES = ("upper", "lower", "legislature")
_LOADER: Any = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

_lock = threading.Lock()
# (archive path, mtime) -> state -> parsed {"people": [(doc, current_dir)], "committees": [doc]}
_memo: dict[tuple[str, float], dict[str, dict[str, list[Any]]]] = {}
# (archive path, mtime) -> state -> [(kind, offset, size)] in the uncompressed tar
_index_memo: dict[tuple[str, float], dict[str, list[tuple[str, int, int]]]] = {}
_KINDS = ("legislature", "retired", "committees")


def _index(path: Path) -> tuple[Path, dict[str, list[tuple[str, int, int]]]]:
    """Decompress the cached tarball once next to it and index each state's YAML members.

    Reading one state then seeks straight to its files instead of decompressing the
    whole 50 MB archive again, so the first batch of any scan arrives quickly.
    """
    key = (str(path), path.stat().st_mtime)
    plain = path.with_name(path.name.removesuffix(".gz"))
    index_file = path.with_name(path.name + ".index.json")
    if key in _index_memo and plain.is_file():
        return plain, _index_memo[key]
    stamp = f"{path.stat().st_size}:{path.stat().st_mtime}"
    try:
        stored = json.loads(index_file.read_text())
        if stored.get("stamp") == stamp and plain.is_file():
            index = {k: [tuple(e) for e in v] for k, v in stored["states"].items()}
            _index_memo.clear()
            _index_memo[key] = index
            return plain, _index_memo[key]
    except OSError, ValueError, KeyError:
        pass
    tmp = plain.with_name(f"{plain.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with gzip.open(path, "rb") as src, open(tmp, "wb") as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    built: dict[str, list[tuple[str, int, int]]] = {}
    with tarfile.open(tmp, mode="r:") as archive:
        for member in archive:
            parts = member.name.split("/")
            if len(parts) != 5 or parts[1] != "data" or not member.isfile() or not parts[4].endswith(".yml"):
                continue
            usps, kind = parts[2].upper(), parts[3]
            if usps in STATES and kind in _KINDS:
                built.setdefault(usps, []).append((kind, member.offset_data, member.size))
    tmp.replace(plain)
    shutil.rmtree(path.with_name(f"{path.name}.json.d"), ignore_errors=True)  # parsed copies of the old archive
    side = index_file.with_name(f"{index_file.name}.{os.getpid()}.tmp")
    side.write_text(json.dumps({"stamp": stamp, "states": built}))
    side.replace(index_file)
    _index_memo.clear()
    _index_memo[key] = built
    return plain, built


def _parse_states(path: Path, states: set[str]) -> dict[str, dict[str, list[Any]]]:
    """Parse the YAML of ``states`` from the cached archive (memoized per archive version)."""
    key = (str(path), path.stat().st_mtime)
    with _lock:
        cache = _memo.setdefault(key, {})
        for stale in [k for k in _memo if k != key]:
            del _memo[stale]
        missing = {s for s in states if s not in cache}
        if missing:
            plain, index = _index(path)
            json_dir = path.with_name(f"{path.name}.json.d")
            stamp = f"{path.stat().st_size}-{int(path.stat().st_mtime)}"
            with open(plain, "rb") as handle:
                for usps in sorted(missing):
                    cache[usps] = _load_state(handle, index.get(usps, []), json_dir / f"{stamp}-{usps}.json")
        return {s: cache[s] for s in states}


def _load_state(handle: Any, members: list[tuple[str, int, int]], json_path: Path) -> dict[str, list[Any]]:
    """Read one state's documents: from the JSON cache if present, else parse the YAML and write it."""
    try:
        stored = json.loads(json_path.read_text())
        return {"people": [(d, bool(c)) for d, c in stored["people"]], "committees": stored["committees"]}
    except OSError, ValueError, KeyError:
        pass
    parsed: dict[str, list[Any]] = {"people": [], "committees": []}
    for kind, offset, size in members:
        handle.seek(offset)
        doc = yaml.load(handle.read(size), Loader=_LOADER)
        if not isinstance(doc, dict):
            continue
        if kind == "committees":
            parsed["committees"].append(doc)
        else:
            parsed["people"].append((doc, kind == "legislature"))
    try:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = json_path.with_name(f"{json_path.name}.{os.getpid()}.tmp")
        # YAML dates become ISO strings; the row mappers accept both.
        tmp.write_text(json.dumps(parsed, default=str))
        tmp.replace(json_path)
    except OSError:
        pass
    return parsed


def _people_for(settings: Settings, state: str) -> dict[str, list[Any]]:
    path = ensure_dataset(settings, PEOPLE).path
    return _parse_states(path, {state})[state]


_A = TypeVar("_A")


class _PeopleSource(PerStateSource[_A]):
    """Parse every requested state's YAML in one pass over the archive."""

    @classmethod
    def prefetch(cls, settings: Settings, states: list[str]) -> None:
        """Build the archive index once (states are then read one at a time by offset)."""
        path = ensure_dataset(settings, PEOPLE).path
        with _lock:
            _index(path)


def _date(value: Any) -> dt.date | None:
    return to_date(value)


def _legislative_roles(doc: dict[str, Any], state: str) -> list[dict[str, Any]]:
    jid = jurisdiction_id(state)
    roles = []
    for role in doc.get("roles") or []:
        if not isinstance(role, dict) or role.get("type") not in LEGISLATIVE_TYPES:
            continue
        if role.get("jurisdiction") not in (None, jid):
            continue
        roles.append(role)
    return roles


def _role_sort_key(role: dict[str, Any]) -> tuple[int, dt.date]:
    """Order roles oldest first; an open-ended role sorts last."""
    end = _date(role.get("end_date"))
    start = _date(role.get("start_date")) or dt.date.min
    return (1 if end is None else 0, end or start)


def _serving(role: dict[str, Any]) -> bool:
    """Return True while a role is served: no end date, or one in the future (terms end in January)."""
    end = _date(role.get("end_date"))
    return end is None or end >= dt.datetime.now(dt.UTC).date()


def _current_party(doc: dict[str, Any]) -> str | None:
    parties = [p for p in doc.get("party") or [] if isinstance(p, dict) and p.get("name")]
    if not parties:
        return None
    open_ended = [p for p in parties if not p.get("end_date")]
    chosen = open_ended[-1] if open_ended else max(parties, key=lambda p: str(p.get("end_date")))
    return clean_str(chosen.get("name"))


_LINK = pa.list_(pa.string())
_PARTY = pa.list_(pa.struct([("party", pa.string()), ("start_date", pa.date32()), ("end_date", pa.date32())]))
_IDENT = pa.list_(pa.struct([("scheme", pa.string()), ("identifier", pa.string())]))
_OFFICE = pa.list_(
    pa.struct(
        [
            ("classification", pa.string()),
            ("address", pa.string()),
            ("voice", pa.string()),
            ("fax", pa.string()),
        ]
    )
)

LEGISLATOR_SCHEMA = pa.schema(
    [
        column("person_id", pa.string(), "Open States person id (ocd-person/<uuid>); stable join key.", nullable=False),
        column("name", pa.string(), "Full name as Open States records it.", nullable=False),
        column("given_name", pa.string(), "Given (first) name."),
        column("family_name", pa.string(), "Family (last) name."),
        column("state", pa.string(), "USPS code of the legislature, upper case.", nullable=False),
        column(
            "current",
            pa.bool_(),
            "True for sitting members (listed under legislature/ with a role not yet ended).",
            nullable=False,
        ),
        column(
            "chamber",
            pa.string(),
            "Chamber of the most recent legislative role: 'upper', 'lower' or 'legislature' (NE, DC).",
        ),
        column(
            "district", pa.string(), "District of the most recent legislative role, as text ('13', 'Chittenden-1')."
        ),
        column(
            "party", pa.string(), "Current (or last recorded) party, e.g. 'Democratic', 'Republican', 'Nonpartisan'."
        ),
        column(
            "first_role_start",
            pa.date32(),
            "Earliest recorded legislative role start date (often missing for old terms).",
        ),
        column(
            "last_role_end",
            pa.date32(),
            "End of the most recent legislative role (may be the scheduled end of the current term).",
        ),
        column("roles", pa.int32(), "Number of legislative roles (terms or seat changes) recorded.", nullable=False),
        column("gender", pa.string(), "Gender as recorded by Open States ('Female', 'Male', ...)."),
        column("birth_date", pa.date32(), "Date of birth, when recorded with full precision."),
        column("death_date", pa.date32(), "Date of death, when recorded."),
        column("email", pa.string(), "Official email address."),
        column("image", pa.string(), "URL of the official photo."),
        column("twitter", pa.string(), "Official X/Twitter handle."),
        column("facebook", pa.string(), "Official Facebook account."),
        column("instagram", pa.string(), "Official Instagram account."),
        column("party_history", _PARTY, "Every party affiliation recorded, with dates when known."),
        column("other_identifiers", _IDENT, "Other ids, e.g. scheme 'legacy_openstates' (NEL000045) or 'votesmart'."),
        column("offices", _OFFICE, "Capitol and district offices: classification, address, phone, fax."),
        column("links", _LINK, "Official and campaign web pages."),
        column("sources", _LINK, "Pages the record was compiled from."),
    ]
)

ROLE_SCHEMA = pa.schema(
    [
        column("person_id", pa.string(), "Open States person id (ocd-person/<uuid>).", nullable=False),
        column("name", pa.string(), "Full name of the legislator.", nullable=False),
        column("state", pa.string(), "USPS code of the legislature, upper case.", nullable=False),
        column("chamber", pa.string(), "'upper', 'lower' or 'legislature' (NE, DC).", nullable=False),
        column("district", pa.string(), "District name or number, as text.", nullable=False),
        column("start_date", pa.date32(), "Role start date; often NULL for terms recorded before 2018."),
        column(
            "end_date",
            pa.date32(),
            "Role end date (scheduled end of term for many sitting members); NULL if open-ended.",
        ),
        column("end_reason", pa.string(), "Why the role ended early, when recorded (e.g. resignation, death)."),
        column(
            "current",
            pa.bool_(),
            "True for a sitting member's role that has not ended (no end date, or one in the future).",
            nullable=False,
        ),
        column("party", pa.string(), "The person's current or last party (not party at the time of the role)."),
        column("jurisdiction_id", pa.string(), "Open States jurisdiction id.", nullable=False),
    ]
)

COMMITTEE_SCHEMA = pa.schema(
    [
        column("committee_id", pa.string(), "Open States organization id (ocd-organization/<uuid>).", nullable=False),
        column("state", pa.string(), "USPS code of the legislature, upper case.", nullable=False),
        column(
            "chamber", pa.string(), "'upper', 'lower' or 'legislature' (joint committees and NE/DC).", nullable=False
        ),
        column("name", pa.string(), "Committee name.", nullable=False),
        column("classification", pa.string(), "'committee' or 'subcommittee'.", nullable=False),
        column("parent_id", pa.string(), "Parent committee id, for subcommittees."),
        column("members", pa.int32(), "Number of members on the roster.", nullable=False),
        column("chair", pa.string(), "Name of the member whose role is 'chair' (or 'co-chair'), if any."),
        column("links", _LINK, "Committee web pages."),
        column("sources", _LINK, "Pages the roster was compiled from."),
        column("jurisdiction_id", pa.string(), "Open States jurisdiction id.", nullable=False),
    ]
)

MEMBER_SCHEMA = pa.schema(
    [
        column("committee_id", pa.string(), "Open States organization id of the committee.", nullable=False),
        column("committee_name", pa.string(), "Committee name.", nullable=False),
        column("state", pa.string(), "USPS code of the legislature, upper case.", nullable=False),
        column("chamber", pa.string(), "Committee chamber: 'upper', 'lower' or 'legislature'.", nullable=False),
        column("person_id", pa.string(), "Open States person id of the member, when matched (join to legislators)."),
        column("member_name", pa.string(), "Member name as listed on the roster.", nullable=False),
        column(
            "role",
            pa.string(),
            "Role on the committee: 'member', 'chair', 'vice chair', 'ranking member', ...",
            nullable=False,
        ),
    ]
)


def _strs(items: Any, key: str = "url") -> list[str]:
    return [str(i[key]) for i in items or [] if isinstance(i, dict) and i.get(key)]


def legislator_rows(docs: list[tuple[dict[str, Any], bool]], state: str) -> list[dict[str, Any]]:
    """Map parsed person YAML documents to ``legislators`` rows (non-legislators dropped)."""
    rows = []
    for doc, in_legislature_dir in docs:
        roles = sorted(_legislative_roles(doc, state), key=_role_sort_key)
        if not roles:
            continue
        latest = roles[-1]
        ids = doc.get("ids") or {}
        starts = [d for d in (_date(r.get("start_date")) for r in roles) if d]
        rows.append(
            {
                "person_id": doc.get("id"),
                "name": clean_str(doc.get("name")),
                "given_name": clean_str(doc.get("given_name")),
                "family_name": clean_str(doc.get("family_name")),
                "state": state,
                "current": in_legislature_dir and _serving(latest),
                "chamber": latest.get("type"),
                "district": clean_str(latest.get("district")),
                "party": _current_party(doc),
                "first_role_start": min(starts) if starts else None,
                "last_role_end": _date(latest.get("end_date")),
                "roles": len(roles),
                "gender": clean_str(doc.get("gender")),
                "birth_date": _date(doc.get("birth_date")),
                "death_date": _date(doc.get("death_date")),
                "email": clean_str(doc.get("email")),
                "image": clean_str(doc.get("image")),
                "twitter": clean_str(ids.get("twitter")),
                "facebook": clean_str(ids.get("facebook")),
                "instagram": clean_str(ids.get("instagram")),
                "party_history": [
                    {
                        "party": clean_str(p.get("name")),
                        "start_date": _date(p.get("start_date")),
                        "end_date": _date(p.get("end_date")),
                    }
                    for p in doc.get("party") or []
                    if isinstance(p, dict)
                ],
                "other_identifiers": [
                    {"scheme": clean_str(i.get("scheme")), "identifier": clean_str(i.get("identifier"))}
                    for i in doc.get("other_identifiers") or []
                    if isinstance(i, dict)
                ],
                "offices": [
                    {k: clean_str(o.get(k)) for k in ("classification", "address", "voice", "fax")}
                    for o in doc.get("offices") or []
                    if isinstance(o, dict)
                ],
                "links": _strs(doc.get("links")),
                "sources": _strs(doc.get("sources")),
            }
        )
    return rows


def role_rows(docs: list[tuple[dict[str, Any], bool]], state: str) -> list[dict[str, Any]]:
    """Map parsed person YAML documents to one ``legislator_roles`` row per legislative role."""
    rows = []
    jid = jurisdiction_id(state)
    for doc, in_legislature_dir in docs:
        party = _current_party(doc)
        for role in sorted(_legislative_roles(doc, state), key=_role_sort_key):
            rows.append(
                {
                    "person_id": doc.get("id"),
                    "name": clean_str(doc.get("name")),
                    "state": state,
                    "chamber": role.get("type"),
                    "district": clean_str(role.get("district")) or "",
                    "start_date": _date(role.get("start_date")),
                    "end_date": _date(role.get("end_date")),
                    "end_reason": clean_str(role.get("end_reason")),
                    "current": in_legislature_dir and _serving(role),
                    "party": party,
                    "jurisdiction_id": jid,
                }
            )
    return rows


def _chair(members: list[Any]) -> str | None:
    for wanted in ("chair", "co-chair", "chairman", "chairperson"):
        for m in members:
            if isinstance(m, dict) and str(m.get("role", "")).strip().lower() == wanted:
                return clean_str(m.get("name"))
    return None


def committee_rows(docs: list[dict[str, Any]], state: str) -> list[dict[str, Any]]:
    """Map parsed committee YAML documents to ``committees`` rows."""
    rows = []
    for doc in docs:
        members = [m for m in doc.get("members") or [] if isinstance(m, dict)]
        rows.append(
            {
                "committee_id": doc.get("id"),
                "state": state,
                "chamber": doc.get("chamber") or "legislature",
                "name": clean_str(doc.get("name")),
                "classification": doc.get("classification") or "committee",
                "parent_id": clean_str(doc.get("parent")),
                "members": len(members),
                "chair": _chair(members),
                "links": _strs(doc.get("links")),
                "sources": _strs(doc.get("sources")),
                "jurisdiction_id": doc.get("jurisdiction") or jurisdiction_id(state),
            }
        )
    return rows


def member_rows(docs: list[dict[str, Any]], state: str) -> list[dict[str, Any]]:
    """Map parsed committee YAML documents to one ``committee_members`` row per roster entry."""
    rows = []
    for doc in docs:
        for m in doc.get("members") or []:
            if not isinstance(m, dict) or not m.get("name"):
                continue
            rows.append(
                {
                    "committee_id": doc.get("id"),
                    "committee_name": clean_str(doc.get("name")),
                    "state": state,
                    "chamber": doc.get("chamber") or "legislature",
                    "person_id": clean_str(m.get("person_id") or m.get("id")),
                    "member_name": clean_str(m.get("name")),
                    "role": clean_str(m.get("role")) or "member",
                }
            )
    return rows


@dataclass(slots=True, frozen=True, kw_only=True)
class StateArgs:
    """``state := 'NE'`` (optional)."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]


@dataclass(slots=True, frozen=True, kw_only=True)
class LegislatorArgs:
    """``state := 'NE'``, ``current := true`` (both optional)."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]
    current: Annotated[
        bool | None,
        Arg("current", default=None, doc="true = sitting members only; false = former members only; NULL = both."),
    ]


_PUSHDOWN_DOC = "`state` (argument or `WHERE state = ...`/`IN (...)`) limits which state files are parsed."
_PROVENANCE = (
    f"Source: [Open States people repository]({PEOPLE_REPO}) (CC0 1.0), downloaded on first use "
    "(about 5 MB) and re-checked weekly."
)


@bind_fixed_schema
@init_single_worker
class Legislators(_PeopleSource[LegislatorArgs]):
    """One row per current or former state legislator recorded by Open States."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LEGISLATOR_SCHEMA

    class Meta:
        """Function metadata."""

        name = "legislators"
        description = "Current and former state legislators from the Open States people data (keyless, CC0)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="State Legislators",
                category="legislators",
                keywords="legislators, state senators, state representatives, members, ocd-person, party, "
                "district, current members, former members",
                doc_llm=(
                    "One row per person who has held a seat in a state legislature (50 states, DC, PR) "
                    "according to the Open States people repository: person_id (ocd-person id), name, state, "
                    "current (sitting member), chamber and district of the most recent seat, party, first/last "
                    "role dates, contact details, social handles, party history and other identifiers "
                    "(including legacy Open States ids). Arguments: `state := 'NE'` and `current := true`. "
                    "Former members are mostly those tracked since about 2010. No API key needed. " + CHAMBER_DOC
                ),
                doc_md=(
                    "## legislators(state := NULL, current := NULL)\n\n"
                    "Current and former state legislators, one row per person. " + _PUSHDOWN_DOC + "\n\n" + _PROVENANCE
                ),
            ),
            "vgi.result_columns_schema": result_columns(LEGISLATOR_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT chamber, party, count(*) AS members FROM us_statelegs.main.legislators("
                "state := 'NC', current := true) GROUP BY ALL ORDER BY chamber, members DESC",
                description="Party balance of the sitting North Carolina General Assembly by chamber",
            ),
            FunctionExample(
                sql="SELECT name, district, email FROM us_statelegs.main.legislators(current := true) "
                "WHERE state = 'NE' ORDER BY district::INTEGER LIMIT 10",
                description="The first ten districts of the unicameral Nebraska Legislature",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[LegislatorArgs], settings: Settings, state: str) -> pa.Table:
        """Parse one state's people files."""
        rows = legislator_rows(_people_for(settings, state)["people"], state)
        if params.args.current is not None:
            rows = [r for r in rows if r["current"] == params.args.current]
        rows.sort(key=lambda r: (r["chamber"] or "", r["district"] or "", r["name"] or ""))
        return pa.Table.from_pylist(rows, schema=LEGISLATOR_SCHEMA)


@bind_fixed_schema
@init_single_worker
class LegislatorRoles(_PeopleSource[StateArgs]):
    """One row per legislative role (seat held) of every current and former legislator."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ROLE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "legislator_roles"
        description = "Every seat held by current and former state legislators, with dates (keyless, CC0)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Legislator Roles (Terms)",
                category="legislators",
                keywords="terms, roles, seats, service history, tenure, district history, start date, end date",
                doc_llm=(
                    "One row per legislative role recorded in the Open States people data: person_id, name, "
                    "state, chamber, district, start_date, end_date (NULL or the scheduled term end while serving), "
                    "end_reason, current, "
                    "party (the person's current/last party) and jurisdiction_id. A person who moved from the "
                    "house to the senate has two rows. Start dates are often NULL for older roles. Argument: "
                    "`state := 'NE'`. " + CHAMBER_DOC
                ),
                doc_md=(
                    "## legislator_roles(state := NULL)\n\n"
                    "Seat-level service history of state legislators. " + _PUSHDOWN_DOC + "\n\n" + _PROVENANCE
                ),
            ),
            "vgi.result_columns_schema": result_columns(ROLE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT person_id, name, count(DISTINCT chamber) AS chambers "
                "FROM us_statelegs.main.legislator_roles(state := 'OH') GROUP BY ALL "
                "HAVING count(DISTINCT chamber) > 1 ORDER BY name LIMIT 20",
                description="Ohio legislators who served in both chambers",
            ),
            FunctionExample(
                sql="SELECT year(end_date) AS year, count(*) AS roles_ended FROM us_statelegs.main.legislator_roles() "
                "WHERE state = 'NE' AND end_date IS NOT NULL GROUP BY ALL ORDER BY year",
                description="How many Nebraska seats turned over each year",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[StateArgs], settings: Settings, state: str) -> pa.Table:
        """Parse one state's people files into roles."""
        rows = role_rows(_people_for(settings, state)["people"], state)
        rows.sort(key=lambda r: (r["person_id"], str(r["start_date"]), r["chamber"]))
        return pa.Table.from_pylist(rows, schema=ROLE_SCHEMA)


@bind_fixed_schema
@init_single_worker
class Committees(_PeopleSource[StateArgs]):
    """Current standing, joint and select committees, one row each."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COMMITTEE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "committees"
        description = "Current state legislative committees and subcommittees from the Open States people data"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Legislative Committees",
                category="legislators",
                keywords="committees, subcommittees, joint committees, committee chair, standing committees",
                doc_llm=(
                    "One row per current committee or subcommittee: committee_id (ocd-organization id), state, "
                    "chamber ('legislature' also marks joint committees), name, classification, parent_id, "
                    "members (roster size), chair, links, sources. Rosters are in committee_members(). "
                    "Coverage varies by state. Argument: `state := 'NE'`. No API key needed."
                ),
                doc_md="## committees(state := NULL)\n\nCurrent committees. " + _PUSHDOWN_DOC + "\n\n" + _PROVENANCE,
            ),
            "vgi.result_columns_schema": result_columns(COMMITTEE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT name, chair, members FROM us_statelegs.main.committees(state := 'NE') "
                "WHERE classification = 'committee' ORDER BY members DESC, name LIMIT 10",
                description="The largest Nebraska Legislature committees and their chairs",
            ),
            FunctionExample(
                sql="SELECT state, chamber, count(*) AS committees FROM us_statelegs.main.committees() "
                "GROUP BY ALL ORDER BY state, chamber",
                description="How many committees each chamber has",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[StateArgs], settings: Settings, state: str) -> pa.Table:
        """Parse one state's committee files."""
        rows = committee_rows(_people_for(settings, state)["committees"], state)
        rows.sort(key=lambda r: (r["chamber"], r["name"] or ""))
        return pa.Table.from_pylist(rows, schema=COMMITTEE_SCHEMA)


@bind_fixed_schema
@init_single_worker
class CommitteeMembers(_PeopleSource[StateArgs]):
    """Committee rosters, one row per member per committee."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MEMBER_SCHEMA

    class Meta:
        """Function metadata."""

        name = "committee_members"
        description = "Current committee rosters: one row per member per committee (Open States people data)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Committee Members",
                category="legislators",
                keywords="committee membership, roster, chair, vice chair, ranking member, assignments",
                doc_llm=(
                    "One row per roster entry of every current committee: committee_id, committee_name, state, "
                    "chamber, person_id (ocd-person id when Open States matched the member; join to "
                    "legislators.person_id), member_name and role ('member', 'chair', 'vice chair', ...). "
                    "Argument: `state := 'NE'`. No API key needed."
                ),
                doc_md="## committee_members(state := NULL)\n\nCommittee rosters. "
                + _PUSHDOWN_DOC
                + "\n\n"
                + _PROVENANCE,
            ),
            "vgi.result_columns_schema": result_columns(MEMBER_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT m.member_name, count(*) AS committees FROM us_statelegs.main.committee_members("
                "state := 'NE') m GROUP BY ALL ORDER BY committees DESC, m.member_name LIMIT 10",
                description="Nebraska senators with the most committee assignments",
            ),
            FunctionExample(
                sql="SELECT l.party, count(*) AS chairs FROM us_statelegs.main.committee_members(state := 'NC') m "
                "JOIN us_statelegs.main.legislators(state := 'NC') l USING (person_id) "
                "WHERE m.role = 'chair' GROUP BY ALL ORDER BY chairs DESC",
                description="Committee chairs in North Carolina by party",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[StateArgs], settings: Settings, state: str) -> pa.Table:
        """Parse one state's committee rosters."""
        rows = member_rows(_people_for(settings, state)["committees"], state)
        rows.sort(key=lambda r: (r["committee_name"] or "", r["member_name"] or ""))
        return pa.Table.from_pylist(rows, schema=MEMBER_SCHEMA)
