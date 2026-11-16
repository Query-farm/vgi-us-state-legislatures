"""Live data from the Open States API v3 (https://v3.openstates.org), run by Plural.

Every function here needs an API key in a DuckDB secret::

    CREATE SECRET (TYPE openstates, API_KEY 'your-key');

The key is read from the secret on every request and sent in the ``X-API-KEY`` header;
it is never stored in the scan cursor. A missing secret fails the scan with a message
naming the secret type.

**Paging.** The cursor (the request URL without the key, the next page and the last
page) travels with each batch, so a scan survives the HTTP transport. One page is one
batch. The free tier is rate limited: requests are spaced by ``OPENSTATES_MIN_INTERVAL``
seconds (ATTACH option, default 1) and HTTP 429 is retried honouring ``Retry-After``.

**Pushdown.** Arguments and simple ``WHERE`` predicates on ``state``, ``session``,
``chamber`` and ``updated_at >= ...`` are sent as the API's own filters.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
import urllib.error
import urllib.parse
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, TypeVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
from vgi.catalog import SecretTypeSpec
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .common import clean_str, http_get, pushed_range, pushed_values, settings_of, to_date, to_timestamp
from .meta import (
    API_KEY_MD,
    API_KEY_NOTE,
    OPENSTATES_DOCS,
    OPENSTATES_SIGNUP,
    SOURCE_URL,
    column,
    object_tags,
    result_columns,
)
from .reference import STATE_ARG_DOC, jurisdiction_id, normalize_state, usps_from_jurisdiction

log = logging.getLogger(__name__)

SECRET_TYPE = "openstates"
PER_PAGE = 20
DEFAULT_MIN_INTERVAL = 1.0
CHAMBER_CHOICES = ["upper", "lower", "legislature"]
# Open Civic Data bill classifications used by Open States.
BILL_CLASSIFICATIONS = [
    "bill",
    "resolution",
    "concurrent resolution",
    "joint resolution",
    "memorial",
    "concurrent memorial",
    "joint memorial",
    "commemoration",
    "constitutional amendment",
    "proposed bill",
    "proclamation",
    "nomination",
    "appointment",
    "contract",
    "claim",
    "petition",
    "order",
    "concurrent order",
    "appropriation",
    "ordinance",
]

OPENSTATES_SECRET = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Open States API v3 key (free from Plural) for the os_* bill, vote, people, committee and "
    "event functions.",
    schema=pa.schema(
        [
            pa.field(
                "api_key",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_doc": f"Open States API v3 key; register free at {OPENSTATES_SIGNUP}",
                },
            )
        ],
        metadata={"vgi_doc_url": f"{SOURCE_URL}#open-states-api-key"},
    ),
)

MISSING_KEY = (
    "the os_* functions call the Open States API and need a key: run "
    "CREATE SECRET (TYPE openstates, API_KEY '<your key>') first; keys are free at " + OPENSTATES_SIGNUP
)


class OpenStatesError(RuntimeError):
    """The Open States API refused or failed a request."""


def api_key(params: ProcessParams[Any]) -> str:
    """Return the API key from the ``openstates`` secret, or raise naming the secret type."""
    for secret in params.secrets.of_type(SECRET_TYPE):
        value = secret.get("api_key")
        if value is not None and value.as_py():
            return str(value.as_py())
    raise OpenStatesError(MISSING_KEY)


_throttle_lock = threading.Lock()
_last_request = [0.0]


def _min_interval(params: ProcessParams[Any]) -> float:
    try:
        opts = {k.lower(): v for k, v in dict(params.attach_options or {}).items()}
    except Exception:
        opts = {}
    value = opts.get("openstates_min_interval")
    return DEFAULT_MIN_INTERVAL if value is None else max(0.0, float(value))


def fetch_page(url: str, key: str, min_interval: float) -> Any:
    """GET one Open States page (spaced by ``min_interval``), translating HTTP errors into clear messages."""
    with _throttle_lock:
        wait = _last_request[0] + min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request[0] = time.monotonic()
    try:
        body, _ = http_get(url, {"X-API-KEY": key, "Accept": "application/json"})
    except urllib.error.HTTPError as err:
        detail = err.read(400).decode(errors="replace")
        if err.code in (401, 403):
            raise OpenStatesError(
                f"Open States rejected the API key (HTTP {err.code}); check the openstates secret: {detail}"
            ) from err
        if err.code == 429:
            raise OpenStatesError(
                "Open States rate limit reached (HTTP 429) after retries; the free tier allows few requests per "
                "minute and per day. Narrow the query (state, session, updated_since) or try again later."
            ) from err
        if err.code in (400, 422):
            raise OpenStatesError(f"Open States rejected the request (HTTP {err.code}): {detail}") from err
        raise OpenStatesError(f"Open States request failed with HTTP {err.code}: {detail}") from err
    return json.loads(body)


# ---------------------------------------------------------------- shared cursor / base


@dataclass(kw_only=True)
class PageCursor(ArrowSerializableDataclass):
    """The request (URL without the key) and where paging has got to."""

    url: str = ""
    page: int = 1
    done: bool = False


A = TypeVar("A")


class OpenStatesSource(TableFunctionGenerator[A, PageCursor]):
    """Base for Open States list endpoints: build a request, page it, map results to rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])
    PAGED: ClassVar[bool] = True

    @classmethod
    def on_bind(
        cls,
        params: BindParams[A],
        *,
        api_key: Annotated[dict[str, Any] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the output schema; the annotation requests the API-key secret for every subclass."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """Return the endpoint path and query parameters."""
        raise NotImplementedError

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """Map one API result object to zero or more rows of ``FIXED_SCHEMA``."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> PageCursor:
        """Resolve the request from arguments and pushed-down filters."""
        path, query = cls.request(params)
        base = settings_of(params).openstates_api_url
        if cls.PAGED:
            query = [*query, ("per_page", str(PER_PAGE))]
        return PageCursor(url=f"{base}{path}?{urllib.parse.urlencode(query)}")

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Fetch one page and emit its rows; finish after the last page."""
        if state.done:
            out.finish()
            return
        key = api_key(params)
        url = f"{state.url}&page={state.page}" if cls.PAGED else state.url
        body = fetch_page(url, key, _min_interval(params))
        results = body.get("results", []) if isinstance(body, dict) else []
        rows = [row for result in results for row in cls.rows(result)]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        pagination = body.get("pagination") or {} if isinstance(body, dict) else {}
        max_page = int(pagination.get("max_page") or 0)
        if not cls.PAGED or not results or state.page >= max_page:
            state.done = True
        else:
            state.page += 1


def _state_param(params: ProcessParams[Any], *, required: bool) -> str | None:
    """Resolve the state from the argument or a pushed ``state = 'XX'`` filter."""
    usps = normalize_state(getattr(params.args, "state", None))
    if usps is None:
        pushed = pushed_values(params, "state")
        if pushed and len(pushed) == 1 and pushed[0] is not None:
            try:
                usps = normalize_state(str(pushed[0]))
            except ValueError:
                usps = None
    if usps is None and required:
        raise OpenStatesError("pass state := 'XX' (or WHERE state = 'XX'): this Open States endpoint needs a state")
    return usps


def _pushed_or_arg(params: ProcessParams[Any], name: str) -> str | None:
    value = getattr(params.args, name, None)
    if value is not None:
        return str(value)
    pushed = pushed_values(params, name)
    if pushed and len(pushed) == 1 and pushed[0] is not None:
        return str(pushed[0])
    return None


def _org_class(obj: Any) -> str | None:
    return clean_str(obj.get("classification")) if isinstance(obj, dict) else None


def _person_id(obj: Any) -> str | None:
    return clean_str(obj.get("id")) if isinstance(obj, dict) else None


_LINKS = pa.list_(pa.string())


def _links(items: Any) -> list[str]:
    return [str(i["url"]) for i in items or [] if isinstance(i, dict) and i.get("url")]


_TS = pa.timestamp("us", tz="UTC")

# ---------------------------------------------------------------- jurisdictions / sessions

JURISDICTION_SCHEMA = pa.schema(
    [
        column("jurisdiction_id", pa.string(), "Open States jurisdiction id.", nullable=False),
        column("state", pa.string(), "USPS code, upper case (NULL for jurisdictions outside the 52 covered)."),
        column("name", pa.string(), "Jurisdiction name.", nullable=False),
        column("classification", pa.string(), "'state' (DC and PR are also listed as states)."),
        column("division_id", pa.string(), "OCD division id."),
        column("url", pa.string(), "Official website."),
        column("latest_bill_update", _TS, "When Open States last updated any bill here."),
        column("latest_people_update", _TS, "When Open States last updated any legislator here."),
    ]
)

_DOWNLOAD = pa.list_(pa.struct([("data_type", pa.string()), ("url", pa.string()), ("updated_at", _TS)]))

SESSION_SCHEMA = pa.schema(
    [
        column("state", pa.string(), "USPS code, upper case.", nullable=False),
        column("jurisdiction_id", pa.string(), "Open States jurisdiction id.", nullable=False),
        column(
            "session", pa.string(), "Session identifier used by os_bills(session := ...), e.g. '2025'.", nullable=False
        ),
        column("name", pa.string(), "Session name, e.g. '2025-2026 Regular Session'.", nullable=False),
        column("classification", pa.string(), "'primary' (regular) or 'special'."),
        column("start_date", pa.date32(), "Session start date."),
        column("end_date", pa.date32(), "Session end date."),
        column("downloads", _DOWNLOAD, "Open States bulk CSV/JSON archives of the session's bills and votes."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """No arguments."""


@dataclass(slots=True, frozen=True, kw_only=True)
class SessionArgs:
    """``state := 'NE'`` (optional)."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]


def _api_md(title: str, body: str) -> str:
    return f"## {title}\n\n{body}\n\nSource: [Open States API v3]({OPENSTATES_DOCS}).\n\n" + API_KEY_MD


@init_single_worker
class OsJurisdictions(OpenStatesSource[NoArgs]):
    """The state jurisdictions Open States tracks, with data-freshness stamps."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = JURISDICTION_SCHEMA
    PAGED: ClassVar[bool] = False

    class Meta:
        """Function metadata."""

        name = "os_jurisdictions"
        description = "State jurisdictions tracked by the Open States API, with last bill and people update times"
        tags = {
            **object_tags(
                title="Open States Jurisdictions",
                category="openstates_api",
                keywords="jurisdictions, freshness, last updated, open states",
                doc_llm=(
                    "Lists the state jurisdictions Open States tracks (one API request): jurisdiction_id, state, "
                    "name, classification, division_id, url, latest_bill_update, latest_people_update. Use it to "
                    "check how fresh the API's data are. " + API_KEY_NOTE
                ),
                doc_md=_api_md("os_jurisdictions()", "Open States jurisdictions and their data freshness."),
            ),
            "vgi.result_columns_schema": result_columns(JURISDICTION_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT state, latest_bill_update FROM us_statelegs.main.os_jurisdictions() "
                "ORDER BY latest_bill_update LIMIT 5",
                description="The five states whose bill data were updated least recently",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """All state jurisdictions in one page."""
        return "/jurisdictions", [("classification", "state"), ("per_page", "60")]

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per jurisdiction."""
        return [
            {
                "jurisdiction_id": result.get("id"),
                "state": usps_from_jurisdiction(result.get("id")),
                "name": result.get("name"),
                "classification": clean_str(result.get("classification")),
                "division_id": clean_str(result.get("division_id")),
                "url": clean_str(result.get("url")),
                "latest_bill_update": to_timestamp(result.get("latest_bill_update")),
                "latest_people_update": to_timestamp(result.get("latest_people_update")),
            }
        ]


@init_single_worker
class OsSessions(OpenStatesSource[SessionArgs]):
    """Legislative sessions per state, with the bulk-data archive links."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SESSION_SCHEMA
    PAGED: ClassVar[bool] = False

    class Meta:
        """Function metadata."""

        name = "os_sessions"
        description = "Legislative sessions of each state from Open States, with bulk CSV/JSON download links"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Open States Legislative Sessions",
                category="openstates_api",
                keywords="sessions, legislative session, special session, session identifier, bulk download",
                doc_llm=(
                    "One row per legislative session: state, jurisdiction_id, session (the identifier to pass "
                    "to os_bills/os_votes), name, classification ('primary' = regular, 'special'), start_date, "
                    "end_date, downloads (Open States bulk archive links). Argument: `state := 'NE'`. " + API_KEY_NOTE
                ),
                doc_md=_api_md("os_sessions(state := NULL)", "Sessions and their bulk-data archives."),
            ),
            "vgi.result_columns_schema": result_columns(SESSION_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT session, name, start_date FROM us_statelegs.main.os_sessions(state := 'NC') "
                "ORDER BY start_date DESC LIMIT 5",
                description="North Carolina's five most recent sessions",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """One jurisdiction, or all of them, with sessions included."""
        usps = _state_param(params, required=False)
        if usps:
            return f"/jurisdictions/{urllib.parse.quote(jurisdiction_id(usps), safe='')}", [
                ("include", "legislative_sessions")
            ]
        return "/jurisdictions", [("classification", "state"), ("per_page", "60"), ("include", "legislative_sessions")]

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Fetch the one page; the single-jurisdiction endpoint returns the object, not a results list."""
        if state.done:
            out.finish()
            return
        body = fetch_page(state.url, api_key(params), _min_interval(params))
        results = body.get("results") if isinstance(body, dict) and "results" in body else [body]
        rows = [row for result in results or [] if isinstance(result, dict) for row in cls.rows(result)]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        state.done = True

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per session of a jurisdiction."""
        usps = usps_from_jurisdiction(result.get("id"))
        if usps is None:
            return []
        return [
            {
                "state": usps,
                "jurisdiction_id": result.get("id"),
                "session": s.get("identifier"),
                "name": s.get("name"),
                "classification": clean_str(s.get("classification")),
                "start_date": to_date(s.get("start_date")),
                "end_date": to_date(s.get("end_date")),
                "downloads": [
                    {
                        "data_type": clean_str(d.get("data_type")),
                        "url": clean_str(d.get("url")),
                        "updated_at": to_timestamp(d.get("updated_at")),
                    }
                    for d in s.get("downloads") or []
                    if isinstance(d, dict)
                ],
            }
            for s in result.get("legislative_sessions") or []
            if isinstance(s, dict)
        ]


# ---------------------------------------------------------------- people

_OFFICE = pa.list_(
    pa.struct([("classification", pa.string()), ("address", pa.string()), ("voice", pa.string()), ("fax", pa.string())])
)

PEOPLE_SCHEMA = pa.schema(
    [
        column("person_id", pa.string(), "Open States person id (ocd-person/<uuid>).", nullable=False),
        column("name", pa.string(), "Full name.", nullable=False),
        column("party", pa.string(), "Current party."),
        column("state", pa.string(), "USPS code, upper case."),
        column("chamber", pa.string(), "Current chamber: 'upper', 'lower' or 'legislature'."),
        column("district", pa.string(), "Current district, as text."),
        column("role_title", pa.string(), "Title of the current role ('Senator', 'Representative')."),
        column("division_id", pa.string(), "OCD division id of the district (ocd-division/.../sldu:3)."),
        column("given_name", pa.string(), "Given name."),
        column("family_name", pa.string(), "Family name."),
        column("gender", pa.string(), "Gender as recorded."),
        column("email", pa.string(), "Official email."),
        column("image", pa.string(), "Photo URL."),
        column("birth_date", pa.date32(), "Date of birth, when known."),
        column("openstates_url", pa.string(), "Open States profile page."),
        column("offices", _OFFICE, "Offices: classification, address, phone, fax."),
        column("links", _LINKS, "Web pages."),
        column("updated_at", _TS, "When Open States last updated the record."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class PeopleArgs:
    """Optional filters for ``os_people``."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]
    name: Annotated[str | None, Arg("name", default=None, doc="Name search (substring, case-insensitive).")]
    district: Annotated[str | None, Arg("district", default=None, doc="District, e.g. '13' (needs state).")]
    chamber: Annotated[
        str | None, Arg("chamber", default=None, doc="'upper', 'lower' or 'legislature'.", choices=CHAMBER_CHOICES)
    ]


@init_single_worker
class OsPeople(OpenStatesSource[PeopleArgs]):
    """Current legislators from the live Open States API."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = PEOPLE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "os_people"
        description = "Current state legislators from the live Open States API (filter by state, name, district)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Open States People (Live)",
                category="openstates_api",
                keywords="legislators, people, current members, live, contact, district lookup",
                doc_llm=(
                    "Current legislators from the Open States API: person_id, name, party, state, chamber, "
                    "district, role_title, division_id, contact details, openstates_url, updated_at. Needs "
                    "`state := 'NE'` or `name := '...'`; `district` and `chamber` narrow further (all sent to "
                    "the API). For keyless or historical data use legislators(). " + API_KEY_NOTE
                ),
                doc_md=_api_md(
                    "os_people(state := NULL, name := NULL, district := NULL, chamber := NULL)",
                    "Live current legislators. `state`, `name`, `district` and `chamber` are API filters.",
                ),
            ),
            "vgi.result_columns_schema": result_columns(PEOPLE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT name, party, district FROM us_statelegs.main.os_people(state := 'NC', chamber := 'upper') "
                "ORDER BY district::INTEGER",
                description="North Carolina's current state senators by district",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """Build the /people query."""
        usps = _state_param(params, required=False)
        a = params.args
        if usps is None and not a.name:
            raise OpenStatesError("os_people needs state := 'XX' or name := '...' (Open States requires one)")
        query: list[tuple[str, str]] = [("include", "offices"), ("include", "links")]
        if usps:
            query.append(("jurisdiction", jurisdiction_id(usps)))
        if a.name:
            query.append(("name", a.name))
        district = _pushed_or_arg(params, "district")
        if district:
            query.append(("district", district))
        chamber = _pushed_or_arg(params, "chamber")
        if chamber:
            query.append(("org_classification", chamber))
        return "/people", query

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per person."""
        role = result.get("current_role") or {}
        jur = result.get("jurisdiction") or {}
        district = role.get("district")
        return [
            {
                "person_id": result.get("id"),
                "name": result.get("name"),
                "party": clean_str(result.get("party")),
                "state": usps_from_jurisdiction(jur.get("id")),
                "chamber": clean_str(role.get("org_classification")),
                "district": None if district in (None, "") else str(district),
                "role_title": clean_str(role.get("title")),
                "division_id": clean_str(role.get("division_id")),
                "given_name": clean_str(result.get("given_name")),
                "family_name": clean_str(result.get("family_name")),
                "gender": clean_str(result.get("gender")),
                "email": clean_str(result.get("email")),
                "image": clean_str(result.get("image")),
                "birth_date": to_date(result.get("birth_date")),
                "openstates_url": clean_str(result.get("openstates_url")),
                "offices": [
                    {k: clean_str(o.get(k)) for k in ("classification", "address", "voice", "fax")}
                    for o in result.get("offices") or []
                    if isinstance(o, dict)
                ],
                "links": _links(result.get("links")),
                "updated_at": to_timestamp(result.get("updated_at")),
            }
        ]


# ---------------------------------------------------------------- bills and votes

_SPONSOR = pa.list_(
    pa.struct(
        [
            ("name", pa.string()),
            ("person_id", pa.string()),
            ("entity_type", pa.string()),
            ("is_primary", pa.bool_()),
            ("classification", pa.string()),
        ]
    )
)
_ACTION = pa.list_(
    pa.struct(
        [
            ("date", pa.date32()),
            ("description", pa.string()),
            ("chamber", pa.string()),
            ("classification", pa.list_(pa.string())),
        ]
    )
)

BILL_SCHEMA = pa.schema(
    [
        column("bill_id", pa.string(), "Open States bill id (ocd-bill/<uuid>).", nullable=False),
        column("state", pa.string(), "USPS code, upper case."),
        column("session", pa.string(), "Session identifier.", nullable=False),
        column("chamber", pa.string(), "Originating chamber: 'upper', 'lower' or 'legislature'."),
        column("identifier", pa.string(), "Bill number as the state writes it ('SB 113', 'LB 1').", nullable=False),
        column("title", pa.string(), "Bill title.", nullable=False),
        column("classification", pa.list_(pa.string()), "Kinds: 'bill', 'resolution', 'joint resolution', ..."),
        column("subjects", pa.list_(pa.string()), "Subject terms assigned by the state."),
        column("abstract", pa.string(), "First abstract/summary, when provided."),
        column("first_action_date", pa.date32(), "Date of the first action (usually introduction)."),
        column("latest_action_date", pa.date32(), "Date of the most recent action."),
        column("latest_action_description", pa.string(), "Text of the most recent action."),
        column("latest_passage_date", pa.date32(), "Date the bill last passed a chamber, if it did."),
        column("sponsors", _SPONSOR, "Sponsorships: name, person_id (when matched), entity type, primary flag."),
        column("actions", _ACTION, "Action history: date, description, chamber, action classifications."),
        column("openstates_url", pa.string(), "Bill page on Open States."),
        column("created_at", _TS, "When Open States first saw the bill."),
        column("updated_at", _TS, "When Open States last updated the bill (filter with >= to push updated_since)."),
    ]
)

_VOTER = pa.list_(pa.struct([("option", pa.string()), ("voter_name", pa.string()), ("person_id", pa.string())]))
_COUNT = pa.list_(pa.struct([("option", pa.string()), ("value", pa.int64())]))

VOTE_SCHEMA = pa.schema(
    [
        column("vote_id", pa.string(), "Open States vote event id (ocd-vote/<uuid>).", nullable=False),
        column("bill_id", pa.string(), "Bill the vote was on.", nullable=False),
        column("bill_identifier", pa.string(), "Bill number ('SB 113').", nullable=False),
        column("state", pa.string(), "USPS code, upper case."),
        column("session", pa.string(), "Session identifier.", nullable=False),
        column("chamber", pa.string(), "Chamber that voted: 'upper', 'lower' or 'legislature'."),
        column("vote_date", pa.date32(), "Date of the vote."),
        column("motion_text", pa.string(), "Motion voted on ('Shall the bill be passed?').", nullable=False),
        column("motion_classification", pa.list_(pa.string()), "Motion kinds, e.g. ['passage']."),
        column("result", pa.string(), "'pass' or 'fail'."),
        column("yes", pa.int64(), "Yes count."),
        column("no", pa.int64(), "No count."),
        column("other", pa.int64(), "All other counted options (absent, excused, not voting, ...)."),
        column("counts", _COUNT, "Every reported count by option."),
        column("votes", _VOTER, "Individual roll-call votes: option, voter_name, person_id (when matched)."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class BillArgs:
    """Filters for ``os_bills`` / ``os_votes`` (all optional; Open States needs state or q)."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]
    session: Annotated[str | None, Arg("session", default=None, doc="Session identifier from os_sessions().")]
    chamber: Annotated[
        str | None,
        Arg(
            "chamber",
            default=None,
            doc="Originating chamber: 'upper', 'lower', 'legislature'.",
            choices=CHAMBER_CHOICES,
        ),
    ]
    classification: Annotated[
        str | None,
        Arg("classification", default=None, doc="Open States bill classification.", choices=BILL_CLASSIFICATIONS),
    ]
    subject: Annotated[str | None, Arg("subject", default=None, doc="Only bills tagged with this subject.")]
    updated_since: Annotated[
        str | None,
        Arg(
            "updated_since",
            default=None,
            doc="Only bills updated on/after this date, 'YYYY-MM-DD' (or WHERE updated_at >= ...).",
            pattern=r"^\d{4}-\d{2}-\d{2}",
        ),
    ]
    q: Annotated[str | None, Arg("q", default=None, doc="Full-text search of bill text and titles.")]
    sponsor: Annotated[str | None, Arg("sponsor", default=None, doc="Sponsor name or ocd-person id.")]


def bill_query(params: ProcessParams[Any], include: list[str]) -> list[tuple[str, str]]:
    """Translate BillArgs plus pushed predicates into the /bills query string."""
    a = params.args
    usps = _state_param(params, required=False)
    if usps is None and not a.q:
        raise OpenStatesError("pass state := 'XX' (or q := '...'): Open States requires a jurisdiction or search")
    query: list[tuple[str, str]] = [("include", i) for i in include]
    if usps:
        query.append(("jurisdiction", jurisdiction_id(usps)))
    session = _pushed_or_arg(params, "session")
    if session:
        query.append(("session", session))
    chamber = _pushed_or_arg(params, "chamber")
    if chamber:
        query.append(("chamber", chamber))
    for name in ("classification", "q", "sponsor"):
        value = getattr(a, name)
        if value:
            query.append((name, value))
    if a.subject:
        query.append(("subject", a.subject))
    since = a.updated_since
    if since is None:
        lo, _ = pushed_range(params, "updated_at")
        if isinstance(lo, dt.datetime | dt.date):
            since = lo.isoformat()
    if since:
        query.append(("updated_since", since))
    query.append(("sort", "updated_desc"))
    return query


def _orgc(obj: Any) -> str | None:
    return _org_class(obj)


_BILL_DOC = (
    "Arguments (all sent to the API): state, session (from os_sessions), chamber, classification, subject, "
    "updated_since 'YYYY-MM-DD', q (full text), sponsor. WHERE state/session/chamber = and updated_at >= "
    "are pushed down too. A state or q is required. Pages hold 20 bills; always filter or LIMIT on the "
    "free tier. "
)


@init_single_worker
class OsBills(OpenStatesSource[BillArgs]):
    """State bills from the live Open States API, with sponsors and actions."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = BILL_SCHEMA

    class Meta:
        """Function metadata."""

        name = "os_bills"
        description = "State bills from the live Open States API with sponsors and actions, newest update first"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Open States Bills (Live)",
                category="openstates_api",
                keywords="bills, legislation, resolutions, sponsors, actions, bill status, state bills",
                doc_llm=(
                    "Bills from the Open States API, most recently updated first: bill_id, state, session, "
                    "chamber, identifier ('SB 113'), title, classification, subjects, abstract, first/latest "
                    "action dates and description, latest_passage_date, sponsors (`LIST` of `STRUCT` with "
                    "person_id), actions (`LIST` of `STRUCT`), openstates_url, created_at, updated_at. "
                    + _BILL_DOC
                    + API_KEY_NOTE
                ),
                doc_md=_api_md(
                    "os_bills(state, session, chamber, classification, subject, updated_since, q, sponsor)",
                    "Live bills with sponsors and actions. " + _BILL_DOC,
                ),
            ),
            "vgi.result_columns_schema": result_columns(BILL_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT identifier, title, latest_action_date FROM us_statelegs.main.os_bills("
                "state := 'NE', session := '109') WHERE latest_passage_date IS NOT NULL "
                "ORDER BY latest_action_date DESC LIMIT 10",
                description="Ten recently passed Nebraska bills",
            ),
            FunctionExample(
                sql="SELECT s.name, count(*) AS bills FROM us_statelegs.main.os_bills(state := 'NC', "
                "updated_since := '2025-06-01'), unnest(sponsors) AS u(s) WHERE s.is_primary GROUP BY ALL "
                "ORDER BY bills DESC LIMIT 10",
                description="North Carolina's most prolific primary sponsors among recently updated bills",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """Build the /bills query."""
        return "/bills", bill_query(params, ["sponsorships", "actions", "abstracts"])

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per bill."""
        jur = result.get("jurisdiction") or {}
        abstracts = [a for a in result.get("abstracts") or [] if isinstance(a, dict) and a.get("abstract")]
        return [
            {
                "bill_id": result.get("id"),
                "state": usps_from_jurisdiction(jur.get("id")),
                "session": result.get("session"),
                "chamber": _orgc(result.get("from_organization")),
                "identifier": result.get("identifier"),
                "title": result.get("title"),
                "classification": list(result.get("classification") or []),
                "subjects": list(result.get("subject") or []),
                "abstract": clean_str(abstracts[0]["abstract"]) if abstracts else None,
                "first_action_date": to_date(result.get("first_action_date")),
                "latest_action_date": to_date(result.get("latest_action_date")),
                "latest_action_description": clean_str(result.get("latest_action_description")),
                "latest_passage_date": to_date(result.get("latest_passage_date")),
                "sponsors": [
                    {
                        "name": clean_str(s.get("name")),
                        "person_id": _person_id(s.get("person")),
                        "entity_type": clean_str(s.get("entity_type")),
                        "is_primary": bool(s.get("primary")),
                        "classification": clean_str(s.get("classification")),
                    }
                    for s in result.get("sponsorships") or []
                    if isinstance(s, dict)
                ],
                "actions": [
                    {
                        "date": to_date(x.get("date")),
                        "description": clean_str(x.get("description")),
                        "chamber": _orgc(x.get("organization")),
                        "classification": list(x.get("classification") or []),
                    }
                    for x in sorted(
                        (x for x in result.get("actions") or [] if isinstance(x, dict)),
                        key=lambda x: int(x.get("order") or 0),
                    )
                ],
                "openstates_url": clean_str(result.get("openstates_url")),
                "created_at": to_timestamp(result.get("created_at")),
                "updated_at": to_timestamp(result.get("updated_at")),
            }
        ]


def _count(counts: list[dict[str, Any]], option: str) -> int | None:
    values = [int(c.get("value") or 0) for c in counts if str(c.get("option", "")).lower() == option]
    return sum(values) if values else None


@init_single_worker
class OsVotes(OpenStatesSource[BillArgs]):
    """Roll-call votes on state bills from the live Open States API, one row per vote event."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = VOTE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "os_votes"
        description = "Roll-call votes on state bills from the live Open States API, with individual legislators' votes"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Open States Roll-Call Votes (Live)",
                category="openstates_api",
                keywords="votes, roll call, vote counts, how legislators voted, yes no, passage vote",
                doc_llm=(
                    "One row per vote event on the bills that match the filters (same arguments as os_bills; "
                    "the API is queried for bills with votes included): vote_id, bill_id, bill_identifier, "
                    "state, session, chamber, vote_date, motion_text, motion_classification, result "
                    "('pass'/'fail'), yes, no, other, counts, votes (`LIST` of `STRUCT`(option, voter_name, "
                    "person_id)); unnest(votes) for how each legislator voted. " + _BILL_DOC + API_KEY_NOTE
                ),
                doc_md=_api_md(
                    "os_votes(state, session, chamber, classification, subject, updated_since, q, sponsor)",
                    "Live roll-call votes with individual votes. " + _BILL_DOC,
                ),
            ),
            "vgi.result_columns_schema": result_columns(VOTE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT bill_identifier, motion_text, yes, no, result FROM us_statelegs.main.os_votes("
                "state := 'NE', session := '109') WHERE 'passage' = ANY(motion_classification) "
                "ORDER BY vote_date DESC LIMIT 10",
                description="Recent final-passage votes in the Nebraska Legislature",
            ),
            FunctionExample(
                sql="SELECT v.voter_name, count(*) FILTER (WHERE v.option = 'no') AS no_votes FROM "
                "us_statelegs.main.os_votes(state := 'NC', updated_since := '2025-06-01'), unnest(votes) AS u(v) "
                "GROUP BY ALL ORDER BY no_votes DESC LIMIT 10",
                description="Who cast the most no votes on recently updated North Carolina bills",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """Build the /bills query with votes included."""
        return "/bills", bill_query(params, ["votes"])

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per vote event on a bill."""
        jur = result.get("jurisdiction") or {}
        rows = []
        for v in result.get("votes") or []:
            if not isinstance(v, dict):
                continue
            counts = [c for c in v.get("counts") or [] if isinstance(c, dict)]
            yes, no = _count(counts, "yes"), _count(counts, "no")
            other = [int(c.get("value") or 0) for c in counts if str(c.get("option", "")).lower() not in ("yes", "no")]
            rows.append(
                {
                    "vote_id": v.get("id"),
                    "bill_id": result.get("id"),
                    "bill_identifier": result.get("identifier"),
                    "state": usps_from_jurisdiction(jur.get("id")),
                    "session": result.get("session"),
                    "chamber": _orgc(v.get("organization")),
                    "vote_date": to_date(v.get("start_date")),
                    "motion_text": v.get("motion_text") or "",
                    "motion_classification": list(v.get("motion_classification") or []),
                    "result": clean_str(v.get("result")),
                    "yes": yes,
                    "no": no,
                    "other": sum(other) if other else None,
                    "counts": [{"option": c.get("option"), "value": c.get("value")} for c in counts],
                    "votes": [
                        {
                            "option": clean_str(p.get("option")),
                            "voter_name": clean_str(p.get("voter_name")),
                            "person_id": _person_id(p.get("voter")),
                        }
                        for p in v.get("votes") or []
                        if isinstance(p, dict)
                    ],
                }
            )
        return rows


# ---------------------------------------------------------------- committees and events

_MEMBERSHIP = pa.list_(pa.struct([("person_name", pa.string()), ("role", pa.string()), ("person_id", pa.string())]))

OS_COMMITTEE_SCHEMA = pa.schema(
    [
        column("committee_id", pa.string(), "Open States organization id.", nullable=False),
        column("state", pa.string(), "USPS code, upper case.", nullable=False),
        column("name", pa.string(), "Committee name.", nullable=False),
        column("classification", pa.string(), "'committee' or 'subcommittee'."),
        column("parent_id", pa.string(), "Parent committee (or chamber) organization id."),
        column("members", _MEMBERSHIP, "Roster: person_name, role, person_id."),
        column("links", _LINKS, "Committee web pages."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class CommitteeArgs:
    """``state`` (required by the API), ``chamber``, ``classification``."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC + " Required by the API.")]
    chamber: Annotated[
        str | None, Arg("chamber", default=None, doc="'upper', 'lower' or 'legislature'.", choices=CHAMBER_CHOICES)
    ]
    classification: Annotated[
        str | None,
        Arg(
            "classification", default=None, doc="'committee' or 'subcommittee'.", choices=["committee", "subcommittee"]
        ),
    ]


@init_single_worker
class OsCommittees(OpenStatesSource[CommitteeArgs]):
    """Committees and rosters from the live Open States API."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OS_COMMITTEE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "os_committees"
        description = "Committees and their rosters for one state from the live Open States API"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Open States Committees (Live)",
                category="openstates_api",
                keywords="committees, rosters, committee members, subcommittees, live",
                doc_llm=(
                    "Committees of one state from the Open States API with their rosters: committee_id, state, "
                    "name, classification, parent_id, members (`LIST` of `STRUCT`(person_name, role, person_id)), "
                    "links. Needs `state := 'NE'`; `chamber` and `classification` are API filters. The keyless "
                    "committees()/committee_members() functions hold the same data from the people repository. "
                    + API_KEY_NOTE
                ),
                doc_md=_api_md("os_committees(state, chamber := NULL, classification := NULL)", "Live committees."),
            ),
            "vgi.result_columns_schema": result_columns(OS_COMMITTEE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT name, len(members) AS size FROM us_statelegs.main.os_committees(state := 'NC', "
                "chamber := 'lower') ORDER BY size DESC LIMIT 5",
                description="The five largest North Carolina House committees",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """Build the /committees query."""
        usps = _state_param(params, required=True)
        assert usps is not None
        query = [("include", "memberships"), ("include", "links"), ("jurisdiction", jurisdiction_id(usps))]
        if params.args.chamber:
            query.append(("chamber", params.args.chamber))
        if params.args.classification:
            query.append(("classification", params.args.classification))
        return "/committees", query

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per committee (state is filled in by ``process``)."""
        return [
            {
                "committee_id": result.get("id"),
                "state": "",
                "name": result.get("name"),
                "classification": clean_str(result.get("classification")),
                "parent_id": clean_str(result.get("parent_id")),
                "members": [
                    {
                        "person_name": clean_str(m.get("person_name")),
                        "role": clean_str(m.get("role")),
                        "person_id": _person_id(m.get("person")),
                    }
                    for m in result.get("memberships") or []
                    if isinstance(m, dict)
                ],
                "links": _links(result.get("links")),
            }
        ]

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Page as usual, stamping the requested state on every row."""
        if state.done:
            out.finish()
            return
        usps = usps_from_jurisdiction(
            dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(state.url).query)).get("jurisdiction")
        )
        body = fetch_page(f"{state.url}&page={state.page}", api_key(params), _min_interval(params))
        results = body.get("results", []) if isinstance(body, dict) else []
        rows = [{**row, "state": usps or ""} for r in results for row in cls.rows(r)]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        max_page = int((body.get("pagination") or {}).get("max_page") or 0) if isinstance(body, dict) else 0
        if not results or state.page >= max_page:
            state.done = True
        else:
            state.page += 1


_PARTICIPANT = pa.list_(pa.struct([("name", pa.string()), ("entity_type", pa.string()), ("note", pa.string())]))
_AGENDA = pa.list_(pa.struct([("position", pa.int64()), ("description", pa.string())]))

EVENT_SCHEMA = pa.schema(
    [
        column("event_id", pa.string(), "Open States event id.", nullable=False),
        column("state", pa.string(), "USPS code, upper case."),
        column("name", pa.string(), "Event name (often the committee).", nullable=False),
        column("description", pa.string(), "Description."),
        column("classification", pa.string(), "'committee-meeting', 'hearing', 'floor', ..."),
        column("start_time", _TS, "Start time (UTC)."),
        column("end_time", _TS, "End time (UTC), when given."),
        column("all_day", pa.bool_(), "True for all-day events."),
        column("status", pa.string(), "'tentative', 'confirmed', 'cancelled' or 'passed'."),
        column("location_name", pa.string(), "Room or venue."),
        column("participants", _PARTICIPANT, "Participants: name, entity_type, note."),
        column("agenda", _AGENDA, "Agenda items in order."),
        column("links", _LINKS, "Event web pages."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class EventArgs:
    """``state`` (required), ``after_date``, ``before_date``."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC + " Required by the API.")]
    after_date: Annotated[
        str | None,
        Arg(
            "after_date",
            default=None,
            doc="Only events starting after this date 'YYYY-MM-DD'.",
            pattern=r"^\d{4}-\d{2}-\d{2}",
        ),
    ]
    before_date: Annotated[
        str | None,
        Arg(
            "before_date",
            default=None,
            doc="Only events starting before this date 'YYYY-MM-DD'.",
            pattern=r"^\d{4}-\d{2}-\d{2}",
        ),
    ]


@init_single_worker
class OsEvents(OpenStatesSource[EventArgs]):
    """Hearings, committee meetings and floor sessions from the live Open States API."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_SCHEMA

    class Meta:
        """Function metadata."""

        name = "os_events"
        description = "Committee meetings, hearings and floor sessions for one state from the live Open States API"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Open States Events (Live)",
                category="openstates_api",
                keywords="events, hearings, committee meetings, calendar, agenda, schedule",
                doc_llm=(
                    "Legislative events of one state from the Open States API: event_id, state, name, "
                    "description, classification, start_time, end_time, all_day, status, location_name, "
                    "participants, agenda, links. Needs `state := 'NE'`; `after_date`/`before_date` "
                    "('YYYY-MM-DD') are API filters. Coverage varies by state. " + API_KEY_NOTE
                ),
                doc_md=_api_md(
                    "os_events(state, after_date := NULL, before_date := NULL)", "Live legislative calendar events."
                ),
            ),
            "vgi.result_columns_schema": result_columns(EVENT_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT start_time, name, location_name FROM us_statelegs.main.os_events(state := 'NC', "
                "after_date := '2026-01-01') WHERE status <> 'cancelled' ORDER BY start_time LIMIT 10",
                description="Upcoming North Carolina committee meetings",
            ),
        ]

    @classmethod
    def request(cls, params: ProcessParams[Any]) -> tuple[str, list[tuple[str, str]]]:
        """Build the /events query."""
        usps = _state_param(params, required=True)
        assert usps is not None
        query = [
            ("include", "participants"),
            ("include", "agenda"),
            ("include", "links"),
            ("jurisdiction", jurisdiction_id(usps)),
        ]
        if params.args.after_date:
            query.append(("after", params.args.after_date))
        if params.args.before_date:
            query.append(("before", params.args.before_date))
        return "/events", query

    @classmethod
    def rows(cls, result: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per event."""
        jur = result.get("jurisdiction") or {}
        location = result.get("location") or {}
        return [
            {
                "event_id": result.get("id"),
                "state": usps_from_jurisdiction(jur.get("id")),
                "name": result.get("name") or "",
                "description": clean_str(result.get("description")),
                "classification": clean_str(result.get("classification")),
                "start_time": to_timestamp(result.get("start_date")),
                "end_time": to_timestamp(result.get("end_date")),
                "all_day": result.get("all_day"),
                "status": clean_str(result.get("status")),
                "location_name": clean_str(location.get("name")) if isinstance(location, dict) else None,
                "participants": [
                    {
                        "name": clean_str(p.get("name")),
                        "entity_type": clean_str(p.get("entity_type")),
                        "note": clean_str(p.get("note")),
                    }
                    for p in result.get("participants") or []
                    if isinstance(p, dict)
                ],
                "agenda": [
                    {"position": a.get("order"), "description": clean_str(a.get("description"))}
                    for a in result.get("agenda") or []
                    if isinstance(a, dict)
                ],
                "links": _links(result.get("links")),
            }
        ]
