"""Historical election returns (Klarner, CC0) and legislator ideology (Shor-McCarty, CC BY-NC-SA).

Both are Harvard Dataverse tab-separated exports, downloaded on first use. The Klarner file
(about 110 MB) is converted once into a Parquet file sorted by state next to it in the cache,
so later queries read only the requested state's row groups.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .common import Settings, ensure_dataset, pushed_range
from .datasets import KLARNER, SHOR_MCCARTY, SHOR_MCCARTY_AGG, PerStateSource
from .meta import CHAMBER_DOC, KLARNER_DOI, SHOR_MCCARTY_AGG_DOI, SHOR_MCCARTY_DOI, column, object_tags, result_columns
from .reference import STATE_ARG_DOC, STATES

# pyarrow-stubs' compute overloads reject valid calls; results are pinned to declared schemas.
pc: Any = _pc

_lock = threading.Lock()
_tab_memo: dict[tuple[str, float], pa.Table] = {}

CHAMBER_CHOICES = ["upper", "lower", "legislature"]


def read_tab(path: Path) -> pa.Table:
    """Read a Dataverse tab export (quoted strings, empty = NULL)."""
    return pacsv.read_csv(
        path,
        parse_options=pacsv.ParseOptions(delimiter="\t"),
        convert_options=pacsv.ConvertOptions(strings_can_be_null=True),
        read_options=pacsv.ReadOptions(block_size=16 << 20),
    )


def _memo_tab(path: Path) -> pa.Table:
    key = (str(path), path.stat().st_mtime)
    with _lock:
        if key not in _tab_memo:
            _tab_memo.clear()
            _tab_memo[key] = read_tab(path)
        return _tab_memo[key]


def _chamber_for(state: str, upper: bool) -> str:
    """Upper/lower, except Nebraska (and DC) whose single chamber is 'legislature'."""
    if state in ("NE", "DC"):
        return "legislature"
    return "upper" if upper else "lower"


def _year_bounds(params: ProcessParams[Any]) -> tuple[int | None, int | None]:
    lo, hi = pushed_range(params, "year")
    first = getattr(params.args, "first_year", None)
    last = getattr(params.args, "last_year", None)
    lo = max(v for v in (lo, first) if v is not None) if (lo, first) != (None, None) else None
    hi = min(v for v in (hi, last) if v is not None) if (hi, last) != (None, None) else None
    return (int(lo) if lo is not None else None, int(hi) if hi is not None else None)


# ---------------------------------------------------------------- Klarner election returns

ELECTION_SCHEMA = pa.schema(
    [
        column(
            "case_id", pa.int64(), "Klarner caseid: unique id of the candidate-election-party line.", nullable=False
        ),
        column("state", pa.string(), "USPS code, upper case.", nullable=False),
        column("state_fips", pa.string(), "Two-digit state FIPS code.", nullable=False),
        column("year", pa.int32(), "Election year.", nullable=False),
        column("election_date", pa.date32(), "Election date, when month and day are recorded."),
        column("chamber", pa.string(), "'upper', 'lower', or 'legislature' for Nebraska.", nullable=False),
        column(
            "district", pa.string(), "District designation as on the ballot (Klarner ddez), e.g. '12', 'Chittenden-1'."
        ),
        column("district_name", pa.string(), "Alphabetic part of the district designation (Klarner dname)."),
        column("district_number", pa.int32(), "Numeric part of the district designation (Klarner dno)."),
        column("geo_post", pa.int32(), "Geographic sub-district post (Klarner geopost)."),
        column("mmd_post", pa.int32(), "Multimember-district post/position (Klarner mmdpost)."),
        column(
            "district_type",
            pa.int32(),
            "Klarner dtype: 1 single-member; 2 multimember with posts; 3 multimember free-for-all; 4-6 staggered "
            "multimember; 7-9 floterial; 10 cumulative voting.",
        ),
        column("district_seats", pa.int32(), "Number of seats in the district (Klarner dseats)."),
        column("seats_up", pa.int32(), "Seats to be filled in this election (Klarner eseats)."),
        column(
            "election_type",
            pa.string(),
            "Klarner etype: 'g' general, 'gs' special with general, 'ssg' special statewide general, 's' special, "
            "'dp'/'rp' Democratic/Republican primary, 'ttp' top-two primary, 'la...' Louisiana, '...runoff'.",
            nullable=False,
        ),
        column(
            "general", pa.bool_(), "True for general elections (etype starting with 'g', or 'ssg').", nullable=False
        ),
        column("determines_seat", pa.bool_(), "Klarner deter: the winner sits at the start of the session."),
        column("term_years", pa.float64(), "Anticipated term length in years (NULL when unknown, code 9)."),
        column("candidate", pa.string(), "Standardized candidate name, lower case 'last, first m.' (Klarner cand)."),
        column("candidate_id", pa.int64(), "Klarner candid: stable across a candidate's elections.", nullable=False),
        column("party", pa.string(), "Simplified party: d, r, b (fused D+R), nonmaj, nonpart, writein, partymiss."),
        column("caucus_party", pa.string(), "One party per candidate-election-season (Klarner partyt)."),
        column("party_detail", pa.string(), "Detailed party label from the returns (Klarner party)."),
        column(
            "experience",
            pa.string(),
            "Prior service: inc (incumbent), other (other chamber), pastinc, pastother, pastboth, none.",
        ),
        column("incumbent", pa.bool_(), "True when experience = 'inc'."),
        column("votes", pa.int64(), "Votes for this candidate on this party line (per county where split, 2013+)."),
        column("outcome", pa.string(), "Klarner outcome: 'w' won, 'l' lost, 'r' advanced to runoff, 'na'."),
        column("won", pa.bool_(), "True when outcome = 'w'."),
        column("county_name", pa.string(), "County, for returns broken down by county (mostly 2013-2016)."),
        column("county_fips", pa.string(), "Three-digit county FIPS code for county-level rows."),
        column("uncertainty", pa.string(), "Klarner uncert: which variables are uncertain for this row."),
    ]
)

_KLARNER_PARQUET = "klarner-slers-1967-2016.parquet"


def _pad(arr: Any, width: int) -> Any:
    as_str = pc.cast(arr, pa.string())
    return pc.utf8_lpad(as_str, width, "0")


def klarner_to_table(raw: pa.Table) -> pa.Table:
    """Map the Klarner tab export to ``ELECTION_SCHEMA`` (vectorized)."""
    sab = pc.utf8_upper(raw["sab"])
    upper = pc.equal(raw["sen"], 1)
    unicameral = pc.is_in(sab, pa.array(["NE", "DC"]))
    chamber = pc.if_else(unicameral, "legislature", pc.if_else(upper, "upper", "lower"))
    year = pc.cast(raw["year"], pa.int32())
    month, day = raw["month"], raw["day"]
    has_date = pc.and_(pc.is_valid(month), pc.is_valid(day))
    date_text = pc.binary_join_element_wise(pc.cast(raw["year"], pa.string()), _pad(month, 2), _pad(day, 2), "-")
    election_date = pc.if_else(has_date, pc.cast(date_text, pa.date32(), safe=False), pa.scalar(None, pa.date32()))
    etype = pc.fill_null(raw["etype"], "")
    general = pc.or_(pc.starts_with(etype, "g"), pc.equal(etype, "ssg"))
    term = pc.cast(raw["term"], pa.float64())
    term = pc.if_else(pc.equal(term, 9.0), pa.scalar(None, pa.float64()), term)
    cfips = raw["cfips"]
    county_fips = pc.if_else(pc.less(cfips, 1000), _pad(cfips, 3), pa.scalar(None, pa.string()))
    exper = raw["exper"]
    outcome = raw["outcome"]

    def i32(name: str) -> Any:
        return pc.cast(raw[name], pa.int32())

    def text(name: str) -> Any:
        return pc.cast(raw[name], pa.string())

    columns = {
        "case_id": pc.cast(raw["caseid"], pa.int64()),
        "state": sab,
        "state_fips": _pad(raw["sfips"], 2),
        "year": year,
        "election_date": election_date,
        "chamber": chamber,
        "district": text("ddez"),
        "district_name": text("dname"),
        "district_number": i32("dno"),
        "geo_post": i32("geopost"),
        "mmd_post": i32("mmdpost"),
        "district_type": i32("dtype"),
        "district_seats": i32("dseats"),
        "seats_up": i32("eseats"),
        "election_type": etype,
        "general": general,
        "determines_seat": pc.equal(raw["deter"], 1),
        "term_years": term,
        "candidate": text("cand"),
        "candidate_id": pc.cast(raw["candid"], pa.int64()),
        "party": text("partyz"),
        "caucus_party": text("partyt"),
        "party_detail": text("party"),
        "experience": text("exper"),
        "incumbent": pc.equal(exper, "inc"),
        "votes": pc.cast(raw["vote"], pa.int64()),
        "outcome": text("outcome"),
        "won": pc.equal(outcome, "w"),
        "county_name": text("cname"),
        "county_fips": county_fips,
        "uncertainty": pc.utf8_trim_whitespace(text("uncert")),
    }
    table = pa.Table.from_arrays([columns[f.name] for f in ELECTION_SCHEMA], schema=ELECTION_SCHEMA)
    # Keep only the legislatures this catalog covers, sorted so Parquet row groups align with states.
    table = table.filter(pc.is_in(table["state"], pa.array(sorted(STATES))))
    return table.sort_by([("state", "ascending"), ("year", "ascending"), ("case_id", "ascending")])


def klarner_parquet(settings: Settings) -> Path:
    """Return the Parquet form of the cached Klarner file, building it once after a download."""
    cached = ensure_dataset(settings, KLARNER)
    target = cached.path.with_name(_KLARNER_PARQUET)
    stamp = str(cached.meta.get("sha256", ""))
    with _lock:
        if target.is_file():
            md = pq.read_schema(target).metadata or {}
            if md.get(b"source_sha256", b"").decode() == stamp:
                return target
        table = klarner_to_table(read_tab(cached.path))
        table = table.replace_schema_metadata({"source_sha256": stamp})
        tmp = target.with_name(f"{target.name}.{threading.get_ident()}.tmp")
        pq.write_table(table, tmp, row_group_size=20_000, compression="zstd")
        tmp.replace(target)
        return target


@dataclass(slots=True, frozen=True, kw_only=True)
class ElectionArgs:
    """``state``, ``first_year``, ``last_year``, ``chamber`` (all optional)."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]
    first_year: Annotated[
        int | None, Arg("first_year", default=None, doc="Earliest election year (1967-2016).", ge=1960, le=2100)
    ]
    last_year: Annotated[
        int | None, Arg("last_year", default=None, doc="Latest election year (1967-2016).", ge=1960, le=2100)
    ]
    chamber: Annotated[
        str | None,
        Arg("chamber", default=None, doc="'upper', 'lower' or 'legislature' (Nebraska).", choices=CHAMBER_CHOICES),
    ]


@bind_fixed_schema
@init_single_worker
class ElectionReturns(PerStateSource[ElectionArgs]):
    """Candidate-level state legislative election returns, 1967-2016 (Klarner)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ELECTION_SCHEMA

    class Meta:
        """Function metadata."""

        name = "election_returns"
        description = "State legislative election returns 1967-2016, one row per candidate (Klarner, CC0)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="State Legislative Election Returns",
                category="elections",
                keywords="election returns, state legislative elections, candidates, votes, incumbents, primaries, "
                "general elections, klarner, slers, vote share",
                doc_llm=(
                    "Carl Klarner's State Legislative Election Returns (1967-2016): one row per candidate per "
                    "election per party line, for general elections everywhere and many primaries/specials. "
                    "Key columns: state, year, election_date, chamber, district (ballot designation, `VARCHAR`), "
                    "election_type ('g' = general; filter `general` for all general-type contests), candidate, "
                    "candidate_id, party (d/r/nonmaj/nonpart/writein/b/partymiss), incumbent, votes, outcome, "
                    "won, seats_up, district_type. Some 2013-2016 returns are split by county (county_name "
                    "non-NULL) and fusion candidates can have several party lines, so SUM(votes) per "
                    "candidate_id and contest before computing shares. Arguments: state, first_year, last_year, "
                    "chamber; WHERE state/year filters are pushed down. The ~110 MB source is downloaded once. "
                    + CHAMBER_DOC
                ),
                doc_md=(
                    "## election_returns(state := NULL, first_year := NULL, last_year := NULL, chamber := NULL)\n\n"
                    "Candidate-level returns for state legislative elections 1967-2016. Sum `votes` over county "
                    "and party-line rows before computing vote shares.\n\n"
                    f"Source: [Klarner, State Legislative Election Returns, 1967-2016]({KLARNER_DOI}) "
                    "(Harvard Dataverse V3, CC0 1.0), downloaded on first use (about 110 MB)."
                ),
            ),
            "vgi.result_columns_schema": result_columns(ELECTION_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT year, round(avg(won::INT) FILTER (WHERE incumbent), 3) AS incumbent_win_rate "
                "FROM us_statelegs.main.election_returns(state := 'WI', chamber := 'lower') "
                "WHERE election_type = 'g' GROUP BY year ORDER BY year",
                description="How often Wisconsin Assembly incumbents won their general elections, by year",
            ),
            FunctionExample(
                sql="SELECT year, party, count(*) AS seats_won FROM us_statelegs.main.election_returns("
                "state := 'MN', first_year := 2000) WHERE general AND won GROUP BY ALL ORDER BY year, party",
                description="Seats won by party in Minnesota general elections since 2000",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[ElectionArgs], settings: Settings, state: str) -> pa.Table:
        """Read one state's row groups from the Parquet cache."""
        filters: list[Any] = [("state", "=", state)]
        lo, hi = _year_bounds(params)
        if lo is not None:
            filters.append(("year", ">=", lo))
        if hi is not None:
            filters.append(("year", "<=", hi))
        if params.args.chamber:
            filters.append(("chamber", "=", params.args.chamber))
        return pq.read_table(klarner_parquet(settings), filters=filters, schema=ELECTION_SCHEMA)


# ---------------------------------------------------------------- Shor-McCarty individual scores

IDEOLOGY_SCHEMA = pa.schema(
    [
        column(
            "legislator_id",
            pa.string(),
            "Shor-McCarty u_id, stable across a career (e.g. 'NE2007U001'); '<ST>:st_id:<n>' when the source has none.",
            nullable=False,
        ),
        column("name", pa.string(), "Name as recorded, usually 'Last, First'.", nullable=False),
        column("party", pa.string(), "'D', 'R' or 'X' (independent/other).", nullable=False),
        column("state", pa.string(), "USPS code, upper case.", nullable=False),
        column(
            "np_score",
            pa.float64(),
            "NP-score ideal point in common space; negative = liberal, positive = conservative.",
        ),
        column("year", pa.int32(), "Year the legislator served in this chamber.", nullable=False),
        column("chamber", pa.string(), "'upper', 'lower', or 'legislature' for Nebraska.", nullable=False),
        column("district", pa.string(), "District that year; numeric districts without leading zeros ('12')."),
    ]
)


def _norm_district(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return str(int(text)) if text.isdigit() else text


def ideology_rows(raw: pa.Table, state: str, lo: int | None, hi: int | None) -> list[dict[str, Any]]:
    """Unpivot the wide Shor-McCarty file (senateYYYY/houseYYYY/sdistrictYYYY/hdistrictYYYY) for one state."""
    sub = raw.filter(pc.equal(raw["st"], state))
    names = set(raw.column_names)
    years = sorted({int(c[-4:]) for c in names if c[:-4] in ("senate", "house") and c[-4:].isdigit()})
    years = [y for y in years if (lo is None or y >= lo) and (hi is None or y <= hi)]
    base = sub.select(["u_id", "st_id", "name", "party", "np_score"]).to_pylist()
    for b in base:
        if not b["u_id"]:
            # ~170 rows of the 2025 release have no u_id; fall back to the state-specific id.
            b["u_id"] = f"{state}:st_id:{int(float(b['st_id']))}" if b["st_id"] is not None else None
    rows: list[dict[str, Any]] = []
    for prefix, dprefix, upper in (("senate", "sdistrict", True), ("house", "hdistrict", False)):
        for year in years:
            served = sub[f"{prefix}{year}"].to_pylist()
            dcol = f"{dprefix}{year}"
            districts = sub[dcol].to_pylist() if dcol in names else [None] * len(served)
            for i, flag in enumerate(served):
                if flag is None or float(str(flag)) != 1.0:
                    continue
                b = base[i]
                if not b["name"] or not b["u_id"]:
                    continue
                rows.append(
                    {
                        "legislator_id": b["u_id"],
                        "name": b["name"],
                        "party": b["party"] or "X",
                        "state": state,
                        "np_score": b["np_score"],
                        "year": year,
                        "chamber": _chamber_for(state, upper),
                        "district": _norm_district(districts[i]),
                    }
                )
    rows.sort(key=lambda r: (r["year"], r["chamber"], r["legislator_id"]))
    return rows


@dataclass(slots=True, frozen=True, kw_only=True)
class IdeologyArgs:
    """``state``, ``first_year``, ``last_year`` (all optional)."""

    state: Annotated[str | None, Arg("state", default=None, doc=STATE_ARG_DOC)]
    first_year: Annotated[
        int | None, Arg("first_year", default=None, doc="Earliest year (data start in 1993).", ge=1960, le=2100)
    ]
    last_year: Annotated[
        int | None, Arg("last_year", default=None, doc="Latest year (data end in 2022).", ge=1960, le=2100)
    ]


_SM_TERMS = (
    "Shor-McCarty data are licensed CC BY-NC-SA 4.0 (non-commercial); they are downloaded at runtime by "
    "you and never redistributed by this worker. Cite Shor & McCarty (2011, 2022)."
)


@bind_fixed_schema
@init_single_worker
class LegislatorIdeology(PerStateSource[IdeologyArgs]):
    """Shor-McCarty NP-score ideology, one row per legislator per year served per chamber."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = IDEOLOGY_SCHEMA

    class Meta:
        """Function metadata."""

        name = "legislator_ideology"
        description = "Shor-McCarty state legislator ideology scores, one row per legislator-year-chamber, 1993-2022"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Legislator Ideology (Shor-McCarty)",
                category="elections",
                keywords="ideology, np-score, shor mccarty, polarization, ideal points, liberal, conservative",
                doc_llm=(
                    "Shor-McCarty NP-scores (common-space ideal points; negative = liberal, positive = "
                    "conservative, roughly -2..2) for state legislators 1993-2022, unpivoted to one row per "
                    "legislator per year served per chamber: legislator_id (u_id, stable for a career), name "
                    "('Last, First'), party ('D'/'R'/'X'), state, np_score (constant for a legislator), year, "
                    "chamber, district. Use count(DISTINCT legislator_id) when counting people. Arguments: "
                    "state, first_year, last_year; WHERE state/year filters are pushed down. Nebraska members "
                    "are coded chamber 'legislature'. January 2025 release, CC BY-NC-SA 4.0 (non-commercial)."
                ),
                doc_md=(
                    "## legislator_ideology(state := NULL, first_year := NULL, last_year := NULL)\n\n"
                    "Shor-McCarty NP-scores by legislator and year served.\n\n"
                    f"Source: [Shor-McCarty individual data, January 2025]({SHOR_MCCARTY_DOI}). " + _SM_TERMS
                ),
            ),
            "vgi.result_columns_schema": result_columns(IDEOLOGY_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT year, party, round(median(np_score), 3) AS median_np FROM "
                "us_statelegs.main.legislator_ideology(state := 'TX') WHERE chamber = 'lower' "
                "GROUP BY ALL ORDER BY year, party",
                description="Texas House party medians by year: how far apart the parties are",
            ),
            FunctionExample(
                sql="SELECT state, count(DISTINCT legislator_id) AS legislators, round(avg(np_score), 3) AS mean_np "
                "FROM us_statelegs.main.legislator_ideology(first_year := 2020) GROUP BY state ORDER BY mean_np",
                description="Average ideology of legislators serving since 2020, by state",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[IdeologyArgs], settings: Settings, state: str) -> pa.Table:
        """Unpivot one state's rows."""
        raw = _memo_tab(ensure_dataset(settings, SHOR_MCCARTY).path)
        lo, hi = _year_bounds(params)
        return pa.Table.from_pylist(ideology_rows(raw, state, lo, hi), schema=IDEOLOGY_SCHEMA)


# ---------------------------------------------------------------- Shor-McCarty chamber aggregates

_AGG_MEASURES = [
    ("chamber", "median", "Whole-chamber median NP-score."),
    ("dem", "dem_median", "Democratic party median NP-score."),
    ("rep", "rep_median", "Republican party median NP-score."),
    ("majority", "majority_median", "Majority party median NP-score."),
    ("minority", "minority_median", "Minority party median NP-score."),
    ("dem_mean", "dem_mean", "Democratic party mean NP-score."),
    ("rep_mean", "rep_mean", "Republican party mean NP-score."),
    ("diffs", "party_gap", "Distance between the party medians: the authors' preferred polarization measure."),
    ("distance", "member_distance", "Average distance between any two members: a party-free polarization measure."),
    ("dem_sd", "dem_sd", "Democratic heterogeneity (standard deviation)."),
    ("rep_sd", "rep_sd", "Republican heterogeneity (standard deviation)."),
    ("chamber_sd", "chamber_sd", "Whole-chamber heterogeneity (standard deviation)."),
]

AGG_SCHEMA = pa.schema(
    [
        column("state", pa.string(), "USPS code, upper case.", nullable=False),
        column("state_fips", pa.string(), "Two-digit state FIPS code.", nullable=False),
        column("year", pa.int32(), "Year.", nullable=False),
        column("chamber", pa.string(), "'upper', 'lower', or 'legislature' for Nebraska.", nullable=False),
        *[column(name, pa.float64(), doc) for _, name, doc in _AGG_MEASURES],
        column("median_error", pa.float64(), "Bootstrapped standard error of the chamber median."),
    ]
)


def _agg_col(raw: pa.Table, upper: bool, measure: str) -> list[Any]:
    """Resolve the source column for a measure; the file mixes 'hou_x'/'sen_x' and 'h_x'/'s_x' prefixes."""
    long_prefix, short_prefix = ("sen", "s") if upper else ("hou", "h")
    for name in (f"{long_prefix}_{measure}", f"{short_prefix}_{measure}"):
        if name in raw.column_names:
            return list(raw[name].to_pylist())
    return [None] * raw.num_rows


def aggregate_rows(raw: pa.Table, state: str, lo: int | None, hi: int | None) -> list[dict[str, Any]]:
    """Reshape one state's chamber-year aggregates into one row per chamber per year (empty rows dropped)."""
    sub = raw.filter(pc.equal(raw["st"], state))
    years = [int(float(str(y))) for y in sub["year"].to_pylist()]
    fips = STATES[state]["fips"]
    rows: list[dict[str, Any]] = []
    for upper in (True, False):
        values = {name: _agg_col(sub, upper, measure) for measure, name, _ in _AGG_MEASURES}
        errors = _agg_col(sub, upper, "chamber_error")
        for i, year in enumerate(years):
            if values["median"][i] is None or (lo is not None and year < lo) or (hi is not None and year > hi):
                continue
            rows.append(
                {
                    "state": state,
                    "state_fips": fips,
                    "year": year,
                    "chamber": _chamber_for(state, upper),
                    **{name: values[name][i] for name in values},
                    "median_error": errors[i],
                }
            )
    rows.sort(key=lambda r: (r["year"], r["chamber"]))
    return rows


@bind_fixed_schema
@init_single_worker
class ChamberIdeology(PerStateSource[IdeologyArgs]):
    """Shor-McCarty chamber medians, party medians and polarization by state, chamber and year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = AGG_SCHEMA

    class Meta:
        """Function metadata."""

        name = "chamber_ideology"
        description = "Shor-McCarty chamber and party ideology medians and polarization by state-chamber-year"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Chamber Ideology and Polarization",
                category="elections",
                keywords="polarization, chamber median, party median, ideology, shor mccarty, aggregate",
                doc_llm=(
                    "Shor-McCarty aggregate ideology for each state legislative chamber and year 1993-2022: "
                    "median (whole chamber), dem_median, rep_median, majority_median, minority_median, "
                    "dem_mean, rep_mean, party_gap (distance between party medians, the preferred "
                    "polarization measure), member_distance, standard deviations and median_error. Rows "
                    "without a chamber median (years with no roll-call data) are omitted. Arguments: state, "
                    "first_year, last_year. Nebraska is chamber 'legislature'. CC BY-NC-SA 4.0."
                ),
                doc_md=(
                    "## chamber_ideology(state := NULL, first_year := NULL, last_year := NULL)\n\n"
                    "Chamber-level ideology and polarization.\n\n"
                    f"Source: [Shor-McCarty aggregate data, January 2025]({SHOR_MCCARTY_AGG_DOI}). " + _SM_TERMS
                ),
            ),
            "vgi.result_columns_schema": result_columns(AGG_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT state, chamber, party_gap FROM us_statelegs.main.chamber_ideology(first_year := 2020, "
                "last_year := 2020) ORDER BY party_gap DESC LIMIT 10",
                description="The ten most polarized state legislative chambers in 2020",
            ),
            FunctionExample(
                sql="SELECT year, median, party_gap FROM us_statelegs.main.chamber_ideology(state := 'NE') "
                "ORDER BY year",
                description="Nebraska's unicameral Legislature drifting over time",
            ),
        ]

    @classmethod
    def rows_for_state(cls, params: ProcessParams[IdeologyArgs], settings: Settings, state: str) -> pa.Table:
        """Reshape one state's aggregate rows."""
        raw = _memo_tab(ensure_dataset(settings, SHOR_MCCARTY_AGG).path)
        lo, hi = _year_bounds(params)
        return pa.Table.from_pylist(aggregate_rows(raw, state, lo, hi), schema=AGG_SCHEMA)
