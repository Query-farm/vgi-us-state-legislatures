"""The runtime-downloaded datasets, how each is verified, and a base class for functions that read them.

Every keyless function reads one of these files from the per-user cache, downloading it
on first use. Downloads are verified before they replace the cached copy.
"""

from __future__ import annotations

import gzip
import io
import tarfile
from dataclasses import dataclass, field
from typing import Any, ClassVar, TypeVar

import pyarrow as pa
from vgi.table_function import OutputCollector, ProcessParams, TableFunctionGenerator
from vgi_rpc import ArrowSerializableDataclass

from .common import Dataset, Settings, UpstreamError, pushed_values, settings_of
from .meta import KLARNER_DOI, PEOPLE_REPO, SHOR_MCCARTY_AGG_DOI, SHOR_MCCARTY_DOI
from .reference import STATES, normalize_state

KLARNER_REQUIRED = {"caseid", "year", "sab", "sfips", "sen", "ddez", "cand", "candid", "partyz", "vote", "outcome"}
SHOR_MCCARTY_REQUIRED = {"name", "party", "st", "st_id", "u_id", "np_score"}
SHOR_MCCARTY_AGG_REQUIRED = {"st", "fips", "year", "hou_chamber", "sen_chamber", "h_diffs", "s_diffs"}


def _verify_people(data: bytes) -> None:
    """Check that ``data`` is a gzip tarball of the openstates/people repository."""
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for count, member in enumerate(archive):
                parts = member.name.split("/")
                if len(parts) >= 5 and parts[1] == "data" and parts[3] == "legislature":
                    return
                if count > 50_000:
                    break
    except (tarfile.TarError, gzip.BadGzipFile, EOFError) as err:
        raise UpstreamError(f"the Open States people download is not a readable tar.gz: {err}") from err
    raise UpstreamError("the Open States people download has no data/<state>/legislature/ files")


def _tab_verifier(name: str, required: set[str]) -> Any:
    def verify(data: bytes) -> None:
        header = data[:20_000].split(b"\n", 1)[0].decode("utf-8", errors="replace")
        columns = {c.strip().strip('"') for c in header.split("\t")}
        missing = required - columns
        if missing:
            raise UpstreamError(
                f"the {name} download is not the expected tab-separated file; missing {sorted(missing)}"
            )

    return verify


PEOPLE = Dataset(
    key="people",
    filename="openstates-people.tar.gz",
    description="Open States people repository: legislators, terms, parties, committees",
    licence="CC0-1.0",
    source_page=PEOPLE_REPO,
    url_of=lambda s: s.people_archive_url,
    verify=_verify_people,
    refreshes=True,
)
KLARNER = Dataset(
    key="klarner_election_returns",
    filename="klarner-slers-1967-2016.tab",
    description="Klarner, State Legislative Election Returns 1967-2016 (Harvard Dataverse, V3)",
    licence="CC0-1.0",
    source_page=KLARNER_DOI,
    url_of=lambda s: s.klarner_url,
    verify=_tab_verifier("Klarner election returns", KLARNER_REQUIRED),
)
SHOR_MCCARTY = Dataset(
    key="shor_mccarty_individual",
    filename="shor-mccarty-individual.tab",
    description="Shor-McCarty individual state legislator ideology, January 2025 update",
    licence="CC-BY-NC-SA-4.0",
    source_page=SHOR_MCCARTY_DOI,
    url_of=lambda s: s.shor_mccarty_url,
    verify=_tab_verifier("Shor-McCarty individual", SHOR_MCCARTY_REQUIRED),
)
SHOR_MCCARTY_AGG = Dataset(
    key="shor_mccarty_aggregate",
    filename="shor-mccarty-aggregate.tab",
    description="Shor-McCarty state legislative chamber aggregates, January 2025 update",
    licence="CC-BY-NC-SA-4.0",
    source_page=SHOR_MCCARTY_AGG_DOI,
    url_of=lambda s: s.shor_mccarty_aggregate_url,
    verify=_tab_verifier("Shor-McCarty aggregate", SHOR_MCCARTY_AGG_REQUIRED),
)

DATASETS: dict[str, Dataset] = {d.key: d for d in (PEOPLE, KLARNER, SHOR_MCCARTY, SHOR_MCCARTY_AGG)}


@dataclass(kw_only=True)
class StateCursor(ArrowSerializableDataclass):
    """Which states are left to emit (one state per batch)."""

    states: list[str] = field(default_factory=list)
    pos: int = 0


A = TypeVar("A")


def requested_states(params: ProcessParams[Any]) -> list[str]:
    """Combine the ``state`` argument with a pushed ``state = / IN`` filter into the states to read."""
    wanted: set[str] = set(STATES)
    arg = getattr(params.args, "state", None)
    if arg is not None:
        wanted &= {normalize_state(arg) or ""}
    pushed = pushed_values(params, "state")
    if pushed is not None:
        allowed = set()
        for value in pushed:
            if value is None:
                continue
            try:
                allowed.add(normalize_state(str(value)) or "")
            except ValueError:
                continue  # an unknown code matches nothing
        wanted &= allowed
    return sorted(wanted)


class PerStateSource(TableFunctionGenerator[A, StateCursor]):
    """Base for dataset-backed functions: emit one batch per requested state."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])

    @classmethod
    def rows_for_state(cls, params: ProcessParams[Any], settings: Settings, state: str) -> pa.Table:
        """Return the rows of ``state`` conforming to ``FIXED_SCHEMA``."""
        raise NotImplementedError

    @classmethod
    def prefetch(cls, settings: Settings, states: list[str]) -> None:
        """Warm per-process caches for the remaining ``states`` in one pass (optional)."""

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> StateCursor:
        """Resolve the states to read from the arguments and pushed-down filters."""
        return StateCursor(states=requested_states(params))

    @classmethod
    def process(cls, params: ProcessParams[Any], state: StateCursor, out: OutputCollector) -> None:
        """Emit the next state's rows; skip states with no rows; finish after the last."""
        settings = settings_of(params)
        if state.pos < len(state.states):
            cls.prefetch(settings, state.states[state.pos :])
        while state.pos < len(state.states):
            usps = state.states[state.pos]
            state.pos += 1
            table = cls.rows_for_state(params, settings, usps)
            if table.num_rows:
                # Exactly one data batch per process call.
                out.emit(table.cast(params.output_schema).combine_chunks().to_batches()[0])
                return
        out.finish()
