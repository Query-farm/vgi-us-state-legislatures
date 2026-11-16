"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
import json
import tarfile
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
import yaml
from vgi.arguments import Arguments
from vgi.client import Client

from us_statelegs_worker import common, datasets, elections, openstates_api, people, reference
from us_statelegs_worker.common import Settings, UpstreamError

SCHEMA = ["main"]
FIXTURES = Path(__file__).resolve().parent.parent / "test" / "fixtures"


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches)


# ---------------------------------------------------------------- reference


def test_states_reference() -> None:
    table = reference.states_table()
    assert table.num_rows == 52
    rows = {r["state"]: r for r in table.to_pylist()}
    assert rows["NE"]["chambers"] == ["legislature"] and rows["NE"]["unicameral"]
    assert rows["NE"]["state_fips"] == "31"
    assert rows["DC"]["jurisdiction_id"] == "ocd-jurisdiction/country:us/district:dc/government"
    assert rows["PR"]["jurisdiction_id"] == "ocd-jurisdiction/country:us/territory:pr/government"
    assert rows["AL"]["state_fips"] == "01"
    assert len({r["state_fips"] for r in rows.values()}) == 52


def test_normalize_state() -> None:
    assert reference.normalize_state("ne") == "NE"
    assert reference.normalize_state("31") == "NE"
    assert reference.normalize_state("6") == "CA"
    assert reference.normalize_state(None) is None
    with pytest.raises(ValueError, match="unknown state"):
        reference.normalize_state("ZZ")
    assert reference.usps_from_jurisdiction("ocd-jurisdiction/country:us/territory:pr/government") == "PR"
    assert reference.usps_from_jurisdiction("ocd-jurisdiction/country:us/government") is None


# ---------------------------------------------------------------- people repository


def _people_docs(state: str) -> tuple[list[tuple[dict[str, Any], bool]], list[dict[str, Any]]]:
    docs: list[tuple[dict[str, Any], bool]] = []
    committees: list[dict[str, Any]] = []
    with tarfile.open(FIXTURES / "people.tar.gz", "r:gz") as archive:
        for m in archive:
            parts = m.name.split("/")
            if len(parts) != 5 or parts[2] != state.lower() or not m.isfile():
                continue
            handle = archive.extractfile(m)
            assert handle is not None
            doc = yaml.safe_load(handle.read())
            if parts[3] == "committees":
                committees.append(doc)
            elif parts[3] in ("legislature", "retired"):
                docs.append((doc, parts[3] == "legislature"))
    return docs, committees


def test_legislator_rows_nebraska_unicameral() -> None:
    docs, _ = _people_docs("NE")
    rows = people.legislator_rows(docs, "NE")
    current = [r for r in rows if r["current"]]
    assert len(current) == 49
    assert {r["chamber"] for r in rows} == {"legislature"}
    spivey = next(r for r in rows if r["name"] == "Ashlei Spivey")
    assert spivey["district"] == "13" and spivey["party"] == "Nonpartisan"
    assert spivey["birth_date"] == dt.date(1986, 8, 15)
    assert spivey["instagram"] == "ashleiforne"


def test_retired_non_legislators_are_dropped() -> None:
    docs, _ = _people_docs("DC")
    rows = people.legislator_rows(docs, "DC")
    with_roles = [d for d, _ in docs if any(r.get("type") in people.LEGISLATIVE_TYPES for r in d.get("roles", []))]
    assert len(rows) == len(with_roles) < len(docs)  # mayors etc. are excluded


def test_role_rows_and_serving() -> None:
    docs, _ = _people_docs("NE")
    roles = people.role_rows(docs, "NE")
    assert all(r["district"] for r in roles)
    assert people._serving({"end_date": "2999-01-03"})
    assert not people._serving({"end_date": "2001-01-03"})
    assert people._serving({})


def test_committee_rows() -> None:
    _, committees = _people_docs("NE")
    rows = people.committee_rows(committees, "NE")
    agri = next(r for r in rows if r["name"] == "Agriculture")
    assert agri["chair"] == "Barry DeKay" and agri["members"] == 8
    members = people.member_rows(committees, "NE")
    assert any(m["role"] == "vice chair" and m["member_name"] == "Teresa Ibach" for m in members)


# ---------------------------------------------------------------- Klarner / Shor-McCarty


def test_klarner_mapping() -> None:
    table = elections.klarner_to_table(elections.read_tab(FIXTURES / "klarner.tab"))
    assert table.schema == elections.ELECTION_SCHEMA
    rows = table.to_pylist()
    ne = [r for r in rows if r["state"] == "NE"]
    assert {r["chamber"] for r in ne} == {"legislature"}
    assert {r["state_fips"] for r in ne} == {"31"}
    vt = [r for r in rows if r["state"] == "VT"]
    assert {r["chamber"] for r in vt} == {"upper", "lower"}
    assert all(r["election_date"] == dt.date(2016, 11, 8) for r in vt if r["election_type"] == "g")
    assert all(r["won"] == (r["outcome"] == "w") for r in vt)
    states = [str(r["state"]) for r in rows]
    assert states == sorted(states)


def test_ideology_unpivot() -> None:
    raw = elections.read_tab(FIXTURES / "shor_mccarty_individual.tab")
    rows = elections.ideology_rows(raw, "NE", 2010, 2012)
    assert rows and {r["chamber"] for r in rows} == {"legislature"}
    assert {r["year"] for r in rows} == {2010, 2011, 2012}
    assert all(r["district"] is None or not r["district"].startswith("0") for r in rows)
    vt = elections.ideology_rows(raw, "VT", None, None)
    assert {r["chamber"] for r in vt} == {"upper", "lower"}


def test_aggregate_rows() -> None:
    raw = elections.read_tab(FIXTURES / "shor_mccarty_aggregate.tab")
    ne = elections.aggregate_rows(raw, "NE", None, None)
    assert ne and {r["chamber"] for r in ne} == {"legislature"} and ne[0]["year"] == 1996
    vt = elections.aggregate_rows(raw, "VT", 2010, 2010)
    assert {r["chamber"] for r in vt} == {"upper", "lower"}
    assert all(r["party_gap"] is not None for r in vt)


# ---------------------------------------------------------------- download cache


def _settings(mock_api: str, tmp_path: Path, **kw: Any) -> Settings:
    return Settings(
        people_archive_url=f"{mock_api}/files/people.tar.gz",
        klarner_url=f"{mock_api}/files/klarner.tab",
        cache_dir=tmp_path,
        **kw,
    )


def test_ensure_dataset_downloads_then_caches(mock_api: str, tmp_path: Path) -> None:
    s = _settings(mock_api, tmp_path)
    first = common.ensure_dataset(s, datasets.PEOPLE)
    assert first.status == "downloaded" and first.path.is_file()
    assert common.ensure_dataset(s, datasets.PEOPLE).status == "cached"
    assert common.ensure_dataset(s, datasets.PEOPLE, force=True).status == "downloaded"
    expired = _settings(mock_api, tmp_path, max_age_days=0)
    assert common.ensure_dataset(expired, datasets.PEOPLE).status == "not_modified"


def test_ensure_dataset_offline_and_verification(mock_api: str, tmp_path: Path) -> None:
    with pytest.raises(UpstreamError, match="OFFLINE"):
        common.ensure_dataset(_settings(mock_api, tmp_path, offline=True), datasets.KLARNER)
    wrong = Settings(klarner_url=f"{mock_api}/files/people.tar.gz", cache_dir=tmp_path)
    with pytest.raises(UpstreamError, match="missing"):
        common.ensure_dataset(wrong, datasets.KLARNER)
    with pytest.raises(UpstreamError, match="HTTP 404"):
        common.ensure_dataset(Settings(klarner_url=f"{mock_api}/files/nope.tab", cache_dir=tmp_path), datasets.KLARNER)


def test_settings_from_options() -> None:
    s = common.settings_from_options({"CACHE_DIR": "/tmp/x", "OFFLINE": True, "openstates_api_url": "http://h/v3/"})
    assert s.cache_dir == Path("/tmp/x") and s.offline and s.openstates_api_url == "http://h/v3"
    assert common.settings_from_options(None).openstates_api_url == common.DEFAULT_OPENSTATES_API_URL


# ---------------------------------------------------------------- Open States API


def _params(**args: Any) -> Any:
    defaults = {"state": None, "session": None, "chamber": None, "classification": None, "subject": None}
    defaults.update({"updated_since": None, "q": None, "sponsor": None})
    defaults.update(args)
    return SimpleNamespace(args=SimpleNamespace(**defaults), current_pushdown_filters=None)


def test_bill_query_pushes_arguments() -> None:
    q = dict(openstates_api.bill_query(_params(state="ne", session="109", updated_since="2026-01-01"), ["votes"]))
    assert q["jurisdiction"] == "ocd-jurisdiction/country:us/state:ne/government"
    assert q["session"] == "109" and q["updated_since"] == "2026-01-01" and q["include"] == "votes"
    with pytest.raises(openstates_api.OpenStatesError, match="jurisdiction or search"):
        openstates_api.bill_query(_params(), [])


def test_bill_and_vote_rows_from_fixture() -> None:
    body = json.loads((FIXTURES / "openstates" / "bills_ne_p1.json").read_text())
    bill = openstates_api.OsBills.rows(body["results"][0])[0]
    assert bill["state"] == "NE" and bill["chamber"] == "legislature"
    assert bill["actions"][0]["description"] == "Date of introduction"
    votes = openstates_api.OsVotes.rows(body["results"][0])
    assert votes[0]["yes"] == 41 and votes[0]["no"] == 6 and votes[0]["other"] == 2
    pa.RecordBatch.from_pylist([bill], schema=openstates_api.BILL_SCHEMA)
    pa.RecordBatch.from_pylist(votes, schema=openstates_api.VOTE_SCHEMA)


def test_api_requires_secret(client: Client) -> None:
    with pytest.raises(Exception, match="TYPE openstates"):
        _table(client, "os_jurisdictions")


def test_mock_rejects_bad_key(mock_api: str) -> None:
    with pytest.raises(openstates_api.OpenStatesError, match="rejected the API key"):
        openstates_api.fetch_page(f"{mock_api}/v3/jurisdictions?x=1", "wrong", 0)
    body = openstates_api.fetch_page(f"{mock_api}/v3/jurisdictions?x=1", "test-key", 0)
    assert len(body["results"]) == 2
    assert urllib.request.urlopen(f"{mock_api}/health").status == 200
