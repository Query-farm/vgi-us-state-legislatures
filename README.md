<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query US State Legislators, Elections, Bills and Votes in DuckDB

> **vgi-us-state-legislatures** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-us-state-legislatures/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-us-state-legislatures/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

The legislatures of the 50 states, the District of Columbia and Puerto Rico as SQL: every current
and former state legislator Open States tracks, their terms and committee assignments, candidate-level
state legislative election returns since 1967, Shor-McCarty ideology scores, and (with a free Open
States API key) live bills, roll-call votes, sessions and hearings. Most of the catalog needs no
account: open datasets are downloaded on first use and cached.

```sql
-- How each sitting Nebraska senator voted on a bill, with their district (needs an API key).
SELECT l.district, l.name, v.option
FROM us_statelegs.main.os_votes(state := 'NE', session := '109') o, unnest(o.votes) AS u(v)
JOIN us_statelegs.main.legislators(state := 'NE', current := true) l ON l.person_id = v.person_id
WHERE o.bill_identifier = 'LB 1'
ORDER BY l.district::INTEGER;
```

Identifiers are the standard ones, so results join across functions and with other workers:
Open States `ocd-person/...` and `ocd-jurisdiction/...` ids, USPS state codes (upper case),
two-digit state FIPS codes (`VARCHAR`, leading zero kept), and `district` as `VARCHAR` (Vermont and
New Hampshire name theirs, e.g. `'Chittenden-1'`).

**Chambers.** `chamber` is `'upper'` (state senate), `'lower'` (house, assembly or house of delegates)
or `'legislature'`. Nebraska's Legislature is unicameral and nonpartisan: its 49 members are
officially senators but every source here codes them `'legislature'` (Klarner and Shor-McCarty file
them as a senate; this worker normalises them so they join with Open States). The Council of the
District of Columbia is the other single-chamber body.

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'us_statelegs' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-us-state-legislatures@v0.1.0 vgi-us-statelegs-worker');

SELECT chamber, party, count(*) AS members
FROM us_statelegs.main.legislators(state := 'NC', current := true)
GROUP BY ALL ORDER BY chamber, members DESC;
```

**Haybarn 2.0** (`uvx haybarn-cli`): use `INSTALL vgi FROM community; LOAD vgi;` and the same `ATTACH`.

**From a checkout:** run `make build`, then use
`LOCATION '/abs/path/to/vgi-us-state-legislatures/.venv/bin/vgi-us-statelegs-worker'`.

### Open States API key

The `os_*` functions call the [Open States API v3](https://docs.openstates.org/api-v3/) (run by
Plural). Register free at <https://open.pluralpolicy.com/accounts/signup/>, then:

```sql
CREATE SECRET (TYPE openstates, API_KEY 'your-key');
SELECT identifier, title, latest_action_date
FROM us_statelegs.main.os_bills(state := 'NE', session := '109')
ORDER BY latest_action_date DESC LIMIT 10;
```

Without the secret, an `os_*` call fails with a message naming `TYPE openstates`. The key is read from
the secret on every request and never stored in the scan cursor. The free tier is rate limited, so
requests are spaced by `OPENSTATES_MIN_INTERVAL` seconds (default 1) and HTTP 429 is retried honouring
`Retry-After`.

## Functions

All functions live in `us_statelegs.main`.

| Function | Source | Key? | What it returns |
|---|---|---|---|
| `states` (table) | bundled | no | The 52 legislatures: USPS, FIPS, Open States jurisdiction id, chamber codes and names, unicameral flag |
| `legislators(state := , current := )` | Open States people repo | no | One row per current or former legislator: ocd-person id, chamber/district/party of latest seat, contact details, party history, other ids |
| `legislator_roles(state := )` | Open States people repo | no | One row per seat held: chamber, district, start/end dates, end reason |
| `committees(state := )` | Open States people repo | no | Current committees: chamber, classification, parent, roster size, chair |
| `committee_members(state := )` | Open States people repo | no | One row per committee roster entry, with the member's ocd-person id |
| `election_returns(state := , first_year := , last_year := , chamber := )` | Klarner SLERs | no | Candidate-level returns 1967-2016: district, election type, candidate, party, incumbency, votes, outcome |
| `legislator_ideology(state := , first_year := , last_year := )` | Shor-McCarty | no | NP-score per legislator, one row per year served per chamber, 1993-2022 |
| `chamber_ideology(state := , first_year := , last_year := )` | Shor-McCarty | no | Chamber and party medians and polarization per state-chamber-year |
| `cache_info()` | local cache | no | Each downloaded dataset: licence, URL, cached?, size, SHA-256, download time (never downloads) |
| `refresh_cache(dataset)` | upstream | no | Download one dataset again now |
| `os_jurisdictions()` | Open States API | yes | Tracked states and when their bills/people were last updated |
| `os_sessions(state := )` | Open States API | yes | Legislative sessions (identifiers for `session :=`), dates, bulk-archive links |
| `os_people(state := , name := , district := , chamber := )` | Open States API | yes | Current legislators, live |
| `os_bills(state := , session := , chamber := , classification := , subject := , updated_since := , q := , sponsor := )` | Open States API | yes | Bills with sponsors and actions, newest update first |
| `os_votes(...same as os_bills)` | Open States API | yes | One row per roll-call vote with counts and each legislator's vote |
| `os_committees(state, chamber := , classification := )` | Open States API | yes | Committees with rosters, live |
| `os_events(state, after_date := , before_date := )` | Open States API | yes | Hearings, committee meetings, floor sessions |

`state` accepts a USPS code in any case or a two-digit FIPS code. Notes:

- **Election returns.** Some 2013-2016 returns are split by county and fusion candidates have one row
  per party line: `SUM(votes)` per `candidate_id` and contest before computing vote shares. `election_type
  = 'g'` is a regular general election; the `general` column also covers specials held with it.
- **Ideology.** `np_score` is constant per legislator; count people with `count(DISTINCT legislator_id)`.
- **Former legislators** in the people data are mostly those tracked since about 2010; Open States
  start dates are often missing for older terms.

## What pushes down

| Function | Pushed to the source |
|---|---|
| keyless functions | `state` argument and `WHERE state = …` / `IN (…)`: only those states' files are parsed |
| `election_returns`, `legislator_ideology`, `chamber_ideology` | also `first_year`/`last_year` and `WHERE year >= / <= / =`; `chamber` for election returns (Parquet row-group filters) |
| `os_people` | `state`, `name`, `district`, `chamber` (also `WHERE district = …`, `WHERE chamber = …`) |
| `os_bills`, `os_votes` | `state`, `session`, `chamber` (argument or `WHERE … =`), `classification`, `subject`, `q`, `sponsor`, `updated_since` (or `WHERE updated_at >= …`) |
| `os_committees`, `os_events`, `os_sessions` | `state`; `chamber`/`classification`; `after_date`/`before_date` |

Every pushed predicate is also re-applied to the rows, so results are exact either way.

## Downloads and the cache

Nothing is downloaded at import or `ATTACH` time. Each dataset is fetched the first time a function
needs it, verified (tarball layout or the expected tab-file header) and kept in the per-user cache
directory (`~/Library/Caches/vgi-us-state-legislatures` on macOS, `~/.cache/vgi-us-state-legislatures`
on Linux):

| Dataset | Size | Refresh |
|---|---|---|
| Open States people repository tarball | ~5 MB | re-checked (ETag) once older than `MAX_AGE_DAYS` (7) |
| Klarner election returns (tab) | ~110 MB, plus a ~10 MB Parquet copy built once | static dataset |
| Shor-McCarty individual / aggregate (tab) | ~7 MB / ~0.5 MB | static dataset |

ATTACH options: `CACHE_DIR`, `MAX_AGE_DAYS`, `OFFLINE` (never download; use the cache only),
`OPENSTATES_API_URL`, `OPENSTATES_MIN_INTERVAL`, `PEOPLE_ARCHIVE_URL`, `KLARNER_URL`,
`SHOR_MCCARTY_URL`, `SHOR_MCCARTY_AGGREGATE_URL` (point these at mirrors; the tests point them at
fixtures). `cache_info()` reports what is cached; `refresh_cache('people')` forces a download.

**Bills and votes without a key.** Open States publishes per-session bill/vote CSV and JSON archives,
but the download links are shown only to logged-in users of open.pluralpolicy.com, so this worker
does not fetch them. `os_sessions()` returns the archive links the API reports.

## Testing

```bash
make check        # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-unit    # pytest only
make test-sql     # SQL tests through DuckDB (Haybarn's sqllogictest runner)
```

Tests run offline. `test/mock_api/server.py` serves `test/fixtures`: a subset of the real Open States
people repository (Nebraska and DC), real Klarner rows (NE 2014/2016, VT 2016, WI 2016 house), real
rows of the CC0 April 2023 Shor-McCarty release (NE, VT), and Open States API responses (people built
from the real people rows; bills, votes, committees and events synthetic, shaped after the API's
OpenAPI schema). `scripts/build_fixtures.py` rebuilds the dataset subsets from full downloads. See
[DATA-SOURCES.md](DATA-SOURCES.md).

The CI metadata lint runs with `execute: false` because the `os_*` examples need an Open States API key.

## Data terms

The code licence below does not cover the data. Upstream terms:

- **Open States people data** ([openstates/people](https://github.com/openstates/people)): CC0 1.0.
  Open States bulk data are public domain unless noted; attribution to Open States/Plural is appreciated.
- **Open States API**: use is governed by the Open States / Plural terms of service
  (<https://open.pluralpolicy.com/tos/>) and your API key.
- **Klarner, State Legislative Election Returns, 1967-2016** (<https://doi.org/10.7910/DVN/3WZFK9>): CC0 1.0.
  Please cite Klarner (2018).
- **Shor-McCarty ideology data, January 2025 update** (individual <https://doi.org/10.7910/DVN/SGOQ7G>,
  aggregate <https://doi.org/10.7910/DVN/T53FFK>): **CC BY-NC-SA 4.0 — non-commercial, share-alike**.
  This worker downloads them at runtime on your behalf and never redistributes them; your use must
  comply with that licence. Cite Shor & McCarty (2011) and Shor & McCarty (2022). The test fixtures
  instead use the April 2023 release (CC0 1.0).

## Support

Open an issue at <https://github.com/Query-farm/vgi-us-state-legislatures/issues>.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Written by [Query.Farm](https://query.farm) and released under the
[BSD Zero Clause License](LICENSE) (0BSD).
