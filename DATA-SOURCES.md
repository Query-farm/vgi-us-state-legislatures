# Data sources

The worker bundles **no upstream dataset** in its package. `states` is a hand-written table of
public facts (USPS and FIPS codes, Open Civic Data identifiers, chamber names). Everything else is
downloaded at runtime into the per-user cache, or fetched live from the Open States API.

Retrieval date for everything below: **2026-11-15**.

## Runtime downloads (defaults)

| Dataset | URL | Licence | Size / SHA-256 at retrieval |
|---|---|---|---|
| Open States people repository (legislators, roles, parties, committees) | https://codeload.github.com/openstates/people/tar.gz/refs/heads/main (repo https://github.com/openstates/people) | CC0 1.0 (repo `LICENSE`) | 5,298,738 B, `9df30edb0180eedd87b2c25a4a05d7124edc43f5dfbf541460b3a125a3c4a6dd` (changes with every commit) |
| Klarner, *State Legislative Election Returns, 1967-2016*, Harvard Dataverse V3, doi:10.7910/DVN/3WZFK9 | https://dataverse.harvard.edu/api/access/datafile/3231005 (tab export of `196slers1967to2016_20180908.dta`) | CC0 1.0 (Dataverse licence field) | 112,736,336 B, `43ec3682e4fd81a105124c0b0e7e13c306cf1610fd996e533b03c2304634332b` |
| Shor & McCarty, *Individual State Legislator Shor-McCarty Ideology Data, January 2025 Update*, doi:10.7910/DVN/SGOQ7G | https://dataverse.harvard.edu/api/access/datafile/10853698 | **CC BY-NC-SA 4.0** | 6,827,030 B, `20de5f97feb680deac01262ae0e54bb7effe747533ee871aef8a390c9b93beec` |
| Shor & McCarty, *Aggregate State Legislator Shor-McCarty Ideology Data, January 2025 update*, doi:10.7910/DVN/T53FFK | https://dataverse.harvard.edu/api/access/datafile/10853749 | **CC BY-NC-SA 4.0** | 536,672 B, `2e288cbf1b44b96ad75c1e55a67d797c650fcf696fa2181c5354a3ac29a71f8f` |

The Shor-McCarty January 2025 files are non-commercial share-alike, so they are fetched at runtime
only and never committed. Downloads are verified by structure (tarball layout, expected tab-file
columns) rather than a pinned checksum, because the people repository changes continuously and
Dataverse regenerates tab exports; the SHA-256 of each cached copy is reported by `cache_info()`.

Live API: Open States API v3, https://v3.openstates.org (OpenAPI schema
https://v3.openstates.org/openapi.json, version 2021.11.12), governed by
https://open.pluralpolicy.com/tos/ and the caller's API key.

Not used: the Open States per-session bill/vote CSV/JSON archives (https://open.pluralpolicy.com/data/session-csv/)
— their download links are shown only to logged-in users, and this project does not create accounts.
The Open States bulk-data page states that data are public domain unless otherwise noted.

## Test fixtures (`test/fixtures`, committed)

| File | Content | Licence | SHA-256 |
|---|---|---|---|
| `people.tar.gz` | Subset of the people tarball above: `data/ne/**`, `data/dc/**` (minus municipalities), one `data/us/` file to test exclusion, repo top-level files | CC0 1.0 | `16680581536e0a890894c13fb4aa910d98dcbf8245b531cbf47496f3ed007438` |
| `klarner.tab` | Real Klarner rows: NE 2014 and 2016, VT 2016, WI 2016 lower house (964 rows + header) | CC0 1.0 | `5b67495da9cfb800209643c8792a451b54d68e6f818e182861e1a123aa8a19b3` |
| `shor_mccarty_individual.tab` | Real rows (NE, VT) of the **April 2023** individual release, doi:10.7910/DVN/NWSYOS, Dataverse file 7067107 | CC0 1.0 | `3a263e4f0ac96e485917de01a805d63fb579a36a17fb92d23a219813be0c69bd` |
| `shor_mccarty_aggregate.tab` | Real rows (NE, VT) of the **April 2023** aggregate release, doi:10.7910/DVN/WI8ERB, Dataverse file 7067103 | CC0 1.0 | `a5cafe5b001f08abead775ebb4fb1d4c1b50320dab55c9d4aa2d4f6f4d4e41cb` |
| `openstates/people_ne_p{1,2,3}.json` | `/people` responses built from the real NE people-repository rows above, in the API's `PersonList` shape | CC0 1.0 (derived) | p1 `2e37328e…9089585`, p2 `d77cebc9…5688437`, p3 `45695f5b…3febee5` |
| `openstates/jurisdictions.json`, `jurisdiction_ne.json`, `bills_ne_p{1,2}.json`, `committees_ne.json`, `events_ne.json` | **Synthetic** responses in the shapes of the OpenAPI schema (no API key was requested; bill titles say "synthetic fixture"). Committee names/ids and person ids are real people-repository values | written for this project | see `shasum -a 256 test/fixtures/openstates/*.json` |

The April 2023 Shor-McCarty files have the same layout as the January 2025 ones (one fewer pair of
year columns), so they exercise the same parser. `scripts/build_fixtures.py` rebuilds the
dataset subsets from full downloads.
