# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.1,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- New `serve` extra (`vgi-python[http,oauth]`); the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.

## 0.1.1 (2026-11-16)

- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.0 (unreleased)

- First version: `states` table; keyless `legislators`, `legislator_roles`, `committees`,
  `committee_members` (Open States people repo), `election_returns` (Klarner 1967-2016),
  `legislator_ideology` / `chamber_ideology` (Shor-McCarty); `cache_info`, `refresh_cache`; and the
  Open States API v3 functions `os_jurisdictions`, `os_sessions`, `os_people`, `os_bills`, `os_votes`,
  `os_committees`, `os_events` (secret `TYPE openstates`).
