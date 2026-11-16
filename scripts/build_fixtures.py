"""Rebuild the recorded fixtures in test/fixtures from full upstream downloads.

    uv run python scripts/build_fixtures.py <dir with the upstream files>

Expected in that directory (see DATA-SOURCES.md for URLs):
  people.tgz      codeload tarball of github.com/openstates/people (CC0)
  klarner.tab     Klarner SLERs 1967-2016 tab export, Dataverse file 3231005 (CC0)
  sm2023.tab      Shor-McCarty individual, April 2023, Dataverse file 7067107 (CC0)
  smagg2023.tab   Shor-McCarty aggregate, April 2023, Dataverse file 7067103 (CC0)

Only CC0 sources are subset into the repository. The worker's default Shor-McCarty URLs point
at the January 2025 release (CC BY-NC-SA 4.0), which is never bundled.
"""

from __future__ import annotations

import io
import sys
import tarfile
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "test" / "fixtures"
PEOPLE_STATES = {"ne", "dc"}


def people(src: Path) -> None:
    out = io.BytesIO()
    with tarfile.open(src / "people.tgz", "r:gz") as tin, tarfile.open(fileobj=out, mode="w:gz") as tout:
        for m in tin:
            parts = m.name.split("/")
            keep = len(parts) <= 2 or (
                parts[1] == "data"
                and (
                    (len(parts) >= 3 and parts[2] in PEOPLE_STATES)
                    # a couple of files that must be ignored: Congress and a governor
                    or (len(parts) == 5 and parts[2] == "us" and parts[4].startswith("Deb-Fischer"))
                )
            )
            if not keep or (m.isfile() and "municipalities" in m.name):
                continue
            tout.addfile(m, tin.extractfile(m) if m.isfile() else None)
    (OUT / "people.tar.gz").write_bytes(out.getvalue())


def tab_subset(src: Path, name: str, out_name: str, keep: object) -> None:
    lines = (src / name).read_text(encoding="utf-8").splitlines(keepends=True)
    header = lines[0].rstrip("\n").split("\t")
    rows = [line for line in lines[1:] if keep(dict(zip(header, line.rstrip("\n").split("\t"), strict=False)))]
    (OUT / out_name).write_text(lines[0] + "".join(rows), encoding="utf-8")


def main() -> None:
    src = Path(sys.argv[1])
    OUT.mkdir(parents=True, exist_ok=True)
    people(src)
    tab_subset(
        src,
        "klarner.tab",
        "klarner.tab",
        lambda r: (
            (r["sab"] == '"ne"' and r["year"] in ("2014", "2016"))
            or (r["sab"] == '"vt"' and r["year"] == "2016")
            or (r["sab"] == '"wi"' and r["year"] == "2016" and r["sen"] == "0")
        ),
    )
    tab_subset(src, "sm2023.tab", "shor_mccarty_individual.tab", lambda r: r["st"] in ('"NE"', '"VT"'))
    tab_subset(src, "smagg2023.tab", "shor_mccarty_aggregate.tab", lambda r: r["st"] in ('"NE"', '"VT"'))


if __name__ == "__main__":
    main()
