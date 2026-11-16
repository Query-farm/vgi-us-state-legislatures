"""``cache_info()`` and ``refresh_cache(dataset)``: what has been downloaded, and a forced refresh."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from .common import Dataset, Settings, ensure_dataset, read_meta, settings_of, to_timestamp
from .datasets import DATASETS
from .meta import column, object_tags, result_columns

CACHE_SCHEMA = pa.schema(
    [
        column(
            "dataset", pa.string(), "Dataset key: people, klarner_election_returns, shor_mccarty_*.", nullable=False
        ),
        column("description", pa.string(), "What the dataset is.", nullable=False),
        column("licence", pa.string(), "SPDX licence of the upstream data.", nullable=False),
        column("source_url", pa.string(), "URL the file is (or would be) downloaded from.", nullable=False),
        column("source_page", pa.string(), "Landing page / DOI of the dataset.", nullable=False),
        column(
            "status",
            pa.string(),
            "'missing', 'cached', or after a refresh 'downloaded'/'not_modified'/'stale'.",
            nullable=False,
        ),
        column("cached", pa.bool_(), "True when a verified copy is in the cache.", nullable=False),
        column("path", pa.string(), "Path of the cached file.", nullable=False),
        column("bytes", pa.int64(), "Size of the cached file."),
        column("sha256", pa.string(), "SHA-256 of the cached file."),
        column("downloaded_at", pa.timestamp("us", tz="UTC"), "When the cached copy was downloaded."),
        column("checked_at", pa.timestamp("us", tz="UTC"), "When upstream was last checked for a newer copy."),
    ]
)


def cache_row(settings: Settings, ds: Dataset, status: str | None = None) -> dict[str, Any]:
    """Describe one dataset's cache entry."""
    path = settings.cache_dir / ds.filename
    meta = read_meta(settings, ds)
    url = ds.url_of(settings)
    cached = path.is_file() and meta.get("source_url") == url
    return {
        "dataset": ds.key,
        "description": ds.description,
        "licence": ds.licence,
        "source_url": url,
        "source_page": ds.source_page,
        "status": status or ("cached" if cached else "missing"),
        "cached": cached,
        "path": str(path),
        "bytes": meta.get("bytes") if cached else None,
        "sha256": meta.get("sha256") if cached else None,
        "downloaded_at": to_timestamp(meta.get("downloaded_at")) if cached else None,
        "checked_at": to_timestamp(meta.get("checked_at")) if cached else None,
    }


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """``cache_info()`` takes no arguments."""


@bind_fixed_schema
@init_single_worker
class CacheInfo(TableFunctionGenerator[NoArgs, None]):
    """One row per runtime-downloaded dataset, describing its cache entry."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "cache_info"
        description = "What each runtime-downloaded dataset is, its licence, and whether it is cached locally"
        tags = {
            **object_tags(
                title="Dataset Cache Info",
                category="cache",
                keywords="cache, provenance, licence, download, vintage, dataset, sha256",
                doc_llm=(
                    "One row per dataset this worker downloads at runtime (people, klarner_election_returns, "
                    "shor_mccarty_individual, shor_mccarty_aggregate): description, licence, source_url, "
                    "source_page, status ('missing' or 'cached'), cached, path, bytes, sha256, downloaded_at, "
                    "checked_at. Never downloads anything. Use it to cite vintages or check what is cached."
                ),
                doc_md="## cache_info()\n\nProvenance and cache status of every downloaded dataset. Never downloads.",
            ),
            "vgi.result_columns_schema": result_columns(CACHE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT dataset, licence, cached, downloaded_at FROM us_statelegs.main.cache_info() "
                "ORDER BY dataset",
                description="Which datasets are cached, and under what licence",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit one row per dataset."""
        settings = settings_of(params)
        rows = [cache_row(settings, ds) for ds in DATASETS.values()]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        out.finish()


@dataclass(slots=True, frozen=True, kw_only=True)
class RefreshArgs:
    """``refresh_cache(dataset)``."""

    dataset: Annotated[
        str,
        Arg(0, doc="Dataset to download again now (see cache_info().dataset).", choices=sorted(DATASETS)),
    ]


@bind_fixed_schema
@init_single_worker
class RefreshCache(TableFunctionGenerator[RefreshArgs, None]):
    """Download one dataset again now, replacing the cached copy."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "refresh_cache"
        description = "Download one dataset again now and report the new cache entry"
        tags = {
            **object_tags(
                title="Refresh Dataset Cache",
                category="cache",
                keywords="refresh, re-download, update cache, force download",
                doc_llm=(
                    "Download `dataset` (one of cache_info().dataset) again unconditionally, verify it, replace "
                    "the cached copy and return its cache_info row with status 'downloaded'. Only needed to pick "
                    "up upstream corrections early; the people data are re-checked weekly on their own."
                ),
                doc_md="## refresh_cache(dataset)\n\nForce a fresh download of one dataset.",
            ),
            "vgi.result_columns_schema": result_columns(CACHE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT dataset, status, bytes FROM us_statelegs.main.refresh_cache('people')",
                description="Pull the latest Open States people data now",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[RefreshArgs], state: None, out: OutputCollector) -> None:
        """Download and report."""
        settings = settings_of(params)
        ds = DATASETS[params.args.dataset]
        result = ensure_dataset(settings, ds, force=True)
        out.emit(pa.RecordBatch.from_pylist([cache_row(settings, ds, result.status)], schema=params.output_schema))
        out.finish()
