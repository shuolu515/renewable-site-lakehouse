"""Bounded, auditable ingestion for OpenStreetMap electricity-grid proxies."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import yaml

SOURCE_ID = "openstreetmap_overpass"
DEFAULT_ENDPOINT = "https://overpass-api.de/api/interpreter"
LICENSE = "Open Data Commons Open Database License (ODbL) 1.0"
LICENSE_URL = "https://www.openstreetmap.org/copyright"
ATTRIBUTION = "© OpenStreetMap contributors"
ALLOWED_POWER_TAGS = frozenset({"substation", "transformer", "line", "minor_line"})
USER_AGENT = "renewable-site-lakehouse/0.1 (+https://github.com/shuolu515/renewable-site-lakehouse)"


@dataclass(frozen=True)
class GridIngestionConfig:
    bbox: tuple[float, float, float, float]
    max_assets: int = 500
    endpoint: str = DEFAULT_ENDPOINT
    timeout_seconds: float = 60.0
    query_timeout_seconds: int = 25
    max_attempts: int = 3

    def validate(self) -> None:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        if not (-180 <= min_lon < max_lon <= 180):
            raise ValueError("bbox longitude values must satisfy -180 <= min < max <= 180")
        if not (-90 <= min_lat < max_lat <= 90):
            raise ValueError("bbox latitude values must satisfy -90 <= min < max <= 90")
        if not 1 <= self.max_assets <= 1000:
            raise ValueError("max_assets must be between 1 and 1000")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not 1 <= self.query_timeout_seconds <= 60:
            raise ValueError("query_timeout_seconds must be between 1 and 60")


def utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build_overpass_query(config: GridIngestionConfig) -> str:
    """Build a bounded Overpass QL query without contributor metadata."""

    config.validate()
    min_lon, min_lat, max_lon, max_lat = config.bbox
    bbox = f"{min_lat:.6f},{min_lon:.6f},{max_lat:.6f},{max_lon:.6f}"
    return "\n".join(
        [
            f"[out:json][timeout:{config.query_timeout_seconds}];",
            "(",
            f'  node["power"~"^(substation|transformer)$"]({bbox});',
            f'  way["power"~"^(substation|transformer)$"]({bbox});',
            f'  way["power"~"^(line|minor_line)$"]({bbox});',
            ");",
            f"out body geom {config.max_assets};",
        ]
    )


def _validate_overpass_payload(payload: object, max_assets: int) -> dict:
    if not isinstance(payload, dict):
        raise TypeError("Overpass response must be a JSON object")
    elements = payload.get("elements")
    if not isinstance(elements, list):
        raise TypeError("Overpass response elements must be a list")
    if len(elements) > max_assets:
        raise ValueError("Overpass returned more elements than the configured maximum")

    seen_keys: set[tuple[str, int]] = set()
    for index, element in enumerate(elements):
        if not isinstance(element, dict):
            raise TypeError(f"elements[{index}] must be an object")
        element_type = element.get("type")
        element_id = element.get("id")
        if element_type not in {"node", "way"} or not isinstance(element_id, int):
            raise ValueError(f"elements[{index}] has no supported OSM type and integer id")
        tags = element.get("tags")
        if not isinstance(tags, dict) or tags.get("power") not in ALLOWED_POWER_TAGS:
            raise ValueError(f"elements[{index}] has no supported power tag")
        key = (element_type, element_id)
        if key in seen_keys:
            raise ValueError(f"Overpass response contains duplicate element {key}")
        seen_keys.add(key)
    return payload


def fetch_grid_assets(
    config: GridIngestionConfig,
    *,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict, httpx.Response, int, str]:
    """Fetch one bounded OSM grid-proxy snapshot."""

    query = build_overpass_query(config)
    owns_client = client is None
    active_client = client or httpx.Client(
        timeout=config.timeout_seconds,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        follow_redirects=True,
    )
    try:
        last_error: Exception | None = None
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active_client.post(config.endpoint, data={"data": query})
                response.raise_for_status()
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    preview = " ".join(response.text[:160].split())
                    raise ValueError(f"Overpass did not return JSON: {preview}") from exc
                validated = _validate_overpass_payload(payload, config.max_assets)
                return validated, response, attempt, query
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                last_error = exc
                if attempt == config.max_attempts:
                    raise
                sleep(float(2 ** (attempt - 1)))
        raise RuntimeError("unreachable retry state") from last_error
    finally:
        if owns_client:
            active_client.close()


def _atomic_json_write(path: Path, payload: object) -> bytes:
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(encoded)
    temporary.replace(path)
    return encoded


def run_grid_ingestion(
    config: GridIngestionConfig,
    output_root: Path,
    *,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = utc_now,
    ingestion_id: str | None = None,
) -> dict:
    """Fetch grid proxies and write an immutable snapshot plus provenance manifest."""

    config.validate()
    run_id = ingestion_id or str(uuid.uuid4())
    started_at = now()
    payload, response, attempts, query = fetch_grid_assets(config, client=client, sleep=sleep)
    completed_at = now()

    run_directory = output_root / SOURCE_ID / run_id
    data_path = run_directory / "grid_assets.json"
    manifest_path = run_directory / "manifest.json"
    encoded = _atomic_json_write(data_path, payload)
    elements = payload["elements"]
    power_counts = Counter(element["tags"]["power"] for element in elements)
    element_type_counts = Counter(element["type"] for element in elements)

    manifest = {
        "schema_version": 1,
        "ingestion_id": run_id,
        "status": "succeeded",
        "source_id": SOURCE_ID,
        "endpoint": config.endpoint,
        "license": LICENSE,
        "license_url": LICENSE_URL,
        "attribution": ATTRIBUTION,
        "contains_osm_contributor_metadata": False,
        "started_at": _iso(started_at),
        "completed_at": _iso(completed_at),
        "duration_ms": round((completed_at - started_at).total_seconds() * 1000),
        "request": {
            "bbox": [float(value) for value in config.bbox],
            "max_assets": config.max_assets,
            "query_timeout_seconds": config.query_timeout_seconds,
            "query_sha256": hashlib.sha256(query.encode("utf-8")).hexdigest(),
        },
        "response": {
            "http_status": response.status_code,
            "content_type": response.headers.get("content-type"),
            "attempts": attempts,
            "element_count": len(elements),
            "power_tag_counts": dict(sorted(power_counts.items())),
            "element_type_counts": dict(sorted(element_type_counts.items())),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        },
        "files": {"raw_json": data_path.name},
        "capacity_status_default": "unknown",
        "data_boundary": (
            "Public OSM electricity assets are screening proxies only. They do not confirm "
            "connection feasibility, operator approval, or remaining grid capacity."
        ),
    }
    _atomic_json_write(manifest_path, manifest)
    return {**manifest, "run_directory": str(run_directory)}


def load_project_config(path: Path) -> GridIngestionConfig:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    region = config["region"]
    return GridIngestionConfig(
        bbox=tuple(float(value) for value in region["bbox"]),
        max_assets=int(region["max_grid_assets"]),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/pipeline.yml"),
        help="Project pipeline YAML configuration.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/raw"),
        help="Root directory for immutable raw snapshots.",
    )
    args = parser.parse_args()
    manifest = run_grid_ingestion(load_project_config(args.config), args.output_root)
    response = manifest["response"]
    print(
        f"Ingestion {manifest['ingestion_id']} succeeded: "
        f"{response['element_count']} grid proxies -> {manifest['run_directory']}"
    )


if __name__ == "__main__":
    main()
