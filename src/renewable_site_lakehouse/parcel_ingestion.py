"""Bounded, auditable ingestion for Hessen ALKIS cadastral parcels."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml

SOURCE_ID = "hessen_alkis_inspire_wfs"
COLLECTION_ID = "cp:CadastralParcel"
DEFAULT_ENDPOINT = (
    "https://www.geoportal.hessen.de/spatial-objects/710/collections/cp:CadastralParcel/items"
)
ALLOWED_LIMITS = frozenset({1, 5, 10, 20, 50, 100, 200, 500})
USER_AGENT = "renewable-site-lakehouse/0.1 educational-portfolio"


@dataclass(frozen=True)
class ParcelIngestionConfig:
    bbox: tuple[float, float, float, float]
    limit: int = 200
    max_features: int = 1000
    endpoint: str = DEFAULT_ENDPOINT
    timeout_seconds: float = 45.0
    max_attempts: int = 3

    def validate(self) -> None:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        if not (-180 <= min_lon < max_lon <= 180):
            raise ValueError("bbox longitude values must satisfy -180 <= min < max <= 180")
        if not (-90 <= min_lat < max_lat <= 90):
            raise ValueError("bbox latitude values must satisfy -90 <= min < max <= 90")
        if self.limit not in ALLOWED_LIMITS:
            allowed = ", ".join(str(value) for value in sorted(ALLOWED_LIMITS))
            raise ValueError(f"limit must be one of: {allowed}")
        if not self.limit <= self.max_features <= 5000:
            raise ValueError("max_features must be between limit and 5000")
        if self.max_features % self.limit != 0:
            raise ValueError("max_features must be a multiple of limit")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


def utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _request_params(config: ParcelIngestionConfig) -> dict[str, str | int]:
    return {
        "f": "json",
        "bbox": ",".join(f"{value:.6f}" for value in config.bbox),
        "limit": config.limit,
    }


def _validate_feature_collection(payload: object, limit: int) -> dict:
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
        raise ValueError("API response is not a GeoJSON FeatureCollection")
    features = payload.get("features")
    if not isinstance(features, list):
        raise TypeError("FeatureCollection.features must be a list")
    if len(features) > limit:
        raise ValueError("API returned more features than the requested limit")
    for index, feature in enumerate(features):
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ValueError(f"features[{index}] is not a GeoJSON Feature")
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or not geometry.get("type"):
            raise ValueError(f"features[{index}] has no valid geometry object")
        if not isinstance(feature.get("properties"), dict):
            raise TypeError(f"features[{index}].properties must be an object")
    return payload


@dataclass(frozen=True)
class ParcelFetchResult:
    payload: dict
    last_response: httpx.Response
    total_attempts: int
    page_count: int
    number_matched: int | None
    matched_count_difference: int | None
    truncated: bool


def _next_link(payload: dict, endpoint: str) -> str | None:
    links = payload.get("links", [])
    if not isinstance(links, list):
        raise TypeError("FeatureCollection.links must be a list when present")
    next_links = [link for link in links if isinstance(link, dict) and link.get("rel") == "next"]
    if len(next_links) > 1:
        raise ValueError("API response contains more than one next link")
    if not next_links:
        return None
    href = next_links[0].get("href")
    if not isinstance(href, str) or not href:
        raise ValueError("API next link has no valid href")
    expected = urlparse(endpoint)
    actual = urlparse(href)
    if actual.scheme != "https" or actual.hostname != expected.hostname:
        raise ValueError("API next link points outside the configured HTTPS host")
    return href


def _feature_key(feature: dict, index: int) -> str:
    properties = feature["properties"]
    key = properties.get("localId") or feature.get("id")
    if not isinstance(key, str) or not key.strip():
        raise ValueError(f"features[{index}] has no stable parcel identifier")
    return key.strip()


def _fetch_page(
    client: httpx.Client,
    config: ParcelIngestionConfig,
    *,
    url: str,
    params: dict[str, str | int] | None,
    sleep: Callable[[float], None],
) -> tuple[dict, httpx.Response, int]:
    last_error: Exception | None = None
    for attempt in range(1, config.max_attempts + 1):
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                preview = " ".join(response.text[:160].split())
                raise ValueError(f"API did not return JSON: {preview}") from exc
            return _validate_feature_collection(payload, config.limit), response, attempt
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
            if attempt == config.max_attempts:
                raise
            sleep(float(2 ** (attempt - 1)))
    raise RuntimeError("unreachable retry state") from last_error


def fetch_parcels(
    config: ParcelIngestionConfig,
    *,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ParcelFetchResult:
    """Follow official next links and return one bounded, deduplicated snapshot."""

    config.validate()
    owns_client = client is None
    active_client = client or httpx.Client(
        timeout=config.timeout_seconds,
        headers={"Accept": "application/geo+json", "User-Agent": USER_AGENT},
        follow_redirects=True,
    )
    try:
        features: list[dict] = []
        seen_keys: set[str] = set()
        page_count = 0
        total_attempts = 0
        number_matched: int | None = None
        next_url: str | None = config.endpoint
        params: dict[str, str | int] | None = _request_params(config)
        first_payload: dict | None = None
        last_response: httpx.Response | None = None

        while next_url and len(features) < config.max_features:
            payload, response, attempts = _fetch_page(
                active_client,
                config,
                url=next_url,
                params=params,
                sleep=sleep,
            )
            first_payload = first_payload or payload
            last_response = response
            page_count += 1
            total_attempts += attempts
            if page_count == 1 and isinstance(payload.get("numberMatched"), int):
                number_matched = payload["numberMatched"]

            page_features = payload["features"]
            for feature in page_features:
                key = _feature_key(feature, len(features))
                if key in seen_keys:
                    raise ValueError(f"API returned duplicate parcel identifier: {key}")
                seen_keys.add(key)
                features.append(feature)

            candidate_next_url = _next_link(payload, config.endpoint)
            reached_reported_total = number_matched is not None and len(features) >= number_matched
            reached_short_final_page = page_count > 1 and len(page_features) < config.limit
            if reached_reported_total or reached_short_final_page:
                next_url = None
            else:
                next_url = candidate_next_url
            params = None

        if first_payload is None or last_response is None:
            raise RuntimeError("parcel API returned no pages")

        combined_payload = {
            key: value
            for key, value in first_payload.items()
            if key not in {"features", "links", "numberReturned"}
        }
        combined_payload["features"] = features
        combined_payload["numberReturned"] = len(features)
        combined_payload["links"] = [
            link
            for link in first_payload.get("links", [])
            if isinstance(link, dict) and link.get("rel") in {"self", "alternate"}
        ]
        return ParcelFetchResult(
            payload=combined_payload,
            last_response=last_response,
            total_attempts=total_attempts,
            page_count=page_count,
            number_matched=number_matched,
            matched_count_difference=(
                None if number_matched is None else number_matched - len(features)
            ),
            truncated=next_url is not None,
        )
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


def run_parcel_ingestion(
    config: ParcelIngestionConfig,
    output_root: Path,
    *,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = utc_now,
    ingestion_id: str | None = None,
) -> dict:
    """Fetch parcels, save immutable raw data, and write a provenance manifest."""

    config.validate()
    run_id = ingestion_id or str(uuid.uuid4())
    started_at = now()
    fetch_result = fetch_parcels(config, client=client, sleep=sleep)
    payload = fetch_result.payload
    completed_at = now()

    run_directory = output_root / SOURCE_ID / run_id
    data_path = run_directory / "parcels.geojson"
    manifest_path = run_directory / "manifest.json"
    encoded = _atomic_json_write(data_path, payload)
    features = payload["features"]

    geometry_types = sorted({feature["geometry"]["type"] for feature in features})
    property_keys = sorted({key for feature in features for key in feature["properties"]})
    manifest = {
        "schema_version": 1,
        "ingestion_id": run_id,
        "status": "succeeded",
        "source_id": SOURCE_ID,
        "collection_id": COLLECTION_ID,
        "endpoint": config.endpoint,
        "license": "Datenlizenz Deutschland - Zero - Version 2.0",
        "license_url": "https://www.govdata.de/dl-de/zero-2-0",
        "contains_personal_data": False,
        "started_at": _iso(started_at),
        "completed_at": _iso(completed_at),
        "duration_ms": round((completed_at - started_at).total_seconds() * 1000),
        "request": {**_request_params(config), "max_features": config.max_features},
        "response": {
            "http_status": fetch_result.last_response.status_code,
            "content_type": fetch_result.last_response.headers.get("content-type"),
            "attempts": fetch_result.total_attempts,
            "page_count": fetch_result.page_count,
            "number_matched": fetch_result.number_matched,
            "matched_count_difference": fetch_result.matched_count_difference,
            "matched_count_consistent": fetch_result.matched_count_difference in {None, 0},
            "feature_count": len(features),
            "truncated": fetch_result.truncated,
            "geometry_types": geometry_types,
            "property_keys": property_keys,
            "sha256": hashlib.sha256(encoded).hexdigest(),
        },
        "files": {"raw_geojson": data_path.name},
        "data_boundary": (
            "Public cadastral parcel subset without owner data. Not a planning approval or "
            "grid-capacity statement."
        ),
    }
    _atomic_json_write(manifest_path, manifest)
    return {**manifest, "run_directory": str(run_directory)}


def load_project_config(path: Path) -> ParcelIngestionConfig:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    region = config["region"]
    return ParcelIngestionConfig(
        bbox=tuple(float(value) for value in region["bbox"]),
        limit=int(region["parcel_page_size"]),
        max_features=int(region["max_parcels"]),
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
    manifest = run_parcel_ingestion(load_project_config(args.config), args.output_root)
    response = manifest["response"]
    print(
        f"Ingestion {manifest['ingestion_id']} succeeded: "
        f"{response['feature_count']} features -> {manifest['run_directory']}"
    )


if __name__ == "__main__":
    main()
