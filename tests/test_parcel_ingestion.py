import hashlib
import json
from datetime import UTC, datetime

import httpx
import pytest

from renewable_site_lakehouse.parcel_ingestion import (
    ParcelIngestionConfig,
    fetch_parcels,
    run_parcel_ingestion,
)

SAMPLE_PAYLOAD = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "parcel-1",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[8.2, 50.1], [8.21, 50.1], [8.2, 50.1]]],
            },
            "properties": {"localId": "parcel-1", "areaValue": 25000},
        }
    ],
}


def make_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_rejects_limit_not_supported_by_official_api() -> None:
    config = ParcelIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), limit=2)

    with pytest.raises(ValueError, match="limit must be one of"):
        config.validate()


def test_fetch_builds_bounded_request() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["bbox"] == "8.200000,50.050000,8.300000,50.120000"
        assert request.url.params["limit"] == "5"
        assert request.url.params["f"] == "json"
        return httpx.Response(200, json=SAMPLE_PAYLOAD, request=request)

    config = ParcelIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), limit=5)
    with make_client(handler) as client:
        payload, response, attempts = fetch_parcels(config, client=client, sleep=lambda _: None)

    assert response.status_code == 200
    assert attempts == 1
    assert payload["features"][0]["properties"]["localId"] == "parcel-1"


def test_retry_then_write_raw_snapshot_and_manifest(tmp_path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, text="temporary outage", request=request)
        return httpx.Response(
            200,
            json=SAMPLE_PAYLOAD,
            headers={"content-type": "application/vnd.geo+json"},
            request=request,
        )

    timestamps = iter(
        [
            datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
            datetime(2026, 8, 12, 10, 0, 1, tzinfo=UTC),
        ]
    )
    config = ParcelIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), limit=5)
    with make_client(handler) as client:
        result = run_parcel_ingestion(
            config,
            tmp_path,
            client=client,
            sleep=lambda _: None,
            now=lambda: next(timestamps),
            ingestion_id="test-run",
        )

    run_dir = tmp_path / "hessen_alkis_inspire_wfs" / "test-run"
    raw_bytes = (run_dir / "parcels.geojson").read_bytes()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert calls == 2
    assert result["response"]["attempts"] == 2
    assert manifest["response"]["feature_count"] == 1
    assert manifest["response"]["geometry_types"] == ["Polygon"]
    assert manifest["response"]["sha256"] == hashlib.sha256(raw_bytes).hexdigest()
    assert manifest["contains_personal_data"] is False


def test_html_error_is_not_accepted_as_geojson() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="Parameter limit is not valid", request=request)

    config = ParcelIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), limit=5, max_attempts=1)
    with make_client(handler) as client, pytest.raises(ValueError, match="did not return JSON"):
        fetch_parcels(config, client=client, sleep=lambda _: None)
