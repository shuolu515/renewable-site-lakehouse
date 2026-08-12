import hashlib
import json
from datetime import UTC, datetime
from urllib.parse import parse_qs

import httpx
import pytest

from renewable_site_lakehouse.grid_ingestion import (
    GridIngestionConfig,
    build_overpass_query,
    fetch_grid_assets,
    run_grid_ingestion,
)

SAMPLE_PAYLOAD = {
    "version": 0.6,
    "generator": "Overpass API",
    "elements": [
        {
            "type": "node",
            "id": 101,
            "lat": 50.1,
            "lon": 8.25,
            "tags": {"power": "transformer", "voltage": "20000"},
        },
        {
            "type": "way",
            "id": 202,
            "nodes": [1, 2],
            "geometry": [
                {"lat": 50.09, "lon": 8.24},
                {"lat": 50.10, "lon": 8.25},
            ],
            "tags": {"power": "line", "voltage": "110000"},
        },
    ],
}


def make_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_builds_bounded_query_with_result_limit() -> None:
    config = GridIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), max_assets=250)

    query = build_overpass_query(config)

    assert "(50.050000,8.200000,50.120000,8.300000)" in query
    assert 'way["power"~"^(line|minor_line)$"]' in query
    assert "out body geom 250;" in query


def test_rejects_unbounded_asset_limit() -> None:
    config = GridIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), max_assets=1001)

    with pytest.raises(ValueError, match="max_assets must be between"):
        config.validate()


def test_fetch_posts_query_without_osm_user_metadata() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode("utf-8"))
        assert request.method == "POST"
        assert "data" in form
        assert "out body geom" in form["data"][0]
        assert "meta" not in form["data"][0]
        return httpx.Response(200, json=SAMPLE_PAYLOAD, request=request)

    config = GridIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12))
    with make_client(handler) as client:
        payload, response, attempts, _ = fetch_grid_assets(
            config, client=client, sleep=lambda _: None
        )

    assert response.status_code == 200
    assert attempts == 1
    assert len(payload["elements"]) == 2


def test_retry_then_write_snapshot_and_manifest(tmp_path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, text="rate limited", request=request)
        return httpx.Response(
            200,
            json=SAMPLE_PAYLOAD,
            headers={"content-type": "application/json"},
            request=request,
        )

    timestamps = iter(
        [
            datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
            datetime(2026, 8, 12, 12, 0, 2, tzinfo=UTC),
        ]
    )
    config = GridIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), max_assets=10)
    with make_client(handler) as client:
        result = run_grid_ingestion(
            config,
            tmp_path,
            client=client,
            sleep=lambda _: None,
            now=lambda: next(timestamps),
            ingestion_id="test-grid-run",
        )

    run_dir = tmp_path / "openstreetmap_overpass" / "test-grid-run"
    raw_bytes = (run_dir / "grid_assets.json").read_bytes()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert calls == 2
    assert result["response"]["attempts"] == 2
    assert manifest["response"]["element_count"] == 2
    assert manifest["response"]["power_tag_counts"] == {"line": 1, "transformer": 1}
    assert manifest["response"]["sha256"] == hashlib.sha256(raw_bytes).hexdigest()
    assert manifest["capacity_status_default"] == "unknown"
    assert manifest["contains_osm_contributor_metadata"] is False


def test_non_json_error_is_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="runtime error", request=request)

    config = GridIngestionConfig(bbox=(8.2, 50.05, 8.3, 50.12), max_assets=10, max_attempts=1)
    with (
        make_client(handler) as client,
        pytest.raises(ValueError, match="Overpass did not return JSON"),
    ):
        fetch_grid_assets(config, client=client, sleep=lambda _: None)
