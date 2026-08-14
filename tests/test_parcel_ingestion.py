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


def payload_for(feature_id: str, *, next_href: str | None = None) -> dict:
    payload = json.loads(json.dumps(SAMPLE_PAYLOAD))
    feature = payload["features"][0]
    feature["id"] = feature_id
    feature["properties"]["localId"] = feature_id
    payload["numberMatched"] = 2 if next_href else 1
    payload["numberReturned"] = 1
    payload["links"] = []
    if next_href:
        payload["links"].append({"rel": "next", "href": next_href, "type": "application/geo+json"})
    return payload


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
        result = fetch_parcels(config, client=client, sleep=lambda _: None)

    assert result.last_response.status_code == 200
    assert result.total_attempts == 1
    assert result.page_count == 1
    assert result.payload["features"][0]["properties"]["localId"] == "parcel-1"


def test_fetch_follows_opaque_next_link_and_combines_pages() -> None:
    endpoint = "https://example.test/items"
    next_href = f"{endpoint}?offset=1&limit=1"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("offset") == "1":
            return httpx.Response(200, json=payload_for("parcel-2"), request=request)
        return httpx.Response(
            200,
            json=payload_for("parcel-1", next_href=next_href),
            request=request,
        )

    config = ParcelIngestionConfig(
        bbox=(8.15, 50.425, 8.175, 50.45),
        limit=1,
        max_features=2,
        endpoint=endpoint,
    )
    with make_client(handler) as client:
        result = fetch_parcels(config, client=client, sleep=lambda _: None)

    assert result.page_count == 2
    assert result.total_attempts == 2
    assert result.number_matched == 2
    assert result.matched_count_difference == 0
    assert result.truncated is False
    assert [feature["id"] for feature in result.payload["features"]] == [
        "parcel-1",
        "parcel-2",
    ]


def test_fetch_stops_at_configured_snapshot_limit() -> None:
    endpoint = "https://example.test/items"
    next_href = f"{endpoint}?offset=1&limit=1"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload_for("parcel-1", next_href=next_href),
            request=request,
        )

    config = ParcelIngestionConfig(
        bbox=(8.15, 50.425, 8.175, 50.45),
        limit=1,
        max_features=1,
        endpoint=endpoint,
    )
    with make_client(handler) as client:
        result = fetch_parcels(config, client=client, sleep=lambda _: None)

    assert result.page_count == 1
    assert result.truncated is True


def test_fetch_stops_when_matched_count_is_reached_despite_stale_next_link() -> None:
    endpoint = "https://example.test/items"
    stale_next_href = f"{endpoint}?offset=1&limit=1"
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        payload = payload_for("parcel-1", next_href=stale_next_href)
        payload["numberMatched"] = 1
        return httpx.Response(200, json=payload, request=request)

    config = ParcelIngestionConfig(
        bbox=(8.15, 50.425, 8.175, 50.45),
        limit=1,
        max_features=2,
        endpoint=endpoint,
    )
    with make_client(handler) as client:
        result = fetch_parcels(config, client=client, sleep=lambda _: None)

    assert calls == 1
    assert result.page_count == 1
    assert result.number_matched == 1
    assert result.matched_count_difference == 0
    assert result.truncated is False


def test_fetch_records_source_count_difference_after_short_final_page() -> None:
    endpoint = "https://example.test/items"
    second_href = f"{endpoint}?offset=5&limit=5"
    stale_href = f"{endpoint}?offset=10&limit=5"
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.params.get("offset") == "5":
            payload = payload_for("parcel-6", next_href=stale_href)
            payload["numberMatched"] = 7
            return httpx.Response(200, json=payload, request=request)
        payload = json.loads(json.dumps(SAMPLE_PAYLOAD))
        payload["features"] = [
            {
                **json.loads(json.dumps(SAMPLE_PAYLOAD["features"][0])),
                "id": f"parcel-{index}",
                "properties": {
                    **SAMPLE_PAYLOAD["features"][0]["properties"],
                    "localId": f"parcel-{index}",
                },
            }
            for index in range(1, 6)
        ]
        payload["numberMatched"] = 7
        payload["numberReturned"] = 5
        payload["links"] = [{"rel": "next", "href": second_href}]
        return httpx.Response(200, json=payload, request=request)

    config = ParcelIngestionConfig(
        bbox=(8.15, 50.425, 8.175, 50.45),
        limit=5,
        max_features=10,
        endpoint=endpoint,
    )
    with make_client(handler) as client:
        result = fetch_parcels(config, client=client, sleep=lambda _: None)

    assert calls == 2
    assert result.page_count == 2
    assert result.number_matched == 7
    assert len(result.payload["features"]) == 6
    assert result.matched_count_difference == 1
    assert result.truncated is False


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
    assert result["response"]["page_count"] == 1
    assert result["response"]["matched_count_consistent"] is True
    assert result["response"]["truncated"] is False
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
