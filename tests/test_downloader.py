# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import patch

import httpx
import pytest
import respx

from coreason_etl_euctr.downloader import EpistemicDownloaderTask


@pytest.fixture
def downloader() -> EpistemicDownloaderTask:
    client = httpx.Client()
    # Set rate_limit to 0 for tests to avoid delays
    return EpistemicDownloaderTask(client=client, rate_limit=0)


@respx.mock
def test_download_protocol_html_success(downloader: EpistemicDownloaderTask) -> None:
    eudract_id = "2020-000000-00"

    # Mock successful responses for all target geographies
    for country in downloader.TARGET_GEOGRAPHIES:
        url = f"{downloader.BASE_URL}/{eudract_id}/{country}"
        respx.get(url).respond(
            status_code=200,
            text=f"<html>Protocol for {eudract_id} in {country}</html>",
        )

    result = downloader.download_protocol_html(eudract_id)

    assert len(result) == len(downloader.TARGET_GEOGRAPHIES)
    for country in downloader.TARGET_GEOGRAPHIES:
        assert country in result
        assert result[country] == f"<html>Protocol for {eudract_id} in {country}</html>"


@respx.mock
def test_download_protocol_html_partial_404(downloader: EpistemicDownloaderTask) -> None:
    eudract_id = "2020-000000-00"

    # Mock successful response for GB
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/GB").respond(
        status_code=200,
        text="<html>Protocol for GB</html>",
    )

    # Mock 404 for DE, BE, 3rd
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/DE").respond(status_code=404)
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/BE").respond(status_code=404)
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/3rd").respond(status_code=404)

    result = downloader.download_protocol_html(eudract_id)

    assert len(result) == 1
    assert "GB" in result
    assert result["GB"] == "<html>Protocol for GB</html>"
    assert "DE" not in result
    assert "BE" not in result
    assert "3rd" not in result


@respx.mock
def test_download_protocol_html_all_404(downloader: EpistemicDownloaderTask) -> None:
    eudract_id = "2020-000000-00"

    # Mock 404 for all
    for country in downloader.TARGET_GEOGRAPHIES:
        respx.get(f"{downloader.BASE_URL}/{eudract_id}/{country}").respond(status_code=404)

    result = downloader.download_protocol_html(eudract_id)

    assert len(result) == 0


@respx.mock
def test_download_protocol_html_500_retry(downloader: EpistemicDownloaderTask) -> None:
    eudract_id = "2020-000000-00"
    url = f"{downloader.BASE_URL}/{eudract_id}/GB"

    # Mock 500 followed by 500 followed by 200
    route = respx.get(url)
    route.side_effect = [
        httpx.Response(500),
        httpx.Response(500),
        httpx.Response(200, text="<html>Success after retry</html>"),
    ]

    # Mock 404 for others
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/DE").respond(status_code=404)
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/BE").respond(status_code=404)
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/3rd").respond(status_code=404)

    # Note: We need to mock tenacity sleep so the test doesn't take 10+ seconds
    # wait_exponential is used in the implementation
    with patch("tenacity.nap.time.sleep"):
        result = downloader.download_protocol_html(eudract_id)

    assert len(result) == 1
    assert "GB" in result
    assert result["GB"] == "<html>Success after retry</html>"
    # Should have called it 3 times
    assert route.call_count == 3


@respx.mock
def test_download_protocol_html_500_max_retries_exceeded(downloader: EpistemicDownloaderTask) -> None:
    eudract_id = "2020-000000-00"
    url = f"{downloader.BASE_URL}/{eudract_id}/GB"

    # Mock 500 constantly
    route = respx.get(url).respond(status_code=500)

    # Mock 404 for others
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/DE").respond(status_code=404)
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/BE").respond(status_code=404)
    respx.get(f"{downloader.BASE_URL}/{eudract_id}/3rd").respond(status_code=404)

    # Note: tenacity will raise HTTPError because we have reraise=True,
    # but the outer try-except catches it and logs it, then continues to next country.
    with patch("tenacity.nap.time.sleep"):
        result = downloader.download_protocol_html(eudract_id)

    assert len(result) == 0
    # stop_after_attempt(3) means it will try initially and then retry 2 more times = 3 total attempts
    assert route.call_count == 3


@respx.mock
def test_download_protocol_html_rate_limiting() -> None:
    eudract_id = "2020-000000-00"
    client = httpx.Client()
    # Explicitly set rate_limit
    downloader = EpistemicDownloaderTask(client=client, rate_limit=1.5)

    for country in downloader.TARGET_GEOGRAPHIES:
        respx.get(f"{downloader.BASE_URL}/{eudract_id}/{country}").respond(
            status_code=200,
            text=f"<html>Protocol for {country}</html>",
        )

    # Patch time.sleep used in download_protocol_html
    with patch("time.sleep") as mock_sleep:
        downloader.download_protocol_html(eudract_id)

    # It should have called time.sleep 4 times (once per country)
    assert mock_sleep.call_count == 4
    # Each call should have been with the rate_limit
    mock_sleep.assert_called_with(1.5)
