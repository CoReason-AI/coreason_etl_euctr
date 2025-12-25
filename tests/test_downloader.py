# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
from coreason_etl_euctr.downloader import Downloader


@pytest.fixture  # type: ignore[misc]
def mock_httpx_client() -> MagicMock:
    return MagicMock(spec=httpx.Client)


def test_downloader_initialization(tmp_path: Path) -> None:
    """Test that Downloader initializes and creates the directory."""
    output_dir = tmp_path / "bronze"
    assert not output_dir.exists()

    downloader = Downloader(output_dir=output_dir)

    assert output_dir.exists()
    assert downloader.client is not None


def test_download_trial_success_primary(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test successful download from the first priority country (3rd)."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    # Mock Response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html>Content</html>"
    mock_httpx_client.get.return_value = mock_response

    with patch("time.sleep") as mock_sleep:
        result = downloader.download_trial("2023-123")

        # Should have slept once
        mock_sleep.assert_called_once_with(1)

    assert result is True

    # Verify call
    expected_url = "https://www.clinicaltrialsregister.eu/ctr-search/trial/2023-123/3rd"
    mock_httpx_client.get.assert_called_once_with(expected_url)

    # Verify file saved
    saved_file = tmp_path / "2023-123.html"
    assert saved_file.exists()
    assert saved_file.read_text(encoding="utf-8") == "<html>Content</html>"

    # Verify metadata sidecar
    meta_file = tmp_path / "2023-123.meta"
    assert meta_file.exists()
    assert "source_country=3rd" in meta_file.read_text(encoding="utf-8")


def test_download_trial_fallback_success(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test fallback to second country (GB) when primary (3rd) fails."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    # Mock Responses: 1st is 404, 2nd is 200
    response_404 = MagicMock()
    response_404.status_code = 404

    response_200 = MagicMock()
    response_200.status_code = 200
    response_200.text = "<html>GB Content</html>"

    mock_httpx_client.get.side_effect = [response_404, response_200]

    with patch("time.sleep"):
        result = downloader.download_trial("2023-123")

    assert result is True

    # Verify calls
    assert mock_httpx_client.get.call_count == 2
    mock_httpx_client.get.assert_has_calls(
        [
            call("https://www.clinicaltrialsregister.eu/ctr-search/trial/2023-123/3rd"),
            call("https://www.clinicaltrialsregister.eu/ctr-search/trial/2023-123/GB"),
        ]
    )

    # Verify file content
    saved_file = tmp_path / "2023-123.html"
    assert saved_file.read_text(encoding="utf-8") == "<html>GB Content</html>"

    # Verify metadata
    meta_file = tmp_path / "2023-123.meta"
    assert "source_country=GB" in meta_file.read_text(encoding="utf-8")


def test_download_trial_all_fail(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test failure when all countries return 404."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    response_404 = MagicMock()
    response_404.status_code = 404

    mock_httpx_client.get.return_value = response_404

    with patch("time.sleep"):
        result = downloader.download_trial("2023-123")

    assert result is False
    assert mock_httpx_client.get.call_count == 3

    # Verify no file saved
    assert not (tmp_path / "2023-123.html").exists()


def test_download_trial_http_error(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test that HTTP errors (e.g. 500) trigger fallback/continue."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    # 1st raises Error, 2nd returns 200
    mock_httpx_client.get.side_effect = [httpx.ConnectError("Connection failed"), MagicMock(status_code=200, text="OK")]

    with patch("time.sleep"):
        result = downloader.download_trial("2023-123")

    assert result is True
    # Should have tried 3rd (fail), then GB (success)
    assert mock_httpx_client.get.call_count == 2

    # Verify file saved
    assert (tmp_path / "2023-123.html").read_text(encoding="utf-8") == "OK"


def test_download_trial_empty_body(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test that empty body is treated as failure and triggers fallback."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    empty_response = MagicMock()
    empty_response.status_code = 200
    empty_response.text = "   "  # Whitespace only

    valid_response = MagicMock()
    valid_response.status_code = 200
    valid_response.text = "Data"

    mock_httpx_client.get.side_effect = [empty_response, valid_response]

    with patch("time.sleep"):
        result = downloader.download_trial("2023-123")

    assert result is True
    assert mock_httpx_client.get.call_count == 2
    assert (tmp_path / "2023-123.html").read_text(encoding="utf-8") == "Data"


def test_save_to_disk_io_error(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test handling of IO errors during file save."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "Content"
    mock_httpx_client.get.return_value = mock_response

    # Mock open to raise IOError
    with patch("builtins.open", side_effect=IOError("Disk full")), patch("time.sleep"):
        with pytest.raises(IOError):
            downloader.download_trial("2023-123")


def test_download_trial_5xx_error_fallback(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test that 5xx Server Errors trigger fallback to the next country."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    # 1st: 500 Internal Server Error
    response_500 = MagicMock()
    response_500.status_code = 500
    def raise_500() -> None:
        raise httpx.HTTPStatusError("Server Error", request=MagicMock(), response=response_500)
    response_500.raise_for_status.side_effect = raise_500

    # 2nd: 503 Service Unavailable
    response_503 = MagicMock()
    response_503.status_code = 503
    def raise_503() -> None:
        raise httpx.HTTPStatusError("Service Unavailable", request=MagicMock(), response=response_503)
    response_503.raise_for_status.side_effect = raise_503

    # 3rd: 200 OK
    response_200 = MagicMock()
    response_200.status_code = 200
    response_200.text = "<html>Success</html>"

    mock_httpx_client.get.side_effect = [response_500, response_503, response_200]

    with patch("time.sleep"):
        result = downloader.download_trial("2023-123")

    assert result is True
    # Should have tried all 3 countries
    assert mock_httpx_client.get.call_count == 3

    # Verify file saved
    assert (tmp_path / "2023-123.html").read_text(encoding="utf-8") == "<html>Success</html>"
    # Verify metadata shows DE (3rd priority)
    assert "source_country=DE" in (tmp_path / "2023-123.meta").read_text(encoding="utf-8")


def test_download_trial_mixed_failures(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test a mix of 404, Network Error, and Success."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    # 1st (3rd): 404 Not Found
    response_404 = MagicMock()
    response_404.status_code = 404

    # 2nd (GB): Network Connection Error
    # 3rd (DE): Success
    response_200 = MagicMock()
    response_200.status_code = 200
    response_200.text = "<html>Success</html>"

    mock_httpx_client.get.side_effect = [
        response_404,
        httpx.ConnectError("Network Down"),
        response_200
    ]

    with patch("time.sleep"):
        result = downloader.download_trial("2023-123")

    assert result is True
    assert mock_httpx_client.get.call_count == 3
    assert (tmp_path / "2023-123.html").read_text(encoding="utf-8") == "<html>Success</html>"
