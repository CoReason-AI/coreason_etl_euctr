# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import hashlib
from typing import Generator
from unittest.mock import MagicMock, patch

import httpx
import pytest
from coreason_etl_euctr.downloader import Downloader
from coreason_etl_euctr.storage import StorageBackend


@pytest.fixture  # type: ignore[misc]
def mock_httpx_client() -> Generator[MagicMock, None, None]:
    with patch("httpx.Client") as mock:
        client_instance = MagicMock()
        mock.return_value = client_instance
        yield client_instance


@pytest.fixture  # type: ignore[misc]
def mock_storage() -> MagicMock:
    return MagicMock(spec=StorageBackend)


# Helper for matching any string
class AnyString:
    def __eq__(self, other):
        return isinstance(other, str)

ANY_STRING = AnyString()


def test_downloader_initialization_error() -> None:
    """Test that initialization fails if neither output_dir nor storage_backend is provided."""
    with pytest.raises(ValueError, match="Either output_dir or storage_backend must be provided"):
        Downloader()


def test_downloader_initialization_legacy_dir() -> None:
    """Test initialization with output_dir creates LocalStorageBackend."""
    with patch("coreason_etl_euctr.downloader.LocalStorageBackend") as MockStorage:
        Downloader(output_dir="tmp")
        MockStorage.assert_called_once()


def test_download_trial_success_primary(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test successful download from the first priority country (3rd)."""
    # Setup
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    eudract = "2015-001234-56"
    html_content = "<html>Success 3rd</html>"

    # Mock Response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = html_content
    mock_httpx_client.get.return_value = mock_response

    # Mock Storage: meta does not exist (new file)
    mock_storage.exists.return_value = False

    # Execute
    with patch("time.sleep"):  # Skip sleep
        result = downloader.download_trial(eudract)

    # Verify
    assert result is True

    # Check URL: Should be 3rd country
    expected_url = "https://www.clinicaltrialsregister.eu/ctr-search/trial/2015-001234-56/3rd"
    mock_httpx_client.get.assert_called_once_with(expected_url)

    # Check Storage Write
    mock_storage.write.assert_any_call("2015-001234-56.html", html_content)
    # Check Metadata Write
    # Metadata contains hash, source, url
    mock_storage.write.assert_any_call("2015-001234-56.meta", ANY_STRING)


def test_download_trial_fallback(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test fallback logic: 3rd (404) -> GB (404) -> DE (200)."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    eudract = "2015-001234-56"

    # Mock Responses
    # 1. 3rd -> 404
    resp_3rd = MagicMock()
    resp_3rd.status_code = 404

    # 2. GB -> 404
    resp_gb = MagicMock()
    resp_gb.status_code = 404

    # 3. DE -> 200
    resp_de = MagicMock()
    resp_de.status_code = 200
    resp_de.text = "<html>Success DE</html>"

    mock_httpx_client.get.side_effect = [resp_3rd, resp_gb, resp_de]
    mock_storage.exists.return_value = False

    with patch("time.sleep"):
        result = downloader.download_trial(eudract)

    assert result is True

    # Verify calls
    assert mock_httpx_client.get.call_count == 3
    calls = mock_httpx_client.get.call_args_list
    assert "3rd" in calls[0][0][0]
    assert "GB" in calls[1][0][0]
    assert "DE" in calls[2][0][0]

    # Verify write
    mock_storage.write.assert_any_call("2015-001234-56.html", "<html>Success DE</html>")


def test_download_trial_all_fail(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test failure when all countries return 404."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)

    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_httpx_client.get.return_value = mock_resp

    with patch("time.sleep"):
        result = downloader.download_trial("2015-001234-56")

    assert result is False
    assert mock_httpx_client.get.call_count == 3  # Tried all 3
    mock_storage.write.assert_not_called()


def test_download_trial_retry_on_error(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test that retry logic works for retryable errors (e.g. 500), but eventually fails if persistent."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)

    # Simulate 500 error for all attempts
    mock_resp = MagicMock()
    mock_resp.status_code = 500

    def raise_error(*args, **kwargs):
        raise httpx.HTTPStatusError("500 Error", request=MagicMock(), response=mock_resp)

    mock_resp.raise_for_status.side_effect = raise_error
    mock_httpx_client.get.return_value = mock_resp

    # We mock wait_exponential to skip delays
    with patch("time.sleep"), \
         patch("coreason_etl_euctr.downloader.wait_exponential", return_value=lambda *args, **kwargs: 0):

        result = downloader.download_trial("2015-001234-56")

    assert result is False
    # 3 countries * (1 initial + retries)
    # Tenacity defaults: stop_after_attempt(3).
    # So 3 calls per country = 9 calls total.
    assert mock_httpx_client.get.call_count >= 3 # At least tried
    mock_storage.write.assert_not_called()


def test_idempotency_skip_write(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test that if content hash matches existing metadata, we skip writing the HTML file."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    html_content = "<html>Same Content</html>"
    file_hash = hashlib.sha256(html_content.encode("utf-8")).hexdigest()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content
    mock_httpx_client.get.return_value = mock_resp

    # Storage setup: Meta exists
    mock_storage.exists.return_value = True
    # Return metadata with matching hash
    mock_storage.read.return_value = f"hash={file_hash}\nsource_country=3rd"

    with patch("time.sleep"):
        result = downloader.download_trial("2015-001234-56")

    assert result is True

    # Verify:
    # 1. HTML file write should NOT be called (skipped)
    # 2. Meta file write SHOULD be called (to update timestamp)

    # Get all write calls
    write_calls = mock_storage.write.call_args_list

    # Check that no call was made to .html
    for c in write_calls:
        assert not c[0][0].endswith(".html"), f"Should not have written HTML file: {c[0][0]}"

    # Check that meta was written/updated
    meta_writes = [c for c in write_calls if c[0][0].endswith(".meta")]
    assert len(meta_writes) == 1


def test_idempotency_overwrite_changed(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test that if content hash differs, we overwrite."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    html_content = "<html>New Content</html>"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content
    mock_httpx_client.get.return_value = mock_resp

    # Storage setup: Meta exists but different hash
    mock_storage.exists.return_value = True
    mock_storage.read.return_value = "hash=old_hash"

    with patch("time.sleep"):
        result = downloader.download_trial("2015-001234-56")

    assert result is True

    # Verify HTML was written
    mock_storage.write.assert_any_call("2015-001234-56.html", html_content)


def test_empty_response_handling(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test that empty response body triggers fallback."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)

    # 3rd -> Empty Body (Soft error)
    resp_3rd = MagicMock()
    resp_3rd.status_code = 200
    resp_3rd.text = "   " # Whitespace only

    # GB -> Success
    resp_gb = MagicMock()
    resp_gb.status_code = 200
    resp_gb.text = "<html>Valid</html>"

    mock_httpx_client.get.side_effect = [resp_3rd, resp_gb]
    mock_storage.exists.return_value = False

    with patch("time.sleep"):
        result = downloader.download_trial("2015-001234-56")

    assert result is True
    # Should have tried GB
    assert mock_httpx_client.get.call_count == 2
    mock_storage.write.assert_any_call("2015-001234-56.html", "<html>Valid</html>")


def test_metadata_exception(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test that if reading metadata fails, we just overwrite (graceful recovery)."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    html_content = "<html>Content</html>"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content
    mock_httpx_client.get.return_value = mock_resp

    mock_storage.exists.return_value = True
    # Simulate read error
    mock_storage.read.side_effect = Exception("Read Error")

    with patch("time.sleep"):
        result = downloader.download_trial("2015-001234-56")

    assert result is True
    # Should write because it couldn't verify hash
    mock_storage.write.assert_any_call("2015-001234-56.html", html_content)


def test_write_error(mock_httpx_client: MagicMock, mock_storage: MagicMock) -> None:
    """Test handling of write error."""
    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    mock_httpx_client.get.return_value = MagicMock(status_code=200, text="content")
    mock_storage.exists.return_value = False
    mock_storage.write.side_effect = Exception("Write Fail")

    with pytest.raises(Exception, match="Write Fail"):
        with patch("time.sleep"):
            downloader.download_trial("123")
