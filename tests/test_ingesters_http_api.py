"""Tests for HTTP API ingester module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os
import orjson

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingesters.http_api import fetch_json
from src.utils import http


class TestHTTPIngester:
  """Test the HTTP API ingester functionality."""

  @pytest.mark.asyncio
  async def test_fetch_json_success(self):
    """Test successful JSON fetch."""
    mock_response_data = {"data": "test"}
    mock_response_text = orjson.dumps(mock_response_data).decode()

    with patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.utils.http.get') as mock_http_get:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3

      mock_response = Mock()
      mock_response.status_code = 200
      mock_response.json.return_value = mock_response_data
      mock_response.text = mock_response_text
      mock_http_get.return_value = mock_response

      result = await fetch_json("http://example.com/api")

      assert result == mock_response_data
      mock_http_get.assert_called_once_with("http://example.com/api",
                                            headers=None,
                                            timeout=None)

  @pytest.mark.asyncio
  async def test_fetch_json_http_error(self):
    """Test JSON fetch with HTTP error."""
    with patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.utils.http.get') as mock_http_get, \
         patch('src.ingesters.http_api.log_error') as mock_log_error:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 1

      mock_response = Mock()
      mock_response.status_code = 404
      mock_http_get.return_value = mock_response

      result = await fetch_json("http://example.com/api")

      assert result is None
      mock_log_error.assert_not_called()  # Error is logged in http util

  @pytest.mark.asyncio
  async def test_fetch_json_with_retries(self):
    """Test JSON fetch with retries after failures."""
    test_data = {"success": True}

    with patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.utils.http.get') as mock_http_get, \
         patch('src.ingesters.http_api.log_warn') as mock_log_warn, \
         patch('asyncio.sleep', new_callable=AsyncMock):

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3

      mock_response_fail = Mock()
      mock_response_fail.status_code = 500
      mock_response_success = Mock()
      mock_response_success.status_code = 200
      mock_response_success.json.return_value = test_data

      mock_http_get.side_effect = [mock_response_fail, mock_response_success]

      result = await fetch_json("http://example.com/api", retry_delay=0.1)

      assert result == test_data
      assert mock_http_get.call_count == 2
      mock_log_warn.assert_called_once()

  @pytest.mark.asyncio
  async def test_fetch_json_exception_handling(self):
    """Test JSON fetch with exception handling."""
    with patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.utils.http.get') as mock_http_get, \
         patch('src.ingesters.http_api.log_warn') as mock_log_warn, \
         patch('asyncio.sleep', new_callable=AsyncMock):

      mock_state.args.verbose = False
      mock_state.args.max_retries = 2

      mock_http_get.side_effect = http.httpx.RequestError("Connection error",
                                                          request=Mock())

      result = await fetch_json("http://example.com/api")

      assert result is None
      assert mock_http_get.call_count == 2
      mock_log_warn.assert_called()

  @pytest.mark.asyncio
  async def test_fetch_json_verbose_logging(self):
    """Test verbose logging."""
    with patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.utils.http.get') as mock_http_get, \
         patch('src.ingesters.http_api.log_info') as mock_log_info:

      mock_state.args.verbose = True

      mock_response = Mock()
      mock_response.status_code = 200
      mock_response.json.return_value = {"data": "test"}
      mock_http_get.return_value = mock_response

      await fetch_json("http://example.com/api")

      mock_log_info.assert_called_with('Fetched http://example.com/api')

  # The following tests for `schedule` remain unchanged as they test logic
  # independent of the underlying HTTP client implementation details.
  # They mock `fetch_json` or other parts that are now covered above.
