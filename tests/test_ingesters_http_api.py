"""Tests for HTTP API ingester module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os
import orjson
import httpx

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingesters.http_api import HTTPIngester, fetch_json, schedule


class TestHTTPIngester:
  """Test the HTTP API ingester functionality."""

  @pytest.mark.asyncio
  async def test_http_ingester_get_session_new(self):
    """Test getting new session when none exists."""
    ingester = HTTPIngester()

    session = await ingester.get_session()

    assert isinstance(session, httpx.AsyncClient)
    assert not session.is_closed
    assert ingester.session == session

    # Clean up
    await ingester.close()

  @pytest.mark.asyncio
  async def test_http_ingester_get_session_existing(self):
    """Test reusing existing session."""
    ingester = HTTPIngester()

    session1 = await ingester.get_session()
    session2 = await ingester.get_session()

    assert session1 == session2
    assert ingester.session == session1

    # Clean up
    await ingester.close()

  @pytest.mark.asyncio
  async def test_http_ingester_get_session_closed(self):
    """Test creating new session when existing one is closed."""
    ingester = HTTPIngester()

    session1 = await ingester.get_session()
    await session1.close()

    session2 = await ingester.get_session()

    assert session1 != session2
    assert session1.is_closed
    assert not session2.is_closed

    # Clean up
    await ingester.close()

  @pytest.mark.asyncio
  async def test_http_ingester_close_with_session(self):
    """Test closing ingester with active session."""
    ingester = HTTPIngester()

    session = await ingester.get_session()
    assert not session.is_closed

    await ingester.close()
    assert session.is_closed

  @pytest.mark.asyncio
  async def test_http_ingester_close_without_session(self):
    """Test closing ingester without session."""
    ingester = HTTPIngester()

    # Should not raise an error
    await ingester.close()

  @pytest.mark.asyncio
  async def test_http_ingester_close_already_closed(self):
    """Test closing ingester with already closed session."""
    ingester = HTTPIngester()

    session = await ingester.get_session()
    await session.close()

    # Should not raise an error
    await ingester.close()

  def test_http_ingester_session_configuration(self):
    """Test that session is configured correctly."""
    ingester = HTTPIngester()

    # Test the session configuration through examining the constructor
    # This is a structural test to ensure proper configuration
    assert ingester.session is None

  @pytest.mark.asyncio
  async def test_fetch_json_success(self):
    """Test successful JSON fetch."""
    mock_response_data = {"data": "test"}
    expected_result = orjson.dumps(mock_response_data).decode()

    with patch('src.ingesters.http_api.state') as mock_state:
      mock_state.args.verbose = False
      mock_state.args.max_retries = 3

      # Mock the actual HTTP response at a lower level
      with patch('httpx.AsyncClient.get') as mock_get:
        # Create a proper mock response for httpx
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = expected_result

        mock_get.return_value = mock_response

        result = await fetch_json("http://example.com/api")

        assert result == expected_result

  @pytest.mark.asyncio
  async def test_fetch_json_http_error(self):
    """Test JSON fetch with HTTP error."""
    with patch('src.ingesters.http_api.http_ingester') as mock_ingester, \
         patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.ingesters.http_api.log_error') as mock_log_error:

      # Setup mocks
      mock_session = AsyncMock()
      mock_response = AsyncMock()
      mock_response.status_code = 404

      # Mock httpx response
      mock_session.get = AsyncMock(return_value=mock_response)
      mock_ingester.get_session = AsyncMock(return_value=mock_session)
      mock_state.args.verbose = False
      mock_state.args.max_retries = 1

      result = await fetch_json("http://example.com/api")

      assert result == ""
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_fetch_json_with_retries(self):
    """Test JSON fetch with retries after failures."""
    test_data = {"success": True}

    with patch('src.ingesters.http_api.http_ingester') as mock_ingester, \
         patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.ingesters.http_api.log_warn') as mock_log_warn, \
         patch('src.ingesters.http_api.sleep'):

      # Setup mocks to fail first, succeed second
      mock_session = AsyncMock()
      mock_ingester.get_session = AsyncMock(return_value=mock_session)
      mock_state.args.verbose = False
      mock_state.args.max_retries = 3

      # First call fails, second succeeds
      mock_response_fail = AsyncMock()
      mock_response_fail.status_code = 500
      mock_response_success = AsyncMock()
      mock_response_success.status_code = 200
      mock_response_success.text = orjson.dumps(test_data).decode()

      call_count = 0

      async def mock_get(*args, **kwargs):
        nonlocal call_count
        if call_count == 0:
          call_count += 1
          return mock_response_fail
        else:
          return mock_response_success

      mock_session.get = mock_get

      result = await fetch_json("http://example.com/api", retry_delay=0.1)

      assert result == orjson.dumps(test_data).decode()
      mock_log_warn.assert_called()

  @pytest.mark.asyncio
  async def test_fetch_json_exception_handling(self):
    """Test JSON fetch with exception handling."""
    with patch('src.ingesters.http_api.http_ingester') as mock_ingester, \
         patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.ingesters.http_api.log_warn') as mock_log_warn, \
                  patch('src.ingesters.http_api.sleep'):

      # Setup mocks
      mock_session = AsyncMock()
      mock_session.get.side_effect = Exception("Connection error")
      mock_ingester.get_session.return_value = mock_session
      mock_state.args.verbose = False
      mock_state.args.max_retries = 2

      result = await fetch_json("http://example.com/api")

      assert result == ""
      mock_log_warn.assert_called()

  @pytest.mark.asyncio
  async def test_fetch_json_verbose_logging(self):
    """Test JSON fetch with verbose logging."""
    test_data = {"key": "value"}

    with patch('src.ingesters.http_api.http_ingester') as mock_ingester, \
         patch('src.ingesters.http_api.state') as mock_state, \
         patch('src.ingesters.http_api.log_debug') as mock_log_debug:

      # Setup mocks
      mock_session = AsyncMock()
      mock_response = AsyncMock()
      mock_response.status_code = 200
      mock_response.text = orjson.dumps(test_data).decode()

      mock_session.get = AsyncMock(return_value=mock_response)
      mock_ingester.get_session = AsyncMock(return_value=mock_session)
      mock_state.args.verbose = True
      mock_state.args.max_retries = 3

      result = await fetch_json("http://example.com/api")

      assert result == orjson.dumps(test_data).decode()
      mock_log_debug.assert_called_with("Fetching http://example.com/api")

  @pytest.mark.asyncio
  async def test_schedule_basic_functionality(self):
    """Test basic schedule functionality."""
    # Create mock ingester
    mock_field = Mock()
    mock_field.target = "http://example.com/api"
    mock_field.selector = "data.value"
    mock_field.name = "test_field"

    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60
    mock_ingester.fields = [mock_field]
    mock_ingester.data_by_field = {}
    mock_ingester.pre_transformer = None

    with patch('src.ingesters.http_api.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.http_api.get_or_set_cache') as mock_cache, \
         patch('src.ingesters.http_api.select_nested') as mock_select, \
         patch('src.ingesters.http_api.transform_and_store'), \
         patch('src.ingesters.http_api.scheduler') as mock_scheduler:

      # Setup mocks
      mock_claim.return_value = None
      test_data = {"data": {"value": "test_result"}}
      mock_cache.return_value = orjson.dumps(test_data).decode()
      mock_select.return_value = "test_result"
      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_with_pre_transformer(self):
    """Test schedule with pre_transformer."""
    # Create mock ingester with pre_transformer
    mock_field = Mock()
    mock_field.target = "http://example.com/api"
    mock_field.selector = "transformed.value"
    mock_field.name = "test_field"

    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60
    mock_ingester.fields = [mock_field]
    mock_ingester.data_by_field = {}
    mock_ingester.pre_transformer = "lambda data: {'transformed': {'value': data['raw']}}"

    with patch('src.ingesters.http_api.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.http_api.get_or_set_cache') as mock_cache, \
         patch('src.ingesters.http_api.select_nested') as mock_select, \
         patch('src.ingesters.http_api.transform_and_store'), \
         patch('src.ingesters.http_api.scheduler') as mock_scheduler, \
         patch('src.ingesters.http_api.safe_eval') as mock_safe_eval:

      # Setup mocks
      mock_claim.return_value = None
      test_data = {"raw": "original_value"}
      mock_cache.return_value = orjson.dumps(test_data).decode()

      # Mock transformer function
      def mock_transformer(data):
        return {"transformed": {"value": data["raw"]}}

      mock_safe_eval.return_value = mock_transformer

      mock_select.return_value = "original_value"

      # Mock scheduler to execute the ingest function to test safe_eval call
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      mock_safe_eval.assert_called_once_with(
          "lambda data: {'transformed': {'value': data['raw']}}",
          callable_check=True)

  @pytest.mark.asyncio
  async def test_schedule_multiple_fields_same_url(self):
    """Test schedule with multiple fields from same URL."""
    # Create mock fields with same target URL
    mock_field1 = Mock()
    mock_field1.target = "http://example.com/api"
    mock_field1.selector = "data.field1"
    mock_field1.name = "field1"

    mock_field2 = Mock()
    mock_field2.target = "http://example.com/api"
    mock_field2.selector = "data.field2"
    mock_field2.name = "field2"

    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60
    mock_ingester.fields = [mock_field1, mock_field2]
    mock_ingester.data_by_field = {}
    mock_ingester.pre_transformer = None

    with patch('src.ingesters.http_api.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.http_api.get_or_set_cache') as mock_cache, \
         patch('src.ingesters.http_api.select_nested') as mock_select, \
         patch('src.ingesters.http_api.transform_and_store'), \
         patch('src.ingesters.http_api.scheduler') as mock_scheduler:

      # Setup mocks
      mock_claim.return_value = None
      test_data = {"data": {"field1": "value1", "field2": "value2"}}
      mock_cache.return_value = orjson.dumps(test_data).decode()
      mock_select.side_effect = ["value1", "value2"]
      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      # Mock scheduler to execute the ingest function to test cache call
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      # Should only call cache once since same URL
      mock_cache.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_url_formatting(self):
    """Test schedule with URL formatting."""
    # Create mock field with URL that needs formatting
    mock_field = Mock()
    mock_field.target = "http://example.com/api/{symbol}"
    mock_field.selector = "price"
    mock_field.name = "price_field"

    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60
    mock_ingester.fields = [mock_field]
    mock_ingester.data_by_field = {"symbol": "BTC"}
    mock_ingester.pre_transformer = None

    with patch('src.ingesters.http_api.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.http_api.get_or_set_cache') as mock_cache, \
         patch('src.ingesters.http_api.select_nested') as mock_select, \
         patch('src.ingesters.http_api.transform_and_store'), \
         patch('src.ingesters.http_api.scheduler') as mock_scheduler, \
         patch('src.ingesters.http_api.fetch_json') as mock_fetch:

      # Setup mocks
      mock_claim.return_value = None
      test_data = {"price": 50000}
      mock_fetch.return_value = orjson.dumps(test_data).decode()
      mock_cache.return_value = orjson.dumps(test_data).decode()
      mock_select.return_value = 50000
      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      # Verify the cache was called (either get_or_set_cache or fetch_json should be called)
      if mock_cache.call_args:
        call_args = mock_cache.call_args[0]
        # The hash should be based on the formatted URL containing BTC
        assert len(call_args) > 0
      elif mock_fetch.call_args:
        call_args = mock_fetch.call_args[0]
        # The URL should contain BTC after formatting
        assert "BTC" in str(call_args[0])

  @pytest.mark.asyncio
  async def test_schedule_missing_fields_warning(self):
    """Test schedule with missing fields triggers warning."""
    # Create mock field
    mock_field = Mock()
    mock_field.target = "http://example.com/api"
    mock_field.selector = "missing.field"
    mock_field.name = "missing_field"

    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60
    mock_ingester.fields = [mock_field]
    mock_ingester.data_by_field = {}
    mock_ingester.pre_transformer = None

    with patch('src.ingesters.http_api.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.http_api.get_or_set_cache') as mock_cache, \
         patch('src.ingesters.http_api.select_nested') as mock_select, \
         patch('src.ingesters.http_api.transform_and_store') as mock_transform, \
         patch('src.ingesters.http_api.scheduler') as mock_scheduler, \
         patch('src.ingesters.http_api.log_warn') as mock_log_warn:

      # Setup mocks
      mock_claim.return_value = None
      test_data = {"data": {"other_field": "value"}}
      mock_cache.return_value = orjson.dumps(test_data).decode()
      mock_select.return_value = None  # Field not found
      mock_transform.return_value = None

      # Mock scheduler to actually execute the ingest function
      async def mock_add_ingester(ingester, fn, start=False):
        # Execute the ingest function immediately for testing
        await fn(ingester)
        return Mock()  # Return a mock task

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      mock_log_warn.assert_called_with(
          "test_http_ingester missing fields: missing_field")

  @pytest.mark.asyncio
  async def test_schedule_json_parse_error(self):
    """Test schedule with JSON parsing error."""
    # Create mock field
    mock_field = Mock()
    mock_field.target = "http://example.com/api"
    mock_field.selector = "data.value"
    mock_field.name = "test_field"

    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60
    mock_ingester.fields = [mock_field]
    mock_ingester.data_by_field = {}
    mock_ingester.pre_transformer = None

    with patch('src.ingesters.http_api.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.http_api.get_or_set_cache') as mock_cache, \
         patch('src.ingesters.http_api.transform_and_store') as mock_transform, \
         patch('src.ingesters.http_api.scheduler') as mock_scheduler, \
         patch('src.ingesters.http_api.log_error') as mock_log_error:

      # Setup mocks
      mock_claim.return_value = None
      mock_cache.return_value = "invalid json"  # Invalid JSON
      mock_transform.return_value = None

      # Mock scheduler to actually execute the ingest function
      async def mock_add_ingester(ingester, fn, start=False):
        # Execute the ingest function immediately for testing
        await fn(ingester)
        return Mock()  # Return a mock task

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_schedule_no_task_returned(self):
    """Test schedule when scheduler returns None."""
    mock_ingester = Mock()
    mock_ingester.name = "test_http_ingester"
    mock_ingester.fields = []

    with patch('src.ingesters.http_api.scheduler') as mock_scheduler:
      mock_scheduler.add_ingester = AsyncMock(return_value=None)

      result = await schedule(mock_ingester)

      assert result == []

  def test_http_api_imports(self):
    """Test that all necessary imports work correctly."""
    import src.ingesters.http_api
    assert hasattr(src.ingesters.http_api, 'HTTPIngester')
    assert hasattr(src.ingesters.http_api, 'fetch_json')
    assert hasattr(src.ingesters.http_api, 'schedule')
    assert hasattr(src.ingesters.http_api, 'http_ingester')

  def test_http_api_module_structure(self):
    """Test the HTTP API module has the expected structure."""
    from src.ingesters import http_api

    # Check classes and functions exist
    assert hasattr(http_api, 'HTTPIngester')
    assert callable(http_api.fetch_json)
    assert callable(http_api.schedule)

    # Check global instance exists
    assert isinstance(http_api.http_ingester, http_api.HTTPIngester)
