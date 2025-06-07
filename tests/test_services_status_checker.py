"""
Purpose: Test suite for services/status_checker module
Tests the status checking functionality for service health monitoring
"""

import pytest
from unittest.mock import Mock, patch
from fastapi import Request
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.status_checker import check_status, ping


class TestStatusChecker:
  """Test status checking functionality."""

  @pytest.mark.asyncio
  @patch('src.services.status_checker.now')
  @patch('src.services.status_checker.requester_id')
  async def test_check_status_success_with_utc_time(self, mock_requester_id, mock_now):
    """Test successful status check with provided UTC time."""
    mock_request = Mock(spec=Request)
    mock_client = Mock()
    mock_client.host = "192.168.1.100"
    mock_request.client = mock_client

    # Mock current timestamp to 1000000
    mock_datetime = Mock()
    mock_datetime.timestamp.return_value = 1000.0  # 1000000ms
    mock_now.return_value = mock_datetime
    mock_requester_id.return_value = "test_id"

    utc_time = 995000  # 5 seconds earlier (5000ms difference)

    error, result = await check_status(mock_request, utc_time)

    assert error == ""
    assert result["status"] == "OK"
    assert result["ping_ms"] == 5000  # server_time - utc_time
    assert result["server_time"] == 1000000
    assert result["id"] == "test_id"
    assert result["ip"] == "192.168.1.100"

  @pytest.mark.asyncio
  @patch('src.services.status_checker.now')
  @patch('src.services.status_checker.requester_id')
  async def test_check_status_success_without_utc_time(self, mock_requester_id, mock_now):
    """Test successful status check without provided UTC time."""
    mock_request = Mock(spec=Request)
    mock_client = Mock()
    mock_client.host = "10.0.0.1"
    mock_request.client = mock_client

    mock_datetime = Mock()
    mock_datetime.timestamp.return_value = 2000.0  # 2000000ms
    mock_now.return_value = mock_datetime
    mock_requester_id.return_value = "test_id_2"

    error, result = await check_status(mock_request, None)

    assert error == ""
    assert result["status"] == "OK"
    assert result["ping_ms"] == 5  # Uses default utc_time = server_time - 5
    assert result["server_time"] == 2000000
    assert result["id"] == "test_id_2"
    assert result["ip"] == "10.0.0.1"

  @pytest.mark.asyncio
  @patch('src.services.status_checker.now')
  @patch('src.services.status_checker.requester_id')
  async def test_check_status_no_client(self, mock_requester_id, mock_now):
    """Test status check when request has no client."""
    mock_request = Mock(spec=Request)
    mock_request.client = None

    mock_datetime = Mock()
    mock_datetime.timestamp.return_value = 1500.0
    mock_now.return_value = mock_datetime
    mock_requester_id.return_value = "test_id_3"

    error, result = await check_status(mock_request, 1495000)

    assert error == ""
    assert result["status"] == "OK"
    assert result["ip"] == "unknown"
    assert result["id"] == "test_id_3"

  @pytest.mark.asyncio
  @patch('src.services.status_checker.now')
  @patch('src.services.status_checker.requester_id')
  async def test_ping_function_alias(self, mock_requester_id, mock_now):
    """Test that ping is an alias for check_status."""
    mock_request = Mock(spec=Request)
    mock_client = Mock()
    mock_client.host = "127.0.0.1"
    mock_request.client = mock_client

    mock_datetime = Mock()
    mock_datetime.timestamp.return_value = 3000.0
    mock_now.return_value = mock_datetime
    mock_requester_id.return_value = "ping_test"

    error, result = await ping(mock_request, 2995000)

    assert error == ""
    assert result["status"] == "OK"
    assert result["ping_ms"] == 5000
    assert result["ip"] == "127.0.0.1"

  @pytest.mark.asyncio
  @patch('src.services.status_checker.now')
  async def test_check_status_exception_handling(self, mock_now):
    """Test error handling when exception occurs."""
    mock_request = Mock(spec=Request)

    # Make now() raise an exception
    mock_now.side_effect = Exception("Test error")

    error, result = await check_status(mock_request, 1000000)

    assert "Error checking status: Test error" in error
    assert result == {}

  @pytest.mark.asyncio
  @patch('src.services.status_checker.state.meta')
  @patch('src.services.status_checker.now')
  @patch('src.services.status_checker.requester_id')
  async def test_check_status_includes_state_metadata(self, mock_requester_id, mock_now, mock_meta):
    """Test that status includes application metadata."""
    mock_request = Mock(spec=Request)
    mock_client = Mock()
    mock_client.host = "192.168.1.1"
    mock_request.client = mock_client

    mock_datetime = Mock()
    mock_datetime.timestamp.return_value = 5000.0
    mock_now.return_value = mock_datetime
    mock_requester_id.return_value = "meta_test"

    # Mock state metadata
    mock_meta.name = "test_app"
    mock_meta.version = "1.0.0"

    error, result = await check_status(mock_request, 4990000)

    assert error == ""
    assert result["name"] == "test_app"
    assert result["version"] == "1.0.0"
    assert result["status"] == "OK"
