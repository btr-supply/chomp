"""Tests for EVM logger ingester module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from chomp.src.utils.deps import safe_import

# Check if EVM dependencies are available
web3 = safe_import("web3")
EVM_AVAILABLE = web3 is not None

# Only import if dependencies are available
if EVM_AVAILABLE:
  from src.ingesters.evm_logger import (parse_event_signature, decode_log_data,
                                        reorder_decoded_params, schedule)
  from src.models import Ingester, ResourceField


@pytest.mark.skipif(not EVM_AVAILABLE,
                    reason="EVM dependencies not available (web3)")
class TestEVMLogger:
  """Test the EVM logger ingester functionality."""

  def test_parse_event_signature_basic(self):
    """Test parsing basic event signature."""
    signature = "Transfer(address indexed from, address indexed to, uint256 value)"

    name, types, indexed = parse_event_signature(signature)

    assert name == "Transfer"
    assert types == ["from", "to", "value"]
    assert indexed == [True, True, False]

  def test_parse_event_signature_no_indexed(self):
    """Test parsing event signature without indexed parameters."""
    signature = "Approval(address owner, address spender, uint256 value)"

    name, types, indexed = parse_event_signature(signature)

    assert name == "Approval"
    assert types == ["owner", "spender", "value"]
    assert indexed == [False, False, False]

  def test_parse_event_signature_all_indexed(self):
    """Test parsing event signature with all indexed parameters."""
    signature = "LogData(bytes32 indexed id, uint256 indexed timestamp)"

    name, types, indexed = parse_event_signature(signature)

    assert name == "LogData"
    assert types == ["id", "timestamp"]
    assert indexed == [True, True]

  def test_reorder_decoded_params_mixed(self):
    """Test reordering decoded parameters with mixed indexed/non-indexed."""
    decoded = [0x123, 0x456, 1000]  # indexed params first, then non-indexed
    indexed = [True, False, True]  # first and third are indexed

    result = reorder_decoded_params(decoded, indexed)

    # Should be: indexed[0], non-indexed[0], indexed[1]
    # decoded = [indexed_1, indexed_2, non_indexed_1] = [0x123, 0x456, 1000]
    # result should be [indexed_1, non_indexed_1, indexed_2] = [0x123, 1000, 0x456]
    assert result == [0x123, 1000, 0x456]

  def test_reorder_decoded_params_all_indexed(self):
    """Test reordering when all parameters are indexed."""
    decoded = [0x123, 0x456, 0x789]
    indexed = [True, True, True]

    result = reorder_decoded_params(decoded, indexed)

    assert result == [0x123, 0x456, 0x789]

  def test_reorder_decoded_params_none_indexed(self):
    """Test reordering when no parameters are indexed."""
    decoded = [0x123, 0x456, 0x789]
    indexed = [False, False, False]

    result = reorder_decoded_params(decoded, indexed)

    assert result == [0x123, 0x456, 0x789]

  @pytest.mark.asyncio
  async def test_decode_log_data(self):
    """Test decoding log data."""
    # Mock Web3 client
    mock_client = Mock()
    mock_client.codec.decode.return_value = [0x123, 0x456, 1000]

    # Mock log entry
    mock_log = {
        'topics': [
            b'\x01\x02\x03\x04',  # This would be the event signature hash
            b'\x05\x06\x07\x08',  # First indexed parameter
            b'\x09\x0a\x0b\x0c'  # Second indexed parameter
        ],
        'data':
        b'\x0d\x0e\x0f\x10'  # Non-indexed data
    }

    topics_first = ["address", "address", "uint256"]
    indexed = [True, True, False]

    with patch('src.ingesters.evm_logger.reorder_decoded_params',
               return_value=[0x123, 0x456, 1000]):
      result = decode_log_data(mock_client, mock_log, topics_first, indexed)

    assert result == (0x123, 0x456, 1000)
    mock_client.codec.decode.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_basic_functionality(self):
    """Test basic schedule functionality."""
    # Create mock ingester
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_logger"
    mock_ingester.interval = "m5"

    # Create mock field
    mock_field = Mock(spec=ResourceField)
    mock_field.target = "1:0x1234567890123456789012345678901234567890"
    mock_field.selector = "Transfer(address indexed from, address indexed to, uint256 value)"
    mock_field.name = "transfer_events"

    mock_ingester.fields = [mock_field]

    with patch('src.ingesters.evm_logger.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_logger.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_logger.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_logger.state') as mock_state:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_multiple_fields(self):
    """Test schedule with multiple fields."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_logger"
    mock_ingester.interval = "m5"

    # Create multiple mock fields
    mock_field1 = Mock(spec=ResourceField)
    mock_field1.target = "1:0x1234567890123456789012345678901234567890"
    mock_field1.selector = "Transfer(address indexed from, address indexed to, uint256 value)"
    mock_field1.name = "transfer_events"

    mock_field2 = Mock(spec=ResourceField)
    mock_field2.target = "1:0xdac17f958d2ee523a2206206994597c13d831ec7"
    mock_field2.selector = "Approval(address indexed owner, address indexed spender, uint256 value)"
    mock_field2.name = "approval_events"

    mock_ingester.fields = [mock_field1, mock_field2]

    with patch('src.ingesters.evm_logger.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_logger.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_logger.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_logger.state') as mock_state, \
         patch('src.ingesters.evm_logger.split_chain_addr') as mock_split:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()
      mock_split.side_effect = [
          (1, "0x1234567890123456789012345678901234567890"),
          (1, "0xdac17f958d2ee523a2206206994597c13d831ec7")
      ]

      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_no_task_returned(self):
    """Test schedule when no task is returned from scheduler."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_logger"
    mock_ingester.fields = []

    with patch('src.ingesters.evm_logger.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_logger.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_logger.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_logger.state') as mock_state:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      mock_scheduler.add_ingester = AsyncMock(return_value=None)

      result = await schedule(mock_ingester)

      assert result == []
      mock_scheduler.add_ingester.assert_called_once()

  def test_evm_logger_imports(self):
    """Test that EVM logger functions can be imported."""
    assert parse_event_signature is not None
    assert decode_log_data is not None
    assert reorder_decoded_params is not None
    assert schedule is not None
    assert callable(parse_event_signature)
    assert callable(decode_log_data)
    assert callable(reorder_decoded_params)
    assert callable(schedule)


# Integration tests that don't require full dependencies
class TestEVMLoggerIntegration:
  """Integration tests for EVM logger module."""

  def test_parse_event_signature_function_exists(self):
    """Test that parse_event_signature function exists."""
    if EVM_AVAILABLE:
      assert parse_event_signature is not None
      assert callable(parse_event_signature)

  def test_schedule_function_exists(self):
    """Test that schedule function exists."""
    if EVM_AVAILABLE:
      assert schedule is not None
      assert callable(schedule)
