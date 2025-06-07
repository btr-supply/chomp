"""Tests for EVM caller ingester module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.deps import safe_import

# Check if EVM dependencies are available
web3 = safe_import("web3")
multicall = safe_import("multicall")
EVM_AVAILABLE = all([web3 is not None, multicall is not None])

# Only import if dependencies are available
if EVM_AVAILABLE:
  from src.ingesters.evm_caller import parse_generic, schedule
  from src.model import Ingester, ResourceField


@pytest.mark.skipif(not EVM_AVAILABLE, reason="EVM dependencies not available (web3, multicall)")
class TestEVMCaller:
  """Test the EVM caller ingester functionality."""

  def test_parse_generic(self):
    """Test that parse_generic returns data unchanged."""
    test_data = {"key": "value", "number": 123}
    result = parse_generic(test_data)
    assert result == test_data

    test_string = "test string"
    result = parse_generic(test_string)
    assert result == test_string

    test_list = [1, 2, 3]
    result = parse_generic(test_list)
    assert result == test_list

  @pytest.mark.asyncio
  async def test_schedule_basic_functionality(self):
    """Test basic schedule functionality."""
    # Create mock ingester
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_caller"
    mock_ingester.data_by_field = {}
    mock_ingester.field_by_name = Mock(return_value={})

    # Create mock field
    mock_field = Mock(spec=ResourceField)
    mock_field.target = "0x1234567890123456789012345678901234567890:ETH"
    mock_field.selector = "totalSupply()"
    mock_field.params = []
    mock_field.selector_outputs = ["uint256"]
    mock_field.name = "total_supply"
    mock_field.id = "test_field_id"
    mock_field.chain_addr = Mock(return_value=("ETH", "0x1234567890123456789012345678901234567890"))

    mock_ingester.fields = [mock_field]

    with patch('src.ingesters.evm_caller.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_caller.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.state') as mock_state:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      # Mock web3 client
      mock_client = Mock()
      mock_client.eth.chain_id = 1  # Ethereum mainnet
      mock_state.web3.client = AsyncMock(return_value=mock_client)

      # Mock multicall result
      mock_multicall_result = {"total_supply:0": 1000000}
      mock_state.thread_pool.submit.return_value.result.return_value = mock_multicall_result

      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_multiple_chains(self):
    """Test schedule with fields from multiple chains."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_caller"
    mock_ingester.data_by_field = {}
    mock_ingester.field_by_name = Mock(return_value={})

    # Create mock fields for different chains
    mock_field1 = Mock(spec=ResourceField)
    mock_field1.target = "0x1234567890123456789012345678901234567890:ETH"
    mock_field1.selector = "totalSupply()"
    mock_field1.params = []
    mock_field1.selector_outputs = ["uint256"]
    mock_field1.name = "eth_total_supply"
    mock_field1.id = "eth_field_id"
    mock_field1.chain_addr = Mock(return_value=("ETH", "0x1234567890123456789012345678901234567890"))

    mock_field2 = Mock(spec=ResourceField)
    mock_field2.target = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd:BSC"
    mock_field2.selector = "balanceOf(address)"
    mock_field2.params = ["0x1111111111111111111111111111111111111111"]
    mock_field2.selector_outputs = ["uint256"]
    mock_field2.name = "bsc_balance"
    mock_field2.id = "bsc_field_id"
    mock_field2.chain_addr = Mock(return_value=("BSC", "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"))

    mock_ingester.fields = [mock_field1, mock_field2]

    with patch('src.ingesters.evm_caller.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_caller.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.state') as mock_state:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      # Mock web3 clients for different chains
      mock_eth_client = Mock()
      mock_eth_client.eth.chain_id = 1
      mock_bsc_client = Mock()
      mock_bsc_client.eth.chain_id = 56

      def mock_client_getter(chain_id, roll=False):
        if chain_id == "ETH":
          return mock_eth_client
        elif chain_id == "BSC":
          return mock_bsc_client
        return mock_eth_client

      mock_state.web3.client = AsyncMock(side_effect=mock_client_getter)

      # Mock multicall results
      mock_results = [
        {"eth_total_supply:0": 1000000},  # ETH result
        {"bsc_balance:0": 500000}         # BSC result
      ]
      mock_state.thread_pool.submit.return_value.result.side_effect = mock_results

      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_with_multicall_failure(self):
    """Test schedule handling multicall failures gracefully."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_caller"
    mock_ingester.data_by_field = {}
    mock_ingester.field_by_name = Mock(return_value={})

    mock_field = Mock(spec=ResourceField)
    mock_field.target = "0x1234567890123456789012345678901234567890:ETH"
    mock_field.selector = "totalSupply()"
    mock_field.params = []
    mock_field.selector_outputs = ["uint256"]
    mock_field.name = "total_supply"
    mock_field.id = "test_field_id"
    mock_field.chain_addr = Mock(return_value=("ETH", "0x1234567890123456789012345678901234567890"))

    mock_ingester.fields = [mock_field]

    with patch('src.ingesters.evm_caller.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_caller.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.state') as mock_state, \
         patch('src.ingesters.evm_caller.log_error'):

      mock_state.args.verbose = False
      mock_state.args.max_retries = 1
      mock_state.thread_pool = Mock()

      mock_client = Mock()
      mock_client.eth.chain_id = 1
      mock_state.web3.client = AsyncMock(return_value=mock_client)

      # Mock multicall failure
      mock_state.thread_pool.submit.return_value.result.side_effect = [Exception("Multicall failed")]

      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_no_fields(self):
    """Test schedule with empty fields list."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_caller"
    mock_ingester.data_by_field = {}
    mock_ingester.field_by_name = Mock(return_value={})
    mock_ingester.fields = []

    with patch('src.ingesters.evm_caller.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_caller.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.state') as mock_state:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_duplicate_targets(self):
    """Test schedule with duplicate target addresses (should skip duplicates)."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_caller"
    mock_ingester.data_by_field = {}
    mock_ingester.field_by_name = Mock(return_value={})

    # Create fields with duplicate targets
    mock_field1 = Mock(spec=ResourceField)
    mock_field1.target = "0x1234567890123456789012345678901234567890:ETH"
    mock_field1.selector = "totalSupply()(uint256)"
    mock_field1.params = []
    mock_field1.selector_outputs = ["uint256"]
    mock_field1.name = "total_supply_1"
    mock_field1.id = "duplicate_id"  # Same ID to trigger duplicate detection
    mock_field1.chain_addr = Mock(return_value=("ETH", "0x1234567890123456789012345678901234567890"))

    mock_field2 = Mock(spec=ResourceField)
    mock_field2.target = "0x1234567890123456789012345678901234567890:ETH"
    mock_field2.selector = "totalSupply()(uint256)"
    mock_field2.params = []
    mock_field2.selector_outputs = ["uint256"]
    mock_field2.name = "total_supply_2"
    mock_field2.id = "duplicate_id"  # Same ID to trigger duplicate detection
    mock_field2.chain_addr = Mock(return_value=("ETH", "0x1234567890123456789012345678901234567890"))

    mock_ingester.fields = [mock_field1, mock_field2]

    with patch('src.ingesters.evm_caller.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_caller.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.state') as mock_state, \
         patch('src.ingesters.evm_caller.log_warn') as mock_log_warn:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      mock_client = Mock()
      mock_client.eth.chain_id = 1
      mock_state.web3.client = AsyncMock(return_value=mock_client)

      # Mock scheduler to execute the ingest function to test duplicate detection
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      # Should log warning about duplicate
      mock_log_warn.assert_called()

  @pytest.mark.asyncio
  async def test_schedule_no_task_returned(self):
    """Test schedule when no task is returned from scheduler."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_evm_caller"
    mock_ingester.data_by_field = {}
    mock_ingester.field_by_name = Mock(return_value={})
    mock_ingester.fields = []

    with patch('src.ingesters.evm_caller.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.evm_caller.transform_and_store', new_callable=AsyncMock), \
         patch('src.ingesters.evm_caller.state') as mock_state:

      mock_state.args.verbose = False
      mock_state.args.max_retries = 3
      mock_state.thread_pool = Mock()

      mock_scheduler.add_ingester = AsyncMock(return_value=None)

      result = await schedule(mock_ingester)

      assert result == []
      mock_scheduler.add_ingester.assert_called_once()

  def test_evm_caller_imports(self):
    """Test that EVM caller functions can be imported."""
    assert parse_generic is not None
    assert schedule is not None
    assert callable(parse_generic)
    assert callable(schedule)


# Integration tests that don't require full dependencies
class TestEVMCallerIntegration:
  """Integration tests for EVM caller module."""

  def test_parse_generic_function_exists(self):
    """Test that parse_generic function exists."""
    if EVM_AVAILABLE:
      assert parse_generic is not None
      assert callable(parse_generic)

  def test_schedule_function_exists(self):
    """Test that schedule function exists."""
    if EVM_AVAILABLE:
      assert schedule is not None
      assert callable(schedule)
