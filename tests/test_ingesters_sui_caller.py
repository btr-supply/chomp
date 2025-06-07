"""Tests for Sui Caller ingester."""
import pytest
from unittest.mock import Mock, AsyncMock, patch

from src.ingesters.sui_caller import schedule, parse_generic, max_batch_size
from src.model import Ingester, ResourceField
from src.adapters.sui_rpc import SuiRpcClient


class TestSuiCaller:
  """Test suite for Sui Caller ingester."""

  @pytest.fixture
  def mock_ingester(self):
    """Create a mock ingester for testing."""
    mock = Mock(spec=Ingester)
    mock.name = "test_sui_caller"
    mock.data_by_field = {}

    # Mock field with Sui object target
    mock_field = Mock(spec=ResourceField)
    mock_field.name = "sui_object_field"
    mock_field.target = "0x123456789abcdef"
    mock_field.selector = "balance"
    mock_field.value = None
    mock_field.transformers = []

    mock.fields = [mock_field]
    return mock

  @pytest.fixture
  def mock_sui_client(self):
    """Create a mock Sui RPC client."""
    mock = Mock(spec=SuiRpcClient)
    mock.endpoint = "https://sui-rpc.example.com"
    mock.get_multi_object_fields = AsyncMock()
    return mock

  def test_parse_generic_function(self):
    """Test the parse_generic utility function."""
    test_data = {"balance": 1000, "type": "coin"}
    result = parse_generic(test_data)
    assert result == test_data

    # Test with different data types
    assert parse_generic("string") == "string"
    assert parse_generic(42) == 42
    assert parse_generic([1, 2, 3]) == [1, 2, 3]

  def test_max_batch_size_constant(self):
    """Test that max_batch_size is properly defined."""
    assert max_batch_size == 50

  @pytest.mark.asyncio
  async def test_schedule_basic_setup(self, mock_ingester):
    """Test basic Sui caller scheduling."""
    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler:
      mock_task = Mock()
      mock_scheduler.add_ingester.return_value = mock_task

      result = await schedule(mock_ingester)

      assert len(result) == 1
      assert result[0] == mock_task
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_no_task_returned(self, mock_ingester):
    """Test when scheduler returns None."""
    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler:
      mock_scheduler.add_ingester.return_value = None

      result = await schedule(mock_ingester)

      assert result == []

  @pytest.mark.asyncio
  async def test_ingest_single_object_success(self, mock_ingester, mock_sui_client):
    """Test successful ingestion of a single Sui object."""
    # Mock the object data response
    mock_object_data = [{"balance": 1000, "type": "coin"}]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'):

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Verify object was fetched
      mock_sui_client.get_multi_object_fields.assert_called_once()

  @pytest.mark.asyncio
  async def test_ingest_multiple_objects_batching(self, mock_sui_client):
    """Test batching of multiple objects when exceeding batch size."""
    # Create ingester with many fields to test batching
    mock = Mock(spec=Ingester)
    mock.name = "test_batching"
    mock.data_by_field = {}

    # Create more fields than batch size
    fields = []
    for i in range(75):  # More than max_batch_size of 50
      field = Mock(spec=ResourceField)
      field.name = f"field_{i}"
      field.target = f"0x{i:040x}"
      field.selector = "balance"
      field.value = None
      field.transformers = []
      fields.append(field)

    mock.fields = fields

    # Mock responses for multiple batches
    mock_object_data = [{"balance": i * 100} for i in range(50)]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'):

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = True
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock)

      # Should call RPC multiple times for batching
      assert mock_sui_client.get_multi_object_fields.call_count >= 2

  @pytest.mark.asyncio
  async def test_ingest_with_selector(self, mock_ingester, mock_sui_client):
    """Test field extraction using selectors."""
    # Mock object data with the field selector
    mock_object_data = [{"balance": 1000, "type": "coin", "owner": "0xabc"}]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'):

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Verify field was extracted using selector
      assert mock_ingester.fields[0].value == [1000]  # balance field
      assert mock_ingester.data_by_field["sui_object_field"] == [1000]

  @pytest.mark.asyncio
  async def test_ingest_object_not_found(self, mock_ingester, mock_sui_client):
    """Test handling when object is not found."""
    # Mock empty response (object not found)
    mock_sui_client.get_multi_object_fields.return_value = [None]

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_error') as mock_log_error:

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should log error for missing object
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_ingest_rpc_error_retry(self, mock_ingester):
    """Test RPC error handling and retry logic."""
    mock_sui_client1 = Mock(spec=SuiRpcClient)
    mock_sui_client1.endpoint = "https://rpc1.example.com"
    mock_sui_client1.get_multi_object_fields = AsyncMock(side_effect=Exception("RPC Error"))

    mock_sui_client2 = Mock(spec=SuiRpcClient)
    mock_sui_client2.endpoint = "https://rpc2.example.com"
    mock_sui_client2.get_multi_object_fields = AsyncMock(return_value=[{"balance": 1000}])

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_error') as mock_log_error:

      # Return different clients on subsequent calls
      mock_state.web3.client.side_effect = [mock_sui_client1, mock_sui_client2]
      mock_state.args.max_retries = 3
      mock_state.args.verbose = True
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should log error about switching RPC
      mock_log_error.assert_called()
      # Should eventually succeed with second client
      mock_sui_client2.get_multi_object_fields.assert_called_once()

  @pytest.mark.asyncio
  async def test_ingest_max_retries_exceeded(self, mock_ingester):
    """Test behavior when max retries is exceeded."""
    mock_sui_client = Mock(spec=SuiRpcClient)
    mock_sui_client.endpoint = "https://rpc.example.com"
    mock_sui_client.get_multi_object_fields = AsyncMock(side_effect=Exception("Persistent Error"))

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_error') as mock_log_error:

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 2
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should log error about max retries
      mock_log_error.assert_called()
      # Should have attempted max_retries times
      assert mock_sui_client.get_multi_object_fields.call_count >= 2

  @pytest.mark.asyncio
  async def test_ingest_client_type_error(self, mock_ingester):
    """Test handling when client is not SuiRpcClient type."""
    # Mock wrong client type
    wrong_client = Mock()  # Not a SuiRpcClient

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_error') as mock_log_error:

      mock_state.web3.client.return_value = wrong_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should handle type error gracefully
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_ingest_verbose_logging(self, mock_ingester, mock_sui_client):
    """Test verbose logging during ingestion."""
    mock_object_data = [{"balance": 1000}]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_debug') as mock_log_debug:

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = True
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should log debug messages when verbose
      mock_log_debug.assert_called()

  @pytest.mark.asyncio
  async def test_ingest_field_processing_error(self, mock_ingester, mock_sui_client):
    """Test handling of field processing errors."""
    mock_object_data = [{"balance": 1000}]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    # Make field processing raise an error
    def side_effect(*args, **kwargs):
      raise Exception("Field processing error")

    mock_ingester.fields[0].selector = property(side_effect)

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_error') as mock_log_error:

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should handle field processing errors gracefully
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_ingest_field_without_target(self, mock_sui_client):
    """Test handling fields that don't have targets."""
    mock = Mock(spec=Ingester)
    mock.name = "test_no_target"
    mock.data_by_field = {}

    # Field without target
    field_no_target = Mock(spec=ResourceField)
    field_no_target.name = "no_target_field"
    field_no_target.target = None
    field_no_target.selector = "balance"
    field_no_target.value = None
    field_no_target.transformers = []

    mock.fields = [field_no_target]

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'):

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock)

      # Should not call RPC when no fields have targets
      mock_sui_client.get_multi_object_fields.assert_not_called()

  @pytest.mark.asyncio
  async def test_ingest_field_without_selector(self, mock_ingester, mock_sui_client):
    """Test field extraction without selector."""
    mock_ingester.fields[0].selector = None
    mock_object_data = [{"balance": 1000, "type": "coin"}]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'):

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should handle field without selector
      mock_sui_client.get_multi_object_fields.assert_called_once()

  @pytest.mark.asyncio
  async def test_ingest_unique_object_deduplication(self, mock_sui_client):
    """Test deduplication of unique object IDs."""
    mock = Mock(spec=Ingester)
    mock.name = "test_dedup"
    mock.data_by_field = {}

    # Create fields with duplicate targets
    target_id = "0x123456789abcdef"
    field1 = Mock(spec=ResourceField)
    field1.name = "field1"
    field1.target = target_id
    field1.selector = "balance"
    field1.value = None
    field1.transformers = []

    field2 = Mock(spec=ResourceField)
    field2.name = "field2"
    field2.target = target_id  # Same target
    field2.selector = "type"
    field2.value = None
    field2.transformers = []

    mock.fields = [field1, field2]

    mock_object_data = [{"balance": 1000, "type": "coin"}]
    mock_sui_client.get_multi_object_fields.return_value = mock_object_data

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'):

      mock_state.web3.client.return_value = mock_sui_client
      mock_state.args.max_retries = 3
      mock_state.args.verbose = False
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock)

      # Should only fetch unique objects once
      call_args = mock_sui_client.get_multi_object_fields.call_args[0][0]
      assert len(call_args) == 1  # Only one unique object ID
      assert call_args[0] == target_id

  @pytest.mark.asyncio
  async def test_ingest_rpc_switching_verbose_logging(self, mock_ingester):
    """Test verbose logging during RPC switching."""
    mock_sui_client1 = Mock(spec=SuiRpcClient)
    mock_sui_client1.endpoint = "https://rpc1.example.com"
    mock_sui_client1.get_multi_object_fields = AsyncMock(side_effect=Exception("RPC Error"))

    mock_sui_client2 = Mock(spec=SuiRpcClient)
    mock_sui_client2.endpoint = "https://rpc2.example.com"
    mock_sui_client2.get_multi_object_fields = AsyncMock(return_value=[{"balance": 1000}])

    with patch('src.ingesters.sui_caller.scheduler') as mock_scheduler, \
         patch('src.ingesters.sui_caller.ensure_claim_task'), \
         patch('src.ingesters.sui_caller.state') as mock_state, \
         patch('src.ingesters.sui_caller.transform_and_store'), \
         patch('src.ingesters.sui_caller.log_error') as mock_log_error, \
         patch('src.ingesters.sui_caller.log_debug') as mock_log_debug:

      mock_state.web3.client.side_effect = [mock_sui_client1, mock_sui_client2]
      mock_state.args.max_retries = 3
      mock_state.args.verbose = True
      mock_scheduler.add_ingester.return_value = Mock()

      await schedule(mock_ingester)

      # Should log RPC switching in verbose mode
      mock_log_debug.assert_called()
      mock_log_error.assert_called()
