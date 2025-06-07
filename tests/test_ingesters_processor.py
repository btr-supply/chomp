"""Tests for processor ingester module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os
import tempfile
import asyncio

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingesters.processor import load_handler, schedule


class TestProcessorIngester:
  """Test the processor ingester functionality."""

  @pytest.mark.asyncio
  async def test_load_handler_with_callable(self):
    """Test load_handler when passed a callable function."""

    def dummy_handler(c, inputs):
      return {"result": "success"}

    result = await load_handler(dummy_handler)
    assert result == dummy_handler
    assert callable(result)

  @pytest.mark.asyncio
  async def test_load_handler_with_python_file(self):
    """Test load_handler with a Python file path."""
    # Create a temporary Python file with a handler function
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py',
                                     delete=False) as f:
      f.write('''
def handler(c, inputs):
    return {"processed": True}
''')
      f.flush()
      temp_file = f.name

    try:
      handler = await load_handler(temp_file)
      assert callable(handler)

      # Test the loaded handler
      result = handler(None, {})
      assert result == {"processed": True}
    finally:
      os.unlink(temp_file)

  @pytest.mark.asyncio
  async def test_load_handler_with_invalid_python_file(self):
    """Test load_handler with invalid Python file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py',
                                     delete=False) as f:
      f.write('invalid python syntax !!!')
      f.flush()
      temp_file = f.name

    try:
      with pytest.raises(Exception):
        await load_handler(temp_file)
    finally:
      os.unlink(temp_file)

  @pytest.mark.asyncio
  async def test_load_handler_with_missing_handler_function(self):
    """Test load_handler with Python file missing handler function."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py',
                                     delete=False) as f:
      f.write('def other_function(): pass')
      f.flush()
      temp_file = f.name

    try:
      with pytest.raises(AttributeError):
        await load_handler(temp_file)
    finally:
      os.unlink(temp_file)

  @pytest.mark.asyncio
  async def test_load_handler_with_nonexistent_file(self):
    """Test load_handler with nonexistent file."""
    with pytest.raises(Exception):
      await load_handler("/nonexistent/file.py")

  @pytest.mark.asyncio
  async def test_load_handler_with_inline_code(self):
    """Test load_handler with inline code evaluation."""
    with patch('src.ingesters.processor.safe_eval') as mock_safe_eval:
      mock_handler = Mock()
      mock_safe_eval.return_value = mock_handler

      result = await load_handler("lambda c, inputs: {'test': True}")

      assert result == mock_handler
      mock_safe_eval.assert_called_once_with(
          "lambda c, inputs: {'test': True}", callable_check=True)

  @pytest.mark.asyncio
  async def test_load_handler_safe_eval_error(self):
    """Test load_handler when safe_eval raises an error."""
    with patch('src.ingesters.processor.safe_eval',
               side_effect=Exception("Eval error")):
      with pytest.raises(Exception, match="Eval error"):
        await load_handler("invalid code")

  @pytest.mark.asyncio
  async def test_schedule_with_handler(self):
    """Test schedule function with a custom handler."""
    # Create mock ingester
    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 10
    mock_ingester.handler = lambda c, inputs: {"result": "processed"}
    mock_ingester.fields = [
        Mock(name="result", selector=None),
    ]
    mock_ingester.dependencies.return_value = ["dep1", "dep2"]

    # Mock dependencies
    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.sleep'), \
         patch('src.ingesters.processor.get_cache') as mock_get_cache, \
         patch('src.ingesters.processor.gather') as mock_gather, \
         patch('src.ingesters.processor.transform_and_store'), \
         patch('src.ingesters.processor.scheduler') as mock_scheduler, \
         patch('src.ingesters.processor.state') as mock_state:

      # Setup mocks
      mock_claim.return_value = None
      mock_get_cache.return_value = {"data": "value"}
      mock_gather.return_value = [{"dep1_data": True}, {"dep2_data": False}]
      mock_task = Mock()
      mock_scheduler.add_ingester = AsyncMock(return_value=mock_task)
      mock_state.args.verbose = False

      result = await schedule(mock_ingester)

      assert result == [mock_task]
      mock_scheduler.add_ingester.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_without_handler(self):
    """Test schedule function without a custom handler."""
    # Create mock ingester without handler
    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.selector = "dep1.value"
    mock_field1.value = None

    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.selector = "dep2.other"
    mock_field2.value = None

    mock_field3 = Mock()
    mock_field3.name = "field3"
    mock_field3.selector = None
    mock_field3.value = None

    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 20
    mock_ingester.fields = [mock_field1, mock_field2, mock_field3]
    mock_ingester.dependencies.return_value = ["dep1", "dep2"]

    # Remove handler attribute
    if hasattr(mock_ingester, 'handler'):
      delattr(mock_ingester, 'handler')

    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.sleep'), \
         patch('src.ingesters.processor.get_cache') as mock_get_cache, \
         patch('src.ingesters.processor.transform_and_store'), \
         patch('src.ingesters.processor.scheduler') as mock_scheduler, \
         patch('src.ingesters.processor.state') as mock_state, \
         patch('src.ingesters.processor.log_warn') as mock_log_warn:

      # Setup mocks
      mock_claim.return_value = None
      mock_get_cache.side_effect = [{
          "value": "dep1_val"
      }, {
          "other": "dep2_val"
      }]
      mock_state.args.verbose = True

      # Mock scheduler to execute the ingest function
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      # Verify field values were set from selectors
      assert mock_field1.value == "dep1_val"
      assert mock_field2.value == "dep2_val"

      # Verify warning for field without selector
      mock_log_warn.assert_called()

  @pytest.mark.asyncio
  async def test_schedule_no_dependencies(self):
    """Test schedule function with no dependencies."""
    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 0  # No wait time
    mock_ingester.fields = []
    mock_ingester.dependencies.return_value = []

    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.get_cache'), \
         patch('src.ingesters.processor.gather') as mock_gather, \
         patch('src.ingesters.processor.transform_and_store'), \
         patch('src.ingesters.processor.scheduler') as mock_scheduler, \
         patch('src.ingesters.processor.log_warn') as mock_log_warn:

      # Setup mocks
      mock_claim.return_value = None
      mock_gather.return_value = []

      # Mock scheduler to execute the ingest function
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      mock_log_warn.assert_called_with(
          "No dependency data available for test_processor")

  @pytest.mark.asyncio
  async def test_schedule_handler_error(self):
    """Test schedule function when handler raises an error."""

    def error_handler(c, inputs):
      raise Exception("Handler error")

    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 10
    mock_ingester.handler = error_handler
    mock_ingester.fields = []
    mock_ingester.dependencies.return_value = ["dep1"]

    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.sleep'), \
         patch('src.ingesters.processor.get_cache') as mock_get_cache, \
         patch('src.ingesters.processor.scheduler') as mock_scheduler, \
         patch('src.ingesters.processor.state') as mock_state, \
         patch('src.ingesters.processor.log_error') as mock_log_error:

      # Setup mocks
      mock_claim.return_value = None
      mock_get_cache.return_value = {"dep_data": True}
      mock_state.args.verbose = False

      # Mock scheduler to execute the ingest function
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_schedule_no_task_returned(self):
    """Test schedule function when scheduler returns None."""
    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 10
    mock_ingester.fields = []
    mock_ingester.dependencies.return_value = []

    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.scheduler') as mock_scheduler:

      mock_claim.return_value = None
      mock_scheduler.add_ingester = AsyncMock(return_value=None)

      result = await schedule(mock_ingester)

      assert result == []

  @pytest.mark.asyncio
  async def test_schedule_verbose_logging(self):
    """Test schedule function with verbose logging enabled."""
    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 30
    mock_ingester.fields = []
    mock_ingester.dependencies.return_value = []

    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.sleep') as mock_sleep, \
         patch('src.ingesters.processor.get_cache'), \
         patch('src.ingesters.processor.gather') as mock_gather, \
         patch('src.ingesters.processor.transform_and_store'), \
         patch('src.ingesters.processor.scheduler') as mock_scheduler, \
         patch('src.ingesters.processor.state') as mock_state, \
         patch('src.ingesters.processor.log_debug') as mock_log_debug:

      # Setup mocks
      mock_claim.return_value = None
      mock_gather.return_value = []
      mock_state.args.verbose = True

      # Mock scheduler to execute the ingest function
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      mock_log_debug.assert_called_with(
          "Waiting 15s for dependencies to be processed...")
      mock_sleep.assert_called_with(15)

  @pytest.mark.asyncio
  async def test_schedule_field_value_update(self):
    """Test that field values are updated correctly from handler results."""

    def test_handler(c, inputs):
      return {"field1": "handler_value1", "field2": "handler_value2"}

    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.selector = None

    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.selector = None

    mock_field3 = Mock()
    mock_field3.name = "field3"
    mock_field3.selector = None  # Not in handler results

    mock_ingester = Mock()
    mock_ingester.name = "test_processor"
    mock_ingester.interval_sec = 10
    mock_ingester.handler = test_handler
    mock_ingester.fields = [mock_field1, mock_field2, mock_field3]
    mock_ingester.dependencies.return_value = ["dep1"]

    with patch('src.ingesters.processor.ensure_claim_task') as mock_claim, \
         patch('src.ingesters.processor.sleep'), \
         patch('src.ingesters.processor.get_cache') as mock_get_cache, \
         patch('src.ingesters.processor.transform_and_store'), \
         patch('src.ingesters.processor.scheduler') as mock_scheduler, \
         patch('src.ingesters.processor.state') as mock_state, \
         patch('src.ingesters.processor.log_warn') as mock_log_warn:

      # Setup mocks
      mock_claim.return_value = None
      mock_get_cache.return_value = {"dep_data": True}
      mock_state.args.verbose = False

      # Mock scheduler to execute the ingest function
      async def mock_add_ingester(ingester, fn=None, start=False):
        if fn:
          await fn(ingester)  # Execute the ingest function
        return Mock()

      mock_scheduler.add_ingester = mock_add_ingester

      result = await schedule(mock_ingester)

      assert len(result) == 1
      assert mock_field1.value == "handler_value1"
      assert mock_field2.value == "handler_value2"

      # Verify warning for missing computed field
      mock_log_warn.assert_called_with(
          "Handler did not return value for field field3")

  @pytest.mark.asyncio
  async def test_load_handler_spec_creation_failure(self):
    """Test load_handler when spec creation fails."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py',
                                     delete=False) as f:
      f.write('def handler(): pass')
      f.flush()
      temp_file = f.name

    try:
      with patch('importlib.util.spec_from_file_location', return_value=None):
        with pytest.raises(ImportError, match="Could not load spec"):
          await load_handler(temp_file)
    finally:
      os.unlink(temp_file)

  def test_processor_imports(self):
    """Test that all necessary imports work correctly."""
    import src.ingesters.processor
    assert hasattr(src.ingesters.processor, 'load_handler')
    assert hasattr(src.ingesters.processor, 'schedule')

  def test_processor_module_structure(self):
    """Test the processor module has the expected structure."""
    from src.ingesters import processor

    # Check functions exist and are callable
    assert callable(processor.load_handler)
    assert callable(processor.schedule)

    # Check they are async functions
    assert asyncio.iscoroutinefunction(processor.load_handler)
    assert asyncio.iscoroutinefunction(processor.schedule)
