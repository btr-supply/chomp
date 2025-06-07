"""Tests for ts_analysis module."""
import pytest
from unittest.mock import AsyncMock, Mock, patch
import sys
import os
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
  import polars as pl
  from src.services.ts_analysis import (ensure_df, add_metrics, get_volatility,
                                        get_trend, get_momentum, get_all,
                                        get_oprange)
  POLARS_AVAILABLE = True
except ImportError:
  POLARS_AVAILABLE = False


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestEnsureDF:
  """Test ensure_df function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1", "field2"]

  @pytest.mark.asyncio
  async def test_ensure_df_with_existing_df(self):
    """Test ensure_df when DataFrame is already provided."""
    mock_df = Mock(spec=pl.DataFrame)

    err, result = await ensure_df(self.resources,
                                  self.fields,
                                  self.from_date,
                                  self.to_date,
                                  df=mock_df)

    assert err == ""
    assert result == mock_df

  @pytest.mark.asyncio
  async def test_ensure_df_no_fields(self):
    """Test ensure_df with no fields provided."""
    err, result = await ensure_df(self.resources, [], self.from_date,
                                  self.to_date)

    assert err == "No fields provided"
    assert isinstance(result, pl.DataFrame)
    assert result.height == 0

  @pytest.mark.asyncio
  async def test_ensure_df_load_data_success(self):
    """Test successful data loading in ensure_df."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0],
        "field2": [3.0, 4.0]
    })

    with patch('src.services.ts_analysis.fit_date_params', return_value=(self.from_date, self.to_date, 'm5', 1000)), \
         patch('src.services.ts_analysis.loader.get_history', new_callable=AsyncMock, return_value=("", mock_df)):

      err, result = await ensure_df(self.resources, self.fields,
                                    self.from_date, self.to_date)

      assert err == ""
      assert isinstance(result, pl.DataFrame)
      assert result.height == 2

  @pytest.mark.asyncio
  async def test_ensure_df_load_data_error(self):
    """Test error handling in data loading."""
    with patch('src.services.ts_analysis.fit_date_params', return_value=(self.from_date, self.to_date, 'm5', 1000)), \
         patch('src.services.ts_analysis.loader.get_history', new_callable=AsyncMock, return_value=("Load error", pl.DataFrame())):

      err, result = await ensure_df(self.resources, self.fields,
                                    self.from_date, self.to_date)

      assert err == "Load error"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_ensure_df_empty_dataframe(self):
    """Test handling of empty DataFrame."""
    empty_df = pl.DataFrame()

    with patch('src.services.ts_analysis.fit_date_params', return_value=(self.from_date, self.to_date, 'm5', 1000)), \
         patch('src.services.ts_analysis.loader.get_history', new_callable=AsyncMock, return_value=("", empty_df)):

      err, result = await ensure_df(self.resources, self.fields,
                                    self.from_date, self.to_date)

      assert err == "No data found"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_ensure_df_invalid_result_type(self):
    """Test handling of invalid result type from get_history."""
    with patch('src.services.ts_analysis.fit_date_params', return_value=(self.from_date, self.to_date, 'm5', 1000)), \
         patch('src.services.ts_analysis.loader.get_history', new_callable=AsyncMock, return_value=("", "not_a_dataframe")):

      err, result = await ensure_df(self.resources, self.fields,
                                    self.from_date, self.to_date)

      assert err == "Expected DataFrame from get_history"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestAddMetrics:
  """Test add_metrics function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]
    self.periods = [10, 20]

    self.mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0],
        "field2": [3.0, 4.0]
    })

  @pytest.mark.asyncio
  async def test_add_metrics_success(self):
    """Test successful metrics addition."""

    def mock_get_metrics(df, col, period):
      return {f"{col}_metric_{period}": pl.Series([1.0, 2.0])}

    mock_future = Mock()
    mock_future.result.return_value = ({
        "field1_metric_10":
        pl.Series([1.0, 2.0])
    }, None)

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state:

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert err == ""
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_add_metrics_ensure_df_error(self):
    """Test error handling when ensure_df fails."""

    def mock_get_metrics(df, col, period):
      return {}

    with patch('src.services.ts_analysis.ensure_df',
               new_callable=AsyncMock,
               return_value=("Data error", pl.DataFrame())):

      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert err == "Data error"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_add_metrics_compute_error(self):
    """Test error handling when compute function fails."""

    def mock_get_metrics(df, col, period):
      return {}

    mock_future = Mock()
    mock_future.result.return_value = ({}, "Compute error")

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state:

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert err == "Compute error"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetVolatility:
  """Test get_volatility function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["price"]

    self.mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "price": [100.0, 110.0]
    })

  @pytest.mark.asyncio
  async def test_get_volatility_success(self):
    """Test successful volatility calculation."""
    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", self.mock_df)):

      err, result = await get_volatility(self.resources, self.fields,
                                         self.from_date, self.to_date, 'm5')

      assert err == ""
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_volatility_add_metrics_error(self):
    """Test error handling when add_metrics fails."""
    with patch('src.services.ts_analysis.add_metrics',
               new_callable=AsyncMock,
               return_value=("Metrics error", pl.DataFrame())):

      err, result = await get_volatility(self.resources, self.fields,
                                         self.from_date, self.to_date, 'm5')

      assert err == "Metrics error"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_get_volatility_format_error(self):
    """Test error handling when format_table fails."""
    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("Format error", None)):

      err, result = await get_volatility(self.resources, self.fields,
                                         self.from_date, self.to_date, 'm5')

      assert err == "Format error"
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_volatility_none_dataframe(self):
    """Test handling of None DataFrame."""
    with patch('src.services.ts_analysis.add_metrics',
               new_callable=AsyncMock,
               return_value=("", None)):

      err, result = await get_volatility(self.resources, self.fields,
                                         self.from_date, self.to_date, 'm5')

      assert err == "Failed to compute volatility metrics"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetTrend:
  """Test get_trend function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["price"]

    self.mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "price": [100.0, 110.0]
    })

  @pytest.mark.asyncio
  async def test_get_trend_success(self):
    """Test successful trend calculation."""
    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", self.mock_df)):

      err, result = await get_trend(self.resources, self.fields,
                                    self.from_date, self.to_date, 'm5')

      assert err == ""
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_trend_polars_format(self):
    """Test trend calculation with polars format."""
    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", self.mock_df)):

      err, result = await get_trend(self.resources,
                                    self.fields,
                                    self.from_date,
                                    self.to_date,
                                    'm5',
                                    format="polars")

      assert err == ""
      assert isinstance(result, pl.DataFrame)


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetMomentum:
  """Test get_momentum function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["price"]

    self.mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "price": [100.0, 110.0]
    })

  @pytest.mark.asyncio
  async def test_get_momentum_success(self):
    """Test successful momentum calculation."""
    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", self.mock_df)):

      err, result = await get_momentum(self.resources, self.fields,
                                       self.from_date, self.to_date, 'm5')

      assert err == ""
      assert isinstance(result, pl.DataFrame)


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetAll:
  """Test get_all function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["price"]

    self.mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "price": [100.0, 110.0]
    })

  @pytest.mark.asyncio
  async def test_get_all_success(self):
    """Test successful calculation of all metrics."""
    with patch('src.services.ts_analysis.get_volatility', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_trend', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_momentum', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"data": "formatted"})):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5')

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_all_volatility_error(self):
    """Test error handling when volatility calculation fails."""
    with patch('src.services.ts_analysis.get_volatility',
               new_callable=AsyncMock,
               return_value=("Volatility error", None)):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5')

      assert err == "Volatility error"
      assert result is None

  @pytest.mark.asyncio
  async def test_get_all_trend_error(self):
    """Test error handling when trend calculation fails."""
    with patch('src.services.ts_analysis.get_volatility', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_trend', new_callable=AsyncMock, return_value=("Trend error", None)):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5')

      assert err == "Trend error"
      assert result is None

  @pytest.mark.asyncio
  async def test_get_all_momentum_error(self):
    """Test error handling when momentum calculation fails."""
    with patch('src.services.ts_analysis.get_volatility', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_trend', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_momentum', new_callable=AsyncMock, return_value=("Momentum error", None)):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5')

      assert err == "Momentum error"
      assert result is None

  @pytest.mark.asyncio
  async def test_get_all_exception_handling(self):
    """Test exception handling in get_all."""
    with patch('src.services.ts_analysis.get_volatility',
               side_effect=Exception("Test exception")):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5')

      assert "Error computing metrics: Test exception" in err
      assert result is None

  @pytest.mark.asyncio
  async def test_get_all_format_error(self):
    """Test error handling when format_table fails."""
    with patch('src.services.ts_analysis.get_volatility', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_trend', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.get_momentum', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("Format error", None)):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5')

      assert err == "Format error"
      assert result is None


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetOprange:
  """Test get_oprange function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["price"]

    self.mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "price": [100.0, 110.0]
    })

  @pytest.mark.asyncio
  async def test_get_oprange_success(self):
    """Test successful operating range calculation."""
    mock_future = Mock()
    mock_future.result.return_value = {
        "price:min": 100.0,
        "price:max": 110.0,
        "price:range": 10.0,
        "price:range_position": 1.0
    }

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["price"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"data": "formatted"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date)

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_oprange_ensure_df_error(self):
    """Test error handling when ensure_df fails."""
    with patch('src.services.ts_analysis.ensure_df',
               new_callable=AsyncMock,
               return_value=("Data error", pl.DataFrame())):

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date)

      assert err == "Data error"
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_oprange_none_dataframe(self):
    """Test handling of None DataFrame."""
    with patch('src.services.ts_analysis.ensure_df',
               new_callable=AsyncMock,
               return_value=("", None)):

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date)

      assert err == "Failed to load DataFrame for range analysis"
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_oprange_format_error(self):
    """Test error handling when format_table fails."""
    mock_future = Mock()
    mock_future.result.return_value = {}

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", self.mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["price"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("Format error", None)):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date)

      assert err == "Format error"
      assert isinstance(result, pl.DataFrame)


# Integration tests without mocking for simple functionality
class TestTSAnalysisIntegration:
  """Integration tests for ts_analysis module."""

  def test_imports_available(self):
    """Test that ts_analysis functions can be imported."""
    if POLARS_AVAILABLE:
      assert ensure_df is not None
      assert add_metrics is not None
      assert get_volatility is not None
      assert get_trend is not None
      assert get_momentum is not None
      assert get_all is not None
      assert get_oprange is not None

  def test_functions_callable(self):
    """Test that all functions are callable."""
    if POLARS_AVAILABLE:
      assert callable(ensure_df)
      assert callable(add_metrics)
      assert callable(get_volatility)
      assert callable(get_trend)
      assert callable(get_momentum)
      assert callable(get_all)
      assert callable(get_oprange)


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestAddMetricsEdgeCases:
  """Test edge cases in add_metrics function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]
    self.periods = [10]

  @pytest.mark.asyncio
  async def test_add_metrics_none_dataframe_after_ensure(self):
    """Test add_metrics when ensure_df returns None DataFrame."""

    def mock_get_metrics(df, col, period):
      return {}

    with patch('src.services.ts_analysis.ensure_df',
               new_callable=AsyncMock,
               return_value=("", None)):
      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert err == "Failed to load DataFrame"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_add_metrics_keyerror_handling(self):
    """Test KeyError handling in compute function."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    def mock_get_metrics(df, col, period):
      # This will cause a KeyError when accessing non-existent column
      raise KeyError(f"Column '{col}' not found")

    mock_future = Mock()
    mock_future.result.return_value = (
        {}, "Required column 'field1' not found in resource resource1")

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state:

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert "Required column 'field1' not found in resource resource1" in err
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_add_metrics_expr_series_handling(self):
    """Test handling of pl.Expr objects in metrics."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    def mock_get_metrics(df, col, period):
      # Return a pl.Series object that is properly aligned
      return {f"{col}_series": pl.Series("test_series", [3.0, 3.0])}

    mock_future = Mock()
    mock_future.result.return_value = ({
        "field1_series":
        pl.Series("test_series", [3.0, 3.0])
    }, None)

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state:

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert err == ""
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_add_metrics_list_values_handling(self):
    """Test handling of list values in metrics."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    def mock_get_metrics(df, col, period):
      # Return list values instead of Series
      return {f"{col}_list": [1.0, 2.0]}

    mock_future = Mock()
    mock_future.result.return_value = ({"field1_list": [1.0, 2.0]}, None)

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state:

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await add_metrics(self.resources, self.fields,
                                      self.from_date, self.to_date, 'm5',
                                      self.periods, None, 6, None,
                                      mock_get_metrics)

      assert err == ""
      assert isinstance(result, pl.DataFrame)


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestVolatilityEdgeCases:
  """Test edge cases in get_volatility function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]

  @pytest.mark.asyncio
  async def test_get_volatility_none_dataframe_post_computation(self):
    """Test get_volatility when add_metrics returns None DataFrame."""
    with patch('src.services.ts_analysis.add_metrics',
               new_callable=AsyncMock,
               return_value=("", None)):
      err, result = await get_volatility(self.resources, self.fields,
                                         self.from_date, self.to_date, 'm5',
                                         [20], None, 6, "json:row", None)

      assert err == "Failed to compute volatility metrics"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_get_volatility_non_polars_format_return(self):
    """Test get_volatility with non-polars format returning original DataFrame."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      err, result = await get_volatility(self.resources, self.fields,
                                         self.from_date, self.to_date, 'm5',
                                         [20], None, 6, "json:row", None)

      assert err == ""
      assert result.equals(
          mock_df)  # Should return original df for non-polars format


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestTrendEdgeCases:
  """Test edge cases in get_trend function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]

  @pytest.mark.asyncio
  async def test_get_trend_none_dataframe_post_computation(self):
    """Test get_trend when add_metrics returns None DataFrame."""
    with patch('src.services.ts_analysis.add_metrics',
               new_callable=AsyncMock,
               return_value=("", None)):
      err, result = await get_trend(self.resources, self.fields,
                                    self.from_date, self.to_date, 'm5', [20],
                                    None, 6, "json:row", None)

      assert err == "Failed to compute trend metrics"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_get_trend_format_error_handling(self):
    """Test get_trend format error handling."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("Format error", None)):

      err, result = await get_trend(self.resources, self.fields,
                                    self.from_date, self.to_date, 'm5', [20],
                                    None, 6, "json:row", None)

      assert err == "Format error"
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_trend_non_polars_format_return(self):
    """Test get_trend with non-polars format returning original DataFrame."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      err, result = await get_trend(self.resources, self.fields,
                                    self.from_date, self.to_date, 'm5', [20],
                                    None, 6, "json:row", None)

      assert err == ""
      assert result.equals(
          mock_df)  # Should return original df for non-polars format


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestMomentumEdgeCases:
  """Test edge cases in get_momentum function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]

  @pytest.mark.asyncio
  async def test_get_momentum_none_dataframe_post_computation(self):
    """Test get_momentum when add_metrics returns None DataFrame."""
    with patch('src.services.ts_analysis.add_metrics',
               new_callable=AsyncMock,
               return_value=("", None)):
      err, result = await get_momentum(self.resources, self.fields,
                                       self.from_date, self.to_date, 'm5',
                                       [20], None, 6, "json:row", None)

      assert err == "Failed to compute momentum metrics"
      assert isinstance(result, pl.DataFrame)
      assert result.height == 0

  @pytest.mark.asyncio
  async def test_get_momentum_format_error_handling(self):
    """Test get_momentum format error handling."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("Format error", None)):

      err, result = await get_momentum(self.resources, self.fields,
                                       self.from_date, self.to_date, 'm5',
                                       [20], None, 6, "json:row", None)

      assert err == "Format error"
      assert isinstance(result, pl.DataFrame)

  @pytest.mark.asyncio
  async def test_get_momentum_non_polars_format_return(self):
    """Test get_momentum with non-polars format returning original DataFrame."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    with patch('src.services.ts_analysis.add_metrics', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      err, result = await get_momentum(self.resources, self.fields,
                                       self.from_date, self.to_date, 'm5',
                                       [20], None, 6, "json:row", None)

      assert err == ""
      assert result.equals(
          mock_df)  # Should return original df for non-polars format


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetAllEdgeCases:
  """Test edge cases in get_all function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]

  @pytest.mark.asyncio
  async def test_get_all_none_dataframe_final_check(self):
    """Test get_all when final df is None after all computations."""
    with patch('src.services.ts_analysis.get_volatility', new_callable=AsyncMock, return_value=("", None)), \
         patch('src.services.ts_analysis.get_trend', new_callable=AsyncMock, return_value=("", None)), \
         patch('src.services.ts_analysis.get_momentum', new_callable=AsyncMock, return_value=("", None)):

      err, result = await get_all(self.resources, self.fields, self.from_date,
                                  self.to_date, 'm5', [20], None, 6, None,
                                  "json:row")

      assert err == "Failed to compute all metrics"
      assert result is None


@pytest.mark.skipif(not POLARS_AVAILABLE,
                    reason="Polars dependencies not available")
class TestGetOprangeComprehensive:
  """Comprehensive tests for get_oprange function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    self.resources = ["resource1"]
    self.fields = ["field1"]

  @pytest.mark.asyncio
  async def test_get_oprange_comprehensive_functionality(self):
    """Test comprehensive functionality of get_oprange."""
    # Create a DataFrame with enough data for ATR calculation
    mock_df = pl.DataFrame({
        "timestamp":
        [self.from_date + pl.duration(minutes=i * 5) for i in range(20)],
        "field1": [1.0 + i * 0.1 for i in range(20)]  # Increasing values
    })

    mock_future = Mock()
    mock_future.result.return_value = {
        "field1:min": 1.0,
        "field1:max": 2.9,
        "field1:range": 1.9,
        "field1:range_position": 0.9,
        "field1:atr_14": 0.1,
        "field1:current": 2.9
    }

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date, 6, None,
                                      "json:row")

      assert err == ""
      assert result == {"formatted": "data"}

  @pytest.mark.asyncio
  async def test_get_oprange_empty_series_handling(self):
    """Test get_oprange with empty series handling."""
    mock_df = pl.DataFrame({"timestamp": [], "field1": []})

    mock_future = Mock()
    mock_future.result.return_value = {}  # Empty metrics for empty series

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date, 6, None,
                                      "json:row")

      assert err == ""
      assert result == {"formatted": "data"}

  @pytest.mark.asyncio
  async def test_get_oprange_non_numeric_values(self):
    """Test get_oprange with non-numeric values."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": ["text", "more_text"]
    })

    mock_future = Mock()
    mock_future.result.return_value = {}  # Empty metrics for non-numeric data

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date, 6, None,
                                      "json:row")

      assert err == ""
      assert result == {"formatted": "data"}

  @pytest.mark.asyncio
  async def test_get_oprange_atr_calculation_exception(self):
    """Test get_oprange when ATR calculation fails."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [1.0, 2.0]
    })

    def mock_compute_with_atr_exception(df, col):
      # Simulate ATR calculation failure
      return {
          f"{col}:min": 1.0,
          f"{col}:max": 2.0,
          f"{col}:range": 1.0,
          f"{col}:range_position": 1.0,
          f"{col}:atr_14": None,  # ATR failed
          f"{col}:current": 2.0
      }

    mock_future = Mock()
    mock_future.result.return_value = mock_compute_with_atr_exception(
        mock_df, "field1")

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date, 6, None,
                                      "json:row")

      assert err == ""
      assert result == {"formatted": "data"}

  @pytest.mark.asyncio
  async def test_get_oprange_zero_range_calculation(self):
    """Test get_oprange with zero range (same min/max values)."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "field1": [2.0, 2.0]  # Same values = zero range
    })

    mock_future = Mock()
    mock_future.result.return_value = {
        "field1:min": 2.0,
        "field1:max": 2.0,
        "field1:range": 0.0,
        "field1:range_position": None,  # Should be None for zero range
        "field1:atr_14": None,
        "field1:current": 2.0
    }

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(self.resources, self.fields,
                                      self.from_date, self.to_date, 6, None,
                                      "json:row")

      assert err == ""
      assert result == {"formatted": "data"}

  @pytest.mark.asyncio
  async def test_get_oprange_multiple_resources(self):
    """Test get_oprange with multiple resources."""
    mock_df = pl.DataFrame({
        "timestamp": [self.from_date, self.to_date],
        "resource1.field1": [1.0, 2.0],
        "resource2.field1": [3.0, 4.0]
    })

    mock_future = Mock()
    mock_future.result.return_value = {
        "resource1.field1:min": 1.0,
        "resource1.field1:max": 2.0,
        "resource1.field1:current": 2.0
    }

    with patch('src.services.ts_analysis.ensure_df', new_callable=AsyncMock, return_value=("", mock_df)), \
         patch('src.services.ts_analysis.numeric_columns', return_value=["field1"]), \
         patch('src.services.ts_analysis.state') as mock_state, \
         patch('src.services.ts_analysis.loader.format_table', return_value=("", {"formatted": "data"})):

      mock_state.thread_pool.submit.return_value = mock_future

      err, result = await get_oprange(["resource1", "resource2"], self.fields,
                                      self.from_date, self.to_date, 6, None,
                                      "json:row")

      assert err == ""
      assert result == {"formatted": "data"}
