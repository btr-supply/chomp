"""Tests for utils.estimators module."""
import sys
from pathlib import Path
import polars as pl

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.estimators import (
  sma, ema, smma, wma, ewma, linreg, polyreg, theil_sen,
  volatility, rolling_volatility, std, wstd, ewstd, close_atr, mad,
  garman_klass, parkinson, rogers_satchell,
  roc, simple_mom, macd, cci, close_cci, stochastic, close_stochastic,
  zscore, cumulative_returns, vol_adjusted_momentum,
  rsi, bollinger_bands, adx, close_dmi,
  correlation, percentile, linear_regression, predict_next,
  normalize, standardize, moving_window,
  _to_series, _to_list
)

class TestMovingAverages:
  """Test moving average functions."""

  def test_sma_basic(self):
    """Test simple moving average."""
    data = [1, 2, 3, 4, 5]
    result = sma(data, 3)
    expected = [2.0, 3.0, 4.0]  # SMA of [1,2,3], [2,3,4], [3,4,5]
    assert result == expected

  def test_sma_single_period(self):
    """Test SMA with period 1."""
    data = [10, 20, 30]
    result = sma(data, 1)
    assert result == [10.0, 20.0, 30.0]

  def test_sma_empty_data(self):
    """Test SMA with empty data."""
    result = sma([], 5)
    assert result == []

  def test_sma_period_larger_than_data(self):
    """Test SMA with period larger than data length."""
    data = [1, 2, 3]
    result = sma(data, 5)
    assert result == []

  def test_sma_with_series(self):
    """Test SMA with Polars Series input."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = sma(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == 5

  def test_ema_basic(self):
    """Test exponential moving average."""
    data = [1, 2, 3, 4, 5]
    result = ema(data, 3)
    assert len(result) > 0
    assert isinstance(result[0], float)

  def test_ema_alpha_calculation(self):
    """Test EMA alpha calculation."""
    data = [10, 20, 30, 40, 50]
    result = ema(data, 2)  # alpha = 2/(2+1) = 0.667
    assert len(result) == len(data)
    assert result[0] == 10.0  # First value should be the same

  def test_ema_empty_data(self):
    """Test EMA with empty data."""
    result = ema([], 5)
    assert result == []

  def test_ema_with_series(self):
    """Test EMA with Polars Series input."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = ema(data, 3)
    assert isinstance(result, pl.Series)

class TestAdvancedMovingAverages:
  """Test advanced moving average functions."""

  def test_smma(self):
    """Test smoothed moving average."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = smma(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_wma(self):
    """Test weighted moving average."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = wma(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_ewma(self):
    """Test exponential weighted moving average."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = ewma(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_linreg(self):
    """Test linear regression slope."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = linreg(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_polyreg(self):
    """Test polynomial regression."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = polyreg(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_theil_sen(self):
    """Test Theil-Sen estimator."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = theil_sen(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

class TestVolatilityEstimators:
  """Test volatility estimation functions."""

  def test_volatility_basic(self):
    """Test basic volatility calculation."""
    returns = [0.01, -0.02, 0.015, -0.01, 0.02, -0.015, 0.01]
    vol = volatility(returns)
    assert vol > 0
    assert isinstance(vol, float)

  def test_volatility_empty_returns(self):
    """Test volatility with empty returns."""
    vol = volatility([])
    assert vol == 0

  def test_volatility_single_return(self):
    """Test volatility with single return."""
    vol = volatility([0.05])
    assert vol == 0  # No variation with single value

  def test_volatility_with_series(self):
    """Test volatility with Polars Series."""
    returns = pl.Series([0.01, -0.02, 0.015, -0.01, 0.02])
    vol = volatility(returns)
    assert vol > 0
    assert isinstance(vol, float)

  def test_rolling_volatility(self):
    """Test rolling volatility calculation."""
    returns = [0.01, -0.02, 0.015, -0.01, 0.02, -0.015, 0.01, 0.03, -0.025]
    result = rolling_volatility(returns, 5)
    assert len(result) == len(returns) - 4  # window size adjustment
    for vol in result:
      assert vol >= 0

  def test_rolling_volatility_insufficient_data(self):
    """Test rolling volatility with insufficient data."""
    returns = [0.01, 0.02]
    result = rolling_volatility(returns, 5)
    assert result == []

  def test_std(self):
    """Test standard deviation."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = std(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_wstd(self):
    """Test weighted standard deviation."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = wstd(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_ewstd(self):
    """Test exponentially weighted standard deviation."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = ewstd(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_close_atr(self):
    """Test ATR using close prices only."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = close_atr(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_mad(self):
    """Test Median Absolute Deviation."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = mad(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

class TestAdvancedVolatilityEstimators:
  """Test advanced volatility estimators."""

  def test_garman_klass(self):
    """Test Garman-Klass volatility estimator."""
    high = pl.Series([2.0, 3.0, 4.0, 5.0, 6.0])
    low = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    open_price = pl.Series([1.5, 2.5, 3.5, 4.5, 5.5])
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    result = garman_klass(high, low, open_price, close, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(high)

  def test_parkinson(self):
    """Test Parkinson volatility estimator."""
    high = pl.Series([2.0, 3.0, 4.0, 5.0, 6.0])
    low = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = parkinson(high, low, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(high)

  def test_rogers_satchell(self):
    """Test Rogers-Satchell volatility estimator."""
    high = pl.Series([2.0, 3.0, 4.0, 5.0, 6.0])
    low = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    open_price = pl.Series([1.5, 2.5, 3.5, 4.5, 5.5])
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    result = rogers_satchell(high, low, open_price, close, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(high)

class TestMomentumIndicators:
  """Test momentum indicator functions."""

  def test_roc(self):
    """Test Rate of Change."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = roc(data, 1)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_simple_mom(self):
    """Test Simple Momentum."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = simple_mom(data, 1)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_macd(self):
    """Test MACD calculation."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
    macd_line, signal_line, histogram = macd(data, 3, 6, 3)
    assert isinstance(macd_line, pl.Series)
    assert isinstance(signal_line, pl.Series)
    assert isinstance(histogram, pl.Series)
    assert len(macd_line) == len(data)

  def test_cci(self):
    """Test Commodity Channel Index."""
    high = pl.Series([2.0, 3.0, 4.0, 5.0, 6.0])
    low = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    result = cci(high, low, close, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(high)

  def test_close_cci(self):
    """Test CCI using close prices only."""
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    result = close_cci(close, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(close)

  def test_stochastic(self):
    """Test Stochastic Oscillator."""
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    high = pl.Series([2.0, 3.0, 4.0, 5.0, 6.0])
    low = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    k, d = stochastic(close, high, low, 3)
    assert isinstance(k, pl.Series)
    assert isinstance(d, pl.Series)
    assert len(k) == len(close)

  def test_close_stochastic(self):
    """Test Stochastic using close prices only."""
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    result = close_stochastic(close, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(close)

  def test_zscore(self):
    """Test Z-score calculation."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = zscore(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_cumulative_returns(self):
    """Test cumulative returns."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = cumulative_returns(data)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

  def test_vol_adjusted_momentum(self):
    """Test volatility-adjusted momentum."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = vol_adjusted_momentum(data, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data)

class TestTechnicalIndicators:
  """Test technical indicator functions."""

  def test_rsi_basic(self):
    """Test RSI calculation."""
    # Create price data with clear trend
    prices = [44, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.85, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41, 46.22, 45.64]
    result = rsi(prices, 14)
    assert len(result) >= 0
    # RSI should be between 0 and 100
    for val in result:
      assert 0 <= val <= 100

  def test_rsi_empty_data(self):
    """Test RSI with empty data."""
    result = rsi([], 14)
    assert result == []

  def test_rsi_with_series(self):
    """Test RSI with Polars Series."""
    prices = pl.Series([44.0, 44.34, 44.09, 44.15, 43.61])
    result = rsi(prices, 3)
    assert isinstance(result, pl.Series)
    assert len(result) == len(prices)

  def test_bollinger_bands_basic(self):
    """Test Bollinger Bands calculation."""
    data = [20, 21, 19, 22, 18, 23, 17, 24, 16, 25]
    upper, middle, lower = bollinger_bands(data, 5, 2)

    assert len(upper) == len(middle) == len(lower)
    # Upper should be above middle, middle above lower
    for i in range(len(upper)):
      assert upper[i] >= middle[i] >= lower[i]

  def test_bollinger_bands_insufficient_data(self):
    """Test Bollinger Bands with insufficient data."""
    data = [1, 2, 3]  # Less than period
    upper, middle, lower = bollinger_bands(data, 5, 2)
    assert upper == [] and middle == [] and lower == []

  def test_bollinger_bands_with_series(self):
    """Test Bollinger Bands with Polars Series."""
    data = pl.Series([20.0, 21.0, 19.0, 22.0, 18.0, 23.0])
    upper, middle, lower = bollinger_bands(data, 3, 2)
    assert isinstance(upper, pl.Series)
    assert isinstance(middle, pl.Series)
    assert isinstance(lower, pl.Series)

class TestAdvancedIndicators:
  """Test advanced technical indicators."""

  def test_adx(self):
    """Test Average Directional Index."""
    high = pl.Series([2.0, 3.0, 4.0, 5.0, 6.0])
    low = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    plus_di, minus_di, adx_val = adx(high, low, close, 3)
    assert isinstance(plus_di, pl.Series)
    assert isinstance(minus_di, pl.Series)
    assert isinstance(adx_val, pl.Series)
    assert len(plus_di) == len(high)

  def test_close_dmi(self):
    """Test DMI using close prices only."""
    close = pl.Series([1.8, 2.8, 3.8, 4.8, 5.8])
    plus_di, minus_di, trend = close_dmi(close, 3)
    assert isinstance(plus_di, pl.Series)
    assert isinstance(minus_di, pl.Series)
    assert isinstance(trend, pl.Series)
    assert len(plus_di) == len(close)

class TestStatisticalFunctions:
  """Test statistical helper functions."""

  def test_correlation_basic(self):
    """Test correlation calculation."""
    x = [1, 2, 3, 4, 5]
    y = [2, 4, 6, 8, 10]  # Perfect positive correlation
    corr = correlation(x, y)
    assert abs(corr - 1.0) < 0.001  # Should be close to 1

  def test_correlation_negative(self):
    """Test negative correlation."""
    x = [1, 2, 3, 4, 5]
    y = [5, 4, 3, 2, 1]  # Perfect negative correlation
    corr = correlation(x, y)
    assert abs(corr - (-1.0)) < 0.001  # Should be close to -1

  def test_correlation_no_correlation(self):
    """Test no correlation."""
    x = [1, 2, 3, 4, 5]
    y = [3, 1, 4, 1, 5]  # Random values
    corr = correlation(x, y)
    assert -1 <= corr <= 1  # Should be within valid range

  def test_correlation_with_series(self):
    """Test correlation with Polars Series."""
    x = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    y = pl.Series([2.0, 4.0, 6.0, 8.0, 10.0])
    corr = correlation(x, y)
    assert abs(corr - 1.0) < 0.001

  def test_percentile_calculation(self):
    """Test percentile calculation."""
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    p50 = percentile(data, 50)  # Median
    p90 = percentile(data, 90)
    assert abs(p50 - 5.5) < 1.0  # Allow for different percentile implementations
    assert p90 > p50   # 90th percentile should be higher

  def test_percentile_with_series(self):
    """Test percentile with Polars Series."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    p50 = percentile(data, 50)
    assert isinstance(p50, float)
    assert p50 > 0

  def test_percentile_empty_data(self):
    """Test percentile with empty data."""
    result = percentile([], 50)
    assert result == 0.0

class TestPredictionModels:
  """Test prediction model functions."""

  def test_linear_regression_basic(self):
    """Test basic linear regression."""
    x = [1, 2, 3, 4, 5]
    y = [2, 4, 6, 8, 10]  # y = 2x
    slope, intercept = linear_regression(x, y)
    assert abs(slope - 2.0) < 0.001
    assert abs(intercept - 0.0) < 0.001

  def test_linear_regression_with_intercept(self):
    """Test linear regression with intercept."""
    x = [1, 2, 3, 4, 5]
    y = [3, 5, 7, 9, 11]  # y = 2x + 1
    slope, intercept = linear_regression(x, y)
    assert abs(slope - 2.0) < 0.001
    assert abs(intercept - 1.0) < 0.001

  def test_linear_regression_with_series(self):
    """Test linear regression with Polars Series."""
    x = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    y = pl.Series([2.0, 4.0, 6.0, 8.0, 10.0])
    slope, intercept = linear_regression(x, y)
    assert abs(slope - 2.0) < 0.001
    assert abs(intercept - 0.0) < 0.001

  def test_predict_next_value(self):
    """Test next value prediction."""
    data = [1, 2, 3, 4, 5]
    next_val = predict_next(data)
    assert isinstance(next_val, (int, float))
    assert next_val > 0  # Should predict positive continuation

  def test_predict_next_with_series(self):
    """Test next value prediction with Polars Series."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    next_val = predict_next(data)
    assert isinstance(next_val, (int, float))
    assert next_val > 0

class TestUtilityFunctions:
  """Test utility functions."""

  def test_normalize_data(self):
    """Test data normalization."""
    data = [1, 2, 3, 4, 5]
    normalized = normalize(data)
    assert len(normalized) == len(data)
    assert min(normalized) >= 0
    assert max(normalized) <= 1

  def test_normalize_empty_data(self):
    """Test normalization of empty data."""
    normalized = normalize([])
    assert normalized == []

  def test_normalize_with_series(self):
    """Test normalization with Polars Series."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    normalized = normalize(data)
    assert len(normalized) == len(data)
    assert min(normalized) >= 0
    assert max(normalized) <= 1

  def test_normalize_constant_data(self):
    """Test normalization with constant data."""
    normalized = normalize([5, 5, 5, 5])
    assert normalized == [0.0, 0.0, 0.0, 0.0]

  def test_standardize_data(self):
    """Test data standardization (z-score)."""
    data = [1, 2, 3, 4, 5]
    standardized = standardize(data)
    assert len(standardized) == len(data)
    # Mean should be close to 0
    assert abs(sum(standardized) / len(standardized)) < 0.001

  def test_standardize_empty_data(self):
    """Test standardization with empty data."""
    standardized = standardize([])
    assert standardized == []

  def test_standardize_with_series(self):
    """Test standardization with Polars Series."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    standardized = standardize(data)
    assert len(standardized) == len(data)

  def test_standardize_constant_data(self):
    """Test standardization with constant data."""
    standardized = standardize([5, 5, 5, 5])
    assert standardized == [0.0, 0.0, 0.0, 0.0]

  def test_moving_window_function(self):
    """Test moving window operations."""
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    windows = moving_window(data, 3)
    assert len(windows) == len(data) - 2  # 3-element windows
    assert windows[0] == [1, 2, 3]
    assert windows[-1] == [8, 9, 10]

  def test_moving_window_insufficient_data(self):
    """Test moving window with insufficient data."""
    data = [1, 2]
    windows = moving_window(data, 5)
    assert windows == []

  def test_moving_window_with_series(self):
    """Test moving window with Polars Series."""
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    windows = moving_window(data, 3)
    assert len(windows) == 3
    assert windows[0] == [1.0, 2.0, 3.0]

class TestHelperFunctions:
  """Test internal helper functions."""

  def test_to_series(self):
    """Test _to_series helper function."""
    # Test with list
    data_list = [1, 2, 3, 4, 5]
    result = _to_series(data_list)
    assert isinstance(result, pl.Series)
    assert len(result) == len(data_list)

    # Test with Series (should return as-is)
    data_series = pl.Series([1.0, 2.0, 3.0])
    result = _to_series(data_series)
    assert isinstance(result, pl.Series)
    assert result.equals(data_series)

  def test_to_list(self):
    """Test _to_list helper function."""
    # Test with clean data
    data = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = _to_list(data)
    assert result == [1.0, 2.0, 3.0, 4.0, 5.0]

    # Test with null values
    data_with_nulls = pl.Series([1.0, None, 3.0, None, 5.0])
    result = _to_list(data_with_nulls)
    assert result == [1.0, 3.0, 5.0]  # Nulls should be filtered out
