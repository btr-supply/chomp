import polars as pl
import numpy as np
from typing import Union, List
from ..utils.maths import (
    ensure_series,
    ewm_alpha,
    to_list,
    SeriesInput
)

__all__ = [
    "sma",
    "smma",
    "wma",
    "ewma",
    "linreg",
    "polyreg",
    "theil_sen",
    "bollinger_bands",
    "hull_ma",
    "donchian_channel",
    "ichimoku_cloud"
]


def sma(data: SeriesInput, period: int = 20) -> Union[List[float], pl.Series]:
  """
  Simple Moving Average.

  Args:
    data: Input time series data
    period: Moving average period (default: 20)

  Returns:
    SMA values (list if input is list, Series if input is Series)
  """
  if isinstance(data, list):
    series = ensure_series(data, "data", min_length=period)
    if series is None:
      return []
    result = series.rolling_mean(window_size=period)
    return to_list(result)
  else:
    series = ensure_series(data, "data")
    if series is None:
      return pl.Series([])
    return series.rolling_mean(window_size=period)


def smma(s: pl.Series, period: int = 20) -> pl.Series:
  """
  Smoothed Moving Average (also known as Modified Moving Average).

  Args:
    s: Input series
    period: Smoothing period (default: 20)

  Returns:
    SMMA values using exponential smoothing with alpha = 1/period
  """
  from ..utils.maths import rolling_alpha
  return s.ewm_mean(alpha=rolling_alpha(period), adjust=False)


def wma(data: SeriesInput, period: int = 20) -> Union[List[float], pl.Series]:
  """
  Weighted Moving Average with linear weights.

  Args:
    data: Input time series data
    period: Moving average period (default: 20)

  Returns:
    WMA values (list if input is list, Series if input is Series)
  """
  if isinstance(data, list):
    series = ensure_series(data, "data", min_length=period)
    if series is None:
      return []

    weights = np.linspace(1, period, period)
    weights = weights / weights.sum()
    result = series.rolling_mean(window_size=period, weights=weights)
    return to_list(result)
  else:
    series = ensure_series(data, "data")
    if series is None:
      return pl.Series([])

    weights = np.linspace(1, period, period)
    weights = weights / weights.sum()
    return series.rolling_mean(window_size=period, weights=weights)


def ewma(data: SeriesInput, period: int = 20) -> Union[List[float], pl.Series]:
  """
  Exponential Weighted Moving Average.

  Args:
    data: Input time series data
    period: EWM period (default: 20)

  Returns:
    EWMA values (list if input is list, Series if input is Series)
  """
  if isinstance(data, list):
    series = ensure_series(data, "data", min_length=1)
    if series is None:
      return []
    alpha = ewm_alpha(period)
    result = series.ewm_mean(alpha=alpha, adjust=False)
    return to_list(result)
  else:
    series = ensure_series(data, "data")
    if series is None:
      return pl.Series([])
    alpha = ewm_alpha(period)
    return series.ewm_mean(alpha=alpha, adjust=False)


def linreg(s: pl.Series, period: int = 20) -> pl.Series:
  """
  Linear regression slope (least squares trend).

  Args:
    s: Input series
    period: Rolling window size (default: 20)

  Returns:
    Slope values indicating trend direction and strength
  """
  # Pre-calculate constants outside rolling function for efficiency
  x = np.arange(period, dtype=np.float64)
  x_centered = x - x.mean()
  x_sq_sum = np.sum(x_centered ** 2)

  def calc_slope(window: pl.Series) -> float:
    if len(window) < period:
      return float('nan')
    window_vals = window.to_numpy()
    y_centered = window_vals - window_vals.mean()
    return float(np.dot(x_centered, y_centered) / x_sq_sum)

  return s.rolling_map(window_size=period, function=calc_slope)


def polyreg(s: pl.Series, period: int = 20, degree: int = 2) -> pl.Series:
  """
  Polynomial regression trend forecast.

  Args:
    s: Input series
    period: Rolling window size (default: 20)
    degree: Polynomial degree (default: 2)

  Returns:
    Forecasted values based on polynomial trend
  """
  x = np.arange(period, dtype=np.float64)
  last_x = period - 1

  def calc_poly_value(window: pl.Series) -> float:
    if len(window) < period:
      return float('nan')
    try:
      window_vals = window.to_numpy()
      coeffs = np.polyfit(x, window_vals, degree)
      return float(np.polyval(coeffs, last_x))
    except np.linalg.LinAlgError:
      # Return last value if polynomial fit fails (singular matrix)
      return float(window.to_numpy()[-1])

  return s.rolling_map(window_size=period, function=calc_poly_value)


def theil_sen(s: pl.Series, period: int = 20) -> pl.Series:
  """
  Theil-Sen estimator (robust median slope).

  Args:
    s: Input series
    period: Rolling window size (default: 20)

  Returns:
    Median slope values (more robust to outliers than linear regression)
  """

  def calc_median_slope(window: pl.Series) -> float:
    if len(window) < 2:
      return float('nan')

    window_vals = window.to_numpy()
    n = len(window_vals)
    # Vectorized slope calculation for all pairs
    i, j = np.triu_indices(n, k=1)

    # Direct vectorized operations - no division by zero since j > i
    x_diff = j - i
    y_diff = window_vals[j] - window_vals[i]
    slopes = y_diff / x_diff

    return float(np.median(slopes))

  return s.rolling_map(window_size=period, function=calc_median_slope)


def bollinger_bands(data: SeriesInput,
                   period: int = 20,
                   std_dev: float = 2.0) -> tuple[pl.Series, pl.Series, pl.Series]:
  """
  Bollinger Bands.

  Args:
    data: Input time series data
    period: Moving average period (default: 20)
    std_dev: Standard deviation multiplier (default: 2.0)

  Returns:
    Tuple of (upper band, middle band, lower band)
  """
  series = ensure_series(data, "data")
  if series is None:
    return pl.Series([]), pl.Series([]), pl.Series([])

  middle = series.rolling_mean(window_size=period)
  std = series.rolling_std(window_size=period)

  upper = middle + (std_dev * std)
  lower = middle - (std_dev * std)

  return upper, middle, lower


def hull_ma(data: SeriesInput, period: int = 14) -> Union[List[float], pl.Series]:
  """
  Hull Moving Average for reduced lag.

  Args:
    data: Input time series data
    period: HMA period (default: 14)

  Returns:
    HMA values (list if input is list, Series if input is Series)
  """
  if isinstance(data, list):
    series = ensure_series(data, "data", min_length=period)
    if series is None:
      return []

    half_period = period // 2
    sqrt_period = int(np.sqrt(period))

    wma_half = series.rolling_mean(window_size=half_period)
    wma_full = series.rolling_mean(window_size=period)
    raw_hma = 2 * wma_half - wma_full
    result = raw_hma.rolling_mean(window_size=sqrt_period)
    return to_list(result)
  else:
    series = ensure_series(data, "data")
    if series is None:
      return pl.Series([])

    half_period = period // 2
    sqrt_period = int(np.sqrt(period))

    wma_half = series.rolling_mean(window_size=half_period)
    wma_full = series.rolling_mean(window_size=period)
    raw_hma = 2 * wma_half - wma_full
    return raw_hma.rolling_mean(window_size=sqrt_period)


def donchian_channel(high: SeriesInput,
                    low: SeriesInput,
                    period: int = 20) -> tuple[pl.Series, pl.Series, pl.Series]:
  """
  Donchian Channel.

  Args:
    high: High price series
    low: Low price series
    period: Lookback period (default: 20)

  Returns:
    Tuple of (upper channel, middle channel, lower channel)
  """
  h_ = ensure_series(high, "high")
  l_ = ensure_series(low, "low")

  if any(x is None for x in [h_, l_]):
    return pl.Series([]), pl.Series([]), pl.Series([])

  upper = h_.rolling_max(window_size=period)  # type: ignore
  lower = l_.rolling_min(window_size=period)  # type: ignore
  middle = (upper + lower) / 2

  return upper, middle, lower


def ichimoku_cloud(high: SeriesInput,
                  low: SeriesInput,
                  close: SeriesInput,
                  conversion_period: int = 9,
                  base_period: int = 26,
                  leading_span_b_period: int = 52,
                  displacement: int = 26) -> tuple[pl.Series, pl.Series, pl.Series, pl.Series, pl.Series]:
  """
  Ichimoku Cloud components.

  Args:
    high: High price series
    low: Low price series
    close: Close price series
    conversion_period: Tenkan-sen period (default: 9)
    base_period: Kijun-sen period (default: 26)
    leading_span_b_period: Senkou Span B period (default: 52)
    displacement: Forward displacement (default: 26)

  Returns:
    Tuple of (Tenkan-sen, Kijun-sen, Senkou Span A, Senkou Span B, Chikou Span)
  """
  h_ = ensure_series(high, "high")
  l_ = ensure_series(low, "low")
  c_ = ensure_series(close, "close")

  if any(x is None for x in [h_, l_, c_]):
    empty = pl.Series([])
    return empty, empty, empty, empty, empty

  # Tenkan-sen (Conversion Line)
  tenkan_sen = (h_.rolling_max(window_size=conversion_period) +  # type: ignore
                l_.rolling_min(window_size=conversion_period)) / 2  # type: ignore

  # Kijun-sen (Base Line)
  kijun_sen = (h_.rolling_max(window_size=base_period) +  # type: ignore
               l_.rolling_min(window_size=base_period)) / 2  # type: ignore

  # Senkou Span A (Leading Span A)
  senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(-displacement)

  # Senkou Span B (Leading Span B)
  senkou_span_b = ((h_.rolling_max(window_size=leading_span_b_period) +  # type: ignore
                    l_.rolling_min(window_size=leading_span_b_period)) / 2).shift(-displacement)  # type: ignore

  # Chikou Span (Lagging Span)
  chikou_span = c_.shift(displacement)  # type: ignore

  return tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b, chikou_span
