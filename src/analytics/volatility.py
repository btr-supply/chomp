import polars as pl
import numpy as np
from typing import Union, List
from ..utils.maths import ensure_series, SeriesInput, to_list, ewm_alpha

__all__ = [
    "std",
    "wstd",
    "ewstd",
    "close_atr",
    "garman_klass",
    "parkinson",
    "rogers_satchell",
    "mad",
]


def std(data: SeriesInput, period: int = 20) -> Union[List[float], pl.Series]:
  """
  Rolling standard deviation.

  Args:
    data: Input time series data
    period: Rolling window size (default: 20)

  Returns:
    Standard deviation values (list if input is list, Series if input is Series)
  """
  if isinstance(data, list):
    series = ensure_series(data, "data", min_length=period)
    if series is None:
      return []
    result = series.rolling_std(window_size=period)
    return to_list(result)
  else:
    series = ensure_series(data, "data")
    if series is None:
      return pl.Series([])  # Return empty Series instead of original data
    return series.rolling_std(window_size=period)


def wstd(s: SeriesInput, period: int = 20) -> pl.Series:
  """
  Weighted rolling standard deviation with linear weights.

  Args:
    s: Input time series data
    period: Rolling window size (default: 20)

  Returns:
    Weighted standard deviation values
  """
  series = ensure_series(s, "data")
  if series is None:
    return pl.Series([])

  try:
    weights = np.linspace(1, period, period)
    weights = weights / weights.sum()
    return series.rolling_std(window_size=period, weights=weights)
  except Exception:
    # Fallback to regular rolling std if weighted fails
    return series.rolling_std(window_size=period)


def ewstd(s: SeriesInput, period: int = 20) -> pl.Series:
  """
  Exponentially weighted standard deviation.

  Args:
    s: Input time series data
    period: EWM decay period (default: 20)

  Returns:
    Exponentially weighted standard deviation values
  """
  series = ensure_series(s, "data")
  if series is None:
    return pl.Series([])

  alpha = ewm_alpha(period)
  return series.ewm_std(alpha=alpha, adjust=False)


def close_atr(close: SeriesInput, period: int = 14) -> pl.Series:
  """
  Average True Range using close prices only.

  Args:
    close: Close price series
    period: Smoothing window (default: 14)

  Returns:
    ATR values based on close price differences
  """
  series = ensure_series(close, "close")
  if series is None:
    return pl.Series([])

  tr = series.diff().abs()
  return tr.rolling_mean(window_size=period)


def garman_klass(high: SeriesInput,
                 low: SeriesInput,
                 open: SeriesInput,
                 close: SeriesInput,
                 period: int = 20) -> pl.Series:
  """
  Garman-Klass volatility estimator using OHLC data.

  Formula: σ = sqrt(252 * mean(0.5*(ln(H/L))² - (2ln2-1)*(ln(C/O))²))

  Args:
    high: High price series
    low: Low price series
    open: Open price series
    close: Close price series
    period: Rolling window size (default: 20)

  Returns:
    Annualized volatility estimates
  """
  _h = ensure_series(high, "high")
  _l = ensure_series(low, "low")
  _o = ensure_series(open, "open")
  _c = ensure_series(close, "close")

  if any(x is None for x in [_h, _l, _o, _c]):
    return pl.Series([])

  # Vectorized calculation
  log_hl = (_h / _l).log() # type: ignore
  log_co = (_c / _o).log() # type: ignore
  vol = 0.5 * log_hl**2 - (2 * np.log(2) - 1) * log_co**2
  return (vol.rolling_mean(window_size=period) * 252).sqrt()


def parkinson(high: SeriesInput, low: SeriesInput, period: int = 20) -> pl.Series:
  """
  Parkinson volatility estimator using high-low data.

  Formula: σ = sqrt(252 * mean((ln(H/L))²) / (4*ln(2)))

  Args:
    high: High price series
    low: Low price series
    period: Rolling window size (default: 20)

  Returns:
    Annualized volatility estimates
  """
  h = ensure_series(high, "high")
  low_val = ensure_series(low, "low")

  if any(x is None for x in [h, low_val]):
    return pl.Series([])

  log_ratio = (h / low_val).log()  # type: ignore
  return (1 / (4 * np.log(2)) *
          (log_ratio**2).rolling_mean(window_size=period) *
          252).sqrt()


def rogers_satchell(high: SeriesInput,
                    low: SeriesInput,
                    open: SeriesInput,
                    close: SeriesInput,
                    period: int = 20) -> pl.Series:
  """
  Rogers-Satchell volatility estimator using OHLC data.

  Formula: σ = sqrt(252 * mean(ln(H/C)*ln(H/O) + ln(L/C)*ln(L/O)))

  Args:
    high: High price series
    low: Low price series
    open: Open price series
    close: Close price series
    period: Rolling window size (default: 20)

  Returns:
    Annualized volatility estimates
  """
  h_ = ensure_series(high, "high")
  l_ = ensure_series(low, "low")
  o_ = ensure_series(open, "open")
  c_ = ensure_series(close, "close")

  if any(x is None for x in [h_, l_, o_, c_]):
    return pl.Series([])

  # Vectorized calculation
  vol = (h_ / c_).log() * (h_ / o_).log() + (l_ / c_).log() * (l_ / o_).log()  # type: ignore
  return (vol.rolling_mean(window_size=period) * 252).sqrt()


def mad(s: SeriesInput, period: int = 20) -> pl.Series:
  """
  Median Absolute Deviation.

  Formula: MAD = median(|x_i - median(x)|)

  Args:
    s: Input time series data
    period: Rolling window size (default: 20)

  Returns:
    Median absolute deviation values
  """
  series = ensure_series(s, "data")
  if series is None:
    return pl.Series([])

  rolling_median = series.rolling_median(window_size=period)
  return (series - rolling_median).abs().rolling_median(window_size=period)
