import polars as pl
from typing import Union, List, Tuple
from ..utils.maths import to_list, rolling_alpha, ensure_series, SeriesInput, ewm_alpha

__all__ = [
    "roc",
    "simple_mom",
    "macd",
    "close_rsi",
    "cci",
    "close_cci",
    "stochastic",
    "close_stochastic",
    "zscore",
    "cumulative_returns",
    "vol_adjusted_momentum",
    "adx",
    "close_dmi",
]


def roc(data: SeriesInput, period: int = 12) -> Union[List[float], pl.Series]:
  """
  Rate of Change.

  Args:
    data: Input time series data
    period: ROC period (default: 12)

  Returns:
    ROC values (list if input is list, Series if input is Series)
  """
  if isinstance(data, list):
    series = ensure_series(data, "data", min_length=period + 1)
    if series is None:
      return []

    shifted = series.shift(period)
    roc_values = ((series - shifted) / shifted) * 100
    return to_list(roc_values)
  else:
    series = ensure_series(data, "data")
    if series is None:
      return pl.Series([])

    shifted = series.shift(period)
    return ((series - shifted) / shifted) * 100


def simple_mom(s: pl.Series, period: int = 1) -> pl.Series:
  """
  Simple Momentum (price difference).

  Args:
    s: Price series
    period: Lookback period (default: 1)

  Returns:
    Price difference from period ago
  """
  return (s - s.shift(period)).fill_null(0)


def macd(data: SeriesInput,
         fast_period: int = 12,
         slow_period: int = 26,
         signal_period: int = 9) -> tuple[pl.Series, pl.Series, pl.Series]:
  """
  MACD (Moving Average Convergence Divergence).

  Args:
    data: Input time series data
    fast_period: Fast EMA period (default: 12)
    slow_period: Slow EMA period (default: 26)
    signal_period: Signal line EMA period (default: 9)

  Returns:
    Tuple of (MACD line, Signal line, Histogram)
  """
  series = ensure_series(data, "data")
  if series is None:
    return pl.Series([]), pl.Series([]), pl.Series([])

  fast_alpha = ewm_alpha(fast_period)
  slow_alpha = ewm_alpha(slow_period)
  signal_alpha = ewm_alpha(signal_period)

  fast_ema = series.ewm_mean(alpha=fast_alpha, adjust=False)
  slow_ema = series.ewm_mean(alpha=slow_alpha, adjust=False)

  macd_line = fast_ema - slow_ema
  signal_line = macd_line.ewm_mean(alpha=signal_alpha, adjust=False)
  histogram = macd_line - signal_line

  return macd_line, signal_line, histogram


def close_rsi(close: Union[List, pl.Series],
              period: int = 14) -> Union[List[float], pl.Series]:
  """
  Relative Strength Index (RSI) using close prices.

  Args:
    close: Close price series (list or Series)
    period: Lookback period (default: 14)

  Returns:
    RSI values 0-100 (list if input is list, Series if input is Series)
  """
  # Handle list input with validation
  if isinstance(close, list):
    s = ensure_series(close, "close", min_length=2)
    if s is None:
      return []
    is_list_input = True
  else:
    s = ensure_series(close, "close", min_length=2)
    if s is None:
      return close
    is_list_input = False

  delta = s.diff()
  alpha = rolling_alpha(period)
  gain = delta.clip(lower_bound=0).ewm_mean(alpha=alpha)
  loss = (-delta).clip(lower_bound=0).ewm_mean(alpha=alpha)
  rsi_values = 100 - (100 / (1 + gain / loss))

  return to_list(rsi_values) if is_list_input else rsi_values


def cci(high: pl.Series,
        low: pl.Series,
        close: pl.Series,
        period: int = 20) -> pl.Series:
  """
  Commodity Channel Index (CCI).

  Args:
    high: High price series
    low: Low price series
    close: Close price series
    period: Lookback period (default: 20)

  Returns:
    CCI values
  """
  from .trend import sma
  from .volatility import mad

  tp = (high + low + close) / 3
  return (tp - sma(tp, period)) / (0.015 * mad(tp, period))


def close_cci(close: pl.Series, period: int = 20) -> pl.Series:
  """
  Commodity Channel Index using close prices only.

  Args:
    close: Close price series
    period: Lookback period (default: 20)

  Returns:
    CCI values based on close prices
  """
  sma_close = close.rolling_mean(window_size=period)
  mad_close = (close - sma_close).abs().rolling_mean(window_size=period)
  return (close - sma_close) / (0.015 * mad_close)


def stochastic(high: SeriesInput,
               low: SeriesInput,
               close: SeriesInput,
               k_period: int = 14,
               d_period: int = 3) -> tuple[pl.Series, pl.Series]:
  """
  Stochastic Oscillator (%K and %D).

  Args:
    high: High price series
    low: Low price series
    close: Close price series
    k_period: %K period (default: 14)
    d_period: %D smoothing period (default: 3)

  Returns:
    Tuple of (%K, %D) series
  """
  h_ = ensure_series(high, "high")
  l_ = ensure_series(low, "low")
  c_ = ensure_series(close, "close")

  if any(x is None for x in [h_, l_, c_]):
    return pl.Series([]), pl.Series([])

  highest_high = h_.rolling_max(window_size=k_period)  # type: ignore
  lowest_low = l_.rolling_min(window_size=k_period)  # type: ignore

  k_percent = 100 * (c_ - lowest_low) / (highest_high - lowest_low)
  d_percent = k_percent.rolling_mean(window_size=d_period)

  return k_percent, d_percent


def close_stochastic(close: pl.Series, period: int = 14) -> pl.Series:
  """
  Stochastic Oscillator using close prices only.

  Args:
    close: Close price series
    period: Lookback period (default: 14)

  Returns:
    %K values based on close price range
  """
  lowest_close = close.rolling_min(window_size=period)
  highest_close = close.rolling_max(window_size=period)
  return 100 * (close - lowest_close) / (highest_close - lowest_close)


def zscore(s: pl.Series, period: int = 20) -> pl.Series:
  """
  Z-score of returns.

  Args:
    s: Price series
    period: Lookback period (default: 20)

  Returns:
    Standardized returns (number of standard deviations from mean)
  """
  returns = roc(s)
  if isinstance(returns, list) or returns is None:
    return pl.Series([])

  rolling_mean = returns.rolling_mean(window_size=period)
  rolling_std = returns.rolling_std(window_size=period)
  return (returns - rolling_mean) / rolling_std


def cumulative_returns(s: pl.Series) -> pl.Series:
  """
  Cumulative returns.

  Args:
    s: Price series

  Returns:
    Cumulative product of (1 + returns)
  """
  returns = roc(s)
  if isinstance(returns, list) or returns is None:
    return pl.Series([])

  return (1 + returns).cum_prod()


def vol_adjusted_momentum(s: pl.Series, period: int = 20) -> pl.Series:
  """
  Volatility-adjusted momentum.

  Args:
    s: Price series
    period: Lookback period (default: 20)

  Returns:
    Momentum scaled by volatility
  """
  from .volatility import ewstd
  return simple_mom(s, period) / ewstd(s, period)


def adx(high: pl.Series,
        low: pl.Series,
        close: pl.Series,
        period: int = 14) -> Tuple[pl.Series, pl.Series, pl.Series]:
  """
  Average Directional Index (ADX) and Directional Indicators.

  Args:
    high: High price series
    low: Low price series
    close: Close price series
    period: Smoothing period (default: 14)

  Returns:
    Tuple of (+DI, -DI, ADX)
  """
  df = pl.DataFrame({"high": high, "low": low, "close": close})
  alpha = rolling_alpha(period)

  # Calculate all components with optimized chaining
  df = df.with_columns([
    # True Range
    pl.max_horizontal([
      pl.col("high") - pl.col("low"),
      (pl.col("high") - pl.col("close").shift()).abs(),
      (pl.col("low") - pl.col("close").shift()).abs(),
    ]).alias("tr"),
    # Directional Movement
    (pl.col("high") - pl.col("high").shift()).alias("up_move"),
    (pl.col("low").shift() - pl.col("low")).alias("down_move"),
  ]).with_columns([
    # Positive and Negative DM with simplified logic
    pl.when(pl.col("up_move") > pl.col("down_move"))
      .then(pl.max_horizontal([pl.col("up_move"), pl.lit(0)]))
      .otherwise(0).alias("pos_dm"),
    pl.when(pl.col("down_move") > pl.col("up_move"))
      .then(pl.max_horizontal([pl.col("down_move"), pl.lit(0)]))
      .otherwise(0).alias("neg_dm"),
  ]).with_columns([
    # Smoothed values
    pl.col("tr").ewm_mean(alpha=alpha).alias("tr14"),
    pl.col("pos_dm").ewm_mean(alpha=alpha).alias("pos_dm_smooth"),
    pl.col("neg_dm").ewm_mean(alpha=alpha).alias("neg_dm_smooth"),
  ]).with_columns([
    # DI values and final ADX calculation
    (100 * pl.col("pos_dm_smooth") / pl.col("tr14")).alias("plus_di14"),
    (100 * pl.col("neg_dm_smooth") / pl.col("tr14")).alias("minus_di14"),
  ]).with_columns([
    (100 * (pl.col("plus_di14") - pl.col("minus_di14")).abs() /
     (pl.col("plus_di14") + pl.col("minus_di14"))).alias("dx")
  ]).with_columns([
    pl.col("dx").ewm_mean(alpha=alpha).alias("adx_value")
  ])

  return df["plus_di14"], df["minus_di14"], df["adx_value"]


def close_dmi(s: pl.Series,
              period: int = 14) -> Tuple[pl.Series, pl.Series, pl.Series]:
  """
  Directional Movement Index using close prices only.

  Args:
    s: Price series
    period: Smoothing period (default: 14)

  Returns:
    Tuple of (+DI, -DI, Trend Strength)
  """
  price_change = s.diff()
  pos_dm = price_change.clip(lower_bound=0)
  neg_dm = (-price_change).clip(lower_bound=0)

  alpha = rolling_alpha(period)
  pos_dm_smooth = pos_dm.ewm_mean(alpha=alpha)
  neg_dm_smooth = neg_dm.ewm_mean(alpha=alpha)

  total_movement = pos_dm_smooth + neg_dm_smooth
  plus_di = 100 * (pos_dm_smooth / total_movement).fill_null(0)
  minus_di = 100 * (neg_dm_smooth / total_movement).fill_null(0)

  # Optimized trend strength calculation
  dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di)).fill_null(0)
  trend_strength = dx.ewm_mean(alpha=alpha)

  return plus_di, minus_di, trend_strength
