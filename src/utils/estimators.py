import polars as pl
import numpy as np
from .maths import normalize

# Volatility Estimators

def std(s: pl.Series, period: int = 20) -> pl.Series:
  """Standard deviation"""
  return s.rolling_std(window_size=period)

def wstd(s: pl.Series, period: int = 20) -> pl.Series:
  """Weighted standard deviation"""
  try:
    weights = np.linspace(1, period, period)
    weights = weights / weights.sum()
    return s.rolling_std(window_size=period, weights=weights)
  except:
    return s.rolling_std(window_size=period)

def ewstd(s: pl.Series, period: int = 20) -> pl.Series:
  """Exponentially weighted standard deviation"""
  alpha = 2 / (period + 1)
  return s.ewm_std(alpha=alpha, adjust=False)

def close_atr(close: pl.Series, period: int = 14) -> pl.Series:
  """ATR using close prices only"""
  tr = close.diff().abs()
  return tr.rolling_mean(window_size=period)

def garman_klass(high: pl.Series, low: pl.Series, open: pl.Series, close: pl.Series, period: int = 20) -> pl.Series:
  """Garman-Klass volatility estimator"""
  log_hl = (high/low).log()
  log_co = (close/open).log()
  vol = 0.5 * log_hl**2 - (2*np.log(2)-1) * log_co**2
  return (vol.rolling_mean(window_size=period) * 252).sqrt()

def parkinson(high: pl.Series, low: pl.Series, period: int = 20) -> pl.Series:
  """Parkinson volatility estimator"""
  return (1 / (4 * np.log(2)) * ((high/low).log()**2)
      .rolling_mean(window_size=period) * 252).sqrt()

def rogers_satchell(high: pl.Series, low: pl.Series, open: pl.Series, close: pl.Series, period: int = 20) -> pl.Series:
  """Rogers-Satchell volatility estimator"""
  vol = (high/close).log() * (high/open).log() + (low/close).log() * (low/open).log()
  return (vol.rolling_mean(window_size=period) * 252).sqrt()

def mad(s: pl.Series, period: int = 20) -> pl.Series:
  """Median Absolute Deviation"""
  rolling_median = s.rolling_median(window_size=period)
  return (s - rolling_median).abs().rolling_median(window_size=period)

# Trend Estimators

def sma(s: pl.Series, period: int = 20) -> pl.Series:
  """Simple Moving Average"""
  return s.rolling_mean(window_size=period)

def smma(s: pl.Series, period: int = 20) -> pl.Series:
  """Smoothed Moving Average"""
  return s.ewm_mean(alpha=1/period, adjust=False)

def wma(s: pl.Series, period: int = 20) -> pl.Series:
  """Weighted Moving Average"""
  weights = np.linspace(1, period, period)
  weights = weights / weights.sum()
  return s.rolling_mean(window_size=period, weights=weights)

def ewma(s: pl.Series, period: int = 20) -> pl.Series:
  """Exponential Weighted Moving Average"""
  alpha = 2 / (period + 1)
  return s.ewm_mean(alpha=alpha, adjust=False)

def linreg(s: pl.Series, period: int = 20) -> pl.Series:
  """Linear regression slope"""
  x = np.arange(period)
  x_mean = x.mean()
  x_diff = x - x_mean
  x_diff_squared_sum = np.sum(x_diff ** 2)

  def rolling_slope(window):
    if len(window) < period:
      return None
    window_array = np.array(window)  # Convert to numpy array
    y_mean = window_array.mean()     # Use numpy mean
    y_diff = window_array - y_mean
    slope = np.sum(x_diff * y_diff) / x_diff_squared_sum
    return slope

  return s.rolling_map(window_size=period, function=rolling_slope)

def polyreg(s: pl.Series, period: int = 20, degree: int = 2) -> pl.Series:
  """Polynomial regression (2nd degree) trend"""
  x = np.arange(period)

  def rolling_poly(window):
    if len(window) < period:
      return None
    coeffs = np.polyfit(x, window, degree)
    return np.polyval(coeffs, x[-1])

  return s.rolling_map(window_size=period, function=rolling_poly)

def theil_sen(s: pl.Series, period: int = 20) -> pl.Series:
  """Theil-Sen estimator (median slope)"""
  def rolling_theil_sen(window):
    if len(window) < period:
      return None
    y = np.array(window)
    x = np.arange(len(y))
    
    # Create meshgrid of all pairs of points
    i, j = np.triu_indices(len(x), k=1)

    # Calculate slopes vectorized
    slopes = (y[j] - y[i]) / (x[j] - x[i])
    return np.median(slopes)

  return s.rolling_map(window_size=period, function=rolling_theil_sen)

# Momentum Estimators

def roc(s: pl.Series, period: int = 1) -> pl.Series:
  """Rate of Change"""
  return ((s - s.shift(period)) / s.shift(period)).fill_null(0)

def simple_mom(s: pl.Series, period: int = 1) -> pl.Series:
  """Simple Momentum"""
  return (s - s.shift(period)).fill_null(0)

def macd(s: pl.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[pl.Series, pl.Series, pl.Series]:
  """MACD (Moving Average Convergence Divergence)"""
  fast_ema = ewma(s, fast)
  slow_ema = ewma(s, slow)
  macd_line = fast_ema - slow_ema
  signal_line = ewma(macd_line, signal)
  histogram = macd_line - signal_line
  return macd_line, signal_line, histogram

def rsi(s: pl.Series, period: int = 14) -> pl.Series:
  """Relative Strength Index"""
  delta = s.diff()
  gain = (delta.clip(lower_bound=0)).ewm_mean(alpha=1/period)
  loss = (-delta.clip(upper_bound=0)).ewm_mean(alpha=1/period)
  rs = gain / loss
  return 100 - (100 / (1 + rs))

def cci(high: pl.Series, low: pl.Series, close: pl.Series, period: int = 20) -> pl.Series:
  """Commodity Channel Index"""
  tp = (high + low + close) / 3
  tp_sma = sma(tp, period)
  mad_val = mad(tp, period)
  return (tp - tp_sma) / (0.015 * mad_val)

def close_cci(close: pl.Series, period: int = 20) -> pl.Series:
  """CCI using close prices only"""
  sma_close = close.rolling_mean(window_size=period)
  mad_close = (close - sma_close).abs().rolling_mean(window_size=period)
  return (close - sma_close) / (0.015 * mad_close)

def stochastic(close: pl.Series, high: pl.Series, low: pl.Series, period: int = 14) -> tuple[pl.Series, pl.Series]:
  """Stochastic Oscillator"""
  lowest_low = low.rolling_min(window_size=period)
  highest_high = high.rolling_max(window_size=period)
  k = 100 * (close - lowest_low) / (highest_high - lowest_low)
  d = sma(k, 3)
  return k, d

def close_stochastic(close: pl.Series, period: int = 14) -> pl.Series:
  """Stochastic Oscillator using close prices only"""
  lowest_close = close.rolling_min(window_size=period)
  highest_close = close.rolling_max(window_size=period)
  k = 100 * (close - lowest_close) / (highest_close - lowest_close)
  return k

def zscore(s: pl.Series, period: int = 20) -> pl.Series:
  """Z-score of returns"""
  returns = roc(s)
  return (returns - returns.rolling_mean(window_size=period)) / returns.rolling_std(window_size=period)

def cumulative_returns(s: pl.Series) -> pl.Series:
  """Cumulative returns"""
  return (1 + roc(s)).cumprod()

def vol_adjusted_momentum(s: pl.Series, period: int = 20) -> pl.Series:
  """Volatility-adjusted momentum"""
  mom = simple_mom(s, period)
  vol = ewstd(s, period)
  return mom / vol

# FIXME
def adx(high: pl.Series, low: pl.Series, close: pl.Series, period: int = 14) -> tuple[pl.Series, pl.Series, pl.Series]:
  """Average Directional Index"""
  # True Range
  tr = pl.max_horizontal(
    high - low,
    (high - close.shift()).abs(),
    (low - close.shift()).abs()
  )

  # Directional Movement
  up_move = high - high.shift()
  down_move = low.shift() - low

  pos_dm = pl.when(up_move > down_move).then(up_move.clip(lower_bound=0)).otherwise(0)
  neg_dm = pl.when(down_move > up_move).then(down_move.clip(lower_bound=0)).otherwise(0)

  # Smoothed values
  tr14 = tr.ewm_mean(alpha=1/period)
  plus_di14 = 100 * (pos_dm.ewm_mean(alpha=1/period) / tr14)
  minus_di14 = 100 * (neg_dm.ewm_mean(alpha=1/period) / tr14)

  # ADX
  dx = 100 * ((plus_di14 - minus_di14).abs() / (plus_di14 + minus_di14))
  adx = dx.ewm_mean(alpha=1/period)

  return plus_di14, minus_di14, adx

# FIXME
def close_dmi(s: pl.Series, period: int = 14) -> tuple[pl.Series, pl.Series, pl.Series]:
  """
  Direction Movement Index using close prices only
  Returns: (plus_di, minus_di, trend_strength)
  """
  # Calculate price changes
  price_change = s.diff()

  # Reuse existing logic for directional movement
  pos_dm = price_change.clip(lower_bound=0)
  neg_dm = (-price_change).clip(lower_bound=0)

  # Calculate directional indicators using existing functions
  plus_di, minus_di, _ = adx(pos_dm, neg_dm, s, period)

  # Calculate trend strength (similar to ADX)
  di_diff = (plus_di - minus_di).abs()
  di_sum = plus_di + minus_di
  dx = (di_diff / di_sum * 100).fill_null(0)
  trend_strength = dx.ewm_mean(alpha=1/period)

  return plus_di, minus_di, trend_strength
