import polars as pl
import numpy as np
from typing import Union, List, Tuple

__all__ = [
    # Utility functions
    "_to_series",
    "_to_list",
    # Volatility Estimators
    "std",
    "wstd",
    "ewstd",
    "close_atr",
    "garman_klass",
    "parkinson",
    "rogers_satchell",
    "mad",
    "volatility",
    "rolling_volatility",
    # Trend Estimators
    "sma",
    "smma",
    "wma",
    "ewma",
    "linreg",
    "polyreg",
    "theil_sen",
    "ema",
    # Momentum Estimators
    "roc",
    "simple_mom",
    "macd",
    "rsi",
    "cci",
    "close_cci",
    "stochastic",
    "close_stochastic",
    "zscore",
    "cumulative_returns",
    "vol_adjusted_momentum",
    "adx",
    "close_dmi",
    # Statistical Functions
    "correlation",
    "percentile",
    "linear_regression",
    "predict_next",
    "normalize",
    "standardize",
    "moving_window",
    # Technical Indicators
    "bollinger_bands",
]

# Utility functions for handling both list and Series inputs


def _to_series(data: Union[List, pl.Series]) -> pl.Series:
    """Convert list to polars Series if needed."""
    if isinstance(data, list):
        return pl.Series(data, dtype=pl.Float64)
    return data


def _to_list(series: pl.Series) -> List[float]:
    """Convert polars Series to list, filtering out nulls."""
    return [x for x in series.to_list() if x is not None]


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
    except Exception:
        return s.rolling_std(window_size=period)


def ewstd(s: pl.Series, period: int = 20) -> pl.Series:
    """Exponentially weighted standard deviation"""
    alpha = 2 / (period + 1)
    return s.ewm_std(alpha=alpha, adjust=False)


def close_atr(close: pl.Series, period: int = 14) -> pl.Series:
    """ATR using close prices only"""
    tr = close.diff().abs()
    return tr.rolling_mean(window_size=period)


def garman_klass(
    high: pl.Series, low: pl.Series, open: pl.Series, close: pl.Series, period: int = 20
) -> pl.Series:
    """Garman-Klass volatility estimator"""
    log_hl = (high / low).log()
    log_co = (close / open).log()
    vol = 0.5 * log_hl**2 - (2 * np.log(2) - 1) * log_co**2
    return (vol.rolling_mean(window_size=period) * 252).sqrt()


def parkinson(high: pl.Series, low: pl.Series, period: int = 20) -> pl.Series:
    """Parkinson volatility estimator"""
    return (
        1
        / (4 * np.log(2))
        * ((high / low).log() ** 2).rolling_mean(window_size=period)
        * 252
    ).sqrt()


def rogers_satchell(
    high: pl.Series, low: pl.Series, open: pl.Series, close: pl.Series, period: int = 20
) -> pl.Series:
    """Rogers-Satchell volatility estimator"""
    vol = (high / close).log() * (high / open).log() + (low / close).log() * (
        low / open
    ).log()
    return (vol.rolling_mean(window_size=period) * 252).sqrt()


def mad(s: pl.Series, period: int = 20) -> pl.Series:
    """Median Absolute Deviation"""
    rolling_median = s.rolling_median(window_size=period)
    return (s - rolling_median).abs().rolling_median(window_size=period)


# Trend Estimators


def sma(
    data: Union[List, pl.Series], period: int = 20
) -> Union[List[float], pl.Series]:
    """Simple Moving Average - works with both lists and Series"""
    if isinstance(data, list):
        if len(data) < period:
            return []
        s = pl.Series(data, dtype=pl.Float64)
        result = s.rolling_mean(window_size=period)
        return _to_list(result)
    else:
        return data.rolling_mean(window_size=period)


def smma(s: pl.Series, period: int = 20) -> pl.Series:
    """Smoothed Moving Average"""
    return s.ewm_mean(alpha=1 / period, adjust=False)


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
    x_diff_squared_sum = np.sum(x_diff**2)

    def rolling_slope(window):
        if len(window) < period:
            return None
        window_array = np.array(window)  # Convert to numpy array
        y_mean = window_array.mean()  # Use numpy mean
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


def macd(
    s: pl.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[pl.Series, pl.Series, pl.Series]:
    """MACD (Moving Average Convergence Divergence)"""
    # Calculate EMAs directly from the Series
    fast_ema = ewma(s, fast)
    slow_ema = ewma(s, slow)

    # Calculate MACD line
    macd_line = fast_ema - slow_ema

    # Calculate signal line
    signal_line = ewma(macd_line, signal)

    # Calculate histogram
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


def rsi(
    data: Union[List, pl.Series], period: int = 14
) -> Union[List[float], pl.Series]:
    """Relative Strength Index - works with both lists and Series"""
    if isinstance(data, list):
        if len(data) < 2:
            return []
        s = pl.Series(data, dtype=pl.Float64)
        delta = s.diff()
        gain = (delta.clip(lower_bound=0)).ewm_mean(alpha=1 / period)
        loss = (-delta.clip(upper_bound=0)).ewm_mean(alpha=1 / period)
        rs = gain / loss
        rsi_values = 100 - (100 / (1 + rs))
        return _to_list(rsi_values)
    else:
        delta = data.diff()
        gain = (delta.clip(lower_bound=0)).ewm_mean(alpha=1 / period)
        loss = (-delta.clip(upper_bound=0)).ewm_mean(alpha=1 / period)
        rs = gain / loss
        return 100 - (100 / (1 + rs))


def cci(
    high: pl.Series, low: pl.Series, close: pl.Series, period: int = 20
) -> pl.Series:
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


def stochastic(
    close: pl.Series, high: pl.Series, low: pl.Series, period: int = 14
) -> tuple[pl.Series, pl.Series]:
    """Stochastic Oscillator"""
    lowest_low = low.rolling_min(window_size=period)
    highest_high = high.rolling_max(window_size=period)
    k = 100 * (close - lowest_low) / (highest_high - lowest_low)
    d = k.rolling_mean(window_size=3)
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
    return (returns - returns.rolling_mean(window_size=period)) / returns.rolling_std(
        window_size=period
    )


def cumulative_returns(s: pl.Series) -> pl.Series:
    """Cumulative returns"""
    return (1 + roc(s)).cum_prod()


def vol_adjusted_momentum(s: pl.Series, period: int = 20) -> pl.Series:
    """Volatility-adjusted momentum"""
    mom = simple_mom(s, period)
    vol = ewstd(s, period)
    return mom / vol


# FIXME
def adx(
    high: pl.Series, low: pl.Series, close: pl.Series, period: int = 14
) -> tuple[pl.Series, pl.Series, pl.Series]:
    """Average Directional Index"""
    # Create a DataFrame to work with expressions properly
    df = pl.DataFrame({"high": high, "low": low, "close": close})

    # True Range calculation using expressions
    df = df.with_columns(
        [
            pl.max_horizontal(
                [
                    pl.col("high") - pl.col("low"),
                    (pl.col("high") - pl.col("close").shift()).abs(),
                    (pl.col("low") - pl.col("close").shift()).abs(),
                ]
            ).alias("tr"),
            # Directional Movement
            (pl.col("high") - pl.col("high").shift()).alias("up_move"),
            (pl.col("low").shift() - pl.col("low")).alias("down_move"),
        ]
    )

    # Calculate directional indicators
    df = df.with_columns(
        [
            pl.when(pl.col("up_move") > pl.col("down_move"))
            .then(pl.col("up_move"))
            .otherwise(0)
            .clip(lower_bound=0)
            .alias("pos_dm"),
            pl.when(pl.col("down_move") > pl.col("up_move"))
            .then(pl.col("down_move"))
            .otherwise(0)
            .clip(lower_bound=0)
            .alias("neg_dm"),
        ]
    )

    # Smoothed values
    df = df.with_columns(
        [
            pl.col("tr").ewm_mean(alpha=1 / period).alias("tr14"),
            pl.col("pos_dm").ewm_mean(alpha=1 / period).alias("pos_dm_smooth"),
            pl.col("neg_dm").ewm_mean(alpha=1 / period).alias("neg_dm_smooth"),
        ]
    )

    # Calculate DI values
    df = df.with_columns(
        [
            (100 * pl.col("pos_dm_smooth") / pl.col("tr14")).alias("plus_di14"),
            (100 * pl.col("neg_dm_smooth") / pl.col("tr14")).alias("minus_di14"),
        ]
    )

    # ADX calculation
    df = df.with_columns(
        [
            (
                100
                * (pl.col("plus_di14") - pl.col("minus_di14")).abs()
                / (pl.col("plus_di14") + pl.col("minus_di14"))
            ).alias("dx")
        ]
    )

    df = df.with_columns([pl.col("dx").ewm_mean(alpha=1 / period).alias("adx_value")])

    return df["plus_di14"], df["minus_di14"], df["adx_value"]


# FIXME
def close_dmi(s: pl.Series, period: int = 14) -> tuple[pl.Series, pl.Series, pl.Series]:
    """
    Direction Movement Index using close prices only
    Returns: (plus_di, minus_di, trend_strength)
    """
    # Calculate price changes
    price_change = s.diff()

    # Simple directional movement from close prices
    pos_dm = price_change.clip(lower_bound=0)
    neg_dm = (-price_change).clip(lower_bound=0)

    # Calculate simple directional indicators
    pos_dm_smooth = pos_dm.ewm_mean(alpha=1 / period)
    neg_dm_smooth = neg_dm.ewm_mean(alpha=1 / period)

    # Normalize to get directional indicators
    total_movement = pos_dm_smooth + neg_dm_smooth
    plus_di = 100 * (pos_dm_smooth / total_movement).fill_null(0)
    minus_di = 100 * (neg_dm_smooth / total_movement).fill_null(0)

    # Calculate trend strength (similar to ADX)
    di_diff = (plus_di - minus_di).abs()
    di_sum = plus_di + minus_di
    dx = (di_diff / di_sum * 100).fill_null(0)
    trend_strength = dx.ewm_mean(alpha=1 / period)

    return plus_di, minus_di, trend_strength


# Exponential Moving Average
def ema(
    data: Union[List, pl.Series], period: int = 20
) -> Union[List[float], pl.Series]:
    """Exponential Moving Average - works with both lists and Series"""
    if isinstance(data, list):
        if len(data) == 0:
            return []
        s = pl.Series(data, dtype=pl.Float64)
        alpha = 2.0 / (period + 1)
        result = s.ewm_mean(alpha=alpha, adjust=False)
        return result.to_list()
    else:
        alpha = 2.0 / (period + 1)
        return data.ewm_mean(alpha=alpha, adjust=False)


# Bollinger Bands
def bollinger_bands(
    data: Union[List, pl.Series], period: int = 20, std_dev: float = 2
) -> Union[
    Tuple[List[float], List[float], List[float]], Tuple[pl.Series, pl.Series, pl.Series]
]:
    """Bollinger Bands calculation"""
    if isinstance(data, list):
        if len(data) < period:
            return [], [], []
        s = pl.Series(data, dtype=pl.Float64)
    else:
        s = data

    middle = s.rolling_mean(window_size=period)
    std = s.rolling_std(window_size=period)
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)

    if isinstance(data, list):
        return _to_list(upper), _to_list(middle), _to_list(lower)
    else:
        return upper, middle, lower


# Volatility functions
def volatility(returns: Union[List, pl.Series]) -> float:
    """Calculate volatility (standard deviation of returns)"""
    if isinstance(returns, list):
        if len(returns) <= 1:
            return 0.0
        s = pl.Series(returns, dtype=pl.Float64)
        result = s.std()
        return float(result) if result is not None else 0.0  # type: ignore[arg-type]
    else:
        result = returns.std()
        return float(result) if result is not None else 0.0  # type: ignore[arg-type]


def rolling_volatility(
    returns: Union[List, pl.Series], period: int = 20
) -> List[float]:
    """Rolling volatility calculation"""
    if isinstance(returns, list):
        if len(returns) < period:
            return []
        s = pl.Series(returns, dtype=pl.Float64)
    else:
        s = returns

    rolling_std = s.rolling_std(window_size=period)
    return _to_list(rolling_std)


# Statistical functions
def correlation(x: Union[List, pl.Series], y: Union[List, pl.Series]) -> float:
    """Pearson correlation coefficient"""
    if isinstance(x, list):
        x = pl.Series(x, dtype=pl.Float64)
    if isinstance(y, list):
        y = pl.Series(y, dtype=pl.Float64)

    # Calculate correlation using numpy for simplicity
    x_array = np.array(x.to_list())
    y_array = np.array(y.to_list())
    return float(np.corrcoef(x_array, y_array)[0, 1])


def percentile(data: Union[List, pl.Series], p: float) -> float:
    """Calculate percentile of data"""
    if isinstance(data, list):
        if len(data) == 0:
            return 0.0
        s = pl.Series(data, dtype=pl.Float64)
    else:
        s = data

    return s.quantile(p / 100.0) or 0.0


# Prediction models
def linear_regression(
    x: Union[List, pl.Series], y: Union[List, pl.Series]
) -> Tuple[float, float]:
    """Simple linear regression returning (slope, intercept)"""
    if isinstance(x, list):
        x_array = np.array(x)
    else:
        x_array = np.array(x.to_list())

    if isinstance(y, list):
        y_array = np.array(y)
    else:
        y_array = np.array(y.to_list())

    # Calculate slope and intercept
    x_mean = np.mean(x_array)
    y_mean = np.mean(y_array)

    slope = np.sum((x_array - x_mean) * (y_array - y_mean)) / np.sum(
        (x_array - x_mean) ** 2
    )
    intercept = y_mean - slope * x_mean

    return float(slope), float(intercept)


def predict_next(data: Union[List, pl.Series]) -> float:
    """Predict next value using linear regression"""
    if isinstance(data, list):
        x = list(range(len(data)))
        y = data
    else:
        x = list(range(len(data)))
        y = data.to_list()

    slope, intercept = linear_regression(x, y)
    next_x = len(x)
    return slope * next_x + intercept


# Utility functions
def normalize(data: Union[List, pl.Series]) -> List[float]:
    """Normalize data to 0-1 range"""
    if isinstance(data, list):
        if len(data) == 0:
            return []
        s = pl.Series(data, dtype=pl.Float64)
    else:
        s = data.cast(pl.Float64)

    min_val = s.min()
    max_val = s.max()

    if min_val is None or max_val is None or min_val == max_val:
        return [0.0] * len(s)

    # Type guard: at this point min_val and max_val are guaranteed to be numeric
    normalized = (s - min_val) / (max_val - min_val)  # type: ignore[operator]
    return normalized.to_list()


def standardize(data: Union[List, pl.Series]) -> List[float]:
    """Standardize data (z-score normalization)"""
    if isinstance(data, list):
        if len(data) == 0:
            return []
        s = pl.Series(data, dtype=pl.Float64)
    else:
        s = data.cast(pl.Float64)

    mean_val = s.mean()
    std_val = s.std()

    if std_val is None or std_val == 0:
        return [0.0] * len(s)

    standardized = (s - mean_val) / std_val
    return standardized.to_list()


def moving_window(data: Union[List, pl.Series], window_size: int) -> List[List[float]]:
    """Create moving windows of specified size"""
    if isinstance(data, list):
        data_list = data
    else:
        data_list = data.to_list()

    if len(data_list) < window_size:
        return []

    windows = []
    for i in range(len(data_list) - window_size + 1):
        windows.append(data_list[i : i + window_size])

    return windows
