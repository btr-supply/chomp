from typing import Callable, Any, Optional
import polars as pl
from datetime import datetime

from ..models import DataFormat
from ..analytics.momentum import macd, close_dmi
from ..utils import numeric_columns, Interval, fit_date_params, log_warn, log_error, safe_float
from ..utils.decorators import service_method
from .. import state
from . import loader

# Estimator lists for dynamic metric computation
VOLATILITY_ESTIMATORS = ["std", "wstd", "ewstd", "close_atr", "mad"]
TREND_ESTIMATORS = [
    "sma", "smma", "wma", "ewma", "linreg", "polyreg", "theil_sen"
]
MOMENTUM_ESTIMATORS = [
    "roc", "simple_mom", "close_rsi", "close_cci", "close_stochastic",
    "zscore", "cumulative_returns", "vol_adjusted_momentum"
]


async def ensure_df(resources: list[str],
                    fields: list[str],
                    from_date: datetime,
                    to_date: datetime,
                    interval: Interval = 'm5',
                    quote: Optional[str] = None,
                    precision: int = 6,
                    df: Optional[pl.DataFrame] = None,
                    target_epochs: int = 1000) -> pl.DataFrame:

  if df is not None and not df.is_empty():
    return df

  if not fields:
    log_warn("No fields provided to ensure_df")
    raise ValueError("No fields provided")

  if df is None:
    # Fetch data with dynamic interval to get ~1000 points
    from_date, to_date, interval, target_epochs = fit_date_params(
        from_date, to_date, interval, target_epochs)
    # Handle the quote parameter properly for get_history
    quote_param = quote if quote is not None else "USDC.idx"
    result = await loader.get_history(resources, fields, from_date, to_date,
                                      interval, quote_param, precision,
                                      "polars")
    # Ensure we have a DataFrame
    if not isinstance(result, pl.DataFrame):
      log_error("Expected DataFrame from get_history")
      raise ValueError("Expected DataFrame from get_history")
    df = result
    # Check if df is empty
    if df.height == 0:
      log_warn(f"No data found for resources {resources}")
      raise ValueError("No data found")

  return df


async def add_metrics(
    resources: list[str], fields: list[str], from_date: datetime,
    to_date: datetime, interval: Interval, periods: list[int],
    quote: Optional[str], precision: int, df: Optional[pl.DataFrame],
    get_metrics: Callable[[pl.DataFrame, str, int], dict]) -> pl.DataFrame:
  """Generic function to analyze series with specified metric function"""

  # Ensure df is loaded and validated
  df = await ensure_df(resources, fields, from_date, to_date, interval, quote,
                       precision, df)
  if df is None:
    raise ValueError("Failed to load DataFrame")

  # Filter numeric columns efficiently
  numeric_fields = numeric_columns(df)

  # Parallel computation with thread pool
  tp = state.thread_pool
  futures = [
      tp.submit(
          lambda r, c, p: (f"{r}.{c}" if len(resources) > 1 else c,
                           get_metrics(df, f"{r}.{c}"
                                       if len(resources) > 1 else c, p)),
          resource, col, period) for resource in resources
      for col in numeric_fields for period in periods
  ]

  # Collect all metrics efficiently
  all_columns: list[pl.Expr] = []
  for future in futures:
    try:
      col_name, metrics = future.result()
      for metric_name, metric_series in metrics.items():
        # Convert everything to Expr since with_columns expects Expr
        if isinstance(metric_series, pl.Series):
          # Convert Series to Expr by creating a literal
          all_columns.append(pl.lit(metric_series).alias(metric_name))
        elif isinstance(metric_series, pl.Expr):
          all_columns.append(metric_series.alias(metric_name))
        else:
          all_columns.append(pl.lit(metric_series).alias(metric_name))
    except KeyError as e:
      log_error(f"Column not found: {e}")
      continue

  # Add all new columns at once (more efficient than iterative)
  return df.with_columns(all_columns) if all_columns else df


@service_method("compute volatility metrics")
async def get_volatility(resources: list[str],
                         fields: list[str],
                         from_date: datetime,
                         to_date: datetime,
                         interval: Interval,
                         periods: list[int] = [20],
                         quote: Optional[str] = None,
                         precision: int = 6,
                         format: DataFormat = "json:row",
                         df: Optional[pl.DataFrame] = None) -> Any:
  """Calculate various volatility metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]
    metrics = {}
    for estimator_name in VOLATILITY_ESTIMATORS:
      estimator_func = globals()[estimator_name]
      metrics[f"{col}:{estimator_name}.{period}"] = estimator_func(
          last, period=period)
    return metrics

  df = await add_metrics(resources, fields, from_date, to_date, interval,
                         periods, quote, precision, df, compute)
  return loader.format_table(df, from_format="polars", to_format=format)


@service_method("compute trend metrics")
async def get_trend(resources: list[str],
                    fields: list[str],
                    from_date: datetime,
                    to_date: datetime,
                    interval: Interval,
                    periods: list[int] = [20],
                    quote: Optional[str] = None,
                    precision: int = 6,
                    format: DataFormat = "json:row",
                    df: Optional[pl.DataFrame] = None) -> Any:
  """Calculate various trend metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]
    metrics = {}
    for estimator_name in TREND_ESTIMATORS:
      estimator_func = globals()[estimator_name]
      metrics[f"{col}:{estimator_name}.{period}"] = estimator_func(
          last, period=period)
    return metrics

  df = await add_metrics(resources, fields, from_date, to_date, interval,
                         periods, quote, precision, df, compute)
  return loader.format_table(df, from_format="polars", to_format=format)


@service_method("compute momentum metrics")
async def get_momentum(resources: list[str],
                       fields: list[str],
                       from_date: datetime,
                       to_date: datetime,
                       interval: Interval,
                       periods: list[int] = [20],
                       quote: Optional[str] = None,
                       precision: int = 6,
                       format: DataFormat = "json:row",
                       df: Optional[pl.DataFrame] = None) -> Any:
  """Calculate various momentum metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]
    metrics = {}

    # Single-return estimators
    for estimator_name in MOMENTUM_ESTIMATORS:
      estimator_func = globals()[estimator_name]
      if estimator_name == "cumulative_returns":
        metrics[f"{col}:{estimator_name}"] = estimator_func(last)
      else:
        metrics[f"{col}:{estimator_name}.{period}"] = estimator_func(
            last, period=period)

    # Tuple-return estimators
    macd_line, signal_line, macd_hist = macd(last)
    metrics[f"{col}:macd"] = macd_line
    metrics[f"{col}:macd_signal"] = signal_line
    metrics[f"{col}:macd_hist"] = macd_hist

    plus_di, minus_di, adx = close_dmi(last, period=period)
    metrics[f"{col}:plus_di.{period}"] = plus_di
    metrics[f"{col}:minus_di.{period}"] = minus_di
    metrics[f"{col}:adx.{period}"] = adx

    return metrics

  df = await add_metrics(resources, fields, from_date, to_date, interval,
                         periods, quote, precision, df, compute)
  return loader.format_table(df, from_format="polars", to_format=format)


@service_method("compute all metrics")
async def get_all(resources: list[str],
                  fields: list[str],
                  from_date: datetime,
                  to_date: datetime,
                  interval: Interval,
                  periods: list[int] = [20],
                  quote: Optional[str] = None,
                  precision: int = 6,
                  df: Optional[pl.DataFrame] = None,
                  format: DataFormat = "json:row") -> Any:
  """Calculate all available metrics (volatility, trend, momentum)"""

  # Define combined metrics function
  def compute_all_metrics(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]
    all_metrics = {}

    # Volatility metrics
    for estimator_name in VOLATILITY_ESTIMATORS:
      estimator_func = globals()[estimator_name]
      all_metrics[f"{col}:{estimator_name}.{period}"] = estimator_func(
          last, period=period)

    # Trend metrics
    for estimator_name in TREND_ESTIMATORS:
      estimator_func = globals()[estimator_name]
      all_metrics[f"{col}:{estimator_name}.{period}"] = estimator_func(
          last, period=period)

    # Momentum metrics (single)
    for estimator_name in MOMENTUM_ESTIMATORS:
      estimator_func = globals()[estimator_name]
      if estimator_name == "cumulative_returns":
        all_metrics[f"{col}:{estimator_name}"] = estimator_func(last)
      else:
        all_metrics[f"{col}:{estimator_name}.{period}"] = estimator_func(
            last, period=period)

    # Momentum metrics (tuple)
    macd_line, signal_line, macd_hist = macd(last)
    all_metrics[f"{col}:macd"] = macd_line
    all_metrics[f"{col}:macd_signal"] = signal_line
    all_metrics[f"{col}:macd_hist"] = macd_hist

    plus_di, minus_di, adx = close_dmi(last, period=period)
    all_metrics[f"{col}:plus_di.{period}"] = plus_di
    all_metrics[f"{col}:minus_di.{period}"] = minus_di
    all_metrics[f"{col}:adx.{period}"] = adx

    return all_metrics

  df = await add_metrics(resources, fields, from_date, to_date, interval,
                         periods, quote, precision, df, compute_all_metrics)
  return loader.format_table(df, from_format="polars", to_format=format)


@service_method("compute operational range metrics")
async def get_oprange(resources: list[str],
                      fields: list[str],
                      from_date: datetime,
                      to_date: datetime,
                      precision: int = 6,
                      quote: Optional[str] = None,
                      format: DataFormat = "json:row") -> Any:
  """Calculate operational range metrics"""

  # Fetch data without specific interval (use default)
  df = await loader.get_history(
      resources,
      fields,
      from_date,
      to_date,
      'm5',  # default interval
      quote,
      precision,
      "polars")

  if not isinstance(df, pl.DataFrame) or df.height == 0:
    log_warn("No data available for operational range calculation")
    raise ValueError("No data available for operational range calculation")

  def _safe_subtract(a: Any, b: Any) -> Optional[float]:
    """Safely subtract two values, handling None cases"""
    if a is None or b is None:
      return None
    a_float = safe_float(a)
    b_float = safe_float(b)
    return a_float - b_float if a_float is not None and b_float is not None else None

  def compute_range_metrics(df: pl.DataFrame, col: str) -> dict:
    # Use polars for efficient operations
    last = df[col].cast(pl.Float64, strict=False)

    # Compute statistics directly (simpler and more reliable)
    min_val = last.min()
    max_val = last.max()
    q25 = last.quantile(0.25)
    q75 = last.quantile(0.75)
    median_val = last.median()

    # Calculate range and IQR using safe arithmetic helper
    range_val = _safe_subtract(max_val, min_val)
    iqr_val = _safe_subtract(q75, q25)

    return {
        f"{col}:min": min_val,
        f"{col}:max": max_val,
        f"{col}:range": range_val,
        f"{col}:iqr": iqr_val,
        f"{col}:q1": q25,
        f"{col}:q3": q75,
        f"{col}:median": median_val
    }

  # Get numeric columns
  numeric_fields = numeric_columns(df)
  result_df = df.clone()

  # Compute range metrics for each numeric column
  for resource in resources:
    for col in numeric_fields:
      col_name = f"{resource}.{col}" if len(resources) > 1 else col
      metrics = compute_range_metrics(df, col_name)
      for metric_name, metric_value in metrics.items():
        # Add scalar metrics as constant columns
        result_df = result_df.with_columns(
            pl.lit(metric_value).alias(metric_name))

  return loader.format_table(result_df, from_format="polars", to_format=format)
