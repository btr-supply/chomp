from typing import Optional, Callable, List, Tuple, Any, Union
import polars as pl
from datetime import datetime

from ..model import DataFormat, ServiceResponse
from ..utils import estimators as est, numeric_columns,\
  Interval, fit_date_params
from .. import state
from . import loader


async def ensure_df(
    resources: list[str],
    fields: list[str],
    from_date: datetime,
    to_date: datetime,
    interval: Interval = 'm5',
    quote: Union[str, None] = None,
    precision: int = 6,
    df: Optional[pl.DataFrame] = None,
    target_epochs: int = 1000) -> ServiceResponse[pl.DataFrame]:

  if df is not None and not df.is_empty():
    return "", df

  if not fields:
    return "No fields provided", pl.DataFrame()

  if df is None:
    # Fetch data with dynamic interval to get ~1000 points
    from_date, to_date, interval, target_epochs = fit_date_params(
        from_date, to_date, interval, target_epochs)
    # Handle the quote parameter properly for get_history
    quote_param = quote if quote is not None else "USDC.idx"
    err, result = await loader.get_history(resources, fields, from_date,
                                           to_date, interval, quote_param,
                                           precision, "polars")
    if err:
      return err, pl.DataFrame()
    # Ensure we have a DataFrame
    if not isinstance(result, pl.DataFrame):
      return "Expected DataFrame from get_history", pl.DataFrame()
    df = result
    # Check if df is empty
    if df.height == 0:
      return "No data found", pl.DataFrame()

  return "", df


async def add_metrics(
    resources: List[str], fields: List[str], from_date: datetime,
    to_date: datetime, interval: Interval, periods: List[int],
    quote: Union[str, None], precision: int, df: Optional[pl.DataFrame],
    get_metrics: Callable[[pl.DataFrame, str, int], dict]
) -> ServiceResponse[pl.DataFrame]:
  """Generic function to analyze series with specified metric function"""

  # Ensure df is loaded
  err, df = await ensure_df(resources, fields, from_date, to_date, interval,
                            quote, precision, df)
  if err:
    return err, pl.DataFrame()

  # Ensure df is not None after successful call
  if df is None:
    return "Failed to load DataFrame", pl.DataFrame()

  # Filter numeric columns
  numeric_fields = numeric_columns(df)

  # Create new dataframe with original columns
  result_df = df.clone()

  def compute(resource: str, col: str,
              period: int) -> Tuple[dict, Union[str, None]]:
    try:
      col = f"{resource}.{col}" if len(resources) > 1 else col
      metrics = get_metrics(df, col, period)
      return metrics, None
    except KeyError:
      return {}, f"Required column '{col}' not found in resource {resource}"

  tp = state.thread_pool
  futures = []
  for resource in resources:
    for col in numeric_fields:
      for period in periods:
        futures.append(tp.submit(compute, resource, col, period))

  # Collect results and handle errors
  for future in futures:
    metrics, error = future.result()
    if error:
      return error, pl.DataFrame()
    # Add each metric series as a new column
    for metric_name, metric_series in metrics.items():
      # Convert metric_series to a properly named polars Series
      if isinstance(metric_series, pl.Series):
        named_series = metric_series.alias(metric_name)
      elif isinstance(metric_series, pl.Expr):
        # For Expr objects, we need to evaluate them first
        named_series = df.select(metric_series.alias(metric_name)).to_series()
      else:
        named_series = pl.Series(name=metric_name, values=metric_series)

      # Add the series as a new column
      result_df = result_df.with_columns(named_series)

  return "", result_df


async def get_volatility(
    resources: List[str],
    fields: List[str],
    from_date: datetime,
    to_date: datetime,
    interval: Interval,
    periods: List[int] = [20],
    quote: Union[str, None] = None,
    precision: int = 6,
    format: DataFormat = "json:row",
    df: Optional[pl.DataFrame] = None) -> ServiceResponse[Any]:
  """Calculate various volatility metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]
    return {
        f"{col}:std.{period}": est.std(last, period),
        f"{col}:wstd.{period}": est.wstd(last, period),
        f"{col}:ewstd.{period}": est.ewstd(last, period),
        f"{col}:mad.{period}": est.mad(last, period),
        f"{col}:close_atr.{period}": est.close_atr(last, period)
    }

  err, df = await add_metrics(resources, fields, from_date, to_date, interval,
                              periods, quote, precision, df, compute)
  if err:
    return err, None

  if df is None:
    return "Failed to compute volatility metrics", None

  err_fmt, result = loader.format_table(df,
                                        from_format="polars",
                                        to_format=format)
  if err_fmt:
    return err_fmt, None

  return "", result


async def get_trend(resources: list[str],
                    fields: list[str],
                    from_date: datetime,
                    to_date: datetime,
                    interval: Interval,
                    periods: list[int] = [20],
                    quote: Union[str, None] = None,
                    precision: int = 6,
                    format: DataFormat = "json:row",
                    df: Optional[pl.DataFrame] = None) -> ServiceResponse[Any]:
  """Calculate various trend metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]

    # Calculate basic trend indicators
    trend_metrics = {
        f"{col}:sma.{period}": est.sma(last, period),
        f"{col}:smma.{period}": est.smma(last, period),
        f"{col}:wma.{period}": est.wma(last, period),
        f"{col}:ewma.{period}": est.ewma(last, period),
        f"{col}:linreg.{period}": est.linreg(last, period),
        f"{col}:polyreg.{period}": est.polyreg(last, period),
        f"{col}:theil_sen.{period}": est.theil_sen(last, period),
    }

    # Calculate close-price based directional movement
    # plus_di, minus_di, dmi = est.close_dmi(price, period)
    # trend_metrics.update({
    #   # f"{col}:plus_di.{period}": plus_di,
    #   # f"{col}:minus_di.{period}": minus_di,
    #   f"{col}:close_dmi.{period}": dmi
    # })

    return trend_metrics

  err, df = await add_metrics(resources, fields, from_date, to_date, interval,
                              periods, quote, precision, df, compute)
  if err:
    return err, None

  if df is None:
    return "Failed to compute trend metrics", None

  err_fmt, result = loader.format_table(df,
                                        from_format="polars",
                                        to_format=format)
  if err_fmt:
    return err_fmt, None

  return "", result


async def get_momentum(
    resources: List[str],
    fields: List[str],
    from_date: datetime,
    to_date: datetime,
    interval: Interval,
    periods: List[int] = [20],
    quote: Union[str, None] = None,
    precision: int = 6,
    format: DataFormat = "json:row",
    df: Optional[pl.DataFrame] = None) -> ServiceResponse[Any]:
  """Calculate various momentum metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    last = df[col]
    return {
        f"{col}:roc.{period}": est.roc(last, period),
        f"{col}:simple_mom.{period}": est.simple_mom(last, period),
        f"{col}:macd_line.{period}": est.macd(last)[0],
        f"{col}:macd_signal.{period}": est.macd(last)[1],
        f"{col}:macd_histogram.{period}": est.macd(last)[2],
        f"{col}:rsi.{period}": est.rsi(last, period),
        f"{col}:stoch_k.{period}": est.close_stochastic(last, period),
        f"{col}:zscore.{period}": est.zscore(last, period),
        f"{col}:vol_adj_mom.{period}": est.vol_adjusted_momentum(last, period),
    }

  err, df = await add_metrics(resources, fields, from_date, to_date, interval,
                              periods, quote, precision, df, compute)
  if err:
    return err, None

  if df is None:
    return "Failed to compute momentum metrics", None

  err_fmt, result = loader.format_table(df,
                                        from_format="polars",
                                        to_format=format)
  if err_fmt:
    return err_fmt, None

  return "", result


async def get_all(resources: list[str],
                  fields: list[str],
                  from_date: datetime,
                  to_date: datetime,
                  interval: Interval,
                  periods: list[int] = [20],
                  quote: Union[str, None] = None,
                  precision: int = 6,
                  df: Optional[pl.DataFrame] = None,
                  format: DataFormat = "json:row") -> ServiceResponse[Any]:
  """Calculate all metrics for given fields"""

  try:
    err, df = await get_volatility(resources, fields, from_date, to_date,
                                   interval, periods, quote, precision,
                                   "polars", df)
    if err:
      return err, None

    err, df = await get_trend(resources, fields, from_date, to_date, interval,
                              periods, quote, precision, "polars", df)
    if err:
      return err, None

    err, df = await get_momentum(resources, fields, from_date, to_date,
                                 interval, periods, quote, precision, "polars",
                                 df)
    if err:
      return err, None

  except Exception as e:
    return f"Error computing metrics: {e}", None

  if df is None:
    return "Failed to compute all metrics", None

  err_fmt, result = loader.format_table(df,
                                        from_format="polars",
                                        to_format=format)
  if err_fmt:
    return err_fmt, None

  return "", result


async def get_oprange(resources: List[str],
                      fields: List[str],
                      from_date: datetime,
                      to_date: datetime,
                      precision: int = 6,
                      quote: Union[str, None] = None,
                      format: DataFormat = "json:row") -> ServiceResponse[Any]:
  """Calculate optimized range analysis for trading/financial metrics"""

  # Ensure df is loaded with a reasonable interval for range analysis
  err, df = await ensure_df(resources, fields, from_date, to_date, "m5", quote,
                            precision, None, 1000)
  if err:
    return err, pl.DataFrame()

  if df is None:
    return "Failed to load DataFrame for range analysis", pl.DataFrame()

  # Filter numeric columns
  numeric_fields = numeric_columns(df)

  # Create new dataframe with original columns
  result_df = df.clone()

  def compute_range_metrics(df: pl.DataFrame, col: str) -> dict:
    """Compute optimized range metrics for a single column"""
    last = df[col]

    if last.is_empty():
      return {}

    # Basic range metrics - ensure we're working with numeric values only
    try:
      # Get min/max as potentially mixed types
      min_val_raw = last.min()
      max_val_raw = last.max()

      # Type-safe conversion to numeric types only
      min_val: Union[float, None] = None
      max_val: Union[float, None] = None

      # Convert to float if numeric, otherwise None
      if isinstance(min_val_raw, (int, float)):
        min_val = float(min_val_raw)
      if isinstance(max_val_raw, (int, float)):
        max_val = float(max_val_raw)

      # Type-safe range calculation
      range_val: Union[float, None] = None
      if min_val is not None and max_val is not None:
        range_val = max_val - min_val

      # Current position in range (0-1 scale)
      current_raw = last.tail(1).item() if len(last) > 0 else None
      current: Union[float, None] = None
      if isinstance(current_raw, (int, float)):
        current = float(current_raw)

      # Type-safe range position calculation
      range_position: Union[float, None] = None
      if (current is not None and min_val is not None and range_val is not None
          and range_val != 0):
        range_position = (current - min_val) / range_val

      # Average True Range (ATR) - 14 period default using the estimator function
      # This avoids direct arithmetic operations on series
      atr_14: Union[float, None] = None
      try:
        if len(last) >= 14:
          atr_result = est.close_atr(last, 14)
          if isinstance(atr_result, (int, float)):
            atr_14 = float(atr_result)
      except Exception:
        atr_14 = None

      return {
          f"{col}:min": min_val,
          f"{col}:max": max_val,
          f"{col}:range": range_val,
          f"{col}:range_position": range_position,
          f"{col}:atr_14": atr_14,
          f"{col}:current": current
      }
    except Exception:
      # Return empty dict if any calculation fails
      return {}

  tp = state.thread_pool
  futures = []

  for resource in resources:
    for col in numeric_fields:
      col_name = f"{resource}.{col}" if len(resources) > 1 else col
      futures.append(tp.submit(compute_range_metrics, df, col_name))

  # Collect results and handle errors
  range_metrics = {}
  for future in futures:
    metrics = future.result()
    range_metrics.update(metrics)

  # Add metrics as scalar values to the result (last row approach)
  if range_metrics:
    # Create a single-row dataframe with the range metrics
    metrics_row = {
        **{
            col: df[col].tail(1).item() if len(df[col]) > 0 else None
            for col in df.columns
        },
        **range_metrics
    }
    result_df = pl.DataFrame([metrics_row])

  err_fmt, result = loader.format_table(result_df,
                                        from_format="polars",
                                        to_format=format)
  if err_fmt:
    return err_fmt, pl.DataFrame()

  return "", result
