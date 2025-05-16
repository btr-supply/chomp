from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable, List, Tuple
import polars as pl
from datetime import datetime

from ..model import DataFormat, ServiceResponse
from ..utils import estimators as est, numeric_columns,\
  Interval, fit_date_params, fit_interval, now, ago
from .. import state
from . import loader


async def ensure_df(
  resources: list[str],
  fields: list[str],
  from_date: datetime,
  to_date: datetime,
  interval: Interval = 'm5',
  quote: str = None,
  precision: int = 6,
  df: Optional[pl.DataFrame] = None,
  target_epochs: int = 1000
) -> ServiceResponse[pl.DataFrame]:

  if df:
    return "", df

  if not fields:
    return "No fields provided", None

  if df is None:
    # Fetch data with dynamic interval to get ~1000 points
    from_date, to_date, interval, target_epochs = fit_date_params(from_date, to_date, interval, target_epochs)
    err, df = await loader.get_history(resources, fields, from_date, to_date, interval, quote, precision, "polars")
    if err:
      return err, None
    # Check if df is empty
    if df.height == 0:
      return "No data found", None

  return "", df

async def add_metrics(
  resources: List[str],
  fields: List[str],
  from_date: datetime,
  to_date: datetime,
  interval: Interval,
  periods: List[int],
  quote: str,
  precision: int,
  df: Optional[pl.DataFrame],
  get_metrics: Callable[[pl.DataFrame, str, int], dict]
) -> ServiceResponse[pl.DataFrame]:
  """Generic function to analyze series with specified metric function"""

  # Ensure df is loaded
  err, df = await ensure_df(resources, fields, from_date, to_date, interval, quote, precision, df)
  if err:
    return err, None

  # Filter numeric columns
  numeric_fields = numeric_columns(df)

  # Create new dataframe with original columns
  result_df = df.clone()

  def compute(resource: str, col: str, period: int) -> Tuple[dict, str]:
    try:
      col = f"{resource}.{col}" if len(resources) > 1 else col
      metrics = get_metrics(df, col, period)
      return metrics, None
    except KeyError:
      return None, f"Required column '{col}' not found in resource {resource}"

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
      return error, None
    # Add each metric series as a new column
    for metric_name, metric_series in metrics.items():
      # Convert metric_series to a properly named polars Series
      if isinstance(metric_series, pl.Series):
        named_series = metric_series.alias(metric_name)
      elif isinstance(metric_series, pl.Expr):
        named_series = metric_series.alias(metric_name)
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
  quote: str = None,
  precision: int = 6,
  format: DataFormat = "json:row",
  df: Optional[pl.DataFrame] = None
) -> ServiceResponse[pl.DataFrame]:
  """Calculate various volatility metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    price = df[col]
    return {
      f"{col}:std.{period}": est.std(price, period),
      f"{col}:wstd.{period}": est.wstd(price, period),
      f"{col}:ewstd.{period}": est.ewstd(price, period),
      f"{col}:mad.{period}": est.mad(price, period),
      f"{col}:close_atr.{period}": est.close_atr(price, period)
    }

  err, df = await add_metrics(
    resources, fields, from_date, to_date, interval, periods,
    quote, precision, df, compute
  )
  return (err, None) if err else loader.format_table(df, from_format="polars", to_format=format)

async def get_trend(
  resources: list[str],
  fields: list[str],
  from_date: datetime,
  to_date: datetime,
  interval: Interval,
  periods: list[int] = [20],
  quote: str = None,
  precision: int = 6,
  format: DataFormat = "json:row",
  df: Optional[pl.DataFrame] = None
) -> ServiceResponse[pl.DataFrame]:
  """Calculate various trend metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    price = df[col]

    # Calculate basic trend indicators
    trend_metrics = {
      f"{col}:sma.{period}": est.sma(price, period),
      f"{col}:smma.{period}": est.smma(price, period),
      f"{col}:wma.{period}": est.wma(price, period),
      f"{col}:ewma.{period}": est.ewma(price, period),
      f"{col}:linreg.{period}": est.linreg(price, period),
      f"{col}:polyreg.{period}": est.polyreg(price, period),
      f"{col}:theil_sen.{period}": est.theil_sen(price, period),
    }

    # Calculate close-price based directional movement
    # plus_di, minus_di, dmi = est.close_dmi(price, period)
    # trend_metrics.update({
    #   # f"{col}:plus_di.{period}": plus_di,
    #   # f"{col}:minus_di.{period}": minus_di,
    #   f"{col}:close_dmi.{period}": dmi
    # })

    return trend_metrics

  err, df = await add_metrics(
    resources, fields, from_date, to_date, interval, periods,
    quote, precision, df, compute
  )
  return (err, None) if err else loader.format_table(df, from_format="polars", to_format=format)

async def get_momentum(
  resources: List[str],
  fields: List[str],
  from_date: datetime,
  to_date: datetime,
  interval: Interval,
  periods: List[int] = [20],
  quote: str = None,
  precision: int = 6,
  format: DataFormat = "json:row",
  df: Optional[pl.DataFrame] = None
) -> ServiceResponse[pl.DataFrame]:
  """Calculate various momentum metrics for given fields"""

  def compute(df: pl.DataFrame, col: str, period: int) -> dict:
    price = df[col]
    return {
      f"{col}:roc.{period}": est.roc(price, period),
      f"{col}:simple_mom.{period}": est.simple_mom(price, period),
      f"{col}:macd_line.{period}": est.macd(price)[0],
      f"{col}:macd_signal.{period}": est.macd(price)[1],
      f"{col}:macd_histogram.{period}": est.macd(price)[2],
      f"{col}:rsi.{period}": est.rsi(price, period),
      f"{col}:stoch_k.{period}": est.close_stochastic(price, period),
      f"{col}:zscore.{period}": est.zscore(price, period),
      f"{col}:vol_adj_mom.{period}": est.vol_adjusted_momentum(price, period),
    }

  err, df = await add_metrics(
    resources, fields, from_date, to_date, interval, periods,
    quote, precision, df, compute
  )
  return (err, None) if err else loader.format_table(df, from_format="polars", to_format=format)

async def get_all(
  resources: list[str],
  fields: list[str],
  from_date: datetime,
  to_date: datetime,
  interval: Interval,
  periods: list[int] = [20],
  quote: str = None,
  precision: int = 6,
  df: Optional[pl.DataFrame] = None,
  format: DataFormat = "json:row"
) -> ServiceResponse[any]:
  """Calculate all metrics for given fields"""

  try:
    df = (await get_volatility(resources, fields, from_date, to_date, interval, periods, quote, precision, df))[1]
    df = (await get_trend(resources, fields, from_date, to_date, interval, periods, quote, precision, df))[1]
    df = (await get_momentum(resources, fields, from_date, to_date, interval, periods, quote, precision, df))[1]
  except Exception as e:
    return f"Error computing metrics: {e}", None
  return loader.format_table(df, from_format="polars", to_format=format)
