import math
import numpy as np
import polars as pl
from typing import Any, Optional, Union, List, Tuple

# Type aliases for better readability
NumericInput = Union[List[float], pl.Series, np.ndarray]
SeriesInput = Union[List[float], pl.Series, np.ndarray]


def safe_float(val: Any) -> Optional[float]:
  """
  Safely convert a value to float, handling edge cases and type errors.

  Parameters:
  - val: Value to convert to float

  Returns:
  - Float value or None if conversion fails
  """
  if val is None:
    return None
  try:
    return float(val)
  except (TypeError, ValueError):
    return None


# Optimized utility functions
def to_numpy(data: NumericInput) -> np.ndarray:
  """Convert various input types to numpy array efficiently."""
  if isinstance(data, pl.Series):
    return data.to_numpy()
  elif isinstance(data, np.ndarray):
    return data
  else:
    return np.asarray(data, dtype=np.float64)


def ensure_valid_arrays(*arrays: np.ndarray) -> Tuple[np.ndarray, ...]:
  """Remove NaN/inf values from arrays and ensure they have the same length."""
  if not arrays:
    return tuple()

  # Use numpy's more efficient column stacking for multi-array validation
  if len(arrays) == 1:
    valid_mask = np.isfinite(arrays[0])
  else:
    valid_mask = np.isfinite(np.column_stack(arrays)).all(axis=1)

  return tuple(arr[valid_mask] for arr in arrays)


def to_series(data: Union[List, pl.Series]) -> pl.Series:
  """Convert list to polars Series if needed."""
  if isinstance(data, list):
    return pl.Series(data, dtype=pl.Float64)
  return data


def to_list(series: pl.Series) -> List[float]:
  """Convert polars Series to list, filtering out nulls."""
  return [x for x in series.to_list() if x is not None]


def ensure_series(data: NumericInput,
                  name: str = "data",
                  min_length: Optional[int] = None) -> Optional[pl.Series]:
  """
  Convert input to Polars Series with optional validation.

  Args:
    data: Input data to convert (list, Series, ndarray, or scalar)
    name: Name for the series
    min_length: Minimum length required (returns None if not met)

  Returns:
    Polars Series or None if validation fails
  """
  # Convert to Series
  if isinstance(data, pl.Series):
    series = data
  elif isinstance(data, (list, np.ndarray)):
    series = pl.Series(name, data, dtype=pl.Float64)
  else:
    series = pl.Series(name, [data], dtype=pl.Float64)

  # Apply length validation if specified
  if min_length is not None and len(series) < min_length:
    return None

  return series


def safe_divide(numerator: pl.Series,
                denominator: pl.Series,
                fill_value: float = 0.0) -> pl.Series:
  """Safely divide two series, handling division by zero."""
  # Create a DataFrame to apply the expression and return the result as a Series
  df = pl.DataFrame({"num": numerator, "den": denominator})
  result = df.with_columns(
      pl.when(pl.col("den") != 0).then(
          pl.col("num") /
          pl.col("den")).otherwise(fill_value).alias("result"))["result"]
  return result


def rolling_alpha(period: int) -> float:
  """Calculate exponential smoothing alpha from period."""
  return 1.0 / period


def ewm_alpha(period: int) -> float:
  """Calculate EWM alpha from period."""
  return 2.0 / (period + 1)


def round_sigfig(value: float, precision: int = 6) -> float:
  """
  Round a number to a specified number of significant figures.

  Parameters:
  - value: The number to round
  - precision: Number of significant figures (default is 6)

  Returns:
  - The rounded number as a float

  Examples:
  >>> round_sigfig(123.456789, 3)  # Returns 123.0
  >>> round_sigfig(0.00123456, 3)  # Returns 0.00123
  >>> round_sigfig(0, 3)           # Returns 0
  """
  if value == 0:
    return 0.0

  abs_value = abs(value)
  magnitude = math.floor(math.log10(abs_value))
  return round(value, precision - 1 - magnitude)


def symlog(s: NumericInput) -> np.ndarray:
  """Symmetrical log transformation with optimized array handling."""
  # Convert to numpy array efficiently
  if isinstance(s, pl.Series):
    arr = s.to_numpy()
  elif isinstance(s, list):
    arr = np.asarray(s, dtype=np.float64)
  else:
    arr = np.asarray(s)

  return np.sign(arr) * np.log1p(np.abs(arr))


def normalize(s: NumericInput,
              min_val: float = 0,
              max_val: float = 1,
              scale: str = 'linear',
              standardize: bool = False) -> np.ndarray:
  """
  Normalize an array, list, or polars Series with improved efficiency.

  Parameters:
  - s: Input data (ndarray, list, or polars Series)
  - min_val: Minimum value after normalization (default is 0)
  - max_val: Maximum value after normalization (default is 1)
  - scale: Type of scaling - 'linear' or 'log' (default is 'linear')
  - standardize: If True, standardize data (mean=0, std=1)

  Returns:
  - Normalized data as ndarray
  """
  # Convert to numpy array efficiently
  if isinstance(s, pl.Series):
    arr = s.to_numpy()
  elif isinstance(s, list):
    arr = np.asarray(s, dtype=np.float64)
  else:
    arr = np.asarray(s)

  # Handle NaN and infinite values
  arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)

  # Apply log scale if specified
  if scale == 'log':
    arr = symlog(arr)

  # Standardize data (mean=0, std=1)
  if standardize:
    mean_val = np.mean(arr)
    std_val = np.std(arr)
    if std_val == 0:
      return arr - mean_val
    return (arr - mean_val) / std_val

  # Min-max normalization
  arr_min, arr_max = np.min(arr), np.max(arr)
  if arr_max == arr_min:
    return np.full_like(arr, min_val)

  return min_val + (arr - arr_min) * (max_val - min_val) / (arr_max - arr_min)


def get_numeric_columns(
    df: pl.DataFrame,
    exclude_columns: Optional[List[str]] = None) -> List[str]:
  """
  Filter out non-numeric columns and specified columns from the DataFrame.

  Parameters:
  - df: Polars DataFrame
  - exclude_columns: List of column names to exclude (default excludes 'ts')

  Returns:
  - List of numeric column names
  """
  if exclude_columns is None:
    exclude_columns = ['ts']

  return [
      col for col in df.columns
      if col not in exclude_columns and df[col].dtype.is_numeric()
  ]


# Legacy alias for backward compatibility
def numeric_columns(df: pl.DataFrame,
                    exclude_columns: List[str] = ['ts']) -> List[str]:
  """Legacy alias for get_numeric_columns"""
  return get_numeric_columns(df, exclude_columns)


# Statistical Functions
def correlation(x: NumericInput, y: NumericInput) -> float:
  """
  Pearson correlation coefficient with optimized computation and error handling.

  Parameters:
  - x, y: Input data arrays

  Returns:
  - Correlation coefficient (0.0 if calculation fails)
  """
  x_arr, y_arr = to_numpy(x), to_numpy(y)

  if len(x_arr) == 0 or len(y_arr) == 0 or len(x_arr) != len(y_arr):
    return 0.0

  # Remove invalid values
  x_clean, y_clean = ensure_valid_arrays(x_arr, y_arr)

  if len(x_clean) < 2:
    return 0.0

  # Use numpy's corrcoef for efficiency
  corr_matrix = np.corrcoef(x_clean, y_clean)
  result = corr_matrix[0, 1]

  return float(result) if np.isfinite(result) else 0.0


def percentile(data: NumericInput, p: float) -> float:
  """
  Calculate percentile using numpy for better performance.

  Parameters:
  - data: Input data
  - p: Percentile (0-100)

  Returns:
  - Percentile value
  """
  arr = to_numpy(data)
  if len(arr) == 0:
    return 0.0

  # Remove invalid values
  valid_data = arr[np.isfinite(arr)]
  if len(valid_data) == 0:
    return 0.0

  return float(np.percentile(valid_data, p))


def linear_regression(x: NumericInput, y: NumericInput) -> Tuple[float, float]:
  """
  Simple linear regression with improved numerical stability.

  Parameters:
  - x, y: Input data arrays

  Returns:
  - Tuple of (slope, intercept)
  """
  x_arr, y_arr = to_numpy(x), to_numpy(y)

  if len(x_arr) == 0 or len(y_arr) == 0 or len(x_arr) != len(y_arr):
    return 0.0, 0.0

  # Remove invalid values
  x_clean, y_clean = ensure_valid_arrays(x_arr, y_arr)

  if len(x_clean) < 2:
    return 0.0, float(np.mean(y_clean)) if len(y_clean) > 0 else 0.0

  # Use numpy's polyfit for better numerical stability
  coefficients = np.polyfit(x_clean, y_clean, 1)
  return float(coefficients[0]), float(coefficients[1])


def predict_next(data: NumericInput) -> float:
  """
  Predict next value using linear regression on indices.

  Parameters:
  - data: Input time series data

  Returns:
  - Predicted next value
  """
  arr = to_numpy(data)
  if len(arr) == 0:
    return 0.0

  x = np.arange(len(arr))
  slope, intercept = linear_regression(x, arr)
  return slope * len(arr) + intercept


def standardize_data(data: NumericInput) -> np.ndarray:
  """
  Standardize data (z-score normalization) returning numpy array.

  Parameters:
  - data: Input data

  Returns:
  - Standardized data as numpy array
  """
  arr = to_numpy(data)
  if len(arr) == 0:
    return np.array([])

  # Remove invalid values
  valid_data = arr[np.isfinite(arr)]
  if len(valid_data) == 0:
    return np.zeros_like(arr)

  mean_val = np.mean(valid_data)
  std_val = np.std(valid_data)

  if std_val == 0:
    return np.zeros_like(arr)

  # Standardize only valid data, keep invalid as NaN
  result = np.full_like(arr, np.nan)
  valid_mask = np.isfinite(arr)
  result[valid_mask] = (arr[valid_mask] - mean_val) / std_val

  return result


def moving_window(data: NumericInput, window_size: int) -> List[np.ndarray]:
  """
  Create moving windows using numpy for better performance.

  Parameters:
  - data: Input data
  - window_size: Size of each window

  Returns:
  - List of numpy arrays representing windows
  """
  arr = to_numpy(data)

  if len(arr) < window_size or window_size <= 0:
    return []

  # Use numpy's stride_tricks for efficient windowing
  try:
    from numpy.lib.stride_tricks import sliding_window_view
    windows = sliding_window_view(arr, window_size)
    return [window.copy() for window in windows]
  except (AttributeError, ImportError):
    # Fallback for older numpy versions
    return [arr[i:i + window_size] for i in range(len(arr) - window_size + 1)]


# Additional utility functions for better API
def rolling_mean(data: NumericInput, window_size: int) -> np.ndarray:
  """Calculate rolling mean efficiently."""
  arr = to_numpy(data)
  if len(arr) < window_size:
    return np.array([])

  # Use numpy's convolve for efficient rolling mean
  kernel = np.ones(window_size) / window_size
  return np.convolve(arr, kernel, mode='valid')


def rolling_std(data: NumericInput, window_size: int) -> np.ndarray:
  """Calculate rolling standard deviation efficiently."""
  arr = to_numpy(data)
  if len(arr) < window_size:
    return np.array([])

  # Efficient rolling std using pandas-style algorithm
  result = np.zeros(len(arr) - window_size + 1)
  for i in range(len(result)):
    window = arr[i:i + window_size]
    result[i] = np.std(window)

  return result
