import math
import numpy as np
import polars as pl


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
    return 0
  value = float(value)
  precision = int(precision)
  return round(value,
               -int(math.floor(math.log10(abs(value)))) + (precision - 1))


def symlog(s):
  """Symmetrical log transformation"""
  if hasattr(s, 'to_numpy'):
    s = s.to_numpy()
  return np.sign(s) * np.log1p(np.abs(s))


def normalize(s, min_val=0, max_val=1, scale='linear', standardize=False):
  """
  Normalize an array, list, or polars Series.

  Parameters:
  - s: Input data (ndarray, list, or polars Series).
  - min_val: Minimum value after normalization (default is 0).
  - max_val: Maximum value after normalization (default is 1).
  - scale: Type of scaling - 'linear' or 'log' (default is 'linear').
  - standardize: If True, the data will be standardized (mean=0, std=1).

  Returns:
  - Normalized data as ndarray.
  """
  # Convert input to numpy array if needed (from polars or polars Series)
  if hasattr(s, 'to_numpy'):
    s = s.to_numpy()
  else:
    s = np.array(s)

  # Handle NaN and infinite values
  s = np.nan_to_num(s, nan=0.0, posinf=0.0, neginf=0.0)

  # Apply log scale if specified
  if scale == 'log':
    s = symlog(s)  # Apply symmetrical log

  # Standardize data (mean=0, std=1)
  if standardize:
    std_dev = np.std(s)
    if std_dev == 0:
      return s - np.mean(s)  # Avoid division by zero
    return (s - np.mean(s)) / std_dev

  # Min-max normalization
  s_min, s_max = np.min(s), np.max(s)
  if s_max == s_min:
    return np.full_like(s, min_val)  # Avoid division by zero
  return min_val + (s - s_min) * (max_val - min_val) / (s_max - s_min)


def numeric_columns(df: pl.DataFrame,
                    exclude_columns: list[str] = ['ts']) -> list[str]:
  """Filter out non-numeric columns and specified columns from the DataFrame"""
  numeric_columns = []
  for col in df.columns:
    if col not in exclude_columns and df[col].dtype.is_numeric(
    ):  # native + polars
      numeric_columns.append(col)
  return numeric_columns
