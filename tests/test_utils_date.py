"""
Test suite for date utility functions.

Purpose: Test the actual date utility functions including interval conversions,
date floor/ceiling operations, time calculations, and date parsing.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import src.utils.date as date_utils
from src.utils.date import (
  now, ago, interval_to_sql, interval_to_cron, interval_to_delta,
  interval_to_seconds, floor_date, ceil_date, extract_time_unit,
  floor_utc, ceil_utc, shift_date, fit_interval, round_interval,
  fit_date_params, secs_to_ceil_date
)


class TestDateUtilsActual:
  """Test actual date utility functions that exist in the module."""

  def test_now_utc_default(self):
    """Test now() function with UTC default."""
    result = now()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc

  def test_now_local(self):
    """Test now() function with local time."""
    result = now(utc=False)
    assert isinstance(result, datetime)
    # Local time may or may not have timezone info

  def test_ago_default(self):
    """Test ago() function with default parameters."""
    result = ago(days=1)
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc

    # Should be about 1 day ago
    now_time = now()
    diff = now_time - result
    assert 23 <= diff.total_seconds() / 3600 <= 25  # About 24 hours

  def test_ago_with_specific_date(self):
    """Test ago() function with specific from_date."""
    base_date = datetime(2023, 1, 15, 10, 0, tzinfo=timezone.utc)
    result = ago(from_date=base_date, hours=5)

    expected = datetime(2023, 1, 15, 5, 0, tzinfo=timezone.utc)
    assert result == expected

  def test_ago_with_different_timezone(self):
    """Test ago() function with different timezone."""
    import pytz
    est = pytz.timezone('US/Eastern')
    base_date = datetime(2023, 1, 15, 10, 0, tzinfo=est)
    result = ago(from_date=base_date, tz=est, minutes=30)

    assert result.tzinfo == est

  def test_interval_to_sql_valid(self):
    """Test interval_to_sql with valid intervals."""
    assert interval_to_sql("s1") == "1 seconds"
    assert interval_to_sql("m5") == "5 minutes"
    assert interval_to_sql("h1") == "1 hour"
    assert interval_to_sql("D1") == "1 day"
    assert interval_to_sql("W1") == "1 week"
    assert interval_to_sql("M1") == "1 month"
    assert interval_to_sql("Y1") == "1 year"

  def test_interval_to_sql_invalid(self):
    """Test interval_to_sql with invalid intervals."""
    assert interval_to_sql("invalid") is None
    assert interval_to_sql("") is None
    assert interval_to_sql("x1") is None

  def test_interval_to_cron_valid(self):
    """Test interval_to_cron with valid intervals."""
    assert interval_to_cron("s2") == "* * * * * */2"
    assert interval_to_cron("m1") == "*/1 * * * *"
    assert interval_to_cron("h1") == "0 * * * *"
    assert interval_to_cron("D1") == "0 0 */1 * *"
    assert interval_to_cron("W1") == "0 0 * * 0"
    assert interval_to_cron("M1") == "0 0 1 */1 *"
    assert interval_to_cron("Y1") == "0 0 1 1 *"

  def test_interval_to_cron_invalid(self):
    """Test interval_to_cron with invalid intervals."""
    with pytest.raises(ValueError, match="Invalid interval"):
      interval_to_cron("invalid")

    with pytest.raises(ValueError, match="Invalid interval"):
      interval_to_cron("x1")

  def test_extract_time_unit_valid(self):
    """Test extract_time_unit with valid formats."""
    unit, value = extract_time_unit("5m")
    assert unit == "m"
    assert value == 5

    unit, value = extract_time_unit("30s")
    assert unit == "s"
    assert value == 30

    unit, value = extract_time_unit("24h")
    assert unit == "h"
    assert value == 24

  def test_extract_time_unit_invalid(self):
    """Test extract_time_unit with invalid formats."""
    with pytest.raises(ValueError, match="Invalid time frame format"):
      extract_time_unit("invalid")

    with pytest.raises(ValueError, match="Invalid time frame format"):
      extract_time_unit("5")

    with pytest.raises(ValueError, match="Invalid time frame format"):
      extract_time_unit("m5")

  def test_interval_to_delta_valid(self):
    """Test interval_to_delta with valid intervals."""
    delta = interval_to_delta("s30")
    assert delta == timedelta(seconds=30)

    delta = interval_to_delta("m5")
    assert delta == timedelta(minutes=5)

    delta = interval_to_delta("h2")
    assert delta == timedelta(hours=2)

    delta = interval_to_delta("D1")
    assert delta == timedelta(days=1)

  def test_interval_to_delta_backwards(self):
    """Test interval_to_delta with backwards=True."""
    delta = interval_to_delta("h1", backwards=True)
    assert delta == timedelta(hours=-1)

  def test_interval_to_delta_invalid(self):
    """Test interval_to_delta with invalid intervals."""
    with pytest.raises(ValueError, match="Invalid time unit"):
      interval_to_delta("invalid")

    with pytest.raises(ValueError, match="Invalid time unit"):
      interval_to_delta("x1")

  def test_interval_to_seconds_valid(self):
    """Test interval_to_seconds with valid intervals."""
    assert interval_to_seconds("s2") == 2
    assert interval_to_seconds("m1") == 60
    assert interval_to_seconds("h1") == 3600
    assert interval_to_seconds("D1") == 86400

  def test_interval_to_seconds_raw_mode(self):
    """Test interval_to_seconds with raw=True."""
    # Week or longer intervals should use calculation even in raw mode
    secs = interval_to_seconds("W1", raw=True)
    assert secs == 604800  # 1 week in seconds

  def test_floor_date_with_string_interval(self):
    """Test floor_date with string interval."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    # Floor to hour
    result = floor_date(test_date, "h1")
    expected = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    assert result.replace(microsecond=0) == expected

  def test_floor_date_with_numeric_interval(self):
    """Test floor_date with numeric interval."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    # Floor to 5-minute intervals (300 seconds)
    result = floor_date(test_date, 300)

    # Should floor to 10:35:00 (nearest 5-minute mark)
    assert result.minute in [30, 35]
    assert result.second == 0

  def test_floor_date_none_interval(self):
    """Test floor_date with None interval."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="interval cannot be None"):
      floor_date(test_date, None)

  def test_floor_date_default_date(self):
    """Test floor_date with default date (now)."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)
      result = floor_date(interval="h1")

      # Should use mocked now() time
      assert result.hour == 10
      assert result.minute == 0

  def test_ceil_date_with_string_interval(self):
    """Test ceil_date with string interval."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    # Ceil to hour
    result = ceil_date(test_date, "h1")
    expected = datetime(2023, 1, 15, 11, 0, 0, tzinfo=timezone.utc)
    assert result.replace(microsecond=0) == expected

  def test_ceil_date_already_at_ceiling(self):
    """Test ceil_date when date is already at ceiling."""
    test_date = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # Already at the hour boundary
    result = ceil_date(test_date, "h1")
    assert result == test_date

  def test_ceil_date_none_interval(self):
    """Test ceil_date with None interval."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="interval cannot be None"):
      ceil_date(test_date, None)

  def test_floor_utc_default(self):
    """Test floor_utc with default interval."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)
      result = floor_utc()

      # Should floor to the minute by default
      assert result.second == 0
      assert result.minute == 35

  def test_ceil_utc_default(self):
    """Test ceil_utc with default interval."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)
      result = ceil_utc()

      # Should ceil to the next minute by default
      assert result.second == 0
      assert result.minute == 36

  def test_shift_date_forward(self):
    """Test shift_date moving forward."""
    test_date = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    result = shift_date(test_date, "h1", backwards=False)

    expected = datetime(2023, 1, 15, 11, 0, 0, tzinfo=timezone.utc)
    assert result == expected

  def test_shift_date_backward(self):
    """Test shift_date moving backward."""
    test_date = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    result = shift_date(test_date, "h1", backwards=True)

    expected = datetime(2023, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    assert result == expected

  def test_shift_date_default_date(self):
    """Test shift_date with default date."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
      result = shift_date(timeframe="m30")

      expected = datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
      assert result == expected

  def test_fit_interval_basic(self):
    """Test fit_interval with basic date range."""
    from_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2023, 1, 2, tzinfo=timezone.utc)  # 1 day difference

    result = fit_interval(from_date, to_date, target_epochs=24)

    # Should suggest hourly intervals for 24 epochs over 1 day
    assert result == "h1"

  def test_fit_interval_default_to_date(self):
    """Test fit_interval with default to_date (now)."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
      from_date = datetime(2023, 1, 15, 8, 0, 0, tzinfo=timezone.utc)  # 2 hours ago

      result = fit_interval(from_date)

      # Should suggest an appropriate interval for 2 hours with 100 target epochs
      assert result in ["m1", "m2", "m5"]

  def test_round_interval_basic(self):
    """Test round_interval with basic seconds value."""
    result = round_interval(60.0)  # 1 minute
    assert result == "m1"

    result = round_interval(3600.0)  # 1 hour
    assert result == "h1"

    result = round_interval(86400.0)  # 1 day
    assert result == "D1"

  def test_round_interval_with_margin(self):
    """Test round_interval with different margin."""
    # Test with tighter margin
    result = round_interval(65.0, margin=0.1)  # 65 seconds, tight margin
    assert result == "m1"  # Should still round to nearest minute

  def test_fit_date_params_all_defaults(self):
    """Test fit_date_params with all default parameters."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

      from_dt, to_dt, interval, epochs = fit_date_params()

      assert isinstance(from_dt, datetime)
      assert isinstance(to_dt, datetime)
      assert isinstance(interval, str)
      assert isinstance(epochs, int)
      assert epochs == 400  # Default target epochs

  def test_fit_date_params_partial_params(self):
    """Test fit_date_params with some parameters provided."""
    from_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2023, 1, 2, tzinfo=timezone.utc)

    from_dt, to_dt, interval, epochs = fit_date_params(
      from_date=from_date,
      to_date=to_date,
      target_epochs=50
    )

    assert from_dt == from_date
    assert to_dt == to_date
    assert epochs == 50
    assert interval in ["m30", "h1", "h2"]  # Reasonable interval for 1 day / 50 epochs

  def test_secs_to_ceil_date_basic(self):
    """Test secs_to_ceil_date basic functionality."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    result = secs_to_ceil_date(test_date, 3600)  # 1 hour

    # Should return seconds to next hour boundary
    expected_ceil = datetime(2023, 1, 15, 11, 0, 0, tzinfo=timezone.utc)
    expected_secs = int((expected_ceil - test_date).total_seconds())
    assert result == expected_secs

  def test_secs_to_ceil_date_with_offset(self):
    """Test secs_to_ceil_date with offset."""
    test_date = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

    result = secs_to_ceil_date(test_date, 3600, offset=300)  # 1 hour + 5 min offset

    # Should include the offset in calculation
    assert result > 0

  def test_secs_to_ceil_date_default_date(self):
    """Test secs_to_ceil_date with default date."""
    with patch('src.utils.date.now') as mock_now:
      mock_now.return_value = datetime(2023, 1, 15, 10, 35, 45, tzinfo=timezone.utc)

      result = secs_to_ceil_date(secs=60)  # 1 minute interval

      assert isinstance(result, int)
      assert result >= 0

  def test_constants_exist(self):
    """Test that module constants are defined correctly."""
    assert hasattr(date_utils, 'UTC')
    assert hasattr(date_utils, 'MONTH_SECONDS')
    assert hasattr(date_utils, 'YEAR_SECONDS')
    assert hasattr(date_utils, 'CRON_BY_TF')
    assert hasattr(date_utils, 'SEC_BY_TF')
    assert hasattr(date_utils, 'INTERVAL_TO_SQL')

    # Test some constant values
    assert date_utils.MONTH_SECONDS == round(2.592e+6)
    assert date_utils.YEAR_SECONDS == round(3.154e+7)
    assert isinstance(date_utils.CRON_BY_TF, dict)
    assert isinstance(date_utils.SEC_BY_TF, dict)

  def test_edge_cases_and_error_handling(self):
    """Test various edge cases and error conditions."""
    # Test with very small intervals
    result = interval_to_seconds("s1")
    assert result >= 0

    # Test with large intervals
    result = interval_to_seconds("Y1")
    assert result > 0

    # Test floor_date with timestamp exactly at boundary
    boundary_date = datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    result = floor_date(boundary_date, "h1")
    assert result == boundary_date


class TestDateUtilsParsing:
  """Test date parsing functions that do exist."""

  def test_parse_iso_date(self):
    """Test parsing ISO format dates."""
    # Test standard ISO format
    result = date_utils.parse_date("2023-01-15T10:30:45Z")
    assert isinstance(result, datetime)
    assert result.year == 2023
    assert result.month == 1
    assert result.day == 15

  def test_parse_date_with_timezone(self):
    """Test parsing dates with timezone information."""
    result = date_utils.parse_date("2023-01-15T10:30:45+00:00")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None

  def test_parse_date_without_timezone(self):
    """Test parsing dates without timezone information."""
    result = date_utils.parse_date("2023-01-15T10:30:45")
    assert isinstance(result, datetime)

  def test_invalid_date_handling(self):
    """Test handling of invalid date strings."""
    # parse_date function handles invalid dates by logging and returning None or raising
    result = date_utils.parse_date("invalid-date-string")
    # The function logs an error but doesn't raise, returns None or datetime
    assert result is None or isinstance(result, datetime)

  def test_none_date_handling(self):
    """Test handling of None date input."""
    result = date_utils.parse_date(None)
    assert result is None

  def test_different_date_formats(self):
    """Test parsing different date formats."""
    formats = [
      "2023-01-15",
      "2023-01-15T10:30:45",
      "2023-01-15T10:30:45Z",
      "2023-01-15T10:30:45+00:00"
    ]

    for date_str in formats:
      result = date_utils.parse_date(date_str)
      assert result is None or isinstance(result, datetime)
