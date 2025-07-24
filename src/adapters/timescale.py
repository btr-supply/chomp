from datetime import datetime, timezone
from typing import Optional, Any
from os import environ as env

from .sql import SqlAdapter
from ..models.base import FieldType
from ..models.ingesters import Ingester
from ..utils import log_error, log_info, Interval, TimeUnit

import asyncpg  # happy mypy

UTC = timezone.utc

# PostgreSQL field type mapping
TYPES: dict[FieldType, str] = {
    "int8": "SMALLINT",
    "uint8": "SMALLINT",  # PostgreSQL doesn't have unsigned types
    "int16": "SMALLINT",
    "uint16": "INTEGER",
    "int32": "INTEGER",
    "uint32": "BIGINT",
    "int64": "BIGINT",
    "uint64": "BIGINT",
    "float32": "REAL",
    "ufloat32": "REAL",
    "float64": "DOUBLE PRECISION",
    "ufloat64": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP WITH TIME ZONE",
    "string": "TEXT",
    "binary": "BYTEA",
    "varbinary": "BYTEA",
}

# Time precision for chunk intervals
partitioning_by_precision = {
    "ms": "1 hour",  # For millisecond precision
    "us": "1 hour",  # For microsecond precision
    "ns": "30 minutes",  # For nanosecond precision
    "s": "1 day",  # For second precision
}

PRECISION: TimeUnit = "ms"


class TimescaleDb(SqlAdapter):
  """TimescaleDB adapter extending SqlAdapter."""

  TYPES = TYPES

  def __init__(self,
               host: str = "localhost",
               port: int = 5432,
               db: str = "chomp",
               user: str = "postgres",
               password: str = "password"):
    super().__init__(host, port, db, user, password)

  @property
  def timestamp_column_type(self) -> str:
    return "timestamptz"

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "TimescaleDb":
    self = cls(host=host or env.get("TIMESCALE_HOST") or "localhost",
               port=int(port or env.get("TIMESCALE_PORT") or 5432),
               db=db or env.get("TIMESCALE_DB") or "postgres",
               user=user or env.get("DB_RW_USER") or "postgres",
               password=password or env.get("DB_RW_PASS") or "password")
    await self.ensure_connected()
    return self

  async def _connect(self):
    """TimescaleDB-specific connection using asyncpg."""
    conn = await asyncpg.connect(host=self.host,
                                 port=self.port,
                                 user=self.user,
                                 password=self.password,
                                 database=self.db)
    log_info(f"Connected to TimescaleDB at {self.host}:{self.port}")
    return conn

  async def _init_db(self):
    """Initialize TimescaleDB database and create default tables."""
    try:
      self.conn = await asyncpg.connect(host=self.host,
                                        port=self.port,
                                        user=self.user,
                                        password=self.password,
                                        database=self.db)
      log_info(f"Connected to TimescaleDB at {self.host}:{self.port}")
    except Exception as e:
      log_error("Failed to initialize TimescaleDB database", e)
      raise e

  async def _create_table(self, name, definition):
    await self.conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({definition})"
                            )
    log_info(f"Created table {name}")

  async def _close_pool(self):
    """TimescaleDB-specific pool closing."""
    if self.conn:
      await self.conn.close()
      self.conn = None

  async def _execute(self, query: str, params: tuple = ()):
    """Execute TimescaleDB query."""
    await self.conn.execute(query, *params)

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """Execute TimescaleDB query and fetch results."""
    result = await self.conn.fetch(query, *params)
    return [tuple(row) for row in result]

  async def _executemany(self, query: str, params_list: list[tuple]):
    """Execute many TimescaleDB queries."""
    await self.conn.executemany(query, params_list)

  def _build_placeholders(self, count: int) -> str:
    """PostgreSQL uses $1, $2, ... for parameters."""
    return ", ".join([f"${i+1}" for i in range(count)])

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """TimescaleDB-specific database creation."""
    try:
      admin_conn = await asyncpg.connect(host=self.host,
                                         port=self.port,
                                         user=self.user,
                                         password=self.password,
                                         database="postgres")
      if force:
        await admin_conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
      await admin_conn.execute(f'CREATE DATABASE "{name}"')
      await admin_conn.close()
      log_info(f"Created database {name}")
    except Exception as e:
      log_error(f"Failed to create database {name}", e)
      raise e

  async def use_db(self, db: str):
    """TimescaleDB-specific database switching."""
    # Close existing connection and create new one for the target database
    if self.conn:
      await self.conn.close()

    self.db = db
    self.conn = await asyncpg.connect(host=self.host,
                                      port=self.port,
                                      database=db,
                                      user=self.user,
                                      password=self.password)

  def _build_create_table_sql(self, ing: Ingester, table_name: str) -> str:
    """TimescaleDB-specific CREATE TABLE with hypertable setup."""
    persistent_fields = [field for field in ing.fields if not field.transient]
    fields = []

    # Add data fields (including ts field)
    for field in persistent_fields:
      field_type = TYPES.get(field.type, "TEXT")
      fields.append(f'"{field.name}" {field_type}')

    if not fields:
      # Fallback for ingesters with no persistent fields
      fields = ['"value" TEXT']

    fields_sql = ",\n      ".join(fields)

    # For TimeSeriesIngester, create hypertable with ts as time dimension
    if getattr(ing, 'ts', None) and ing.resource_type == 'timeseries':
      chunk_interval = partitioning_by_precision.get(PRECISION, "1 day")
      return f'''
      CREATE TABLE IF NOT EXISTS "{table_name}" (
        {fields_sql}
      );

      SELECT create_hypertable('"{table_name}"', 'ts',
                               chunk_time_interval => INTERVAL '{chunk_interval}',
                               if_not_exists => TRUE);
      '''
    else:
      # Regular table for non-timeseries ingesters
      return f'''
      CREATE TABLE IF NOT EXISTS "{table_name}" (
        {fields_sql}
      );
      '''

  async def create_table(self,
                         ing: Ingester,
                         name: str = "",
                         force: bool = False):
    """TimescaleDB-specific table creation with hypertable."""
    await self.ensure_connected()
    table = name or ing.name

    try:
      if force:
        await self._execute(f'DROP TABLE IF EXISTS "{table}"')

      # Build and execute table creation SQL (includes hypertable conversion for time series)
      create_sql = self._build_create_table_sql(ing, table)
      await self._execute(create_sql)

      # Log appropriate message based on table type
      if getattr(ing, 'ts', None) and ing.resource_type == 'timeseries':
        log_info(f"Created TimescaleDB hypertable {self.db}.{table}")
      else:
        log_info(f"Created TimescaleDB table {self.db}.{table}")

    except Exception as e:
      log_error(f"Failed to create table {self.db}.{table}", e)
      raise e

  def _build_aggregation_sql(
      self, table_name: str, columns: list[str], from_date: datetime,
      to_date: datetime,
      aggregation_interval: Interval) -> tuple[str, list[Any]]:
    """TimescaleDB-specific aggregation using time_bucket."""

    # Convert interval format
    interval_map = {
        "s1": "1 second",
        "s2": "2 seconds",
        "s5": "5 seconds",
        "s10": "10 seconds",
        "s15": "15 seconds",
        "s20": "20 seconds",
        "s30": "30 seconds",
        "m1": "1 minute",
        "m2": "2 minutes",
        "m5": "5 minutes",
        "m10": "10 minutes",
        "m15": "15 minutes",
        "m30": "30 minutes",
        "h1": "1 hour",
        "h2": "2 hours",
        "h4": "4 hours",
        "h6": "6 hours",
        "h8": "8 hours",
        "h12": "12 hours",
        "D1": "1 day",
        "D2": "2 days",
        "D3": "3 days",
        "W1": "1 week",
        "M1": "1 month",
        "Y1": "1 year"
    }

    bucket_interval = interval_map.get(aggregation_interval, "5 minutes")

    # Build time bucket aggregation
    select_cols = [f"time_bucket('{bucket_interval}', ts) as ts"]
    select_cols.extend([
        f"LAST({self._quote_identifier(col)}, ts) as {col}" for col in columns
    ])
    select_clause = ", ".join(select_cols)

    query = f"""
    SELECT {select_clause}
    FROM {self._quote_identifier(table_name)}
    WHERE ts >= $1 AND ts <= $2
    GROUP BY time_bucket('{bucket_interval}', ts)
    ORDER BY ts DESC
    """

    return query, [from_date, to_date]

  async def _get_table_columns(self, table: str) -> list[str]:
    """TimescaleDB-specific column information query."""
    try:
      result = await self._fetch(
          """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = 'public'
        ORDER BY ordinal_position
      """, (table, ))
      # Return all columns including ts since it's now a proper field
      return [row[0] for row in result]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """TimescaleDB-specific table listing."""
    await self.ensure_connected()
    try:
      result = await self._fetch(
          "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
      return [row[0] for row in result]
    except Exception as e:
      log_error(f"Failed to list tables from {self.db}", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs in a single TimescaleDB query for efficiency"""
    await self.ensure_connected()
    try:
      if not uids:
        return []

      # Build parameterized query for TimescaleDB (PostgreSQL syntax)
      placeholders = ",".join([f"${i+1}" for i in range(len(uids))])
      query = f"SELECT * FROM {table} WHERE uid IN ({placeholders}) ORDER BY updated_at DESC"

      result = await self._fetch(query, tuple(uids))
      return result if result else []
    except Exception as e:
      log_error(
          f"Failed to fetch batch records by IDs from TimescaleDB {table}: {e}"
      )
      return []
