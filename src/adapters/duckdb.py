from asyncio import get_event_loop
from concurrent.futures import Executor
from datetime import datetime, timezone
from os import environ as env
from typing import Optional, cast

from ..cache import get_or_set_cache
from ..utils import log_error, log_info, log_warn, Interval, TimeUnit, fmt_date, ago, now
from ..models.base import FieldType
from ..models.ingesters import UpdateIngester
from .sql import SqlAdapter
from .. import state

import duckdb  # happy mypy

UTC = timezone.utc

TYPES: dict[FieldType, str] = {
    "int8": "TINYINT",
    "uint8": "UTINYINT",
    "int16": "SMALLINT",
    "uint16": "USMALLINT",
    "int32": "INTEGER",
    "uint32": "UINTEGER",
    "int64": "BIGINT",
    "uint64": "UBIGINT",
    "float32": "REAL",
    "ufloat32": "REAL",
    "float64": "DOUBLE",
    "ufloat64": "DOUBLE",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "string": "VARCHAR",
    "binary": "BLOB",
    "varbinary": "BLOB",
}

INTERVALS: dict[str, str] = {
    "s1": "1 second",
    "s2": "2 second",
    "s5": "5 second",
    "s10": "10 second",
    "s15": "15 second",
    "s20": "20 second",
    "s30": "30 second",
    "m1": "1 minute",
    "m2": "2 minute",
    "m5": "5 minute",
    "m10": "10 minute",
    "m15": "15 minute",
    "m30": "30 minute",
    "h1": "1 hour",
    "h2": "2 hour",
    "h4": "4 hour",
    "h6": "6 hour",
    "h8": "8 hour",
    "h12": "12 hour",
    "D1": "1 day",
    "D2": "2 day",
    "D3": "3 day",
    "W1": "7 day",
    "M1": "1 month",
    "Y1": "1 year"
}

PRECISION: TimeUnit = "ms"


class DuckDB(SqlAdapter):
  """DuckDB adapter extending SqlAdapter."""

  TYPES = TYPES

  def __init__(self,
               host: str = "localhost",
               port: int = 0,
               db: str = ":memory:",
               user: str = "",
               password: str = ""):
    super().__init__(host, port, db, user, password)

  @property
  def timestamp_column_type(self) -> str:
    return "TIMESTAMP"

  @property
  def connection_string(self) -> str:
    """Return connection string for DuckDB"""
    return f"duckdb:///{self.db}"

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "DuckDB":
    # Use **kwargs approach for cleaner constructor call
    params = {
        'host': host or env.get("DUCKDB_HOST") or "localhost",
        'port': int(port or env.get("DUCKDB_PORT") or 0),
        'db': db or env.get("DUCKDB_DB") or ":memory:",  # Default to in-memory
        'user': user or env.get("DB_RW_USER") or "",
        'password': password or env.get("DB_RW_PASS") or ""
    }

    self = cls(**params)
    await self.ensure_connected()
    return self

  async def _connect(self):
    """DuckDB-specific connection."""
    try:
      if self.db == ":memory:":
        self.conn = duckdb.connect()
      else:
        self.conn = duckdb.connect(self.db)

      log_info(f"Connected to DuckDB database: {self.db}")
    except Exception:
      raise ValueError(f"Failed to connect to DuckDB database: {self.db}")

  async def ensure_connected(self):
    """DuckDB-specific connection setup."""
    if not self.conn:
      self.conn = await self._connect()
      # DuckDB connection serves as both connection and cursor
      self.cursor = self.conn

  async def _close_connection(self):
    """DuckDB-specific connection closing."""
    if self.conn:
      await self._execute_async_void(lambda: self.conn.close())
      self.conn = None
      self.cursor = None

  async def _execute_async_void(self, func):
    """Execute a function asynchronously using the thread pool."""
    await self.ensure_connected()
    loop = get_event_loop()
    return await loop.run_in_executor(cast(Executor, state.thread_pool), func)

  async def _execute(self, query: str, params: tuple = ()):
    """DuckDB-specific query execution."""
    result = await self._execute_async(query, params)
    return result

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """DuckDB-specific query execution and fetch."""
    return await self._execute_async(query, params)

  async def _executemany(self, query: str, params_list: list[tuple]):
    """DuckDB batch execution using transactions."""
    try:
      await self._execute_async("BEGIN TRANSACTION")
      for params in params_list:
        await self._execute_async(query, params)
      await self._execute_async("COMMIT")
    except Exception as e:
      await self._execute_async("ROLLBACK")
      raise e

  async def _execute_async(self, query: str, params=None):
    """Execute a query asynchronously using the thread pool."""
    await self.ensure_connected()
    loop = get_event_loop()

    def execute_sync():
      if params:
        return self.conn.execute(query, params).fetchall()
      else:
        return self.conn.execute(query).fetchall()

    return await loop.run_in_executor(cast(Executor, state.thread_pool),
                                      execute_sync)

  def _quote_identifier(self, identifier: str) -> str:
    """DuckDB uses double quotes for identifiers."""
    return f'"{identifier}"'

  def _build_aggregation_sql(
      self, table_name: str, columns: list[str], from_date: datetime,
      to_date: datetime, aggregation_interval: Interval) -> tuple[str, list]:
    """DuckDB-specific aggregation using time_bucket function."""
    agg_interval = INTERVALS[aggregation_interval]

    # DuckDB has excellent time-series support with time_bucket function
    select_cols = [
        f"last(\"{col}\") AS \"{col}\"" for col in columns if col != 'ts'
    ]
    select_cols.insert(0, f"time_bucket(INTERVAL '{agg_interval}', ts) AS ts")
    select_cols_str = ", ".join(select_cols)

    conditions = [
        f"ts >= '{fmt_date(from_date, keepTz=False)}'" if from_date else None,
        f"ts <= '{fmt_date(to_date, keepTz=False)}'" if to_date else None,
    ]
    where_clause = f"WHERE {' AND '.join(filter(None, conditions))}" if any(
        conditions) else ""

    query = f'SELECT {select_cols_str} FROM "{table_name}" {where_clause} GROUP BY time_bucket(INTERVAL \'{agg_interval}\', ts) ORDER BY ts DESC'

    return query, []

  async def fetch(self,
                  table: str,
                  from_date: Optional[datetime] = None,
                  to_date: Optional[datetime] = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = [],
                  use_first: bool = False) -> tuple[list[str], list[tuple]]:
    """DuckDB-specific fetch with time_bucket aggregation."""
    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    if not columns:
      columns = await self.get_cache_columns(table)
    else:
      if 'ts' not in columns:
        columns.insert(0, 'ts')

    if not columns:
      log_warn(f"No columns found for table {table}")
      return (columns, [])

    try:
      # Try advanced aggregation first
      query, params = self._build_aggregation_sql(table, columns, from_date,
                                                  to_date,
                                                  aggregation_interval)
      results = await self._execute_async(query)
      return (columns, results)
    except Exception:
      # Fallback to base class implementation if time_bucket is not available
      log_warn(
          "time_bucket function not available, falling back to simple query")
      return await super().fetch(table, from_date, to_date,
                                 aggregation_interval, columns)

  async def create_db(self, name: str, options={}, force=False):
    """DuckDB database creation (schema-based)."""
    try:
      await self._execute_async(f"CREATE SCHEMA IF NOT EXISTS {name}")
      log_info(f"Created schema {name} in DuckDB")
    except Exception as e:
      log_error(f"Failed to create schema {name}", e)

  async def use_db(self, db: str):
    """DuckDB database switching (file-based)."""
    if self.db != db:
      await self.close()
      self.db = str(db)
      await self.ensure_connected()

  async def get_columns(self, table: str) -> list[tuple]:
    """DuckDB-specific column information."""
    try:
      results = await self._execute_async(f'DESCRIBE "{table}"')
      return results
    except Exception as e:
      log_error(f"Failed to get columns from {table}", e)
      return []

  async def get_cache_columns(self, table: str) -> list[str]:
    """Get cached column names for DuckDB table."""
    column_descs = await get_or_set_cache(
        f"{table}:columns",
        callback=lambda: self.get_columns(table),
        expiry=300,
        pickled=True)
    return [col[0] for col in column_descs]

  async def _get_table_columns(self, table: str) -> list[str]:
    """DuckDB-specific table column listing."""
    try:
      result = await self._execute_async(f'DESCRIBE "{table}"')
      return [row[0] for row in result]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """DuckDB-specific table listing."""
    try:
      results = await self._execute_async("SHOW TABLES")
      return [table[0] for table in results]
    except Exception as e:
      log_error("Failed to list tables from DuckDB", e)
      return []

  async def upsert(self, ing: UpdateIngester, table: str = "", uid: str = ""):
    """DuckDB upsert using INSERT OR REPLACE."""
    await self.ensure_connected()
    table = table or ing.name
    uid = uid or ing.uid

    if not uid:
      raise ValueError("UID is required for upsert operations")

    # Get non-transient fields and their values
    fields = [field for field in ing.fields if not field.transient]
    field_names = [field.name for field in fields]
    field_values = [field.value for field in fields]

    # Ensure uid is in the values
    if 'uid' not in field_names:
      field_names.append('uid')
      field_values.append(uid)

    placeholders = ", ".join(["?" for _ in field_values])
    columns = ", ".join([f'"{name}"' for name in field_names])

    # Use INSERT OR REPLACE for upsert behavior
    sql = f'INSERT OR REPLACE INTO "{table}" ({columns}) VALUES ({placeholders})'

    try:
      await self._execute_async(sql, field_values)
      log_info(f"Upserted record with uid {uid} into {table}")
    except Exception as e:
      log_error(f"Failed to upsert into {table}", e)
      raise e

  async def fetch_by_id(self, table: str, uid: str):
    """DuckDB-specific fetch by ID."""
    await self.ensure_connected()

    try:
      sql = f'SELECT * FROM "{table}" WHERE uid = ? LIMIT 1'
      result = await self._execute_async(sql, [uid])

      if result:
        # Get column names from DuckDB description
        loop = get_event_loop()
        columns = await loop.run_in_executor(
            cast(Executor, state.thread_pool),
            lambda: [desc[0] for desc in self.conn.description])
        return dict(zip(columns, result[0]))
      return None

    except Exception as e:
      log_error(f"Failed to fetch record with uid {uid} from {table}", e)
      return None

  async def fetchall(self):
    """DuckDB-specific fetchall."""
    try:
      await self.ensure_connected()
      loop = get_event_loop()
      result = await loop.run_in_executor(cast(Executor, state.thread_pool),
                                          lambda: self.conn.fetchall())
      return result if result else []
    except Exception as e:
      log_error("Failed to fetch all results from DuckDB", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """DuckDB-specific batch fetch by UIDs."""
    try:
      if not uids:
        return []
      placeholders = ",".join(["?" for _ in uids])
      sql = f'SELECT * FROM "{table}" WHERE uid IN ({placeholders})'
      results = await self._execute_async(sql, uids)
      return results
    except Exception as e:
      log_error(f"Failed to fetch batch records by IDs from {table}: {e}")
      return []
