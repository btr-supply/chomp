from asyncio import sleep
from datetime import datetime, timezone
from os import environ as env
from typing import Optional

from ..models.base import FieldType
from ..models.ingesters import Ingester, UpdateIngester
from ..cache import get_or_set_cache
from ..utils import log_error, log_info, log_warn, Interval, TimeUnit, fmt_date, ago, now
from .sql import SqlAdapter
from .. import state

from asynch.connection import Connection  # happy mypy

UTC = timezone.utc

TYPES: dict[FieldType, str] = {
    "int8": "Int8",
    "uint8": "UInt8",
    "int16": "Int16",
    "uint16": "UInt16",
    "int32": "Int32",
    "uint32": "UInt32",
    "int64": "Int64",
    "uint64": "UInt64",
    "float32": "Float32",
    "ufloat32": "Float32",
    "float64": "Float64",
    "ufloat64": "Float64",
    "bool": "UInt8",
    "timestamp": "DateTime",
    "string": "String",
    "binary": "String",
    "varbinary": "String",
}

INTERVALS: dict[str, str] = {
    "s1": "1",
    "s2": "2",
    "s5": "5",
    "s10": "10",
    "s15": "15",
    "s20": "20",
    "s30": "30",
    "m1": "60",
    "m2": "120",
    "m5": "300",
    "m10": "600",
    "m15": "900",
    "m30": "1800",
    "h1": "3600",
    "h2": "7200",
    "h4": "14400",
    "h6": "21600",
    "h8": "28800",
    "h12": "43200",
    "D1": "86400",
    "D2": "172800",
    "D3": "259200",
    "W1": "604800",
    "M1": "2592000",
    "Y1": "31536000"
}

PRECISION: TimeUnit = "ms"


class ClickHouse(SqlAdapter):
  """ClickHouse adapter extending SqlAdapter."""

  TYPES = TYPES

  def __init__(self,
               host: str = "localhost",
               port: int = 9000,
               db: str = "default",
               user: str = "default",
               password: str = ""):
    super().__init__(host, port, db, user, password)
    self.executor = state.thread_pool

  @property
  def timestamp_column_type(self) -> str:
    return "DateTime"

  @property
  def connection_string(self) -> str:
    """Return connection string for ClickHouse"""
    return f"clickhouse://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "ClickHouse":
    self = cls(host=host or env.get("CLICKHOUSE_HOST") or "localhost",
               port=int(port or env.get("CLICKHOUSE_PORT") or 9000),
               db=db or env.get("CLICKHOUSE_DB") or "default",
               user=user or env.get("DB_RW_USER") or "default",
               password=password or env.get("DB_RW_PASS") or "")
    await self.ensure_connected()
    return self

  async def _connect(self):
    """ClickHouse-specific connection using asynch."""
    try:
      conn = Connection(host=self.host,
                        port=self.port,
                        database=self.db,
                        user=self.user,
                        password=self.password)
      await conn.connect()
      log_info(f"Connected to ClickHouse at {self.host}:{self.port}")
      return conn
    except Exception as e:
      log_error(f"Failed to connect to ClickHouse: {e}")
      raise e

  async def ensure_connected(self):
    """ClickHouse-specific connection with database creation."""
    if not self.conn:
      try:
        self.conn = Connection(host=self.host,
                               port=self.port,
                               database=self.db,
                               user=self.user,
                               password=self.password)
        await self.conn.connect()
        log_info(
            f"Connected to ClickHouse on {self.host}:{self.port}/{self.db} as {self.user}"
        )
      except Exception as e:
        e_str = str(e).lower()
        if "database" in e_str and "doesn't exist" in e_str:
          log_warn(
              f"Database '{self.db}' does not exist on {self.host}:{self.port}, creating it now..."
          )
          temp_conn = Connection(host=self.host,
                                 port=self.port,
                                 user=self.user,
                                 password=self.password)
          await temp_conn.connect()
          temp_cursor = await temp_conn.cursor()
          await temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
          await temp_cursor.close()
          await temp_conn.close()

          # Now connect to the created database
          self.conn = Connection(host=self.host,
                                 port=self.port,
                                 database=self.db,
                                 user=self.user,
                                 password=self.password)
          await self.conn.connect()
          log_info(
              f"Connected to ClickHouse on {self.host}:{self.port}/{self.db} as {self.user}"
          )
        else:
          raise ValueError(
              f"Failed to connect to ClickHouse on {self.user}@{self.host}:{self.port}/{self.db}"
          )

    if not self.cursor:
      self.cursor = await self.conn.cursor()
    if not self.cursor:
      raise ValueError(
          f"Failed to create cursor for ClickHouse on {self.user}@{self.host}:{self.port}/{self.db}"
      )

  async def _execute(self, query: str, params: tuple = ()):
    """ClickHouse-specific query execution."""
    await self.ensure_connected()
    # ClickHouse uses %s placeholders instead of ?
    if params:
      formatted_query = query.replace("?", "%s")
      await self.cursor.execute(formatted_query, list(params))
    else:
      await self.cursor.execute(query)
    return self.cursor

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """ClickHouse-specific query execution and fetch."""
    await self._execute(query, params)
    result = await self.cursor.fetchall()
    return result

  async def _executemany(self, query: str, params_list: list[tuple]):
    """ClickHouse batch execution."""
    await self.ensure_connected()
    formatted_query = query.replace("?", "%s")
    await self.cursor.executemany(formatted_query, params_list)

  def _quote_identifier(self, identifier: str) -> str:
    """ClickHouse uses backticks for identifiers."""
    return f"`{identifier}`"

  def _build_placeholders(self, count: int) -> str:
    """ClickHouse uses %s placeholders."""
    return ", ".join(["%s" for _ in range(count)])

  def _build_create_table_sql(self, ing: Ingester, table_name: str) -> str:
    """ClickHouse-specific CREATE TABLE with ENGINE specification."""
    persistent_fields = [field for field in ing.fields if not field.transient]
    fields = []

    # Add data fields (TimeSeriesIngester and UpdateIngester already include their standard fields)
    for field in persistent_fields:
      field_type = TYPES.get(field.type, "String")
      fields.append(f"`{field.name}` {field_type}")

    if not fields:
      # Fallback for ingesters with no persistent fields
      fields = ["`value` String"]

    fields_sql = ",\n      ".join(fields)

    # ClickHouse requires an engine specification
    if ing.resource_type == 'update':
      # UpdateIngester - use MergeTree with ORDER BY uid for upserts
      return f"""
      CREATE TABLE IF NOT EXISTS {self.db}.`{table_name}` (
        {fields_sql}
      ) ENGINE = MergeTree() ORDER BY uid
      """
    else:
      # TimeSeriesIngester - use MergeTree with ORDER BY ts for time series
      return f"""
      CREATE TABLE IF NOT EXISTS {self.db}.`{table_name}` (
        {fields_sql}
      ) ENGINE = MergeTree() ORDER BY ts
      """

  def _build_aggregation_sql(
      self, table_name: str, columns: list[str], from_date: datetime,
      to_date: datetime, aggregation_interval: Interval) -> tuple[str, list]:
    """ClickHouse-specific aggregation using toStartOfInterval."""
    agg_seconds = INTERVALS[aggregation_interval]

    # ClickHouse time-based aggregation using toStartOfInterval
    select_cols = [
        f"argMax(`{col}`, ts) AS `{col}`" for col in columns if col != 'ts'
    ]
    select_cols.insert(
        0, f"toStartOfInterval(ts, INTERVAL {agg_seconds} second) AS ts")
    select_cols_str = ", ".join(select_cols)

    conditions = [
        f"ts >= '{fmt_date(from_date, keepTz=False)}'" if from_date else None,
        f"ts <= '{fmt_date(to_date, keepTz=False)}'" if to_date else None,
    ]
    where_clause = f"WHERE {' AND '.join(filter(None, conditions))}" if any(
        conditions) else ""

    query = f"SELECT {select_cols_str} FROM {self.db}.`{table_name}` {where_clause} GROUP BY toStartOfInterval(ts, INTERVAL {agg_seconds} second) ORDER BY ts DESC"

    return query, []

  async def create_db(self, name: str, options={}, force=False):
    """ClickHouse-specific database creation with retries."""
    base = "CREATE DATABASE IF NOT EXISTS" if not force else "CREATE DATABASE"
    max_retries = 10

    for i in range(max_retries):
      try:
        await self.cursor.execute(f"{base} {name}")
        break
      except Exception as e:
        log_warn(
            f"Retrying to create database {name} in {i+1}/{max_retries} ({e})..."
        )
        await sleep(1)

    if i < max_retries - 1:
      log_info(f"Created database {name}")
      # Readiness check
      for j in range(max_retries):
        try:
          await self.cursor.execute(f"USE {name}")
          log_info(f"Database {name} is now ready.")
          return
        except Exception as e:
          log_warn(
              f"Retrying to use database {name} in {j+1}/{max_retries} ({e})..."
          )
          await sleep(1)

    raise ValueError(f"Database {name} readiness check failed.")

  async def use_db(self, db: str):
    """ClickHouse-specific database switching."""
    if not self.conn:
      await self.connect(db=db)
    else:
      await self.cursor.execute(f"USE {db}")

  async def get_columns(self, table: str) -> list[tuple[str, str, str]]:
    """ClickHouse-specific column information."""
    try:
      await self.cursor.execute(f"DESCRIBE TABLE {self.db}.`{table}`")
      return await self.cursor.fetchall()
    except Exception as e:
      log_error(f"Failed to get columns from {self.db}.{table}", e)
      return []

  async def get_cache_columns(self, table: str) -> list[str]:
    """Get cached column names for ClickHouse table."""
    column_descs = await get_or_set_cache(
        f"{table}:columns",
        callback=lambda: self.get_columns(table),
        expiry=300,
        pickled=True)
    return [col[0] for col in column_descs]

  async def _get_table_columns(self, table: str) -> list[str]:
    """ClickHouse-specific table column listing."""
    try:
      result = await self._fetch(f"DESCRIBE TABLE {self.db}.`{table}`")
      return [row[0] for row in result]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """ClickHouse-specific table listing."""
    await self.cursor.execute(f"SHOW TABLES FROM {self.db}")
    results = await self.cursor.fetchall()
    return [table[0] for table in results]

  async def fetch(self,
                  table: str,
                  from_date: Optional[datetime] = None,
                  to_date: Optional[datetime] = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = [],
                  use_first: bool = False) -> tuple[list[str], list[tuple]]:
    """ClickHouse-specific fetch with advanced aggregation."""
    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    if not columns:
      columns = await self.get_cache_columns(table)
    else:
      if 'ts' not in columns:
        columns.insert(0, 'ts')

    if not columns:
      log_warn(f"No columns found for table {self.db}.{table}")
      return (columns, [])

    # Use ClickHouse-specific aggregation
    query, params = self._build_aggregation_sql(table, columns, from_date,
                                                to_date, aggregation_interval)

    try:
      await self.cursor.execute(query)
      results = await self.cursor.fetchall()
      return (columns, results)
    except Exception as e:
      log_error(f"Failed to fetch data from {self.db}.{table}", e)
      raise e

  async def upsert(self, ing: UpdateIngester, table="", uid=""):
    """ClickHouse upsert using INSERT (ReplacingMergeTree pattern)."""
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

    placeholders = ", ".join(["%s"] * len(field_values))
    columns = ", ".join([f"`{name}`" for name in field_names])

    # Use INSERT with ReplacingMergeTree pattern
    sql = f"INSERT INTO {self.db}.`{table}` ({columns}) VALUES ({placeholders})"

    try:
      await self.cursor.execute(sql, field_values)
      log_info(f"Upserted record with uid {uid} into {table}")
    except Exception as e:
      log_error(f"Failed to upsert into {table}", e)
      raise e

  async def fetch_by_id(self, table: str, uid: str):
    """ClickHouse-specific fetch by ID."""
    await self.ensure_connected()

    try:
      sql = f"SELECT * FROM {self.db}.`{table}` WHERE uid = %s ORDER BY updated_at DESC LIMIT 1"
      await self.cursor.execute(sql, [uid])
      result = await self.cursor.fetchone()

      if result:
        # Get column names
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, result))
      return None

    except Exception as e:
      log_error(f"Failed to fetch record with uid {uid} from {table}", e)
      return None

  async def fetchall(self):
    """ClickHouse-specific fetchall."""
    try:
      return await self.cursor.fetchall()
    except Exception as e:
      log_error("Failed to fetch all results", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """ClickHouse-specific batch fetch by UIDs."""
    await self.ensure_connected()
    try:
      if not uids:
        return []

      # Build parameterized query for ClickHouse
      placeholders = ",".join([f"'{uid}'" for uid in uids])
      query = f"SELECT * FROM {self.db}.`{table}` WHERE uid IN ({placeholders}) ORDER BY updated_at DESC"

      await self.cursor.execute(query)
      results = await self.cursor.fetchall()
      return results if results else []
    except Exception as e:
      log_error(
          f"Failed to fetch batch records by IDs from {self.db}.{table}: {e}")
      return []
