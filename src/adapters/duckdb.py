from asyncio import gather, get_event_loop
from datetime import datetime, timezone
from os import environ as env
import duckdb
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor

from ..cache import get_or_set_cache
from ..utils import log_error, log_info, log_warn, Interval, TimeUnit, fmt_date, ago, now
from ..model import Ingester, FieldType, Tsdb

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


class DuckDB(Tsdb):
  conn: duckdb.DuckDBPyConnection
  cursor: duckdb.DuckDBPyConnection
  executor: ThreadPoolExecutor

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "DuckDB":
    self = cls(
        host=host or env.get("DUCKDB_HOST") or "localhost",
        port=int(port or env.get("DUCKDB_PORT") or 0),
        db=db or env.get("DUCKDB_DB") or ":memory:",  # Default to in-memory
        user=user or env.get("DB_RW_USER") or "",
        password=password or env.get("DB_RW_PASS") or "")
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    try:
      await self.ensure_connected()
      await self._execute_async("SELECT 1")
      return True
    except Exception as e:
      log_error("DuckDB ping failed", e)
      return False

  async def close(self):
    if self.conn:
      await self._execute_async_void(lambda: self.conn.close())
    if self.executor:
      self.executor.shutdown(wait=True)

  async def _execute_async(self, query: str, params=None):
    """Execute a query asynchronously using thread pool"""
    loop = get_event_loop()
    if params:
      return await loop.run_in_executor(
          self.executor, lambda: self.conn.execute(query, params).fetchall())
    else:
      return await loop.run_in_executor(
          self.executor, lambda: self.conn.execute(query).fetchall())

  async def _execute_async_void(self, func):
    """Execute a function asynchronously using thread pool"""
    loop = get_event_loop()
    return await loop.run_in_executor(self.executor, func)

  async def ensure_connected(self):
    if not self.conn:
      try:
        self.executor = ThreadPoolExecutor(max_workers=1)
        loop = get_event_loop()

        if self.db == ":memory:":
          self.conn = await loop.run_in_executor(self.executor, duckdb.connect)
        else:
          self.conn = await loop.run_in_executor(self.executor, duckdb.connect,
                                                 self.db)

        log_info(f"Connected to DuckDB database: {self.db}")
      except Exception:
        raise ValueError(f"Failed to connect to DuckDB database: {self.db}")

    # DuckDB connection serves as both connection and cursor
    self.cursor = self.conn

  async def get_dbs(self):
    # DuckDB can have multiple schemas, but databases are file-based
    results = await self._execute_async("SHOW SCHEMAS")
    return results

  async def create_db(self, name: str, options={}, force=False):
    # In DuckDB, databases are files. Schemas can be created instead.
    try:
      await self._execute_async(f"CREATE SCHEMA IF NOT EXISTS {name}")
      log_info(f"Created schema {name} in DuckDB")
    except Exception as e:
      log_error(f"Failed to create schema {name}", e)

  async def use_db(self, db: str):
    # DuckDB uses file-based databases, so we'd need to reconnect
    if self.db != db:
      await self.close()
      self.db = db
      await self.ensure_connected()

  async def create_table(self, c: Ingester, name=""):
    table = name or c.name
    log_info(f"Creating table {table}...")
    fields = ", ".join([
        f'"{field.name}" {TYPES[field.type]}' for field in c.fields
        if not field.transient
    ])
    sql = f'''
    CREATE TABLE IF NOT EXISTS "{table}" (
      ts TIMESTAMP,
      {fields}
    );
    '''
    try:
      await self._execute_async(sql)
      log_info(f"Created table {table}")
    except Exception as e:
      log_error(f"Failed to create table {table}\nSQL: {sql}", e)
      raise e

  async def alter_table(self,
                        table: str,
                        add_columns: list[tuple[str, str]] = [],
                        drop_columns: list[str] = []):
    await self.ensure_connected()
    for column_name, column_type in add_columns:
      try:
        await self._execute_async(
            f'ALTER TABLE "{table}" ADD COLUMN "{column_name}" {column_type}')
        log_info(
            f"Added column {column_name} of type {column_type} to {table}")
      except Exception as e:
        log_error(f"Failed to add column {column_name} to {table}", e)
        raise e

    for column_name in drop_columns:
      try:
        await self._execute_async(
            f'ALTER TABLE "{table}" DROP COLUMN "{column_name}"')
        log_info(f"Dropped column {column_name} from {table}")
      except Exception as e:
        log_error(f"Failed to drop column {column_name} from {table}", e)
        raise e

  async def insert(self, c: Ingester, table=""):
    await self.ensure_connected()
    table = table or c.name
    persistent_data = [field for field in c.fields if not field.transient]
    fields = '", "'.join(field.name for field in persistent_data)
    placeholders = ", ".join(["?" for _ in persistent_data])
    sql = f'INSERT INTO "{table}" (ts, "{fields}") VALUES (?, {placeholders})'
    values = [c.last_ingested] + [field.value for field in persistent_data]

    try:
      await self._execute_async(sql, values)
    except Exception as e:
      error_message = str(e).lower()
      if "table" in error_message and "does not exist" in error_message:
        log_warn(f"Table {table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self.insert(c, table=table)
      elif "column" in error_message and "does not exist" in error_message:
        log_warn(
            f"Column mismatch detected, altering table {table} to add missing columns..."
        )
        existing_columns = await self.get_columns(table)
        existing_column_names = [col[0] for col in existing_columns]
        add_columns = [(field.name, TYPES[field.type])
                       for field in persistent_data
                       if field.name not in existing_column_names]
        await self.alter_table(table, add_columns=add_columns)
        await self.insert(c, table=table)
      else:
        log_error(f"Failed to insert data into {table}", e)
        raise e

  async def insert_many(self, c: Ingester, values: list[tuple], table=""):
    table = table or c.name
    persistent_fields = [
        field.name for field in c.fields if not field.transient
    ]
    fields = '", "'.join(persistent_fields)
    placeholders = ", ".join(["?" for _ in range(len(persistent_fields) + 1)
                              ])  # +1 for timestamp
    sql = f'INSERT INTO "{table}" (ts, "{fields}") VALUES ({placeholders})'

    try:
      # DuckDB doesn't have executemany, so we'll use a transaction
      await self._execute_async("BEGIN TRANSACTION")
      for value_tuple in values:
        await self._execute_async(sql, value_tuple)
      await self._execute_async("COMMIT")
    except Exception as e:
      await self._execute_async("ROLLBACK")
      log_error(f"Failed to insert many records into {table}", e)
      raise e

  async def get_columns(self, table: str) -> list[tuple]:
    try:
      results = await self._execute_async(f'DESCRIBE "{table}"')
      return results
    except Exception as e:
      log_error(f"Failed to get columns from {table}", e)
      return []

  async def get_cache_columns(self, table: str) -> list[str]:
    column_descs = await get_or_set_cache(
        f"{table}:columns",
        callback=lambda: self.get_columns(table),
        expiry=300,
        pickled=True)
    return [
        col[0] for col in column_descs
    ]  # DuckDB DESCRIBE returns (column_name, column_type, null, key, default, extra)

  async def fetch(self,
                  table: str,
                  from_date: datetime | None = None,
                  to_date: datetime | None = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = [],
                  use_first: bool = False) -> tuple[list[str], list[tuple]]:

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)
    agg_interval = INTERVALS[aggregation_interval]

    if not columns:
      columns = await self.get_cache_columns(table)
    else:
      if 'ts' not in columns:
        columns.insert(0, 'ts')

    if not columns:
      log_warn(f"No columns found for table {table}")
      return (columns, [])

    # DuckDB has excellent time-series support with time_bucket function
    select_cols = [
        f"{'first' if use_first else 'last'}(\"{col}\") AS \"{col}\""
        for col in columns if col != 'ts'
    ]
    select_cols.insert(0, f"time_bucket(INTERVAL '{agg_interval}', ts) AS ts")
    select_cols_str = ", ".join(select_cols)

    conditions = [
        f"ts >= '{fmt_date(from_date, keepTz=False)}'" if from_date else None,
        f"ts <= '{fmt_date(to_date, keepTz=False)}'" if to_date else None,
    ]
    where_clause = f"WHERE {' AND '.join(filter(None, conditions))}" if any(
        conditions) else ""
    sql = f'SELECT {select_cols_str} FROM "{table}" {where_clause} GROUP BY time_bucket(INTERVAL \'{agg_interval}\', ts) ORDER BY ts DESC'

    try:
      results = await self._execute_async(sql)
      return (columns, results)
    except Exception:
      # Fallback to simpler query if time_bucket is not available
      log_warn(
          "time_bucket function not available, falling back to simple query")
      select_cols_simple = ", ".join([f'"{col}"' for col in columns])
      sql_simple = f'SELECT {select_cols_simple} FROM "{table}" {where_clause} ORDER BY ts DESC'
      results = await self._execute_async(sql_simple)
      return (columns, results)

  async def fetch_batch(
      self,
      tables: list[str],
      from_date: datetime | None = None,
      to_date: datetime | None = None,
      aggregation_interval: Interval = "m5",
      columns: list[str] = []) -> tuple[list[str], list[tuple]]:

    if not columns:
      columns = await self.get_cache_columns(tables[0])
    to_date = to_date or now()
    from_date = from_date or to_date - relativedelta(years=10)
    results = await gather(*[
        self.fetch(table, from_date, to_date, aggregation_interval, columns)
        for table in tables
    ])
    # Flatten the results from multiple tables
    all_rows = []
    for _, rows in results:
      all_rows.extend(rows)
    return (columns, all_rows)

  async def fetch_all(self, query: str) -> list:
    return await self._execute_async(query)

  async def list_tables(self) -> list[str]:
    results = await self._execute_async("SHOW TABLES")
    return [table[0] for table in results]

  async def commit(self):
    # DuckDB auto-commits by default, but we can explicitly commit transactions
    await self._execute_async("COMMIT")
