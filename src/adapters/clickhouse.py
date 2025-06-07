from asyncio import gather, sleep
from datetime import datetime, timezone
from os import environ as env
from asynch import connect, Connection, Cursor
from dateutil.relativedelta import relativedelta

from ..cache import get_or_set_cache
from ..utils import log_error, log_info, log_warn, Interval, TimeUnit, fmt_date, ago, now
from ..model import Ingester, FieldType, Tsdb

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


class ClickHouse(Tsdb):
  conn: Connection
  cursor: Cursor

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "ClickHouse":
    self = cls(host=host or env.get("CLICKHOUSE_HOST") or "localhost",
               port=int(port or env.get("CLICKHOUSE_PORT") or 9000),
               db=db or env.get("CLICKHOUSE_DB") or "default",
               user=user or env.get("DB_RW_USER") or "default",
               password=password or env.get("DB_RW_PASS") or "")
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    try:
      await self.ensure_connected()
      await self.cursor.execute("SELECT 1")
      return True
    except Exception as e:
      log_error("ClickHouse ping failed", e)
      return False

  async def close(self):
    if self.cursor:
      await self.cursor.close()
    if self.conn:
      await self.conn.close()

  async def ensure_connected(self):
    if not self.conn:
      try:
        self.conn = await connect(host=self.host,
                                  port=self.port,
                                  database=self.db,
                                  user=self.user,
                                  password=self.password)
        log_info(
            f"Connected to ClickHouse on {self.host}:{self.port}/{self.db} as {self.user}"
        )
      except Exception as e:
        e_str = str(e).lower()
        if "database" in e_str and "doesn't exist" in e_str:
          log_warn(
              f"Database '{self.db}' does not exist on {self.host}:{self.port}, creating it now..."
          )
          # Connect without specifying database to create it
          temp_conn = await connect(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password)
          temp_cursor = await temp_conn.cursor()
          await temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
          await temp_cursor.close()
          await temp_conn.close()

          # Now connect to the created database
          self.conn = await connect(host=self.host,
                                    port=self.port,
                                    database=self.db,
                                    user=self.user,
                                    password=self.password)
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

  async def get_dbs(self):
    await self.cursor.execute("SHOW DATABASES")
    return await self.cursor.fetchall()

  async def create_db(self, name: str, options={}, force=False):
    base = "CREATE DATABASE IF NOT EXISTS" if force else "CREATE DATABASE"
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
    if not self.conn:
      await self.connect(db=db)
    else:
      await self.cursor.execute(f"USE {db}")

  async def create_table(self, c: Ingester, name=""):
    table = name or c.name
    log_info(f"Creating table {self.db}.{table}...")
    fields = ", ".join([
        f"`{field.name}` {TYPES[field.type]}" for field in c.fields
        if not field.transient
    ])

    # ClickHouse requires an engine specification - using MergeTree for time series data
    sql = f"""
    CREATE TABLE IF NOT EXISTS {self.db}.`{table}` (
      ts DateTime,
      {fields}
    ) ENGINE = MergeTree()
    ORDER BY ts
    """
    try:
      await self.cursor.execute(sql)
      log_info(f"Created table {self.db}.{table}")
    except Exception as e:
      log_error(f"Failed to create table {self.db}.{table}\nSQL: {sql}", e)
      raise e

  async def alter_table(self,
                        table: str,
                        add_columns: list[tuple[str, str]] = [],
                        drop_columns: list[str] = []):
    await self.ensure_connected()
    for column_name, column_type in add_columns:
      try:
        await self.cursor.execute(
            f"ALTER TABLE {self.db}.`{table}` ADD COLUMN `{column_name}` {column_type}"
        )
        log_info(
            f"Added column {column_name} of type {column_type} to {self.db}.{table}"
        )
      except Exception as e:
        log_error(f"Failed to add column {column_name} to {self.db}.{table}",
                  e)
        raise e

    for column_name in drop_columns:
      try:
        await self.cursor.execute(
            f"ALTER TABLE {self.db}.`{table}` DROP COLUMN `{column_name}`")
        log_info(f"Dropped column {column_name} from {self.db}.{table}")
      except Exception as e:
        log_error(
            f"Failed to drop column {column_name} from {self.db}.{table}", e)
        raise e

  async def insert(self, c: Ingester, table=""):
    await self.ensure_connected()
    table = table or c.name
    persistent_data = [field for field in c.fields if not field.transient]
    fields = "`, `".join(field.name for field in persistent_data)
    values = ", ".join([field.sql_escape() for field in persistent_data])
    sql = f"INSERT INTO {self.db}.`{table}` (ts, `{fields}`) VALUES ('{c.last_ingested}', {values})"

    try:
      await self.cursor.execute(sql)
    except Exception as e:
      error_message = str(e).lower()
      if "doesn't exist" in error_message and "table" in error_message:
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self.insert(c, table=table)
      elif "no such column" in error_message:
        log_warn(
            f"Column mismatch detected, altering table {self.db}.{table} to add missing columns..."
        )
        existing_columns = await self.get_columns(table)
        existing_column_names = [col[0] for col in existing_columns]
        add_columns = [(field.name, TYPES[field.type])
                       for field in persistent_data
                       if field.name not in existing_column_names]
        await self.alter_table(table, add_columns=add_columns)
        await self.insert(c, table=table)
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  async def insert_many(self, c: Ingester, values: list[tuple], table=""):
    table = table or c.name
    persistent_fields = [
        field.name for field in c.fields if not field.transient
    ]
    fields = "`, `".join(persistent_fields)
    placeholders = ", ".join(["%s" for _ in range(len(persistent_fields) + 1)
                              ])  # +1 for timestamp
    sql = f"INSERT INTO {self.db}.`{table}` (ts, `{fields}`) VALUES ({placeholders})"

    try:
      await self.cursor.executemany(sql, values)
    except Exception as e:
      log_error(f"Failed to insert many records into {self.db}.{table}", e)
      raise e

  async def get_columns(self, table: str) -> list[tuple[str, str, str]]:
    try:
      await self.cursor.execute(f"DESCRIBE TABLE {self.db}.`{table}`")
      return await self.cursor.fetchall()
    except Exception as e:
      log_error(f"Failed to get columns from {self.db}.{table}", e)
      return []

  async def get_cache_columns(self, table: str) -> list[str]:
    column_descs = await get_or_set_cache(
        f"{table}:columns",
        callback=lambda: self.get_columns(table),
        expiry=300,
        pickled=True)
    return [col[0] for col in column_descs]

  async def fetch(self,
                  table: str,
                  from_date: datetime | None = None,
                  to_date: datetime | None = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = [],
                  use_first: bool = False) -> tuple[list[str], list[tuple]]:

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)
    agg_seconds = INTERVALS[aggregation_interval]

    if not columns:
      columns = await self.get_cache_columns(table)
    else:
      if 'ts' not in columns:
        columns.insert(0, 'ts')

    if not columns:
      log_warn(f"No columns found for table {self.db}.{table}")
      return (columns, [])

    # ClickHouse time-based aggregation using toStartOfInterval
    select_cols = [
        f"{'argMin' if use_first else 'argMax'}(`{col}`, ts) AS `{col}`"
        for col in columns if col != 'ts'
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
    sql = f"SELECT {select_cols_str} FROM {self.db}.`{table}` {where_clause} GROUP BY toStartOfInterval(ts, INTERVAL {agg_seconds} second) ORDER BY ts DESC"

    try:
      await self.cursor.execute(sql)
      results = await self.cursor.fetchall()
      return (columns, results)
    except Exception as e:
      log_error(f"Failed to fetch data from {self.db}.{table}", e)
      raise e

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
    await self.cursor.execute(query)
    return await self.cursor.fetchall()

  async def list_tables(self) -> list[str]:
    await self.cursor.execute(f"SHOW TABLES FROM {self.db}")
    results = await self.cursor.fetchall()
    return [table[0] for table in results]

  async def commit(self):
    # ClickHouse doesn't require explicit commits for most operations
    pass
