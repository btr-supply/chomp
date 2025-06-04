from datetime import datetime
from os import environ as env
from typing import Any

from ..utils import log_error, log_info, log_warn, Interval
from ..model import Ingester, FieldType
from .sql import SqlAdapter

# TDengine data type mapping
TYPES: dict[FieldType, str] = {
  "int8": "tinyint",  # 8 bits char
  "uint8": "tinyint unsigned",  # 8 bits uchar
  "int16": "smallint",  # 16 bits short
  "uint16": "smallint unsigned",  # 16 bits ushort
  "int32": "int",  # 32 bits int
  "uint32": "int unsigned",  # 32 bits uint
  "int64": "bigint",  # 64 bits long
  "uint64": "bigint unsigned",  # 64 bits ulong
  "float32": "float",  # 32 bits float
  "ufloat32": "float unsigned",  # 32 bits ufloat
  "float64": "double",  # 64 bits double
  "ufloat64": "double unsigned",  # 64 bits udouble
  "bool": "bool",  # 8 bits bool
  "timestamp": "timestamp",  # 64 bits timestamp, default precision is ms, us and ns are also supported
  "string": "nchar",  # fixed length array of 4 bytes uchar
  "binary": "binary",  # fixed length array of 1 byte uchar
  "varbinary": "varbinary",  # variable length array of 1 byte uchar
}

# TDengine interval mapping
INTERVALS: dict[str, str] = {
  "s1": "1s",
  "s2": "2s",
  "s5": "5s",
  "s10": "10s",
  "s15": "15s",
  "s20": "20s",
  "s30": "30s",
  "m1": "1m",
  "m2": "2m",
  "m5": "5m",
  "m10": "10m",
  "m15": "15m",
  "m30": "30m",
  "h1": "1h",
  "h2": "2h",
  "h4": "4h",
  "h6": "6h",
  "h8": "8h",
  "h12": "12h",
  "D1": "1d",
  "D2": "2d",
  "D3": "3d",
  "W1": "1w",
  "M1": "1n",
  "Y1": "1y"
}

PRECISION = "ms"  # ns, us, ms, s, m
TIMEZONE = "UTC"  # making sure the front-end and back-end are in sync

class Taos(SqlAdapter):
  """TDengine adapter extending SqlAdapter."""

  TYPES = TYPES

  def __init__(self, host: str = "localhost", port: int = 40002, db: str = "default",
               user: str = "rw", password: str = "pass"):
    super().__init__(host, port, db, user, password)
    self._taos_module = None

  @property
  def taos_module(self):
    """Lazy load TDengine module to avoid import errors if not installed."""
    if self._taos_module is None:
      try:
        import taos
        self._taos_module = taos
      except ImportError as e:
        raise ImportError("TDengine Python client (taospy) is required for TDengine adapter. Install with: pip install taospy") from e
    return self._taos_module

  @property
  def timestamp_column_type(self) -> str:
    return "timestamp"

  @classmethod
  async def connect(
    cls,
    host: str | None = None,
    port: int | None = None,
    db: str | None = None,
    user: str | None = None,
    password: str | None = None
  ) -> "Taos":
    self = cls(
      host=host or env.get("TAOS_HOST") or "localhost",
      port=int(port or env.get("TAOS_PORT") or 40002),  # 6030
      db=db or env.get("TAOS_DB") or "default",
      user=user or env.get("DB_RW_USER") or "rw",
      password=password or env.get("DB_RW_PASS") or "pass"
    )
    await self.ensure_connected()
    return self

  async def _connect(self):
    """TDengine-specific connection."""
    try:
      taos = self.taos_module
      self.conn = taos.connect(
        host=self.host,
        port=self.port,
        database=self.db,
        user=self.user,
        password=self.password
      )
      self.cursor = self.conn.cursor()
      log_info(f"Connected to TDengine on {self.host}:{self.port}/{self.db} as {self.user}")
    except Exception as e:
      e_str = str(e).lower()
      if "not exist" in e_str:
        log_warn(f"Database '{self.db}' does not exist on {self.host}:{self.port}, creating it now...")
        # Connect without database to create it
        taos = self.taos_module
        self.conn = taos.connect(host=self.host, port=self.port, user=self.user, password=self.password)
        self.cursor = self.conn.cursor()
        await self.create_db(self.db)
        # Reconnect to the created database
        self.conn.close()
        self.conn = taos.connect(
          host=self.host,
          port=self.port,
          database=self.db,
          user=self.user,
          password=self.password
        )
        self.cursor = self.conn.cursor()
      else:
        raise ValueError(f"Failed to connect to TDengine on {self.user}@{self.host}:{self.port}/{self.db}")

  async def _close_connection(self):
    """TDengine-specific connection closing."""
    if self.cursor:
      self.cursor.close()
    if self.conn:
      self.conn.close()
    self.conn = None
    self.cursor = None

  async def _execute(self, query: str, params: tuple = ()):
    """Execute TDengine query."""
    # TDengine doesn't support parameterized queries the same way
    # For now, we'll use simple string formatting (not ideal for production)
    if params:
      formatted_query = query
      for param in params:
        if isinstance(param, str):
          formatted_query = formatted_query.replace("?", f"'{param}'", 1)
        else:
          formatted_query = formatted_query.replace("?", str(param), 1)
    else:
      formatted_query = query

    self.cursor.execute(formatted_query)

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """Execute TDengine query and fetch results."""
    await self._execute(query, params)
    return self.cursor.fetchall()

  async def _executemany(self, query: str, params_list: list[tuple]):
    """Execute many TDengine queries."""
    for params in params_list:
      await self._execute(query, params)

  def _quote_identifier(self, identifier: str) -> str:
    """TDengine uses backticks for identifiers."""
    return f"`{identifier}`"

  async def create_db(self, name: str, options: dict = {}, force: bool = False):
    """TDengine-specific database creation."""
    from asyncio import sleep

    base = "CREATE DATABASE IF NOT EXISTS" if not force else "CREATE DATABASE"
    max_retries = 10

    for i in range(max_retries):
      try:
        self.cursor.execute(f"{base} {name} PRECISION '{PRECISION}' BUFFER 256 KEEP 3650d;")  # 10 years max archiving
        break
      except Exception as e:
        log_warn(f"Retrying to create database {name} in {i}/{max_retries} ({e})...")
        await sleep(1)

    if i < max_retries - 1:
      log_info(f"Created database {name} with time precision {PRECISION}")

      # Readiness check
      for i in range(max_retries):
        try:
          self.cursor.execute(f"USE {name};")
          log_info(f"Database {name} is now ready.")
          return
        except Exception as e:
          log_warn(f"Retrying to use database {name} in {i}/{max_retries} ({e})...")
          await sleep(1)

    raise ValueError(f"Database {name} readiness check failed.")

  async def use_db(self, db: str):
    """TDengine-specific database switching."""
    if not self.conn:
      await self._connect()
    else:
      self.conn.select_db(db)

  def _build_create_table_sql(self, c: Ingester, table_name: str) -> str:
    """TDengine-specific CREATE TABLE syntax."""
    persistent_fields = [field for field in c.fields if not field.transient]
    fields = ", ".join([f"`{field.name}` {TYPES[field.type]}" for field in persistent_fields])

    return f"""
    CREATE TABLE IF NOT EXISTS {self.db}.`{table_name}` (
      ts timestamp,
      {fields}
    );
    """

  def _build_aggregation_sql(
    self,
    table_name: str,
    columns: list[str],
    from_date: datetime,
    to_date: datetime,
    aggregation_interval: Interval
  ) -> tuple[str, list[Any]]:
    """TDengine-specific aggregation query."""
    interval_sql = INTERVALS.get(aggregation_interval, "5m")

    # Build aggregation with TDengine syntax
    select_cols = ["ts"]
    select_cols.extend([f"LAST({self._quote_identifier(col)}) as {col}" for col in columns])
    select_clause = ", ".join(select_cols)

    query = f"""
    SELECT {select_clause}
    FROM {self.db}.`{table_name}`
    WHERE ts >= ? AND ts <= ?
    INTERVAL({interval_sql})
    ORDER BY ts DESC
    """

    return query, [from_date, to_date]

  async def _get_table_columns(self, table: str) -> list[str]:
    """TDengine-specific column information query."""
    try:
      result = await self._fetch(f"DESCRIBE {self.db}.`{table}`")
      return [row[0] for row in result if row[0] != "ts"]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """TDengine-specific table listing."""
    await self.ensure_connected()
    try:
      result = await self._fetch("SHOW TABLES")
      return [row[0] for row in result]
    except Exception as e:
      log_error(f"Failed to list tables from {self.db}", e)
      return []

  async def alter_table(self, table: str, add_columns: list[tuple[str, str]] = [], drop_columns: list[str] = []):
    """TDengine-specific ALTER TABLE."""
    await self.ensure_connected()

    for column_name, column_type in add_columns:
      try:
        sql = f"ALTER TABLE {self.db}.`{table}` ADD COLUMN `{column_name}` {column_type}"
        await self._execute(sql)
        log_info(f"Added column {column_name} of type {column_type} to {self.db}.{table}")
      except Exception as e:
        log_error(f"Failed to add column {column_name} to {self.db}.{table}", e)
        raise e

    for column_name in drop_columns:
      try:
        sql = f"ALTER TABLE {self.db}.`{table}` DROP COLUMN `{column_name}`"
        await self._execute(sql)
        log_info(f"Dropped column {column_name} from {self.db}.{table}")
      except Exception as e:
        log_error(f"Failed to drop column {column_name} from {self.db}.{table}", e)
        raise e
