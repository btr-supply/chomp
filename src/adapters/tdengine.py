from datetime import datetime
from os import environ as env
from typing import Any

from ..utils import log_error, log_info, log_warn, log_debug, Interval
from ..models.base import FieldType
from ..models.ingesters import Ingester
from .. import state
from .sql import SqlAdapter

import taos  # happy mypy

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
    "timestamp":
    "timestamp",  # 64 bits timestamp, default precision is ms, us and ns are also supported
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

  def __init__(self,
               host: str = "localhost",
               port: int = 40002,
               db: str = "default",
               user: str = "rw",
               password: str = "pass"):
    super().__init__(host, port, db, user, password)

  @property
  def timestamp_column_type(self) -> str:
    return "timestamp"

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "Taos":
    # Use logging instead of print statements
    actual_host = cls._get_config_value(host, "DB_HOST", "localhost")
    actual_port = cls._get_port_value(port, "DB_PORT", 40002)
    actual_db = cls._get_config_value(db, "DB_NAME", "default")
    actual_user = cls._get_config_value(user, "DB_RW_USER", "rw")
    actual_password = cls._get_config_value(password, "DB_RW_PASS", "pass")

    log_debug(f"""TDengine Connect Debug:
  Environment DB_HOST: {env.get('DB_HOST')}
  Environment DB_PORT: {env.get('DB_PORT')}
  Environment DB_NAME: {env.get('DB_NAME')}
  Environment DB_RW_USER: {env.get('DB_RW_USER')}
  Actual values: {actual_user}@{actual_host}:{actual_port}/{actual_db}""")

    # Use **kwargs approach for cleaner constructor call
    params = {
        'host': actual_host,
        'port': actual_port,
        'db': actual_db,
        'user': actual_user,
        'password': actual_password
    }

    self = cls(**params)
    await self.ensure_connected()
    return self

  async def _connect(self):
    """TDengine-specific connection."""
    try:
      self.conn = taos.connect(host=self.host,
                               port=self.port,
                               database=self.db,
                               user=self.user,
                               password=self.password)
      self.cursor = self.conn.cursor()
      log_info(
          f"Connected to TDengine on {self.host}:{self.port}/{self.db} as {self.user}"
      )
    except Exception as e:
      e_str = str(e).lower()
      if "not exist" in e_str:
        log_warn(
            f"Database '{self.db}' does not exist on {self.host}:{self.port}, creating it now..."
        )
        # Connect without database to create it
        self.conn = taos.connect(host=self.host,
                                 port=self.port,
                                 user=self.user,
                                 password=self.password)
        self.cursor = self.conn.cursor()
        await self.create_db(self.db)
        # Reconnect to the created database
        self.conn.close()
        self.conn = taos.connect(host=self.host,
                                 port=self.port,
                                 database=self.db,
                                 user=self.user,
                                 password=self.password)
        self.cursor = self.conn.cursor()
      else:
        raise ValueError(
            f"Failed to connect to TDengine on {self.user}@{self.host}:{self.port}/{self.db}"
        )

  async def _close_connection(self):
    """TDengine-specific connection closing."""
    if self.cursor:
      try:
        self.cursor.close()
      except Exception:
        pass  # Ignore close errors
    if self.conn:
      try:
        self.conn.close()
      except Exception:
        pass  # Ignore close errors
    self.conn = None
    self.cursor = None

  async def _execute(self, query: str, params: tuple = ()):
    """Execute TDengine query with safer parameter handling."""
    # TDengine doesn't support parameterized queries the same way
    # Use safer parameter substitution
    if params:
      formatted_query = query
      for param in params:
        if isinstance(param, datetime):
          # Use proper timestamp formatting for TDengine
          formatted_query = formatted_query.replace(
              "?", self._format_timestamp(param), 1)
        elif isinstance(param, str):
          # Escape single quotes to prevent SQL injection
          escaped_param = param.replace("'", "''")
          formatted_query = formatted_query.replace("?", f"'{escaped_param}'",
                                                    1)
        elif param is None:
          formatted_query = formatted_query.replace("?", "NULL", 1)
        else:
          formatted_query = formatted_query.replace("?", str(param), 1)
    else:
      formatted_query = query

    # Only log debug messages when verbose flag is enabled
    if state.args.verbose:
      log_debug(f"TDengine executing: {formatted_query[:200]}..."
                )  # Just show first 200 chars

    self.cursor.execute(formatted_query)
    return self.cursor

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """Execute TDengine query and fetch results."""
    await self._execute(query, params)
    result = self.cursor.fetchall()

    # Debug: Log the structure to understand the issue
    if state.args.verbose and result:
      debug_info = [f"TDengine _fetch result type: {type(result)}"]
      debug_info.append(f"TDengine _fetch result length: {len(result)}")
      if result:
        debug_info.append(f"TDengine _fetch first row type: {type(result[0])}")
        debug_info.append(f"TDengine _fetch first row content: {result[0]}")
        if hasattr(result[0], '__len__') and len(result[0]) > 0:
          debug_info.append(
              f"TDengine _fetch first row first element type: {type(result[0][0])}"
          )
      log_debug("\n".join(debug_info))

    return result

  async def _executemany(self, query: str, params_list: list[tuple]):
    """Execute many TDengine queries."""
    for params in params_list:
      await self._execute(query, params)

  def _quote_identifier(self, identifier: str) -> str:
    """TDengine uses backticks for identifiers."""
    return f"`{identifier}`"

  def _format_timestamp(self, timestamp: datetime) -> str:
    """Format timestamp for TDengine with timezone handling."""
    # TDengine doesn't handle ISO 8601 timezone suffixes well
    # Convert to UTC if timezone-aware, then format
    if timestamp.tzinfo is not None:
      utc_tuple = timestamp.utctimetuple()
      timestamp = datetime(*utc_tuple[:6])

    # Use format: 'YYYY-MM-DD HH:MM:SS.mmm'
    return f"'{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """TDengine-specific database creation with improved retry logic."""
    from asyncio import sleep

    base = "CREATE DATABASE IF NOT EXISTS" if not force else "CREATE DATABASE"
    max_retries = 10

    # Database creation with retry
    for attempt in range(max_retries):
      try:
        self.cursor.execute(
            f"{base} {name} PRECISION '{PRECISION}' BUFFER 256 KEEP 3650d;"
        )  # 10 years max archiving
        break
      except Exception as e:
        if attempt < max_retries - 1:
          log_warn(
              f"Retrying to create database {name} attempt {attempt + 1}/{max_retries}: {e}"
          )
          await sleep(1)
        else:
          raise ValueError(
              f"Failed to create database {name} after {max_retries} attempts: {e}"
          )

    log_info(f"Created database {name} with time precision {PRECISION}")

    # Readiness check with retry
    for attempt in range(max_retries):
      try:
        self.cursor.execute(f"USE {name};")
        log_info(f"Database {name} is now ready.")
        return
      except Exception as e:
        if attempt < max_retries - 1:
          log_warn(
              f"Retrying to use database {name} attempt {attempt + 1}/{max_retries}: {e}"
          )
          await sleep(1)
        else:
          raise ValueError(
              f"Database {name} readiness check failed after {max_retries} attempts: {e}"
          )

  async def use_db(self, db: str):
    """TDengine-specific database switching."""
    if not self.conn:
      await self._connect()
    else:
      self.conn.select_db(db)

  def _build_create_table_sql(self, c: Ingester, table_name: str) -> str:
    """TDengine-specific CREATE TABLE syntax."""
    persistent_fields = [field for field in c.fields if not field.transient]

    if not persistent_fields:
      # Fallback for ingesters with no persistent fields
      fields = "`value` nchar"
    else:
      fields = ", ".join([
          f"`{field.name}` {TYPES[field.type]}" for field in persistent_fields
      ])

    return f"""
    CREATE TABLE IF NOT EXISTS {self.db}.`{table_name}` (
      {fields}
    );
    """

  def _build_aggregation_sql(
      self, table_name: str, columns: list[str], from_date: datetime,
      to_date: datetime,
      aggregation_interval: Interval) -> tuple[str, list[Any]]:
    """TDengine-specific aggregation query."""
    interval_sql = INTERVALS.get(aggregation_interval, "5m")

    # Build aggregation with TDengine syntax
    select_cols = ["ts"]
    select_cols.extend(
        [f"LAST({self._quote_identifier(col)}) as {col}" for col in columns])
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
      return [row[0] for row in result]
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

  async def alter_table(self,
                        table: str,
                        add_columns: list[tuple[str, str]] = [],
                        drop_columns: list[str] = []):
    """TDengine-specific ALTER TABLE."""
    await self.ensure_connected()

    for column_name, column_type in add_columns:
      try:
        sql = f"ALTER TABLE {self.db}.`{table}` ADD COLUMN `{column_name}` {column_type}"
        await self._execute(sql)
        log_info(
            f"Added column {column_name} of type {column_type} to {self.db}.{table}"
        )
      except Exception as e:
        log_error(f"Failed to add column {column_name} to {self.db}.{table}",
                  e)
        raise e

    for column_name in drop_columns:
      try:
        sql = f"ALTER TABLE {self.db}.`{table}` DROP COLUMN `{column_name}`"
        await self._execute(sql)
        log_info(f"Dropped column {column_name} from {self.db}.{table}")
      except Exception as e:
        log_error(
            f"Failed to drop column {column_name} from {self.db}.{table}", e)
        raise e
