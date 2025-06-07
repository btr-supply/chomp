from datetime import datetime
from os import environ as env
from typing import Any

from ..utils import log_error, log_info, Interval, TimeUnit
from ..model import Ingester, FieldType
from .sql import SqlAdapter

# TimescaleDB data type mapping (PostgreSQL compatible)
TYPES: dict[FieldType, str] = {
    "int8": "smallint",  # 16 bits minimum in postgres
    "uint8": "smallint",  # no unsigned in postgres
    "int16": "smallint",  # 16 bits
    "uint16": "integer",  # use 32-bit for unsigned 16-bit
    "int32": "integer",  # 32 bits
    "uint32": "bigint",  # use 64-bit for unsigned 32-bit
    "int64": "bigint",  # 64 bits
    "uint64": "bigint",  # no unsigned in postgres
    "float32": "real",  # 32 bits
    "ufloat32": "real",  # no unsigned in postgres
    "float64": "double precision",  # 64 bits
    "ufloat64": "double precision",  # no unsigned in postgres
    "bool": "boolean",
    "timestamp": "timestamptz",  # timestamp with timezone
    "string": "text",
    "binary": "bytea",
    "varbinary": "bytea",
}

# Define precision and timezone
PRECISION: TimeUnit = "ms"  # Supported values: ns, us, ms, s, m
TIMEZONE = "UTC"

partitioning_by_precision: dict[TimeUnit, str] = {
    "ns": "1 day",
    "us": "1 day",
    "ms": "1 day",
    "s": "7 days",
    "m": "7 days",
    "h": "1 month",
    "D": "6 months",
    "W": "5 years",
    "M": "10 years",
    "Y": "100 years"
}


class TimescaleDb(SqlAdapter):
  """TimescaleDB adapter extending SqlAdapter."""

  TYPES = TYPES

  def __init__(self,
               host: str = "localhost",
               port: int = 5432,
               db: str = "postgres",
               user: str = "postgres",
               password: str = "password"):
    super().__init__(host, port, db, user, password)
    self._asyncpg_module = None

  @property
  def asyncpg_module(self):
    """Lazy load asyncpg module to avoid import errors if not installed."""
    if self._asyncpg_module is None:
      try:
        import asyncpg
        self._asyncpg_module = asyncpg
      except ImportError as e:
        raise ImportError(
            "asyncpg is required for TimescaleDB adapter. Install with: pip install asyncpg"
        ) from e
    return self._asyncpg_module

  @property
  def timestamp_column_type(self) -> str:
    return "timestamptz"

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "TimescaleDb":
    self = cls(host=host or env.get("TIMESCALE_HOST") or "localhost",
               port=int(port or env.get("TIMESCALE_PORT") or 5432),
               db=db or env.get("TIMESCALE_DB") or "postgres",
               user=user or env.get("DB_RW_USER") or "postgres",
               password=password or env.get("DB_RW_PASS") or "password")
    await self.ensure_connected()
    return self

  async def _connect(self):
    """TimescaleDB-specific connection using asyncpg."""
    try:
      asyncpg = self.asyncpg_module

      self.conn = await asyncpg.connect(host=self.host,
                                        port=self.port,
                                        database=self.db,
                                        user=self.user,
                                        password=self.password)
      log_info(
          f"Connected to TimescaleDB on {self.host}:{self.port}/{self.db} as {self.user}"
      )

    except Exception as e:
      raise ValueError(
          f"Failed to connect to TimescaleDB on {self.user}@{self.host}:{self.port}/{self.db}: {e}"
      )

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
      # Connect to postgres database to create the target database
      asyncpg = self.asyncpg_module
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
    asyncpg = self.asyncpg_module
    self.conn = await asyncpg.connect(host=self.host,
                                      port=self.port,
                                      database=db,
                                      user=self.user,
                                      password=self.password)

  def _build_create_table_sql(self, c: Ingester, table_name: str) -> str:
    """TimescaleDB-specific CREATE TABLE with hypertable creation."""
    persistent_fields = [field for field in c.fields if not field.transient]
    fields = ", ".join([
        f'"{field.name}" {self.TYPES[field.type]}'
        for field in persistent_fields
    ])

    return f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
      ts TIMESTAMPTZ NOT NULL,
      {fields}
    );
    '''

  async def create_table(self,
                         c: Ingester,
                         name: str = "",
                         force: bool = False):
    """TimescaleDB-specific table creation with hypertable."""
    await self.ensure_connected()
    table = name or c.name

    drop_sql = f'DROP TABLE IF EXISTS "{table}";' if force else ""
    create_sql = self._build_create_table_sql(c, table)

    # Create hypertable with appropriate chunk interval
    hypertable_sql = f"""
    SELECT create_hypertable('"{table}"', by_range('ts', INTERVAL '{partitioning_by_precision[c.precision]}'), if_not_exists => TRUE);
    """

    try:
      if drop_sql:
        await self.conn.execute(drop_sql)
      await self.conn.execute(create_sql)
      await self.conn.execute(hypertable_sql)
      log_info(f"Created hypertable {self.db}.{table}")
    except Exception as e:
      log_error(f"Failed to create hypertable {self.db}.{table}", e)
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
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = $1 AND column_name != $2
    ORDER BY ordinal_position
    """

    try:
      result = await self._fetch(query, (table, self.timestamp_column_name))
      return [row[0] for row in result]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """TimescaleDB-specific table listing."""
    await self.ensure_connected()

    # Query for both regular tables and hypertables
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
    """

    try:
      result = await self._fetch(query)
      return [row[0] for row in result]
    except Exception as e:
      log_error(f"Failed to list tables from {self.db}", e)
      return []
