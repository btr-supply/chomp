from datetime import datetime
from os import environ as env
import aiohttp
from typing import Any

from ..utils import log_error, log_info, Interval
from ..model import Ingester, FieldType
from .sql import SqlAdapter

# QuestDB data type mapping
TYPES: dict[FieldType, str] = {
  "int8": "byte",
  "uint8": "short",  # No unsigned byte
  "int16": "short",
  "uint16": "int",  # No unsigned short
  "int32": "int",
  "uint32": "long",  # No unsigned int
  "int64": "long",
  "uint64": "long",  # No unsigned long in QuestDB
  "float32": "float",
  "ufloat32": "float",  # No unsigned float
  "float64": "double",
  "ufloat64": "double",  # No unsigned double
  "bool": "boolean",
  "timestamp": "timestamp",
  "string": "string",
  "binary": "string",  # QuestDB doesn't have binary type
  "varbinary": "string",
}

# QuestDB interval mapping for SAMPLE BY
INTERVALS: dict[Interval, str] = {
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
  "W2": "2w",
  "M1": "1M",
  "M2": "2M",
  "M3": "3M",
  "M6": "6M",
  "Y1": "1y",
  "Y2": "2y",
  "Y3": "3y",
}

class QuestDb(SqlAdapter):
  """QuestDB adapter that extends SqlAdapter but uses HTTP API."""

  TYPES = TYPES
  session: aiohttp.ClientSession | None = None
  base_url: str

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
  ) -> "QuestDb":
    self = cls(
      host=host or env.get("QUESTDB_HOST") or "localhost",
      port=int(port or env.get("QUESTDB_PORT") or 9000),
      db=db or env.get("QUESTDB_DB") or "default",
      user=user or env.get("DB_RW_USER") or "admin",
      password=password or env.get("DB_RW_PASS") or "quest"
    )
    await self.ensure_connected()
    return self

  async def _connect(self):
    """QuestDB uses HTTP API instead of SQL connections."""
    if not self.session:
      self.base_url = f"http://{self.host}:{self.port}"

      # Create session with basic auth if credentials provided
      auth = None
      if self.user and self.password:
        auth = aiohttp.BasicAuth(self.user, self.password)

      self.session = aiohttp.ClientSession(auth=auth)
      log_info(f"Connected to QuestDB on {self.host}:{self.port}")

  async def _close_connection(self):
    """Close QuestDB HTTP session."""
    if self.session:
      await self.session.close()
      self.session = None

  async def _execute(self, query: str, params: tuple = ()) -> Any:
    """Execute SQL via QuestDB HTTP API."""
    await self.ensure_connected()

    # QuestDB HTTP API doesn't support parameterized queries the same way
    # So we need to format the query with parameters
    if params:
      formatted_query = query
      for param in params:
        if isinstance(param, str):
          formatted_query = formatted_query.replace("?", f"'{param}'", 1)
        elif isinstance(param, datetime):
          timestamp_micros = int(param.timestamp() * 1_000_000)
          formatted_query = formatted_query.replace("?", str(timestamp_micros), 1)
        else:
          formatted_query = formatted_query.replace("?", str(param), 1)
    else:
      formatted_query = query

    if self.session is None:
      raise Exception("QuestDB session not connected")
    async with self.session.get(
      f"{self.base_url}/exec",
      params={"query": formatted_query}
    ) as resp:
      if resp.status != 200:
        error_text = await resp.text()
        raise Exception(f"QuestDB query failed: {resp.status} - {error_text}")
      return await resp.json()

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """Execute SQL query and return results."""
    result = await self._execute(query, params)
    return result.get("dataset", [])

  async def _executemany(self, query: str, params_list: list[tuple]) -> Any:
    """Execute multiple queries - QuestDB doesn't have native batch support via HTTP."""
    for params in params_list:
      await self._execute(query, params)

  def _quote_identifier(self, identifier: str) -> str:
    """QuestDB uses single quotes for table names in some contexts."""
    return f"'{identifier}'"

  def _build_create_table_sql(self, c: Ingester, table_name: str) -> str:
    """QuestDB-specific CREATE TABLE syntax with designated timestamp."""
    persistent_fields = [field for field in c.fields if not field.transient]
    fields = ", ".join([f"{field.name} {self.TYPES[field.type]}" for field in persistent_fields])

    # QuestDB requires a designated timestamp column for time-series tables
    return f"""
    CREATE TABLE IF NOT EXISTS '{table_name}' (
      {self.timestamp_column_name} timestamp,
      {fields}
    ) timestamp({self.timestamp_column_name}) PARTITION BY DAY
    """

  def _build_aggregation_sql(
    self,
    table_name: str,
    columns: list[str],
    from_date: datetime,
    to_date: datetime,
    aggregation_interval: Interval
  ) -> tuple[str, list[Any]]:
    """QuestDB-specific aggregation using SAMPLE BY syntax."""

    # Convert timestamps to QuestDB format (microseconds)
    from_timestamp = int(from_date.timestamp() * 1_000_000)
    to_timestamp = int(to_date.timestamp() * 1_000_000)

    sample_by_interval = INTERVALS.get(aggregation_interval, "5m")

    # Build aggregation query using SAMPLE BY
    select_cols = [self.timestamp_column_name]
    select_cols.extend([f"last({col}) as {col}" for col in columns])
    select_clause = ", ".join(select_cols)

    query = f"""
    SELECT {select_clause}
    FROM '{table_name}'
    WHERE {self.timestamp_column_name} >= {from_timestamp}
      AND {self.timestamp_column_name} <= {to_timestamp}
    SAMPLE BY {sample_by_interval}
    ORDER BY {self.timestamp_column_name} DESC
    """

    return query, []  # No parameters since we embedded them

  async def _get_table_columns(self, table: str) -> list[str]:
    """QuestDB-specific column information query."""
    try:
      result = await self._fetch(f"SHOW COLUMNS FROM '{table}'")
      return [row[0] for row in result if row[0] != self.timestamp_column_name]
    except Exception:
      return []

  async def create_db(self, name: str, options: dict = {}, force: bool = False):
    """QuestDB doesn't have separate databases, everything is in one namespace."""
    log_info(f"QuestDB database '{name}' ready (single namespace)")

  async def use_db(self, db: str):
    """QuestDB doesn't have separate databases."""
    log_info(f"QuestDB uses single namespace, ignoring database switch to '{db}'")

  async def list_tables(self) -> list[str]:
    """QuestDB-specific table listing."""
    await self.ensure_connected()
    try:
      result = await self._fetch("SHOW TABLES")
      return [row[0] for row in result]
    except Exception as e:
      log_error("Failed to list tables from QuestDB", e)
      return []

  async def insert(self, c: Ingester, table: str = ""):
    """QuestDB-specific insert using ILP (InfluxDB Line Protocol) for better performance."""
    await self.ensure_connected()
    table = table or c.name

    # Convert to ILP format for better ingestion performance
    persistent_fields = [field for field in c.fields if not field.transient]

    # Build field values
    field_values = []
    for field in persistent_fields:
      if field.type in ["string", "binary", "varbinary"]:
        field_values.append(f'{field.name}="{field.value}"')
      elif field.type in ["int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"]:
        field_values.append(f'{field.name}={field.value}i')
      elif field.type in ["float32", "ufloat32", "float64", "ufloat64"]:
        field_values.append(f'{field.name}={field.value}')
      elif field.type == "bool":
        field_values.append(f'{field.name}={"t" if field.value else "f"}')
      else:
        field_values.append(f'{field.name}={field.value}')

    # Convert timestamp to nanoseconds
    if c.last_ingested is None:
      raise Exception("No timestamp available for ingester")
    timestamp_ns = int(c.last_ingested.timestamp() * 1_000_000_000)

    # ILP format: table_name field_name=value timestamp
    ilp_line = f'{table} {",".join(field_values)} {timestamp_ns}'

    try:
      if self.session is None:
        raise Exception("QuestDB session not connected")
      async with self.session.post(
        f"{self.base_url}/write",
        data=ilp_line,
        headers={"Content-Type": "text/plain"}
      ) as resp:
        if resp.status not in [200, 204]:
          error_text = await resp.text()
          raise Exception(f"QuestDB ILP insert failed: {resp.status} - {error_text}")
    except Exception as e:
      error_message = str(e).lower()
      if "does not exist" in error_message:
        log_info(f"Table {table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        # Retry the insert
        if self.session is None:
          raise Exception("QuestDB session not connected")
        async with self.session.post(
          f"{self.base_url}/write",
          data=ilp_line,
          headers={"Content-Type": "text/plain"}
        ) as resp:
          if resp.status not in [200, 204]:
            error_text = await resp.text()
            raise Exception(f"QuestDB ILP insert failed: {resp.status} - {error_text}")
      else:
        log_error(f"Failed to insert data into {table}", e)
        raise e
