from datetime import datetime
from os import environ as env
from typing import Any, Optional

from ..utils import log_error, log_info, Interval, now
from ..utils.http import get
from ..models.ingesters import Ingester
from ..models.base import FieldType
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
  base_url: str

  @property
  def timestamp_column_type(self) -> str:
    return "timestamp"

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "QuestDb":
    self = cls(host=host or env.get("QUESTDB_HOST") or "localhost",
               port=int(port or env.get("QUESTDB_PORT") or 9000),
               db=db or env.get("QUESTDB_DB") or "default",
               user=user or env.get("DB_RW_USER") or "admin",
               password=password or env.get("DB_RW_PASS") or "quest")
    await self.ensure_connected()
    return self

  async def _connect(self):
    """QuestDB uses HTTP API instead of SQL connections."""
    self.base_url = f"http://{self.host}:{self.port}"
    log_info(f"Connected to QuestDB on {self.host}:{self.port}")

  async def _close_connection(self):
    """Close QuestDB HTTP session."""
    # Singleton client is managed globally
    pass

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
          formatted_query = formatted_query.replace("?", str(timestamp_micros),
                                                    1)
        else:
          formatted_query = formatted_query.replace("?", str(param), 1)
    else:
      formatted_query = query

    response = await get(f"{self.base_url}/exec",
                         params={"query": formatted_query},
                         user=self.user if self.user else None,
                         password=self.password if self.password else None)
    if response.status_code != 200:
      error_text = response.text
      raise Exception(
          f"QuestDB query failed: {response.status_code} - {error_text}")
    return response.json()

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

  def _build_create_table_sql(self, ing: Ingester, table_name: str) -> str:
    """QuestDB-specific CREATE TABLE syntax with designated timestamp."""
    persistent_fields = [field for field in ing.fields if not field.transient]

    # Build field definitions including ts field
    field_definitions = []
    for field in persistent_fields:
      field_type = TYPES.get(field.type, "string")
      field_definitions.append(f"{field.name} {field_type}")

    if not field_definitions:
      # Fallback for ingesters with no persistent fields
      field_definitions = ["value string"]

    fields = ", ".join(field_definitions)

    # For TimeSeriesIngester, use ts field as designated timestamp
    if getattr(ing, 'ts', None) and ing.resource_type == 'timeseries':
      return f"""
      CREATE TABLE IF NOT EXISTS '{table_name}' (
        {fields}
      ) timestamp(ts) PARTITION BY DAY
      """
    else:
      # Regular table for non-timeseries ingesters
      return f"""
      CREATE TABLE IF NOT EXISTS '{table_name}' (
        {fields}
      )
      """

  def _build_aggregation_sql(
      self, table_name: str, columns: list[str], from_date: datetime,
      to_date: datetime,
      aggregation_interval: Interval) -> tuple[str, list[Any]]:
    """QuestDB-specific aggregation using SAMPLE BY syntax."""

    # Convert timestamps to QuestDB format (microseconds)
    from_timestamp = int(from_date.timestamp() * 1_000_000)
    to_timestamp = int(to_date.timestamp() * 1_000_000)

    sample_by_interval = INTERVALS.get(aggregation_interval, "5m")

    # Build aggregation query using SAMPLE BY with ts field directly
    select_cols = ["ts"]  # Use ts field directly
    select_cols.extend([f"last({col}) as {col}" for col in columns])
    select_clause = ", ".join(select_cols)

    query = f"""
    SELECT {select_clause}
    FROM '{table_name}'
    WHERE ts >= {from_timestamp}
      AND ts <= {to_timestamp}
    SAMPLE BY {sample_by_interval}
    ORDER BY ts DESC
    """

    return query, []  # No parameters since we embedded them

  async def _get_table_columns(self, table: str) -> list[str]:
    """QuestDB-specific column information query."""
    try:
      result = await self._fetch(f"SHOW COLUMNS FROM '{table}'")
      # Return all columns including ts since it's now a proper field
      return [row[0] for row in result]
    except Exception:
      return []

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """QuestDB doesn't have separate databases, everything is in one namespace."""
    log_info(f"QuestDB database '{name}' ready (single namespace)")

  async def use_db(self, db: str):
    """QuestDB doesn't have separate databases."""
    log_info(
        f"QuestDB uses single namespace, ignoring database switch to '{db}'")

  async def list_tables(self) -> list[str]:
    """QuestDB-specific table listing."""
    await self.ensure_connected()
    try:
      result = await self._fetch("SHOW TABLES")
      return [row[0] for row in result]
    except Exception as e:
      log_error("Failed to list tables from QuestDB", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs in a single QuestDB query for efficiency"""
    await self.ensure_connected()
    try:
      if not uids:
        return []

      # Build parameterized query for QuestDB (similar to PostgreSQL syntax)
      placeholders = ",".join([f"'{uid}'" for uid in uids])
      query = f"SELECT * FROM {table} WHERE uid IN ({placeholders}) ORDER BY updated_at DESC"

      result = await self._fetch(query)
      return result if result else []
    except Exception as e:
      log_error(
          f"Failed to fetch batch records by IDs from QuestDB {table}: {e}")
      return []

  async def insert(self, ing: Ingester, table: str = ""):
    """QuestDB-specific insert using ILP (InfluxDB Line Protocol) for better performance."""
    await self.ensure_connected()
    table = table or ing.name

    # Convert to ILP format for better ingestion performance
    persistent_fields = [field for field in ing.fields if not field.transient]

    # Get timestamp from ts field
    ts_value = None
    for field in persistent_fields:
      if field.name == 'ts':
        ts_value = field.value
        break

    # Fallback to last_ingested if no ts field found
    if ts_value is None:
      ts_value = ing.last_ingested or now()

    # Build field values (excluding ts since it's handled separately as timestamp)
    field_values = []
    for field in persistent_fields:
      if field.name == 'ts':  # Skip ts field since it's handled as timestamp
        continue

      if field.type in ["string", "binary", "varbinary"]:
        field_values.append(f'{field.name}="{field.value}"')
      elif field.type in [
          "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64",
          "uint64"
      ]:
        field_values.append(f'{field.name}={field.value}i')
      elif field.type in ["float32", "ufloat32", "float64", "ufloat64"]:
        field_values.append(f'{field.name}={field.value}')
      elif field.type == "bool":
        field_values.append(f'{field.name}={"t" if field.value else "f"}')
      else:
        field_values.append(f'{field.name}={field.value}')

    # Convert timestamp to nanoseconds
    if ts_value is None:
      raise Exception("No timestamp available for ingester")
    timestamp_ns = int(ts_value.timestamp() * 1_000_000_000)

    # ILP format: table_name field_name=value timestamp
    ilp_line = f'{table} {",".join(field_values)} {timestamp_ns}'

    try:
      if self.session is None:
        raise Exception("QuestDB session not connected")
      resp = await self.session.post(f"{self.base_url}/write",
                                     content=ilp_line,
                                     headers={"Content-Type": "text/plain"})
      if resp.status_code not in [200, 204]:
        error_text = resp.text
        raise Exception(
            f"QuestDB ILP insert failed: {resp.status_code} - {error_text}")
    except Exception as e:
      error_message = str(e).lower()
      if "does not exist" in error_message:
        log_info(f"Table {table} does not exist, creating it now...")
        await self.create_table(ing, name=table)
        # Retry the insert
        if self.session is None:
          raise Exception("QuestDB session not connected")
        resp = await self.session.post(f"{self.base_url}/write",
                                       content=ilp_line,
                                       headers={"Content-Type": "text/plain"})
        if resp.status_code not in [200, 204]:
          error_text = resp.text
          raise Exception(
              f"QuestDB ILP insert failed: {resp.status_code} - {error_text}")
      else:
        log_error(f"Failed to insert data into {table}", e)
        raise e
