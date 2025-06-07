# KX kdb+ adapter for time series data storage and retrieval

from datetime import datetime, timezone
from os import environ as env
from typing import Any

from ..utils import log_error, log_info, log_warn, Interval, ago, now
from ..model import Ingester, FieldType, Tsdb
from ..deps import lazy_import

UTC = timezone.utc

# KX kdb+ data type mapping
TYPES: dict[FieldType, str] = {
    "int8": "short",  # kdb+ doesn't have 8-bit types, use 16-bit
    "uint8": "short",  # kdb+ doesn't have unsigned types
    "int16": "short",  # 16-bit integer (h)
    "uint16": "int",  # use 32-bit for unsigned 16-bit
    "int32": "int",  # 32-bit integer (i)
    "uint32": "long",  # use 64-bit for unsigned 32-bit
    "int64": "long",  # 64-bit integer (j)
    "uint64": "long",  # kdb+ doesn't have unsigned types
    "float32": "real",  # 32-bit float (e)
    "ufloat32": "real",  # kdb+ doesn't have unsigned types
    "float64": "float",  # 64-bit float (f)
    "ufloat64": "float",  # kdb+ doesn't have unsigned types
    "bool": "boolean",  # boolean (b)
    "timestamp": "timestamp",  # timestamp (p)
    "string": "symbol",  # symbol (s)
    "binary": "byte",  # byte array (x)
    "varbinary": "byte",  # byte array (x)
}

# KX kdb+ interval mapping for time-based aggregations
INTERVALS: dict[Interval, str] = {
    "s1": "0D00:00:01",
    "s2": "0D00:00:02",
    "s5": "0D00:00:05",
    "s10": "0D00:00:10",
    "s15": "0D00:00:15",
    "s30": "0D00:00:30",
    "m1": "0D00:01:00",
    "m2": "0D00:02:00",
    "m5": "0D00:05:00",
    "m10": "0D00:10:00",
    "m15": "0D00:15:00",
    "m30": "0D00:30:00",
    "h1": "0D01:00:00",
    "h2": "0D02:00:00",
    "h4": "0D04:00:00",
    "h6": "0D06:00:00",
    "h8": "0D08:00:00",
    "h12": "0D12:00:00",
    "D1": "1D00:00:00",
    "D2": "2D00:00:00",
    "D3": "3D00:00:00",
    "W1": "7D00:00:00",
    "M1": "1m",  # kdb+ monthly aggregation
    "Y1": "1y",  # kdb+ yearly aggregation
}


class Kx(Tsdb):
  """
  KX kdb+ adapter for high-performance time series data storage and analytics.

  This adapter provides integration with kdb+ database using PyKX library,
  optimized for real-time analytics and historical data queries.
  """

  conn: Any = None

  def __init__(self,
               host: str = "localhost",
               port: int = 5000,
               db: str = "default",
               user: str = "admin",
               password: str = "pass"):
    super().__init__(host, port, db, user, password)
    self._pykx = None

  @property
  def pykx(self):
    """Lazy load PyKX to avoid import errors if not installed."""
    if self._pykx is None:
      self._pykx = lazy_import("pykx", "pykx", "kx")
    return self._pykx

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "Kx":
    """Factory method to create and connect to kdb+ database."""
    self = cls(host=host or env.get("KX_HOST") or "localhost",
               port=int(port or env.get("KX_PORT") or 5000),
               db=db or env.get("KX_DB") or "default",
               user=user or env.get("DB_RW_USER") or "admin",
               password=password or env.get("DB_RW_PASS") or "pass")
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    """Test database connectivity."""
    try:
      await self.ensure_connected()
      result = await self._execute("1+1")
      return result == 2
    except Exception as e:
      log_error("KX ping failed", e)
      return False

  async def ensure_connected(self):
    """Ensure connection to kdb+ database."""
    if not self.conn:
      await self._connect()

  async def _connect(self):
    """Establish connection to kdb+ database."""
    try:
      # Use PyKX to establish connection
      kx = self.pykx

      # Create connection with authentication if provided
      self.conn = kx.QConnection(
          host=self.host,
          port=self.port,
          username=self.user if self.user != "admin" else None,
          password=self.password if self.password != "pass" else None)

      log_info(
          f"Connected to KX kdb+ on {self.host}:{self.port} as {self.user}")

      # Test connection
      await self._execute("1+1")

    except Exception as e:
      raise ValueError(
          f"Failed to connect to KX kdb+ on {self.user}@{self.host}:{self.port}: {e}"
      )

  async def close(self):
    """Close database connection."""
    if self.conn:
      try:
        self.conn.close()
      except Exception as e:
        log_warn(f"Error closing KX connection: {e}")
      finally:
        self.conn = None

  async def _execute(self, query: str, *args) -> Any:
    """Execute q query with optional parameters."""
    await self.ensure_connected()

    try:
      # Execute query using PyKX
      if args:
        result = self.conn(query, *args)
      else:
        result = self.conn(query)
      return result
    except Exception as e:
      log_error(f"Failed to execute KX query: {query}", e)
      raise

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """
    kdb+ doesn't have explicit database creation like SQL databases.
    This is a no-op but logs a warning for consistency.
    """
    log_info(
        f"KX kdb+ uses namespaces/contexts instead of databases. Context '{name}' will be used as needed."
    )

  async def use_db(self, db: str):
    """Switch to a different namespace/context in kdb+."""
    log_info(f"KX kdb+ context switched to: {db}")
    self.db = db

  async def create_table(self, c: Ingester, name: str = ""):
    """Create a table in kdb+ for the ingester data."""
    table_name = name or c.name
    persistent_fields = [field for field in c.fields if not field.transient]

    try:
      # Build table schema
      columns = ["ts:timestamp$()"]  # timestamp column
      for field in persistent_fields:
        kx_type = TYPES.get(field.type, "symbol")
        columns.append(f"{field.name}:`{kx_type}$()")

      # Create table with proper schema
      table_def = f"{table_name}:([] {'; '.join(columns)})"
      await self._execute(table_def)

      log_info(f"Created KX table: {table_name}")

    except Exception as e:
      log_error(f"Failed to create KX table {table_name}", e)
      raise

  async def insert(self, c: Ingester, table: str = ""):
    """Insert single row of data into kdb+ table."""
    table_name = table or c.name
    persistent_fields = [field for field in c.fields if not field.transient]

    try:
      # Prepare timestamp
      ts = c.last_ingested or now()
      ts_ns = int(ts.timestamp() * 1_000_000_000)  # nanoseconds since epoch

      # Prepare values
      values: list[str] = [str(ts_ns)]
      for field in persistent_fields:
        value = field.value
        # Convert Python values to kdb+ compatible format
        if field.type in ["string", "binary", "varbinary"]:
          value = f"`{value}" if value else "`"
        elif field.type == "bool":
          value = "1b" if value else "0b"
        elif value is None:
          value = "0N"  # kdb+ null
        values.append(str(value))

      # Insert data
      insert_stmt = f"`{table_name} insert ({';'.join(map(str, values))})"
      await self._execute(insert_stmt)

    except Exception as e:
      log_error(f"Failed to insert data into KX table {table_name}", e)
      raise

  async def insert_many(self,
                        c: Ingester,
                        values: list[tuple],
                        table: str = ""):
    """Insert multiple rows of data into kdb+ table."""
    table_name = table or c.name

    if not values:
      return

    try:
      # Convert values to kdb+ format and insert in batch
      for value_tuple in values:
        # Create temporary ingester with these values
        temp_c = c
        for i, field in enumerate([f for f in c.fields if not f.transient]):
          if i < len(value_tuple):
            field.value = value_tuple[i]
        await self.insert(temp_c, table_name)

    except Exception as e:
      log_error(f"Failed to insert batch data into KX table {table_name}", e)
      raise

  async def fetch(self,
                  table: str,
                  from_date: datetime | None = None,
                  to_date: datetime | None = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    """Fetch data from kdb+ table with optional aggregation."""
    to_date = to_date or now()
    from_date = from_date or ago(years=1)

    try:
      # Convert dates to kdb+ timestamp format (nanoseconds)
      from_ts = int(from_date.timestamp() * 1_000_000_000)
      to_ts = int(to_date.timestamp() * 1_000_000_000)

      # Build column selection
      if columns:
        select_cols = ",".join(["ts"] + columns)
      else:
        select_cols = "*"

      # Build aggregation if interval specified
      if aggregation_interval and aggregation_interval in INTERVALS:
        interval_spec = INTERVALS[aggregation_interval]

        # Use kdb+ time-based aggregation
        if columns:
          agg_cols = ",".join([f"last {col}" for col in columns])
        else:
          agg_cols = "last price,last size"  # default aggregation

        query = f"select ts:{interval_spec} xbar ts,{agg_cols} from {table} where ts within ({from_ts};{to_ts})"
      else:
        # Simple select without aggregation
        query = f"select {select_cols} from {table} where ts within ({from_ts};{to_ts})"

      result = await self._execute(query)

      # Convert PyKX result to Python format
      if hasattr(result, 'pd'):
        df = result.pd()
        return df.columns.tolist(), [tuple(row) for row in df.values]
      else:
        # Handle different PyKX result types
        return [], []

    except Exception as e:
      log_error(f"Failed to fetch data from KX table {table}", e)
      raise

  async def fetch_batch(
      self,
      tables: list[str],
      from_date: datetime | None = None,
      to_date: datetime | None = None,
      aggregation_interval: Interval = "m5",
      columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    """Fetch data from multiple kdb+ tables."""
    all_columns: list[str] = []
    all_data: list[tuple] = []

    try:
      # Fetch from each table and combine results
      for table in tables:
        cols, data = await self.fetch(table, from_date, to_date,
                                      aggregation_interval, columns)
        if not all_columns:
          all_columns = cols
        all_data.extend(data)

      return all_columns, all_data

    except Exception as e:
      log_error(f"Failed to fetch batch data from KX tables {tables}", e)
      raise

  async def fetchall(self):
    """Fetch all data from current context. Not recommended for large tables."""
    log_warn(
        "fetchall() not recommended for kdb+ due to potential large datasets")
    return [], []

  async def commit(self):
    """kdb+ auto-commits, so this is a no-op."""
    pass

  async def list_tables(self) -> list[str]:
    """List all tables in current kdb+ context."""
    try:
      result = await self._execute("tables[]")
      if hasattr(result, 'py'):
        tables = result.py()
        return [str(table)
                for table in tables] if isinstance(tables, list) else []
      return []
    except Exception as e:
      log_error("Failed to list KX tables", e)
      return []

  def _format_timestamp(self, timestamp: datetime) -> str:
    """Format timestamp for kdb+ queries."""
    return str(int(timestamp.timestamp() * 1_000_000_000))  # nanoseconds

  def _escape_value(self, value: Any, field_type: FieldType) -> str:
    """Escape and format value for kdb+ insertion."""
    if value is None:
      return "0N"  # kdb+ null
    elif field_type in ["string", "binary", "varbinary"]:
      return f"`{value}"
    elif field_type == "bool":
      return "1b" if value else "0b"
    else:
      return str(value)
