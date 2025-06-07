from asyncio import gather
from datetime import datetime, timezone
from os import environ as env
from typing import Dict, Any
from abc import ABC, abstractmethod

from ..utils import log_error, log_info, log_warn, Interval, ago, now
from ..model import Ingester, FieldType, Tsdb

UTC = timezone.utc


class SqlAdapter(Tsdb, ABC):
  """
  Base adapter for SQL-based time series databases.
  This provides common functionality for databases like TimescaleDB, QuestDB,
  ClickHouse, SQLite, etc.
  """

  # Type mappings - to be overridden by subclasses
  TYPES: Dict[FieldType, str] = {}

  # Connection object - type varies by database
  conn: Any = None
  pool: Any = None

  def __init__(self,
               host: str = "localhost",
               port: int = 5432,
               db: str = "default",
               user: str = "admin",
               password: str = "pass"):
    super().__init__(host=host, port=port, db=db, user=user, password=password)

  @property
  @abstractmethod
  def timestamp_column_type(self) -> str:
    """Return the SQL type for timestamp columns (e.g., 'TIMESTAMP', 'DATETIME')"""
    pass

  @property
  def timestamp_column_name(self) -> str:
    """Return the name of the timestamp column (defaults to 'ts')"""
    return "ts"

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "SqlAdapter":
    """Factory method to create and connect to database."""
    self = cls(host=host or env.get("DB_HOST") or "localhost",
               port=int(port) if port is not None else int(
                   env.get("DB_PORT") or 5432),
               db=db or env.get("DB_NAME") or "default",
               user=user or env.get("DB_RW_USER") or "admin",
               password=password or env.get("DB_RW_PASS") or "pass")
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    """Test database connectivity."""
    try:
      await self.ensure_connected()
      await self._execute("SELECT 1")
      return True
    except Exception as e:
      log_error(f"{self.__class__.__name__} ping failed", e)
      return False

  async def close(self):
    """Close database connections."""
    if self.pool:
      await self._close_pool()
    if self.conn:
      await self._close_connection()

  @abstractmethod
  async def _connect(self) -> Any:
    """Create and return a database connection. Implementation varies by database."""
    pass

  async def _close_connection(self):
    """Close single connection. Can be overridden by subclasses."""
    if hasattr(self.conn, 'close'):
      if hasattr(self.conn.close, '__call__'):
        if hasattr(self.conn.close, '__await__'):
          await self.conn.close()
        else:
          self.conn.close()
    self.conn = None

  async def _close_pool(self):
    """Close connection pool. Can be overridden by subclasses."""
    if hasattr(self.pool, 'close'):
      if hasattr(self.pool.close, '__call__'):
        if hasattr(self.pool.close, '__await__'):
          await self.pool.close()
        else:
          self.pool.close()
    self.pool = None

  @abstractmethod
  async def _execute(self, query: str, params: tuple = ()) -> Any:
    """Execute a query. Implementation varies by database."""
    pass

  @abstractmethod
  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """Execute a query and fetch results. Implementation varies by database."""
    pass

  @abstractmethod
  async def _executemany(self, query: str, params_list: list[tuple]) -> Any:
    """Execute a query with multiple parameter sets. Implementation varies by database."""
    pass

  def _quote_identifier(self, identifier: str) -> str:
    """Quote an identifier (table/column name). Can be overridden by subclasses."""
    return f'"{identifier}"'

  def _format_timestamp(self, timestamp: datetime) -> str:
    """Format timestamp for SQL queries. Can be overridden by subclasses."""
    return f"'{timestamp.isoformat()}'"

  def _escape_string_value(self, value: Any, field_type: FieldType) -> str:
    """Escape and format a value for SQL insertion. Can be overridden by subclasses."""
    if field_type in ["string", "binary", "varbinary"]:
      # Escape single quotes
      escaped_value = str(value).replace("'", "''")
      return f"'{escaped_value}'"
    elif field_type == "bool":
      return "TRUE" if value else "FALSE"
    elif value is None:
      return "NULL"
    else:
      return str(value)

  def _build_create_table_sql(self, c: Ingester, table_name: str) -> str:
    """Build CREATE TABLE SQL. Can be overridden by subclasses for specific features."""
    persistent_fields = [field for field in c.fields if not field.transient]
    fields = []

    # Add timestamp column
    fields.append(
        f"{self._quote_identifier(self.timestamp_column_name)} {self.timestamp_column_type}"
    )

    # Add data fields
    for field in persistent_fields:
      field_type = self.TYPES.get(field.type, "TEXT")
      fields.append(f"{self._quote_identifier(field.name)} {field_type}")

    fields_sql = ",\n      ".join(fields)

    return f"""
    CREATE TABLE IF NOT EXISTS {self._quote_identifier(table_name)} (
      {fields_sql}
    )
    """

  def _build_insert_sql(self, c: Ingester,
                        table_name: str) -> tuple[str, list[Any]]:
    """Build INSERT SQL and parameters."""
    persistent_fields = [field for field in c.fields if not field.transient]

    columns = [self.timestamp_column_name
               ] + [field.name for field in persistent_fields]
    quoted_columns = [self._quote_identifier(col) for col in columns]

    # Build placeholders - this may need to be overridden for different parameter styles
    placeholders = self._build_placeholders(len(columns))

    insert_sql = f"""
    INSERT INTO {self._quote_identifier(table_name)}
    ({', '.join(quoted_columns)})
    VALUES ({placeholders})
    """

    # Build parameter values
    params = [c.last_ingested] + [field.value for field in persistent_fields]

    return insert_sql, params

  def _build_placeholders(self, count: int) -> str:
    """Build parameter placeholders. Can be overridden for different styles (?, $1, %s, etc.)"""
    return ", ".join(["?" for _ in range(count)])

  def _build_aggregation_sql(
      self, table_name: str, columns: list[str], from_date: datetime,
      to_date: datetime,
      aggregation_interval: Interval) -> tuple[str, list[Any]]:
    """Build aggregation query SQL. Should be overridden by subclasses for database-specific syntax."""

    # Basic SQL aggregation - subclasses should override for time bucket functions
    quoted_table = self._quote_identifier(table_name)
    quoted_ts = self._quote_identifier(self.timestamp_column_name)

    if not columns:
      select_cols = [
          f"LAST({self._quote_identifier(col)}) AS {self._quote_identifier(col)}"
          for col in ["*"]
      ]
    else:
      select_cols = [quoted_ts] + [
          f"LAST({self._quote_identifier(col)}) AS {self._quote_identifier(col)}"
          for col in columns
      ]

    select_clause = ", ".join(select_cols)

    query = f"""
    SELECT {select_clause}
    FROM {quoted_table}
    WHERE {quoted_ts} >= ? AND {quoted_ts} <= ?
    ORDER BY {quoted_ts} DESC
    """

    return query, [from_date, to_date]

  async def ensure_connected(self):
    if not self.conn and not self.pool:
      await self._connect()

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """Create database. Default implementation - can be overridden."""
    try:
      await self.ensure_connected()

      if force:
        drop_sql = f'DROP DATABASE IF EXISTS {self._quote_identifier(name)}'
        await self._execute(drop_sql)

      create_sql = f'CREATE DATABASE IF NOT EXISTS {self._quote_identifier(name)}'
      await self._execute(create_sql)
      log_info(f"Created database {name}")
    except Exception as e:
      log_error(f"Failed to create database {name}", e)
      raise e

  async def use_db(self, db: str):
    """Switch to different database. Default implementation - can be overridden."""
    try:
      await self.ensure_connected()
      use_sql = f'USE {self._quote_identifier(db)}'
      await self._execute(use_sql)
      self.db = db
      log_info(f"Switched to database {db}")
    except Exception as e:
      log_error(f"Failed to switch to database {db}", e)
      raise e

  async def create_table(self, c: Ingester, name: str = ""):
    await self.ensure_connected()
    table = name or c.name

    create_sql = self._build_create_table_sql(c, table)

    try:
      await self._execute(create_sql)
      log_info(f"Created table {self.db}.{table}")
    except Exception as e:
      log_error(f"Failed to create table {self.db}.{table}", e)
      raise e

  async def insert(self, c: Ingester, table: str = ""):
    await self.ensure_connected()
    table = table or c.name

    insert_sql, params = self._build_insert_sql(c, table)

    try:
      await self._execute(insert_sql, tuple(params))
    except Exception as e:
      error_message = str(e).lower()
      if any(phrase in error_message
             for phrase in ["does not exist", "no such table", "relation"]):
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self._execute(insert_sql, tuple(params))
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  async def insert_many(self,
                        c: Ingester,
                        values: list[tuple],
                        table: str = ""):
    await self.ensure_connected()
    table = table or c.name

    persistent_fields = [field for field in c.fields if not field.transient]
    columns = [self.timestamp_column_name
               ] + [field.name for field in persistent_fields]
    quoted_columns = [self._quote_identifier(col) for col in columns]

    placeholders = self._build_placeholders(len(columns))

    insert_sql = f"""
    INSERT INTO {self._quote_identifier(table)}
    ({', '.join(quoted_columns)})
    VALUES ({placeholders})
    """

    try:
      await self._executemany(insert_sql, values)
    except Exception as e:
      error_message = str(e).lower()
      if any(phrase in error_message
             for phrase in ["does not exist", "no such table", "relation"]):
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self._executemany(insert_sql, values)
      else:
        log_error(f"Failed to batch insert data into {self.db}.{table}", e)
        raise e

  async def fetch(self,
                  table: str,
                  from_date: datetime | None = None,
                  to_date: datetime | None = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    await self.ensure_connected()

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    # Get columns if not specified
    if not columns:
      columns = await self._get_table_columns(table)

    if not columns:
      return ([], [])

    query, params = self._build_aggregation_sql(table, columns, from_date,
                                                to_date, aggregation_interval)

    try:
      result = await self._fetch(query, tuple(params))
      result_columns = [self.timestamp_column_name] + columns
      return (result_columns, result)
    except Exception as e:
      log_error(f"Failed to fetch data from {self.db}.{table}", e)
      raise e

  async def _get_table_columns(self, table: str) -> list[str]:
    """Get column names for a table. Should be overridden by subclasses for database-specific syntax."""
    # Generic SQL information schema query
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = ? AND column_name != ?
    ORDER BY ordinal_position
    """

    try:
      result = await self._fetch(query, (table, self.timestamp_column_name))
      return [row[0] for row in result]
    except Exception:
      return []

  async def fetch_batch(
      self,
      tables: list[str],
      from_date: datetime | None = None,
      to_date: datetime | None = None,
      aggregation_interval: Interval = "m5",
      columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    results = await gather(*[
        self.fetch(table, from_date, to_date, aggregation_interval, columns)
        for table in tables
    ])

    # Combine results from all tables
    all_columns: list[str] = []
    all_data: list[tuple] = []

    for columns_result, data in results:
      if not all_columns:
        all_columns = columns_result
      all_data.extend(data)

    return (all_columns, all_data)

  async def fetchall(self):
    # SQL databases need table name for queries
    raise NotImplementedError("fetchall requires table name for SQL databases")

  async def commit(self):
    # Most modern SQL databases auto-commit, but can be overridden
    pass

  async def list_tables(self) -> list[str]:
    await self.ensure_connected()

    # Generic SQL information schema query
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' OR table_schema = ?
    ORDER BY table_name
    """

    try:
      result = await self._fetch(query, (self.db, ))
      return [row[0] for row in result]
    except Exception as e:
      log_error(f"Failed to list tables from {self.db}", e)
      return []

  async def alter_table(self,
                        table: str,
                        add_columns: list[tuple[str, str]] = [],
                        drop_columns: list[str] = []):
    """Alter table structure. Can be overridden by subclasses for database-specific syntax."""
    await self.ensure_connected()

    for column_name, column_type in add_columns:
      try:
        # Determine field_type from column_type
        from typing import cast
        if column_type in self.TYPES:
          sql_type = self.TYPES[cast(FieldType, column_type)]
        else:
          sql_type = 'TEXT'
        sql = f"ALTER TABLE {self._quote_identifier(table)} ADD COLUMN {self._quote_identifier(column_name)} {sql_type}"
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
        sql = f"ALTER TABLE {self._quote_identifier(table)} DROP COLUMN {self._quote_identifier(column_name)}"
        await self._execute(sql)
        log_info(f"Dropped column {column_name} from {self.db}.{table}")
      except Exception as e:
        log_error(
            f"Failed to drop column {column_name} from {self.db}.{table}", e)
        raise e

  async def get_columns(self, table: str) -> list[tuple]:
    """Get table column information. Default implementation - can be overridden."""
    columns = await self._get_table_columns(table)
    return [(col, "TEXT")
            for col in columns]  # Return tuples with default type
