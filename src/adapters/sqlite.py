from os import environ as env

from ..models.base import FieldType
from ..models.ingesters import UpdateIngester
from .sql import SqlAdapter
from ..utils import log_info, log_error
import aiosqlite  # happy mypy

# SQLite data type mapping
TYPES: dict[FieldType, str] = {
    "int8": "INTEGER",
    "uint8": "INTEGER",
    "int16": "INTEGER",
    "uint16": "INTEGER",
    "int32": "INTEGER",
    "uint32": "INTEGER",
    "int64": "INTEGER",
    "uint64": "INTEGER",
    "float32": "REAL",
    "ufloat32": "REAL",
    "float64": "REAL",
    "ufloat64": "REAL",
    "bool": "INTEGER",
    "timestamp": "TEXT",
    "string": "TEXT",
    "binary": "BLOB",
    "varbinary": "BLOB",
}


class SQLite(SqlAdapter):
  """SQLite adapter extending SqlAdapter."""

  TYPES = TYPES

  def __init__(self,
               host: str = "localhost",
               port: int = 0,
               db: str = "./data.db",
               user: str = "",
               password: str = ""):
    """Initialize SQLite adapter with SQLite-specific defaults."""
    super().__init__(host, port, db, user, password)

  @property
  def timestamp_column_type(self) -> str:
    return "TEXT"

  @classmethod
  async def connect(cls,
                    host=None,
                    port=None,
                    db=None,
                    user=None,
                    password=None) -> "SQLite":
    # Use **kwargs approach for cleaner constructor call
    params = {
        'host': host or env.get("SQLITE_HOST", "localhost"),
        'port': int(port or env.get("SQLITE_PORT", 0)),
        'db': db or env.get("SQLITE_DB", "./data.db"),
        'user': user or env.get("DB_RW_USER", ""),
        'password': password or env.get("DB_RW_PASS", "")
    }

    self = cls(**params)
    await self.ensure_connected()
    return self

  async def _connect(self):
    """SQLite-specific connection using aiosqlite."""
    conn = await aiosqlite.connect(self.db)
    log_info(f"Connected to SQLite database: {self.db}")
    return conn

  async def _close_connection(self):
    """SQLite-specific connection closing."""
    if self.cursor:
      await self.cursor.close()
    if self.conn:
      await self.conn.close()
    self.conn = None
    self.cursor = None

  async def _execute(self, query: str, params: tuple = ()):
    """Execute SQLite query."""
    if self.cursor is None or self.conn is None:
      raise RuntimeError("SQLite connection not established")
    await self.cursor.execute(query, params)
    await self.conn.commit()

  async def _fetch(self, query: str, params: tuple = ()) -> list[tuple]:
    """Execute SQLite query and fetch results."""
    if self.cursor is None:
      raise RuntimeError("SQLite connection not established")
    await self.cursor.execute(query, params)
    result = await self.cursor.fetchall()
    return [tuple(row) for row in result]

  async def _executemany(self, query: str, params_list: list[tuple]):
    """Execute many SQLite queries."""
    if self.cursor is None or self.conn is None:
      raise RuntimeError("SQLite connection not established")
    await self.cursor.executemany(query, params_list)
    await self.conn.commit()

  def _quote_identifier(self, identifier: str) -> str:
    """SQLite uses backticks for identifiers."""
    return f"`{identifier}`"

  async def upsert(self, ing: UpdateIngester, table: str = "", uid: str = ""):
    """Upsert (INSERT OR REPLACE) record for SQLite."""
    await self.ensure_connected()
    table = table or ing.name
    uid = uid or ing.uid

    if not uid:
      raise ValueError("UID is required for upsert operations")

    # Get non-transient fields and their values
    field_names = ing.get_persistent_field_names()
    field_values = ing.get_persistent_field_values()

    # Ensure uid is in the values
    if 'uid' not in field_names:
      field_names.append('uid')
      field_values.append(uid)

    placeholders = ", ".join(["?"] * len(field_values))
    columns = ", ".join([self._quote_identifier(name) for name in field_names])

    # Use INSERT OR REPLACE for upsert behavior
    sql = f"INSERT OR REPLACE INTO {self._quote_identifier(table)} ({columns}) VALUES ({placeholders})"

    try:
      await self._execute(sql, tuple(field_values))
      log_info(f"Upserted record with uid {uid} into {table}")
    except Exception as e:
      log_error(f"Failed to upsert into {table}", e)
      raise e

  async def fetch_by_id(self, table: str, uid: str):
    """Fetch single record by UID from SQLite."""
    await self.ensure_connected()

    try:
      sql = f"SELECT * FROM {self._quote_identifier(table)} WHERE uid = ? LIMIT 1"
      result = await self._fetch(sql, (uid, ))

      if result:
        # Get column names
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, result[0]))
      return None

    except Exception as e:
      log_error(f"Failed to fetch record with uid {uid} from {table}", e)
      return None

  async def fetchall(self):
    """Fetch all results from last executed query."""
    try:
      if self.cursor is None:
        raise RuntimeError("SQLite connection not established")
      return await self.cursor.fetchall()
    except Exception as e:
      log_error("Failed to fetch all results", e)
      return []

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """SQLite database is the file itself, so this is essentially a no-op."""
    log_info(f"SQLite database '{name}' ready (file-based)")

  async def use_db(self, db: str):
    """SQLite uses file-based databases, so we'd need to reconnect."""
    if self.db != db:
      await self.close()
      self.db = db
      await self.ensure_connected()

  async def alter_table(self,
                        table: str,
                        add_columns: list[tuple[str, str]] = [],
                        drop_columns: list[str] = []):
    """SQLite-specific ALTER TABLE (no DROP COLUMN support in older versions)."""
    await self.ensure_connected()

    for column_name, column_type in add_columns:
      try:
        sql = f"ALTER TABLE `{table}` ADD COLUMN `{column_name}` {column_type}"
        await self._execute(sql)
        log_info(
            f"Added column {column_name} of type {column_type} to {table}")
      except Exception as e:
        log_error(f"Failed to add column {column_name} to {table}", e)
        raise e

    # SQLite doesn't support DROP COLUMN in older versions
    if drop_columns:
      log_error(
          f"SQLite doesn't support dropping columns directly. Columns {drop_columns} will remain in table {table}"
      )

  async def _get_table_columns(self, table: str) -> list[str]:
    """SQLite-specific column information query."""
    try:
      result = await self._fetch("PRAGMA table_info(?)", (table, ))
      # Return all columns including ts since it's now a proper field
      return [row[1] for row in result]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """SQLite-specific table listing."""
    try:
      return [
          row[0] for row in await self._fetch(
              "SELECT name FROM sqlite_master WHERE type='table'")
      ]
    except Exception as e:
      log_error("Failed to list tables from SQLite", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs in a single SQLite query for efficiency"""
    try:
      if not uids:
        return []

      # Build parameterized query for SQLite
      placeholders = ",".join(["?" for _ in uids])
      query = f"SELECT * FROM {table} WHERE uid IN ({placeholders}) ORDER BY updated_at DESC"

      result = await self._fetch(query, tuple(uids))
      return result if result else []
    except Exception as e:
      log_error(
          f"Failed to fetch batch records by IDs from SQLite {table}: {e}")
      return []
