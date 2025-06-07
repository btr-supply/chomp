from os import environ as env
import aiosqlite

from ..utils import log_info, log_error
from ..model import FieldType
from .sql import SqlAdapter

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
  conn: aiosqlite.Connection | None = None
  cursor: aiosqlite.Cursor | None = None

  def __init__(self,
               host: str = "localhost",
               port: int = 0,
               db: str = "./data.db",
               user: str = "",
               password: str = ""):
    """Initialize SQLite adapter with SQLite-specific defaults."""
    super().__init__(host=host, port=port, db=db, user=user, password=password)

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
    self = cls(host=host or env.get("SQLITE_HOST", "localhost"),
               port=int(port or env.get("SQLITE_PORT", 0)),
               db=db or env.get("SQLITE_DB", "./data.db"),
               user=user or env.get("DB_RW_USER", ""),
               password=password or env.get("DB_RW_PASS", ""))
    await self.ensure_connected()
    return self

  async def _connect(self):
    """SQLite-specific connection using file path."""
    self.conn = await aiosqlite.connect(self.db)
    self.conn.row_factory = aiosqlite.Row
    self.cursor = await self.conn.cursor()
    log_info(f"Connected to SQLite database: {self.db}")

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
      return [row[1] for row in result if row[1] != self.timestamp_column_name]
    except Exception:
      return []

  async def list_tables(self) -> list[str]:
    """SQLite-specific table listing."""
    await self.ensure_connected()
    try:
      result = await self._fetch(
          "SELECT name FROM sqlite_master WHERE type='table'")
      return [row[0] for row in result]
    except Exception as e:
      log_error(f"Failed to list tables from {self.db}", e)
      return []
