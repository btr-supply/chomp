"""Tests for adapters.sqlite module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from os import environ as env

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.adapters.sqlite import SQLite, TYPES


class TestSQLiteAdapter:
  """Test SQLite adapter."""

  def test_types_mapping(self):
    """Test SQLite type mapping."""
    assert TYPES["int32"] == "INTEGER"
    assert TYPES["float64"] == "REAL"
    assert TYPES["string"] == "TEXT"
    assert TYPES["binary"] == "BLOB"
    assert TYPES["bool"] == "INTEGER"

  def test_inheritance(self):
    """Test SQLite inherits from SqlAdapter."""
    from src.adapters.sql import SqlAdapter
    assert issubclass(SQLite, SqlAdapter)

  def test_initialization(self):
    """Test SQLite adapter initialization."""
    adapter = SQLite(host="localhost",
                     port=0,
                     db="test.db",
                     user="",
                     password="")

    assert adapter.host == "localhost"
    assert adapter.port == 0
    assert adapter.db == "test.db"
    assert adapter.user == ""
    assert adapter.password == ""
    assert adapter.TYPES == TYPES

  def test_initialization_defaults(self):
    """Test SQLite adapter with defaults."""
    adapter = SQLite()

    assert adapter.host == "localhost"
    assert adapter.port == 0
    assert adapter.db == "./data.db"
    assert adapter.user == ""
    assert adapter.password == ""

  def test_timestamp_column_type(self):
    """Test timestamp column type property."""
    adapter = SQLite()
    assert adapter.timestamp_column_type == "TEXT"

  @pytest.mark.asyncio
  async def test_connect_with_defaults(self):
    """Test connect with default parameters."""
    with patch.dict(env, {}, clear=True), \
         patch.object(SQLite, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      adapter = await SQLite.connect()

      assert adapter.host == "localhost"
      assert adapter.port == 0
      assert adapter.db == "./data.db"
      assert adapter.user == ""
      assert adapter.password == ""
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_env_vars(self):
    """Test connect with environment variables."""
    env_vars = {
        'SQLITE_HOST': 'sqlite-host',
        'SQLITE_PORT': '1234',
        'SQLITE_DB': '/path/to/test.db',
        'DB_RW_USER': 'test-user',
        'DB_RW_PASS': 'test-pass'
    }

    with patch.dict(env, env_vars), \
         patch.object(SQLite, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      adapter = await SQLite.connect()

      assert adapter.host == "sqlite-host"
      assert adapter.port == 1234
      assert adapter.db == "/path/to/test.db"
      assert adapter.user == "test-user"
      assert adapter.password == "test-pass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_parameters(self):
    """Test connect with explicit parameters."""
    with patch.object(SQLite, 'ensure_connected',
                      new_callable=AsyncMock) as mock_ensure:

      adapter = await SQLite.connect(host="custom-host",
                                     port=9999,
                                     db="custom.db",
                                     user="custom-user",
                                     password="custom-pass")

      assert adapter.host == "custom-host"
      assert adapter.port == 9999
      assert adapter.db == "custom.db"
      assert adapter.user == "custom-user"
      assert adapter.password == "custom-pass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_sqlite_connect(self):
    """Test SQLite-specific connection."""
    adapter = SQLite(db=":memory:")

    with patch('aiosqlite.connect') as mock_connect, \
         patch('src.adapters.sqlite.log_info') as mock_log:

      mock_conn = AsyncMock()
      mock_cursor = AsyncMock()
      mock_conn.cursor.return_value = mock_cursor
      mock_connect.return_value = mock_conn

      await adapter._connect()

      mock_connect.assert_called_once_with(":memory:")
      assert adapter.conn == mock_conn
      assert adapter.cursor == mock_cursor
      mock_log.assert_called_once_with(
          "Connected to SQLite database: :memory:")

  @pytest.mark.asyncio
  async def test_close_connection(self):
    """Test SQLite connection closing."""
    adapter = SQLite()
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    adapter.cursor = mock_cursor
    adapter.conn = mock_conn

    await adapter._close_connection()

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
    assert adapter.cursor is None
    assert adapter.conn is None

  @pytest.mark.asyncio
  async def test_execute(self):
    """Test SQLite query execution."""
    adapter = SQLite()
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    adapter.cursor = mock_cursor
    adapter.conn = mock_conn

    await adapter._execute("INSERT INTO test VALUES (?)", ("value", ))

    mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (?)",
                                                ("value", ))
    mock_conn.commit.assert_called_once()

  @pytest.mark.asyncio
  async def test_execute_no_connection(self):
    """Test execute with no connection raises error."""
    adapter = SQLite()
    adapter.cursor = None
    adapter.conn = None

    with pytest.raises(RuntimeError,
                       match="SQLite connection not established"):
      await adapter._execute("SELECT 1")

  @pytest.mark.asyncio
  async def test_fetch(self):
    """Test SQLite query fetch."""
    adapter = SQLite()
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    # Mock Row-like objects
    mock_row1 = Mock()
    mock_row1.__iter__ = Mock(return_value=iter(["value1", "value2"]))
    mock_row2 = Mock()
    mock_row2.__iter__ = Mock(return_value=iter(["value3", "value4"]))

    mock_cursor.fetchall.return_value = [mock_row1, mock_row2]

    result = await adapter._fetch("SELECT * FROM test", ("param", ))

    mock_cursor.execute.assert_called_once_with("SELECT * FROM test",
                                                ("param", ))
    assert result == [("value1", "value2"), ("value3", "value4")]

  @pytest.mark.asyncio
  async def test_executemany(self):
    """Test SQLite executemany."""
    adapter = SQLite()
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    adapter.cursor = mock_cursor
    adapter.conn = mock_conn

    params_list = [("val1", ), ("val2", )]

    await adapter._executemany("INSERT INTO test VALUES (?)", params_list)

    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO test VALUES (?)", params_list)
    mock_conn.commit.assert_called_once()

  def test_quote_identifier(self):
    """Test SQLite identifier quoting."""
    adapter = SQLite()
    assert adapter._quote_identifier("table_name") == "`table_name`"
    assert adapter._quote_identifier("column") == "`column`"

  @pytest.mark.asyncio
  async def test_create_db(self):
    """Test SQLite database creation (no-op)."""
    adapter = SQLite()

    with patch('src.adapters.sqlite.log_info') as mock_log:
      await adapter.create_db("test_db")
      mock_log.assert_called_once_with(
          "SQLite database 'test_db' ready (file-based)")

  @pytest.mark.asyncio
  async def test_use_db_same_db(self):
    """Test use_db with same database."""
    adapter = SQLite(db="test.db")

    with patch.object(adapter, 'close') as mock_close, \
         patch.object(adapter, 'ensure_connected') as mock_ensure:

      await adapter.use_db("test.db")

      mock_close.assert_not_called()
      mock_ensure.assert_not_called()

  @pytest.mark.asyncio
  async def test_use_db_different_db(self):
    """Test use_db with different database."""
    adapter = SQLite(db="old.db")

    with patch.object(adapter, 'close', new_callable=AsyncMock) as mock_close, \
         patch.object(adapter, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      await adapter.use_db("new.db")

      assert adapter.db == "new.db"
      mock_close.assert_called_once()
      mock_ensure.assert_called_once()
