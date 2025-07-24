"""Tests for adapters.clickhouse module."""
import pytest
import sys
from os import environ as env, path
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, path.join(path.dirname(__file__), '..'))

from chomp.src.utils.deps import safe_import

# Check if ClickHouse dependencies are available
asynch = safe_import("asynch")
CLICKHOUSE_AVAILABLE = asynch is not None

# Only import if dependencies are available
if CLICKHOUSE_AVAILABLE:
  from src.adapters.clickhouse import ClickHouse, TYPES, INTERVALS, PRECISION  # noqa: E402
  from src.models import Ingester  # noqa: E402


@pytest.mark.skipif(not CLICKHOUSE_AVAILABLE,
                    reason="ClickHouse dependencies not available (asynch)")
class TestClickHouseAdapter:
  """Test ClickHouse adapter functionality."""

  def test_clickhouse_imports(self):
    """Test that ClickHouse can be imported."""
    assert ClickHouse is not None
    assert TYPES is not None
    assert INTERVALS is not None
    assert PRECISION == "ms"

  def test_types_mapping(self):
    """Test ClickHouse type mapping."""
    assert TYPES["int8"] == "Int8"
    assert TYPES["uint8"] == "UInt8"
    assert TYPES["int32"] == "Int32"
    assert TYPES["uint32"] == "UInt32"
    assert TYPES["float64"] == "Float64"
    assert TYPES["string"] == "String"
    assert TYPES["bool"] == "UInt8"

  def test_intervals_mapping(self):
    """Test ClickHouse interval mapping."""
    assert INTERVALS["s1"] == "1"
    assert INTERVALS["m5"] == "300"
    assert INTERVALS["h1"] == "3600"
    assert INTERVALS["D1"] == "86400"
    assert INTERVALS["W1"] == "604800"

  def test_inheritance(self):
    """Test ClickHouse inherits from Tsdb."""
    from src.models import Tsdb
    assert issubclass(ClickHouse, Tsdb)

  @pytest.mark.asyncio
  async def test_connect_class_method(self):
    """Test ClickHouse connect class method."""
    with patch.dict(
        env, {
            'CLICKHOUSE_HOST': 'envhost',
            'CLICKHOUSE_PORT': '9000',
            'CLICKHOUSE_DB': 'envdb',
            'DB_RW_USER': 'envuser',
            'DB_RW_PASS': 'envpass'
        }):
      with patch.object(ClickHouse, 'ensure_connected',
                        new_callable=AsyncMock) as mock_ensure:
        adapter = await ClickHouse.connect()

        assert adapter.host == 'envhost'
        assert adapter.port == 9000
        assert adapter.db == 'envdb'
        assert adapter.user == 'envuser'
        assert adapter.password == 'envpass'
        mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_parameters(self):
    """Test ClickHouse connect with explicit parameters."""
    with patch.object(ClickHouse, 'ensure_connected',
                      new_callable=AsyncMock) as mock_ensure:
      adapter = await ClickHouse.connect(host="testhost",
                                         port=9000,
                                         db="testdb",
                                         user="testuser",
                                         password="testpass")

      assert adapter.host == "testhost"
      assert adapter.port == 9000
      assert adapter.db == "testdb"
      assert adapter.user == "testuser"
      assert adapter.password == "testpass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test successful ping."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      result = await adapter.ping()

      assert result is True
      mock_cursor.execute.assert_called_once_with("SELECT 1")

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test ping failure."""
    adapter = ClickHouse()

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock, side_effect=Exception("Connection failed")), \
         patch('src.adapters.clickhouse.log_error') as mock_log_error:

      result = await adapter.ping()

      assert result is False
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_close(self):
    """Test closing connection."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    adapter.cursor = mock_cursor
    adapter.conn = mock_conn

    await adapter.close()

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_ensure_connected_success(self):
    """Test successful connection establishment."""
    adapter = ClickHouse(host="localhost",
                         port=9000,
                         db="testdb",
                         user="testuser",
                         password="testpass")

    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch('src.adapters.clickhouse.connect', new_callable=AsyncMock, return_value=mock_conn) as mock_connect, \
         patch('src.adapters.clickhouse.log_info') as mock_log_info:

      await adapter.ensure_connected()

      mock_connect.assert_called_once_with(host="localhost",
                                           port=9000,
                                           database="testdb",
                                           user="testuser",
                                           password="testpass")
      assert adapter.conn == mock_conn
      assert adapter.cursor == mock_cursor
      mock_log_info.assert_called_once()

  @pytest.mark.asyncio
  async def test_ensure_connected_database_not_exist(self):
    """Test connection when database doesn't exist."""
    adapter = ClickHouse(host="localhost",
                         port=9000,
                         db="nonexistent",
                         user="testuser",
                         password="testpass")

    mock_temp_conn = AsyncMock()
    mock_temp_cursor = AsyncMock()
    mock_temp_conn.cursor.return_value = mock_temp_cursor

    mock_final_conn = AsyncMock()
    mock_final_cursor = AsyncMock()
    mock_final_conn.cursor.return_value = mock_final_cursor

    # First connect fails, second (temp) succeeds, third (final) succeeds
    with patch('src.adapters.clickhouse.connect', new_callable=AsyncMock) as mock_connect, \
         patch('src.adapters.clickhouse.log_warn') as mock_log_warn, \
         patch('src.adapters.clickhouse.log_info'):

      mock_connect.side_effect = [
          Exception("Database doesn't exist"), mock_temp_conn, mock_final_conn
      ]

      await adapter.ensure_connected()

      # Should create database and reconnect
      assert mock_connect.call_count == 3
      mock_log_warn.assert_called_once()
      mock_temp_cursor.execute.assert_called_once_with(
          "CREATE DATABASE IF NOT EXISTS nonexistent")
      assert adapter.conn == mock_final_conn
      assert adapter.cursor == mock_final_cursor

  @pytest.mark.asyncio
  async def test_ensure_connected_other_error(self):
    """Test connection with other errors."""
    adapter = ClickHouse()

    with patch('src.adapters.clickhouse.connect',
               new_callable=AsyncMock,
               side_effect=Exception("Connection refused")):
      with pytest.raises(ValueError) as exc_info:
        await adapter.ensure_connected()

      assert "Failed to connect to ClickHouse" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_get_dbs(self):
    """Test getting databases."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [("db1", ), ("db2", )]
    adapter.cursor = mock_cursor

    result = await adapter.get_dbs()

    assert result == [("db1", ), ("db2", )]
    mock_cursor.execute.assert_called_once_with("SHOW DATABASES")

  @pytest.mark.asyncio
  async def test_create_db_success(self):
    """Test successful database creation."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    with patch('asyncio.sleep', new_callable=AsyncMock):
      await adapter.create_db("test_db")

    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args[0][0]
    assert "CREATE DATABASE IF NOT EXISTS test_db" in call_args

  @pytest.mark.asyncio
  async def test_create_db_with_retries(self):
    """Test database creation with retries."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    # First call fails, second succeeds
    mock_cursor.execute.side_effect = [
        Exception("Temporary error"), None, None
    ]

    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
         patch('src.adapters.clickhouse.log_warn') as mock_log_warn:
      await adapter.create_db("test_db")

      assert mock_cursor.execute.call_count >= 2
      mock_sleep.assert_called()
      mock_log_warn.assert_called()

  @pytest.mark.asyncio
  async def test_create_db_force(self):
    """Test database creation with force flag."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    await adapter.create_db("test_db", force=True)

    call_args = mock_cursor.execute.call_args[0][0]
    assert "CREATE DATABASE test_db" in call_args
    assert "IF NOT EXISTS" not in call_args

  @pytest.mark.asyncio
  async def test_use_db_existing_connection(self):
    """Test switching database with existing connection."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor
    adapter.conn = Mock()  # Existing connection

    await adapter.use_db("new_db")

    mock_cursor.execute.assert_called_once_with("USE new_db")

  @pytest.mark.asyncio
  async def test_use_db_no_connection(self):
    """Test switching database without existing connection."""
    adapter = ClickHouse()
    adapter.conn = None

    with patch.object(adapter, 'connect',
                      new_callable=AsyncMock) as mock_connect:
      await adapter.use_db("new_db")

      mock_connect.assert_called_once_with(db="new_db")

  @pytest.mark.asyncio
  async def test_create_table(self):
    """Test creating table."""
    adapter = ClickHouse()
    adapter.db = "test_db"
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_table"

    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.type = "int32"
    mock_field1.transient = False

    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.type = "string"
    mock_field2.transient = True  # Should be excluded

    mock_ingester.fields = [mock_field1, mock_field2]

    with patch('src.adapters.clickhouse.log_info') as mock_log_info:
      await adapter.create_table(mock_ingester)

      mock_cursor.execute.assert_called_once()
      sql = mock_cursor.execute.call_args[0][0]
      assert "CREATE TABLE IF NOT EXISTS test_db.`test_table`" in sql
      assert "field1 Int32" in sql
      assert "field2" not in sql  # Transient field excluded
      assert "ENGINE = MergeTree()" in sql
      assert "ORDER BY ts" in sql
      mock_log_info.assert_called()

  @pytest.mark.asyncio
  async def test_create_table_failure(self):
    """Test table creation failure."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.execute.side_effect = Exception("Table creation failed")
    adapter.cursor = mock_cursor

    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_table"
    mock_ingester.fields = []

    with patch('src.adapters.clickhouse.log_error') as mock_log_error:
      with pytest.raises(Exception, match="Table creation failed"):
        await adapter.create_table(mock_ingester)

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert(self):
    """Test inserting data."""
    adapter = ClickHouse()
    adapter.db = "test_db"
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_table"
    mock_ingester.last_ingested = datetime(2023,
                                           1,
                                           1,
                                           12,
                                           0,
                                           0,
                                           tzinfo=timezone.utc)

    mock_field = Mock()
    mock_field.name = "field1"
    mock_field.value = 100
    mock_field.transient = False

    mock_ingester.fields = [mock_field]

    with patch('src.adapters.clickhouse.log_info') as mock_log_info:
      await adapter.insert(mock_ingester)

      mock_cursor.execute.assert_called_once()
      sql = mock_cursor.execute.call_args[0][0]
      assert "INSERT INTO test_db.`test_table`" in sql
      assert "field1" in sql
      mock_log_info.assert_called()

  @pytest.mark.asyncio
  async def test_insert_failure(self):
    """Test insert failure."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.execute.side_effect = Exception("Insert failed")
    adapter.cursor = mock_cursor

    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_table"
    mock_ingester.fields = []

    with patch('src.adapters.clickhouse.log_error') as mock_log_error:
      with pytest.raises(Exception, match="Insert failed"):
        await adapter.insert(mock_ingester)

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_many(self):
    """Test inserting multiple records."""
    adapter = ClickHouse()
    adapter.db = "test_db"
    mock_cursor = AsyncMock()
    adapter.cursor = mock_cursor

    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_table"

    mock_field = Mock()
    mock_field.name = "field1"
    mock_field.transient = False

    mock_ingester.fields = [mock_field]

    values = [
        (datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 100),
        (datetime(2023, 1, 1, 12, 1, 0, tzinfo=timezone.utc), 200),
    ]

    await adapter.insert_many(mock_ingester, values)

    mock_cursor.executemany.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_columns(self):
    """Test getting table columns."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [
        ("col1", "Int32", ""),
        ("col2", "String", ""),
    ]
    adapter.cursor = mock_cursor

    result = await adapter.get_columns("test_table")

    assert result == [("col1", "Int32", ""), ("col2", "String", "")]
    mock_cursor.execute.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_cache_columns(self):
    """Test getting cache columns."""
    adapter = ClickHouse()

    with patch.object(adapter,
                      'get_columns',
                      new_callable=AsyncMock,
                      return_value=[("ts", "DateTime", ""),
                                    ("field1", "Int32", "")]):
      columns = await adapter.get_cache_columns("test_table")

      assert columns == ["field1"]  # Should exclude 'ts'

  @pytest.mark.asyncio
  async def test_fetch(self):
    """Test fetching data."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [
        (datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 100),
        (datetime(2023, 1, 1, 12, 5, 0, tzinfo=timezone.utc), 200),
    ]
    adapter.cursor = mock_cursor

    with patch.object(adapter,
                      'get_cache_columns',
                      new_callable=AsyncMock,
                      return_value=["field1"]):
      columns, rows = await adapter.fetch(
          "test_table",
          from_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
          to_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
          aggregation_interval="m5",
          columns=["field1"])

      assert "ts" in columns
      assert "field1" in columns
      assert len(rows) == 2
      mock_cursor.execute.assert_called_once()

  @pytest.mark.asyncio
  async def test_fetch_batch(self):
    """Test fetching batch data."""
    adapter = ClickHouse()

    with patch.object(adapter,
                      'fetch',
                      new_callable=AsyncMock,
                      return_value=(["ts", "field1"], [(datetime.now(), 100)
                                                       ])) as mock_fetch:
      columns, rows = await adapter.fetch_batch(
          ["table1", "table2"],
          from_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
          to_date=datetime(2023, 1, 2, tzinfo=timezone.utc))

      # Should fetch from each table and combine results
      assert mock_fetch.call_count == 2
      assert isinstance(columns, list)
      assert isinstance(rows, list)

  @pytest.mark.asyncio
  async def test_fetch_all(self):
    """Test fetch all with custom query."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [("result1", ), ("result2", )]
    adapter.cursor = mock_cursor

    result = await adapter.fetch_all("SELECT * FROM test_table")

    assert result == [("result1", ), ("result2", )]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")

  @pytest.mark.asyncio
  async def test_list_tables(self):
    """Test listing tables."""
    adapter = ClickHouse()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [("table1", ), ("table2", )]
    adapter.cursor = mock_cursor

    tables = await adapter.list_tables()

    assert tables == ["table1", "table2"]
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")

  @pytest.mark.asyncio
  async def test_commit(self):
    """Test commit method (no-op for ClickHouse)."""
    adapter = ClickHouse()

    # Should not raise any exceptions
    await adapter.commit()
