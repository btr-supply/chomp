"""Tests for TDengine adapter module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
import sys
from os import environ as env, path

# Add src to path for imports
sys.path.insert(0, path.join(path.dirname(__file__), '..'))

from chomp.src.utils.deps import safe_import

# Check if TDengine dependencies are available
taos = safe_import("taos")
DB_AVAILABLE = taos is not None

# Only import if dependencies are available
if DB_AVAILABLE:
  from src.adapters.tdengine import Taos, TYPES, INTERVALS, PRECISION, TIMEZONE
  from src.models import Ingester


@pytest.mark.skipif(not DB_AVAILABLE,
                    reason="TDengine dependencies not available (taos/taospy)")
class TestTaosAdapter:
  """Test the TDengine (Taos) adapter functionality."""

  def test_DB_imports(self):
    """Test that TDengine adapter imports correctly."""
    assert Taos is not None
    assert TYPES is not None
    assert INTERVALS is not None
    assert PRECISION == "ms"
    assert TIMEZONE == "UTC"

  def test_types_mapping(self):
    """Test TDengine type mappings are correct."""
    expected_types = {
        "int8": "tinyint",
        "uint8": "tinyint unsigned",
        "int16": "smallint",
        "uint16": "smallint unsigned",
        "int32": "int",
        "uint32": "int unsigned",
        "int64": "bigint",
        "uint64": "bigint unsigned",
        "float32": "float",
        "ufloat32": "float unsigned",
        "float64": "double",
        "ufloat64": "double unsigned",
        "bool": "bool",
        "timestamp": "timestamp",
        "string": "nchar",
        "binary": "binary",
        "varbinary": "varbinary",
    }

    for field_type, expected_sql_type in expected_types.items():
      assert TYPES[field_type] == expected_sql_type

  def test_intervals_mapping(self):
    """Test TDengine interval mappings are correct."""
    expected_intervals = {
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

    for chomp_interval, tdengine_interval in expected_intervals.items():
      assert INTERVALS[chomp_interval] == tdengine_interval

  def test_DB_initialization(self):
    """Test TDengine adapter initialization."""
    adapter = Taos(host="testhost",
                   port=6030,
                   db="testdb",
                   user="testuser",
                   password="testpass")

    assert adapter.host == "testhost"
    assert adapter.port == 6030
    assert adapter.db == "testdb"
    assert adapter.user == "testuser"
    assert adapter.password == "testpass"
    assert adapter._DB_module is None

  def test_DB_initialization_defaults(self):
    """Test TDengine adapter initialization with defaults."""
    adapter = Taos()

    assert adapter.host == "localhost"
    assert adapter.port == 40002
    assert adapter.db == "default"
    assert adapter.user == "rw"
    assert adapter.password == "pass"

  def test_DB_module_lazy_loading_success(self):
    """Test successful lazy loading of taos module."""
    adapter = Taos()
    mock_taos = Mock()

    with patch('builtins.__import__', return_value=mock_taos):
      result = adapter.DB_module
      assert result == mock_taos
      assert adapter._DB_module == mock_taos

  def test_DB_module_lazy_loading_failure(self):
    """Test lazy loading failure when taos module not available."""
    adapter = Taos()

    with patch('builtins.__import__',
               side_effect=ImportError("No module named 'taos'")):
      with pytest.raises(ImportError) as exc_info:
        adapter.DB_module

      assert "TDengine Python client (taospy) is required" in str(
          exc_info.value)
      assert "pip install taospy" in str(exc_info.value)

  def test_timestamp_column_type(self):
    """Test timestamp column type property."""
    adapter = Taos()
    assert adapter.timestamp_column_type == "timestamp"

  @pytest.mark.asyncio
  async def test_connect_class_method(self):
    """Test TDengine connect class method."""
    mock_taos = Mock()
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_taos.connect.return_value = mock_conn

    with patch.dict(
        env, {
            'DB_HOST': 'envhost',
            'DB_PORT': '6030',
            'DB_NAME': 'envdb',
            'DB_RW_USER': 'envuser',
            'DB_RW_PASS': 'envpass'
        }):
      with patch('builtins.__import__', return_value=mock_taos):
        adapter = await Taos.connect()

        assert adapter.host == 'envhost'
        assert adapter.port == 6030
        assert adapter.db == 'envdb'
        assert adapter.user == 'envuser'
        assert adapter.password == 'envpass'

  @pytest.mark.asyncio
  async def test_connect_with_parameters(self):
    """Test TDengine connect with explicit parameters."""
    mock_taos = Mock()
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_taos.connect.return_value = mock_conn

    with patch('builtins.__import__', return_value=mock_taos):
      adapter = await Taos.connect(host="testhost",
                                   port=6030,
                                   db="testdb",
                                   user="testuser",
                                   password="testpass")

      assert adapter.host == "testhost"
      assert adapter.port == 6030
      assert adapter.db == "testdb"
      assert adapter.user == "testuser"
      assert adapter.password == "testpass"

  @pytest.mark.asyncio
  async def test_connect_success(self):
    """Test successful TDengine connection."""
    adapter = Taos()
    mock_taos = Mock()
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_taos.connect.return_value = mock_conn

    # Set the mock directly on the private attribute
    adapter._DB_module = mock_taos

    await adapter._connect()

    mock_taos.connect.assert_called_once_with(host=adapter.host,
                                              port=adapter.port,
                                              database=adapter.db,
                                              user=adapter.user,
                                              password=adapter.password)
    assert adapter.conn == mock_conn
    assert adapter.cursor == mock_cursor

  @pytest.mark.asyncio
  async def test_connect_database_not_exist(self):
    """Test connection when database doesn't exist - should create it."""
    adapter = Taos()
    mock_taos = Mock()
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.close = Mock()

    # First connection fails with "not exist" error
    mock_taos.connect.side_effect = [
        Exception("Database not exist"),
        mock_conn,  # Connection without database
        mock_conn  # Final connection with database
    ]

    # Set the mock directly on the private attribute
    adapter._DB_module = mock_taos

    with patch.object(adapter, 'create_db',
                      new_callable=AsyncMock) as mock_create_db:
      await adapter._connect()

      # Should create the database
      mock_create_db.assert_called_once_with(adapter.db)
      assert adapter.conn == mock_conn
      assert adapter.cursor == mock_cursor

  @pytest.mark.asyncio
  async def test_connect_other_error(self):
    """Test connection with other errors."""
    adapter = Taos()
    mock_taos = Mock()
    mock_taos.connect.side_effect = Exception("Connection refused")

    # Set the mock directly on the private attribute
    adapter._DB_module = mock_taos

    with pytest.raises(ValueError) as exc_info:
      await adapter._connect()

    assert "Failed to connect to TDengine" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_close_connection(self):
    """Test closing TDengine connection."""
    adapter = Taos()
    mock_cursor = Mock()
    mock_conn = Mock()
    adapter.cursor = mock_cursor
    adapter.conn = mock_conn

    await adapter._close_connection()

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
    assert adapter.conn is None
    assert adapter.cursor is None

  @pytest.mark.asyncio
  async def test_close_connection_no_objects(self):
    """Test closing connection when no connection objects exist."""
    adapter = Taos()
    adapter.cursor = None
    adapter.conn = None

    # Should not raise an exception
    await adapter._close_connection()

  @pytest.mark.asyncio
  async def test_execute_no_params(self):
    """Test executing query without parameters."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    await adapter._execute("SELECT 1")

    mock_cursor.execute.assert_called_once_with("SELECT 1")

  @pytest.mark.asyncio
  async def test_execute_with_params(self):
    """Test executing query with parameters."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    await adapter._execute("SELECT * FROM table WHERE id = ? AND name = ?",
                           (123, "test"))

    # Should format the query with parameters
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM table WHERE id = 123 AND name = 'test'")

  @pytest.mark.asyncio
  async def test_fetch(self):
    """Test fetching query results."""
    adapter = Taos()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("row1", ), ("row2", )]
    adapter.cursor = mock_cursor

    result = await adapter._fetch("SELECT * FROM table")

    assert result == [("row1", ), ("row2", )]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM table")
    mock_cursor.fetchall.assert_called_once()

  @pytest.mark.asyncio
  async def test_executemany(self):
    """Test executing multiple queries."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    params_list = [(1, "test1"), (2, "test2")]
    await adapter._executemany("INSERT INTO table VALUES (?, ?)", params_list)

    # Should execute for each parameter set
    assert mock_cursor.execute.call_count == 2

  def test_quote_identifier(self):
    """Test identifier quoting."""
    adapter = Taos()
    assert adapter._quote_identifier("table_name") == "`table_name`"

  @pytest.mark.asyncio
  async def test_create_db_success(self):
    """Test successful database creation."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    await adapter.create_db("test_db")

    # Should execute the CREATE DATABASE command
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args[0][0]
    assert "CREATE DATABASE IF NOT EXISTS test_db" in call_args
    assert "PRECISION 'ms'" in call_args

  @pytest.mark.asyncio
  async def test_create_db_with_retries(self):
    """Test database creation with retries."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    # First call fails, second succeeds
    mock_cursor.execute.side_effect = [
        Exception("Temporary error"), None, None
    ]

    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
      await adapter.create_db("test_db")

      # Should retry and eventually succeed
      assert mock_cursor.execute.call_count >= 2
      mock_sleep.assert_called()

  @pytest.mark.asyncio
  async def test_create_db_force(self):
    """Test database creation with force flag."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    await adapter.create_db("test_db", force=True)

    # Should use CREATE DATABASE instead of CREATE DATABASE IF NOT EXISTS
    call_args = mock_cursor.execute.call_args[0][0]
    assert "CREATE DATABASE test_db" in call_args
    assert "IF NOT EXISTS" not in call_args

  @pytest.mark.asyncio
  async def test_use_db(self):
    """Test switching database."""
    adapter = Taos()

    with patch.object(adapter, 'connect',
                      new_callable=AsyncMock) as mock_connect:
      await adapter.use_db("new_db")

      mock_connect.assert_called_once_with(db="new_db")

  def test_build_create_table_sql(self):
    """Test building CREATE TABLE SQL."""
    adapter = Taos()
    mock_ingester = Mock(spec=Ingester)
    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.type = "int32"
    mock_field1.transient = False
    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.type = "string"
    mock_field2.transient = True  # Should be excluded
    mock_ingester.fields = [mock_field1, mock_field2]

    sql = adapter._build_create_table_sql(mock_ingester, "test_table")

    assert "CREATE TABLE" in sql
    assert "test_table" in sql
    assert "field1 int" in sql
    assert "field2" not in sql  # Transient field excluded

  def test_build_aggregation_sql(self):
    """Test building aggregation SQL."""
    adapter = Taos()
    from_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2023, 1, 2, tzinfo=timezone.utc)

    sql, params = adapter._build_aggregation_sql("test_table",
                                                 ["field1", "field2"],
                                                 from_date, to_date, "m5")

    assert "SELECT" in sql
    assert "test_table" in sql
    assert "field1" in sql
    assert "field2" in sql
    assert "INTERVAL(5m)" in sql
    assert len(params) == 2  # from_date and to_date

  @pytest.mark.asyncio
  async def test_get_table_columns(self):
    """Test getting table columns."""
    adapter = Taos()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("field1", ), ("field2", )]
    adapter.cursor = mock_cursor

    columns = await adapter._get_table_columns("test_table")

    assert columns == ["field1", "field2"]
    mock_cursor.execute.assert_called_once()

  @pytest.mark.asyncio
  async def test_list_tables(self):
    """Test listing tables."""
    adapter = Taos()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("table1", ), ("table2", )]
    adapter.cursor = mock_cursor

    tables = await adapter.list_tables()

    assert tables == ["table1", "table2"]
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")

  @pytest.mark.asyncio
  async def test_list_tables_error(self):
    """Test listing tables with error."""
    adapter = Taos()
    mock_cursor = Mock()
    mock_cursor.execute.side_effect = Exception("Query failed")
    adapter.cursor = mock_cursor

    with patch('src.adapters.tdengine.log_error') as mock_log_error:
      tables = await adapter.list_tables()

      assert tables == []
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_alter_table_add_columns(self):
    """Test altering table to add columns."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    await adapter.alter_table("test_table", add_columns=[("new_field", "int")])

    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args[0][0]
    assert "ALTER TABLE test_table ADD COLUMN new_field int" in call_args

  @pytest.mark.asyncio
  async def test_alter_table_drop_columns(self):
    """Test altering table to drop columns."""
    adapter = Taos()
    mock_cursor = Mock()
    adapter.cursor = mock_cursor

    await adapter.alter_table("test_table", drop_columns=["old_field"])

    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args[0][0]
    assert "ALTER TABLE test_table DROP COLUMN old_field" in call_args

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test successful ping."""
    adapter = Taos()

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch.object(adapter, '_execute', new_callable=AsyncMock):
      result = await adapter.ping()

      assert result is True

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test ping failure."""
    adapter = Taos()

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock, side_effect=Exception("Connection failed")), \
         patch('src.adapters.tdengine.log_error') as mock_log_error:
      result = await adapter.ping()

      assert result is False
      mock_log_error.assert_called_once()

  def test_inheritance(self):
    """Test that Taos inherits from SqlAdapter."""
    from src.adapters.sql import SqlAdapter
    assert issubclass(Taos, SqlAdapter)
