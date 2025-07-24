"""Tests for DuckDB adapter."""
import pytest
import sys
import os
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from chomp.src.utils.deps import safe_import

# Check if DuckDB dependencies are available
duckdb = safe_import("duckdb")
DUCKDB_AVAILABLE = duckdb is not None

# Only import if dependencies are available
if DUCKDB_AVAILABLE:
  from src.adapters.duckdb import DuckDB, TYPES, INTERVALS, PRECISION


@pytest.mark.skipif(not DUCKDB_AVAILABLE,
                    reason="DuckDB dependencies not available (duckdb)")
class TestDuckDBAdapter:
  """Test the DuckDB adapter functionality."""

  def test_duckdb_imports(self):
    """Test that DuckDB can be imported."""
    assert DuckDB is not None

  def test_types_mapping(self):
    """Test that TYPES mapping is correctly defined."""
    assert TYPES["int8"] == "TINYINT"
    assert TYPES["uint8"] == "UTINYINT"
    assert TYPES["float64"] == "DOUBLE"
    assert TYPES["timestamp"] == "TIMESTAMP"
    assert TYPES["string"] == "VARCHAR"

  def test_intervals_mapping(self):
    """Test that INTERVALS mapping is correctly defined."""
    assert INTERVALS["s1"] == "1 second"
    assert INTERVALS["m1"] == "1 minute"
    assert INTERVALS["h1"] == "1 hour"
    assert INTERVALS["D1"] == "1 day"

  def test_precision_constant(self):
    """Test that PRECISION constant is correctly defined."""
    assert PRECISION == "ms"

  def test_duckdb_initialization(self):
    """Test DuckDB adapter initialization."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    assert adapter.host == "localhost"
    assert adapter.port == 0
    assert adapter.db == ":memory:"
    assert adapter.user == "test_user"
    assert adapter.password == "test_pass"

  @pytest.mark.asyncio
  async def test_connect_class_method(self):
    """Test the connect class method."""
    with patch('src.adapters.duckdb.duckdb.connect') as mock_connect, \
         patch('src.adapters.duckdb.ThreadPoolExecutor') as mock_executor:

      mock_connection = Mock()
      mock_connect.return_value = mock_connection
      mock_executor_instance = Mock()
      mock_executor.return_value = mock_executor_instance

      adapter = await DuckDB.connect(host="test_host",
                                     port=0,
                                     db="test.db",
                                     user="test_user",
                                     password="test_pass")

      assert isinstance(adapter, DuckDB)
      assert adapter.host == "test_host"
      assert adapter.port == 0
      assert adapter.db == "test.db"
      assert adapter.user == "test_user"
      assert adapter.password == "test_pass"

  @pytest.mark.asyncio
  async def test_connect_with_environment_variables(self):
    """Test connect with environment variables."""
    with patch.dict('src.adapters.duckdb.env', {
      'DUCKDB_HOST': 'env_host',
      'DUCKDB_PORT': '8080',
      'DUCKDB_DB': 'env.db',
      'DB_RW_USER': 'env_user',
      'DB_RW_PASS': 'env_pass'
    }), \
    patch('src.adapters.duckdb.duckdb.connect') as mock_connect, \
    patch('src.adapters.duckdb.ThreadPoolExecutor') as mock_executor:

      mock_connection = Mock()
      mock_connect.return_value = mock_connection
      mock_executor_instance = Mock()
      mock_executor.return_value = mock_executor_instance

      adapter = await DuckDB.connect()

      assert adapter.host == "env_host"
      assert adapter.port == 8080
      assert adapter.db == "env.db"
      assert adapter.user == "env_user"
      assert adapter.password == "env_pass"

  @pytest.mark.asyncio
  async def test_ensure_connected_memory_db(self):
    """Test ensure_connected with in-memory database."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch('src.adapters.duckdb.duckdb.connect') as mock_connect, \
         patch('src.adapters.duckdb.ThreadPoolExecutor') as mock_executor, \
         patch('src.adapters.duckdb.get_event_loop') as mock_loop:

      mock_connection = Mock()
      mock_connect.return_value = mock_connection
      mock_executor_instance = Mock()
      mock_executor.return_value = mock_executor_instance
      mock_loop_instance = Mock()
      mock_loop_instance.run_in_executor = AsyncMock(
          return_value=mock_connection)
      mock_loop.return_value = mock_loop_instance

      await adapter.ensure_connected()

      assert adapter.conn == mock_connection
      assert adapter.cursor == mock_connection

  @pytest.mark.asyncio
  async def test_ensure_connected_file_db(self):
    """Test ensure_connected with file-based database."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db="test.db",
                     user="test_user",
                     password="test_pass")

    with patch('src.adapters.duckdb.duckdb.connect') as mock_connect, \
         patch('src.adapters.duckdb.ThreadPoolExecutor') as mock_executor, \
         patch('src.adapters.duckdb.get_event_loop') as mock_loop:

      mock_connection = Mock()
      mock_connect.return_value = mock_connection
      mock_executor_instance = Mock()
      mock_executor.return_value = mock_executor_instance
      mock_loop_instance = Mock()
      mock_loop_instance.run_in_executor = AsyncMock(
          return_value=mock_connection)
      mock_loop.return_value = mock_loop_instance

      await adapter.ensure_connected()

      assert adapter.conn == mock_connection

  @pytest.mark.asyncio
  async def test_ensure_connected_error(self):
    """Test ensure_connected with connection error."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db="test.db",
                     user="test_user",
                     password="test_pass")

    with patch('src.adapters.duckdb.ThreadPoolExecutor') as mock_executor, \
         patch('src.adapters.duckdb.get_event_loop') as mock_loop:

      mock_executor_instance = Mock()
      mock_executor.return_value = mock_executor_instance
      mock_loop_instance = Mock()
      mock_loop_instance.run_in_executor = AsyncMock(
          side_effect=Exception("Connection failed"))
      mock_loop.return_value = mock_loop_instance

      with pytest.raises(ValueError,
                         match="Failed to connect to DuckDB database"):
        await adapter.ensure_connected()

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test ping with successful connection."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async', return_value=[(1,)]):
      result = await adapter.ping()

      assert result is True

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test ping with connection failure."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter,
                      'ensure_connected',
                      side_effect=Exception("Connection failed")):
      result = await adapter.ping()

      assert result is False

  @pytest.mark.asyncio
  async def test_close_connection(self):
    """Test closing connections."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    mock_connection = Mock()
    mock_executor = Mock()
    adapter.conn = mock_connection
    adapter.executor = mock_executor

    with patch.object(adapter, '_execute_async_void') as mock_execute:
      await adapter.close()
      mock_execute.assert_called_once()
      mock_executor.shutdown.assert_called_once_with(wait=True)

  @pytest.mark.asyncio
  async def test_execute_async_with_params(self):
    """Test _execute_async with parameters."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    mock_connection = Mock()
    mock_execute_result = Mock()
    mock_execute_result.fetchall.return_value = [("result", )]
    mock_connection.execute.return_value = mock_execute_result
    adapter.conn = mock_connection

    with patch('src.adapters.duckdb.get_event_loop') as mock_loop:
      mock_loop_instance = Mock()
      mock_loop_instance.run_in_executor = AsyncMock(
          return_value=[("result", )])
      mock_loop.return_value = mock_loop_instance

      result = await adapter._execute_async("SELECT ?", ["param"])

      assert result == [("result", )]

  @pytest.mark.asyncio
  async def test_execute_async_without_params(self):
    """Test _execute_async without parameters."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    mock_connection = Mock()
    adapter.conn = mock_connection

    with patch('src.adapters.duckdb.get_event_loop') as mock_loop:
      mock_loop_instance = Mock()
      mock_loop_instance.run_in_executor = AsyncMock(
          return_value=[("result", )])
      mock_loop.return_value = mock_loop_instance

      result = await adapter._execute_async("SELECT 1")

      assert result == [("result", )]

  @pytest.mark.asyncio
  async def test_execute_async_void(self):
    """Test _execute_async_void method."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch('src.adapters.duckdb.get_event_loop') as mock_loop:
      mock_loop_instance = Mock()
      mock_loop_instance.run_in_executor = AsyncMock(return_value="result")
      mock_loop.return_value = mock_loop_instance

      def test_func():
        return "test"

      result = await adapter._execute_async_void(test_func)

      assert result == "result"

  @pytest.mark.asyncio
  async def test_get_dbs(self):
    """Test getting databases (schemas)."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter,
                      '_execute_async',
                      return_value=[("schema1", ), ("schema2", )]):
      result = await adapter.get_dbs()

      assert result == [("schema1", ), ("schema2", )]

  @pytest.mark.asyncio
  async def test_create_db_success(self):
    """Test creating database (schema) successfully."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, '_execute_async') as mock_execute:
      await adapter.create_db("new_schema")

      mock_execute.assert_called_once_with(
          "CREATE SCHEMA IF NOT EXISTS new_schema")

  @pytest.mark.asyncio
  async def test_create_db_error(self):
    """Test create_db error handling."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter,
                      '_execute_async',
                      side_effect=Exception("Schema error")):
      await adapter.create_db("new_schema")  # Should not raise, just log error

  @pytest.mark.asyncio
  async def test_use_db_same_database(self):
    """Test using same database."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db="test.db",
                     user="test_user",
                     password="test_pass")

    # Should not close/reconnect if same database
    with patch.object(adapter, 'close') as mock_close, \
         patch.object(adapter, 'ensure_connected') as mock_connect:
      await adapter.use_db("test.db")

      mock_close.assert_not_called()
      mock_connect.assert_not_called()

  @pytest.mark.asyncio
  async def test_use_db_different_database(self):
    """Test using different database."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db="test.db",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'close') as mock_close, \
         patch.object(adapter, 'ensure_connected') as mock_connect:
      await adapter.use_db("other.db")

      mock_close.assert_called_once()
      mock_connect.assert_called_once()
      assert adapter.db == "other.db"

  @pytest.mark.asyncio
  async def test_create_table(self):
    """Test creating table."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    # Create mock ingester
    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.fields = [
        Mock(name="field1", type="int32", transient=False),
        Mock(name="field2", type="string", transient=False),
        Mock(name="temp_field", type="float64",
             transient=True)  # Should be excluded
    ]

    with patch.object(adapter, '_execute_async') as mock_execute:
      await adapter.create_table(ingester)

      # Verify SQL was executed
      args, _ = mock_execute.call_args
      sql = args[0]
      assert 'CREATE TABLE IF NOT EXISTS "test_table"' in sql
      assert '"field1" INTEGER' in sql
      assert '"field2" VARCHAR' in sql
      assert "temp_field" not in sql  # Transient fields should be excluded

  @pytest.mark.asyncio
  async def test_create_table_error_handling(self):
    """Test create_table error handling."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.fields = []

    with patch.object(adapter,
                      '_execute_async',
                      side_effect=Exception("SQL Error")):
      with pytest.raises(Exception, match="SQL Error"):
        await adapter.create_table(ingester)

  @pytest.mark.asyncio
  async def test_alter_table_add_columns(self):
    """Test altering table to add columns."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async') as mock_execute:

      await adapter.alter_table("test_table",
                                add_columns=[("new_col", "INTEGER")])

      mock_execute.assert_called_with(
          'ALTER TABLE "test_table" ADD COLUMN "new_col" INTEGER')

  @pytest.mark.asyncio
  async def test_alter_table_drop_columns(self):
    """Test altering table to drop columns."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async') as mock_execute:

      await adapter.alter_table("test_table", drop_columns=["old_col"])

      mock_execute.assert_called_with(
          'ALTER TABLE "test_table" DROP COLUMN "old_col"')

  @pytest.mark.asyncio
  async def test_alter_table_error_handling(self):
    """Test alter_table error handling."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async', side_effect=Exception("Alter error")):

      with pytest.raises(Exception, match="Alter error"):
        await adapter.alter_table("test_table",
                                  add_columns=[("new_col", "INTEGER")])

  @pytest.mark.asyncio
  async def test_insert(self):
    """Test inserting data."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    # Create mock ingester with data
    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.last_ingested = datetime(2024,
                                      1,
                                      1,
                                      12,
                                      0,
                                      0,
                                      tzinfo=timezone.utc)
    ingester.fields = [
        Mock(name="field1", type="int32", transient=False, value=123),
        Mock(name="field2", type="string", transient=False, value="test"),
        Mock(name="temp_field", type="float64", transient=True,
             value=45.6)  # Should be excluded
    ]

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async') as mock_execute:

      await adapter.insert(ingester)

      # Verify INSERT was called
      args, _ = mock_execute.call_args
      sql, values = args
      assert 'INSERT INTO "test_table"' in sql
      assert "VALUES" in sql
      assert values[0] == ingester.last_ingested
      assert 123 in values
      assert "test" in values

  @pytest.mark.asyncio
  async def test_insert_error_handling(self):
    """Test insert error handling."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.last_ingested = datetime(2024,
                                      1,
                                      1,
                                      12,
                                      0,
                                      0,
                                      tzinfo=timezone.utc)
    ingester.fields = []

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async', side_effect=Exception("Insert error")):

      with pytest.raises(Exception, match="Insert error"):
        await adapter.insert(ingester)

  @pytest.mark.asyncio
  async def test_list_tables(self):
    """Test listing tables."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter,
                      '_execute_async',
                      return_value=[("table1", ), ("table2", )]):
      result = await adapter.list_tables()

      assert result == ["table1", "table2"]

  @pytest.mark.asyncio
  async def test_commit(self):
    """Test commit operation."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, '_execute_async') as mock_execute:
      await adapter.commit()

      mock_execute.assert_called_once_with("COMMIT")

  def test_duckdb_inheritance(self):
    """Test that DuckDB inherits from Tsdb."""
    from src.models import Tsdb
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")
    assert isinstance(adapter, Tsdb)

  @pytest.mark.asyncio
  async def test_insert_table_not_exists(self):
    """Test insert when table doesn't exist."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.last_ingested = datetime(2024,
                                      1,
                                      1,
                                      12,
                                      0,
                                      0,
                                      tzinfo=timezone.utc)
    ingester.fields = [
        Mock(name="field1", type="int32", transient=False, value=123)
    ]

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async', side_effect=[
           Exception("table test_table does not exist"),
           None  # Second call for create_table
         ]), \
         patch.object(adapter, 'create_table') as mock_create, \
         patch.object(adapter, 'insert', side_effect=[None, None]):

      await adapter.insert(ingester)

      mock_create.assert_called_once_with(ingester, name="test_table")

  @pytest.mark.asyncio
  async def test_insert_column_not_exists(self):
    """Test insert when column doesn't exist."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.last_ingested = datetime(2024,
                                      1,
                                      1,
                                      12,
                                      0,
                                      0,
                                      tzinfo=timezone.utc)
    ingester.fields = [
        Mock(name="field1", type="int32", transient=False, value=123)
    ]

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async', side_effect=[
           Exception("column field1 does not exist"),
           [("field1", "INTEGER")],  # get_columns result
           None  # alter_table call
         ]), \
         patch.object(adapter, 'get_columns', return_value=[("existing_field", "VARCHAR")]), \
         patch.object(adapter, 'alter_table') as mock_alter, \
         patch.object(adapter, 'insert', side_effect=[None, None]):

      await adapter.insert(ingester)

      mock_alter.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_many(self):
    """Test inserting multiple records."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.fields = [
        Mock(name="field1", type="int32", transient=False),
        Mock(name="field2", type="string", transient=False),
        Mock(name="temp_field", type="float64",
             transient=True)  # Should be excluded
    ]

    values = [(datetime(2024, 1, 1, 12, 0, 0,
                        tzinfo=timezone.utc), 123, "test1"),
              (datetime(2024, 1, 1, 12, 1, 0,
                        tzinfo=timezone.utc), 456, "test2")]

    with patch.object(adapter, '_execute_async') as mock_execute:
      await adapter.insert_many(ingester, values)

      # Should call BEGIN TRANSACTION, INSERT for each value, then COMMIT
      assert mock_execute.call_count == 4  # BEGIN, INSERT, INSERT, COMMIT
      call_args = [call[0][0] for call in mock_execute.call__argslist]
      assert "BEGIN TRANSACTION" in call_args
      assert "COMMIT" in call_args
      assert any("INSERT INTO" in arg for arg in call_args)

  @pytest.mark.asyncio
  async def test_insert_many_error(self):
    """Test insert_many error handling."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "test_table"
    ingester.fields = [Mock(name="field1", type="int32", transient=False)]

    values = [(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 123)]

    with patch.object(
        adapter,
        '_execute_async',
        side_effect=[
            None,  # BEGIN TRANSACTION
            Exception("Insert failed"),  # INSERT error
            None  # ROLLBACK
        ]) as mock_execute:

      with pytest.raises(Exception, match="Insert failed"):
        await adapter.insert_many(ingester, values)

      # Should call ROLLBACK after error
      call_args = [call[0][0] for call in mock_execute.call__argslist]
      assert "ROLLBACK" in call_args

  @pytest.mark.asyncio
  async def test_get_columns(self):
    """Test getting table columns."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    expected_columns = [("col1", "INTEGER"), ("col2", "VARCHAR")]

    with patch.object(adapter, '_execute_async',
                      return_value=expected_columns):
      result = await adapter.get_columns("test_table")

      assert result == expected_columns

  @pytest.mark.asyncio
  async def test_get_columns_error(self):
    """Test get_columns error handling."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter,
                      '_execute_async',
                      side_effect=Exception("Table not found")):
      result = await adapter.get_columns("nonexistent_table")

      assert result == []

  @pytest.mark.asyncio
  async def test_get_cache_columns(self):
    """Test getting cached table columns."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    mock_columns = [("col1", "INTEGER"), ("col2", "VARCHAR")]

    with patch('src.adapters.duckdb.get_or_set_cache',
               return_value=mock_columns):
      result = await adapter.get_cache_columns("test_table")

      assert result == ["col1", "col2"]

  @pytest.mark.asyncio
  async def test_fetch_basic(self):
    """Test basic fetch functionality."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    expected_data = [(datetime(2024, 1, 1, 12, 0, 0,
                               tzinfo=timezone.utc), 123, "test"),
                     (datetime(2024, 1, 1, 12, 5, 0,
                               tzinfo=timezone.utc), 456, "test2")]

    with patch.object(adapter, 'get_cache_columns', return_value=["ts", "col1", "col2"]), \
         patch.object(adapter, '_execute_async', return_value=expected_data):

      columns, data = await adapter.fetch("test_table")

      assert columns == ["ts", "col1", "col2"]
      assert data == expected_data

  @pytest.mark.asyncio
  async def test_fetch_with_parameters(self):
    """Test fetch with date range and interval parameters."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    from_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    to_date = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    with patch.object(adapter, 'get_cache_columns', return_value=["ts", "col1"]), \
         patch.object(adapter, '_execute_async', return_value=[]) as mock_execute:

      await adapter.fetch("test_table",
                          from_date=from_date,
                          to_date=to_date,
                          aggregation_interval="h1",
                          columns=["ts", "col1"],
                          use_first=True)

      # Verify the SQL query was constructed correctly
      call_args = mock_execute.call_args[0][0]
      assert "time_bucket(INTERVAL '1 hour', ts)" in call_args
      assert "first(" in call_args
      assert "WHERE ts >=" in call_args

  @pytest.mark.asyncio
  async def test_fetch_time_bucket_fallback(self):
    """Test fetch fallback when time_bucket is not available."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'get_cache_columns', return_value=["ts", "col1"]), \
         patch.object(adapter, '_execute_async', side_effect=[
           Exception("time_bucket function not available"),
           [(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 123)]
         ]) as mock_execute:

      columns, data = await adapter.fetch("test_table")

      # Should fallback to simple SELECT
      assert mock_execute.call_count == 2
      fallback_sql = mock_execute.call__argslist[1][0][0]
      assert "time_bucket" not in fallback_sql
      assert 'SELECT "ts", "col1"' in fallback_sql

  @pytest.mark.asyncio
  async def test_fetch_no_columns(self):
    """Test fetch when no columns are found."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    with patch.object(adapter, 'get_cache_columns', return_value=[]):
      columns, data = await adapter.fetch("test_table")

      assert columns == []
      assert data == []

  @pytest.mark.asyncio
  async def test_fetch_batch(self):
    """Test fetch_batch for multiple tables."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    expected_columns = ["ts", "col1", "col2"]
    table1_data = [(datetime(2024, 1, 1, 12, 0, 0,
                             tzinfo=timezone.utc), 123, "test1")]
    table2_data = [(datetime(2024, 1, 1, 12, 5, 0,
                             tzinfo=timezone.utc), 456, "test2")]

    with patch.object(adapter, 'get_cache_columns', return_value=expected_columns), \
         patch.object(adapter, 'fetch', side_effect=[
           (expected_columns, table1_data),
           (expected_columns, table2_data)
         ]):

      columns, data = await adapter.fetch_batch(["table1", "table2"])

      assert columns == expected_columns
      assert len(data) == 2
      assert table1_data[0] in data
      assert table2_data[0] in data

  @pytest.mark.asyncio
  async def test_fetch_batch_with_parameters(self):
    """Test fetch_batch with parameters."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    from_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    to_date = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    with patch.object(adapter, 'get_cache_columns', return_value=["ts", "col1"]), \
         patch.object(adapter, 'fetch', return_value=(["ts", "col1"], [])) as mock_fetch:

      await adapter.fetch_batch(["table1", "table2"],
                                from_date=from_date,
                                to_date=to_date,
                                aggregation_interval="m15",
                                columns=["ts", "col1"])

      # Should call fetch for each table with the same parameters
      assert mock_fetch.call_count == 2
      for call in mock_fetch.call__argslist:
        args, kwargs = call
        assert args[1] == from_date  # from_date
        assert args[2] == to_date  # to_date
        assert args[3] == "m15"  # aggregation_interval

  @pytest.mark.asyncio
  async def test_fetch_all(self):
    """Test fetch_all method."""
    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    expected_result = [("row1", ), ("row2", )]

    with patch.object(adapter, '_execute_async', return_value=expected_result):
      result = await adapter.fetch_all("SELECT * FROM test_table")

      assert result == expected_result

  @pytest.mark.asyncio
  async def test_insert_many_custom_table(self):
    """Test insert_many with custom table name."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "default_table"
    ingester.fields = [Mock(name="field1", type="int32", transient=False)]

    values = [(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 123)]

    with patch.object(adapter, '_execute_async') as mock_execute:
      await adapter.insert_many(ingester, values, table="custom_table")

      # Verify INSERT was called with custom table name
      insert_calls = [
          call for call in mock_execute.call__argslist
          if call[0][0].startswith("INSERT")
      ]
      assert len(insert_calls) == 1
      assert "custom_table" in insert_calls[0][0][0]

  @pytest.mark.asyncio
  async def test_create_table_custom_name(self):
    """Test create_table with custom table name."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "default_table"
    ingester.fields = [Mock(name="field1", type="int32", transient=False)]

    with patch.object(adapter, '_execute_async') as mock_execute:
      await adapter.create_table(ingester, name="custom_table")

      args, _ = mock_execute.call_args
      sql = args[0]
      assert 'CREATE TABLE IF NOT EXISTS "custom_table"' in sql

  @pytest.mark.asyncio
  async def test_insert_custom_table(self):
    """Test insert with custom table name."""
    from src.models import Ingester

    adapter = DuckDB(host="localhost",
                     port=0,
                     db=":memory:",
                     user="test_user",
                     password="test_pass")

    ingester = Mock(spec=Ingester)
    ingester.name = "default_table"
    ingester.last_ingested = datetime(2024,
                                      1,
                                      1,
                                      12,
                                      0,
                                      0,
                                      tzinfo=timezone.utc)
    ingester.fields = [
        Mock(name="field1", type="int32", transient=False, value=123)
    ]

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, '_execute_async') as mock_execute:

      await adapter.insert(ingester, table="custom_table")

      args, _ = mock_execute.call_args
      sql = args[0]
      assert 'INSERT INTO "custom_table"' in sql

  def test_constants_coverage(self):
    """Test that all constants are properly defined and accessible."""
    # Test all type mappings
    assert len(TYPES) > 0
    assert all(isinstance(k, str) for k in TYPES.keys())
    assert all(isinstance(v, str) for v in TYPES.values())

    # Test all interval mappings
    assert len(INTERVALS) > 0
    assert all(isinstance(k, str) for k in INTERVALS.keys())
    assert all(isinstance(v, str) for v in INTERVALS.values())

    # Test specific mappings for edge cases
    assert TYPES["binary"] == "BLOB"
    assert TYPES["varbinary"] == "BLOB"
    assert INTERVALS["Y1"] == "1 year"
