"""Tests for MongoDB adapter."""
import pytest
import sys
import os
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.deps import safe_import

# Check if MongoDB dependencies are available
motor = safe_import("motor")
MONGODB_AVAILABLE = motor is not None

# Only import if dependencies are available
if MONGODB_AVAILABLE:
  from src.adapters.mongodb import MongoDb, GRANULARITY_MAP, BUCKET_GRANULARITY


@pytest.mark.skipif(not MONGODB_AVAILABLE, reason="MongoDB dependencies not available (motor)")
class TestMongoDBAdapter:
  """Test the MongoDB adapter functionality."""

  @pytest.fixture
  def mock_ingester(self):
    """Create a mock ingester for testing."""
    mock = Mock()
    mock.name = "test_ingester"
    mock.interval = "m5"
    mock.tags = ["tag1", "tag2"]
    mock.last_ingested = datetime.now(timezone.utc)

    # Mock fields
    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.value = 100
    mock_field1.transient = False

    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.value = "test_value"
    mock_field2.transient = False

    mock_field3 = Mock()
    mock_field3.name = "transient_field"
    mock_field3.value = "ignored"
    mock_field3.transient = True

    mock.fields = [mock_field1, mock_field2, mock_field3]
    return mock

  def test_mongodb_imports(self):
    """Test that MongoDb can be imported."""
    assert MongoDb is not None

  def test_granularity_mappings(self):
    """Test granularity mappings are properly defined."""
    assert GRANULARITY_MAP["s1"] == "seconds"
    assert GRANULARITY_MAP["m1"] == "minutes"
    assert GRANULARITY_MAP["h1"] == "hours"
    assert GRANULARITY_MAP["D1"] == "hours"

    assert "s1" in BUCKET_GRANULARITY
    assert "m5" in BUCKET_GRANULARITY
    assert "h1" in BUCKET_GRANULARITY

  def test_mongodb_initialization(self):
    """Test MongoDB adapter initialization."""
    adapter = MongoDb(
      host="localhost",
      port=27017,
      db="test_db",
      user="test_user",
      password="test_pass"
    )

    assert adapter.host == "localhost"
    assert adapter.port == 27017
    assert adapter.db == "test_db"
    assert adapter.user == "test_user"
    assert adapter.password == "test_pass"
    assert adapter.client is None
    assert adapter.database is None

  @pytest.mark.asyncio
  async def test_connect_class_method(self):
    """Test the connect class method."""
    with patch('src.adapters.mongodb.AsyncIOMotorClient') as mock_client, \
         patch('src.adapters.mongodb.log_info'):
      mock_client_instance = Mock()
      mock_database = Mock()
      mock_client_instance.__getitem__.return_value = mock_database
      mock_client.return_value = mock_client_instance

      adapter = await MongoDb.connect(
        host="test_host",
        port=27017,
        db="test_db",
        user="test_user",
        password="test_pass"
      )

      assert isinstance(adapter, MongoDb)
      assert adapter.host == "test_host"
      assert adapter.port == 27017
      assert adapter.db == "test_db"
      assert adapter.user == "test_user"
      assert adapter.password == "test_pass"
      mock_client.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_environment_variables(self):
    """Test connect with environment variables."""
    with patch.dict('src.adapters.mongodb.env', {
      'MONGO_HOST': 'env_host',
      'MONGO_PORT': '27018',
      'MONGO_DB': 'env_db',
      'DB_RW_USER': 'env_user',
      'DB_RW_PASS': 'env_pass'
    }), \
    patch('src.adapters.mongodb.AsyncIOMotorClient') as mock_client, \
    patch('src.adapters.mongodb.log_info'):
      mock_client_instance = Mock()
      mock_database = Mock()
      mock_client_instance.__getitem__.return_value = mock_database
      mock_client.return_value = mock_client_instance

      adapter = await MongoDb.connect()

      assert adapter.host == "env_host"
      assert adapter.port == 27018
      assert adapter.db == "env_db"
      assert adapter.user == "env_user"
      assert adapter.password == "env_pass"

  @pytest.mark.asyncio
  async def test_connect_defaults(self):
    """Test connect with default values."""
    with patch('src.adapters.mongodb.AsyncIOMotorClient') as mock_client, \
         patch('src.adapters.mongodb.log_info'):
      mock_client_instance = Mock()
      mock_database = Mock()
      mock_client_instance.__getitem__.return_value = mock_database
      mock_client.return_value = mock_client_instance

      adapter = await MongoDb.connect()

      assert adapter.host == "localhost"
      assert adapter.port == 27017
      assert adapter.db == "default"
      assert adapter.user == "admin"
      assert adapter.password == "pass"

  @pytest.mark.asyncio
  async def test_ensure_connected_success_with_auth(self):
    """Test ensure_connected with authentication."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    with patch('src.adapters.mongodb.AsyncIOMotorClient') as mock_client, \
         patch('src.adapters.mongodb.log_info'):
      mock_client_instance = Mock()
      mock_database = Mock()
      mock_client_instance.__getitem__.return_value = mock_database
      mock_client.return_value = mock_client_instance

      await adapter.ensure_connected()

      expected_connection_string = "mongodb://test_user:test_pass@localhost:27017/test_db?authSource=admin"
      mock_client.assert_called_once_with(expected_connection_string)
      assert adapter.client == mock_client_instance
      assert adapter.database == mock_database

  @pytest.mark.asyncio
  async def test_ensure_connected_success_without_auth(self):
    """Test ensure_connected without authentication."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user=None, password=None)

    with patch('src.adapters.mongodb.AsyncIOMotorClient') as mock_client, \
         patch('src.adapters.mongodb.log_info'):
      mock_client_instance = Mock()
      mock_database = Mock()
      mock_client_instance.__getitem__.return_value = mock_database
      mock_client.return_value = mock_client_instance

      await adapter.ensure_connected()

      expected_connection_string = "mongodb://localhost:27017/test_db"
      mock_client.assert_called_once_with(expected_connection_string)

  @pytest.mark.asyncio
  async def test_ensure_connected_already_connected(self):
    """Test ensure_connected when already connected."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    # Simulate already connected
    mock_client = Mock()
    adapter.client = mock_client

    with patch('src.adapters.mongodb.AsyncIOMotorClient') as mock_client_constructor:
      await adapter.ensure_connected()

      # Should not create new client
      mock_client_constructor.assert_not_called()

  @pytest.mark.asyncio
  async def test_ensure_connected_error(self):
    """Test ensure_connected with connection error."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    with patch('src.adapters.mongodb.AsyncIOMotorClient', side_effect=Exception("Connection failed")), \
         patch('src.adapters.mongodb.log_error') as mock_log_error:
      with pytest.raises(Exception, match="Connection failed"):
        await adapter.ensure_connected()

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test ping with successful connection."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_client = Mock()
    mock_client.admin.command = AsyncMock(return_value={"ok": 1})
    adapter.client = mock_client

    with patch.object(adapter, 'ensure_connected'):
      result = await adapter.ping()

      assert result is True
      mock_client.admin.command.assert_called_once_with("ping")

  @pytest.mark.asyncio
  async def test_ping_no_client(self):
    """Test ping when client is None."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.client = None

    with patch.object(adapter, 'ensure_connected'):
      result = await adapter.ping()

      assert result is False

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test ping with connection failure."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    with patch.object(adapter, 'ensure_connected', side_effect=Exception("Connection failed")), \
         patch('src.adapters.mongodb.log_error') as mock_log_error:
      result = await adapter.ping()

      assert result is False
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_close_connection(self):
    """Test closing connections."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_client = Mock()
    mock_client.close = Mock()
    adapter.client = mock_client
    adapter.database = Mock()

    await adapter.close()

    mock_client.close.assert_called_once()
    assert adapter.client is None
    assert adapter.database is None

  @pytest.mark.asyncio
  async def test_close_no_client(self):
    """Test closing when no client exists."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.client = None

    # Should not raise error
    await adapter.close()

  @pytest.mark.asyncio
  async def test_create_db_success(self, mock_ingester):
    """Test creating database successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_client = Mock()
    mock_database = Mock()
    mock_client.__getitem__.return_value = mock_database
    adapter.client = mock_client

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_info'):
      await adapter.create_db("new_db")

      assert adapter.database == mock_database
      assert adapter.db == "new_db"

  @pytest.mark.asyncio
  async def test_create_db_force(self, mock_ingester):
    """Test creating database with force flag."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_client = Mock()
    mock_client.drop_database = AsyncMock()
    mock_database = Mock()
    mock_client.__getitem__.return_value = mock_database
    adapter.client = mock_client

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_info'):
      await adapter.create_db("new_db", force=True)

      mock_client.drop_database.assert_called_once_with("new_db")
      assert adapter.db == "new_db"

  @pytest.mark.asyncio
  async def test_create_db_no_client(self):
    """Test creating database when client is None."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.client = None

    with patch.object(adapter, 'ensure_connected'):
      with pytest.raises(Exception, match="MongoDB client not connected"):
        await adapter.create_db("new_db")

  @pytest.mark.asyncio
  async def test_use_db_success(self):
    """Test switching database successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_client = Mock()
    mock_database = Mock()
    mock_client.__getitem__.return_value = mock_database
    adapter.client = mock_client

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_info'):
      await adapter.use_db("new_db")

      assert adapter.database == mock_database
      assert adapter.db == "new_db"

  @pytest.mark.asyncio
  async def test_use_db_no_client(self):
    """Test switching database when client is None."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.client = None

    with patch.object(adapter, 'ensure_connected'):
      with pytest.raises(Exception, match="MongoDB client not connected"):
        await adapter.use_db("new_db")

  @pytest.mark.asyncio
  async def test_create_table_success(self, mock_ingester):
    """Test creating time series collection successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_database.list_collection_names = AsyncMock(return_value=[])
    mock_database.create_collection = AsyncMock()
    mock_collection = Mock()
    mock_collection.create_index = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_info'):
      await adapter.create_table(mock_ingester)

      mock_database.create_collection.assert_called_once()
      collection_args = mock_database.create_collection.call_args
      assert collection_args[0][0] == "test_ingester"
      assert "timeseries" in collection_args[1]
      mock_collection.create_index.assert_called_once_with("ts")

  @pytest.mark.asyncio
  async def test_create_table_already_exists(self, mock_ingester):
    """Test creating collection that already exists."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_database.list_collection_names = AsyncMock(return_value=["test_ingester"])
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_info'):
      await adapter.create_table(mock_ingester)

      # Should not try to create collection
      assert not hasattr(mock_database, 'create_collection')

  @pytest.mark.asyncio
  async def test_create_table_custom_name(self, mock_ingester):
    """Test creating collection with custom name."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_database.list_collection_names = AsyncMock(return_value=[])
    mock_database.create_collection = AsyncMock()
    mock_collection = Mock()
    mock_collection.create_index = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_info'):
      await adapter.create_table(mock_ingester, name="custom_table")

      collection_args = mock_database.create_collection.call_args
      assert collection_args[0][0] == "custom_table"

  @pytest.mark.asyncio
  async def test_create_table_no_database(self, mock_ingester):
    """Test creating table when database is None."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.database = None

    with patch.object(adapter, 'ensure_connected'):
      with pytest.raises(Exception, match="MongoDB database not connected"):
        await adapter.create_table(mock_ingester)

  @pytest.mark.asyncio
  async def test_create_table_error(self, mock_ingester):
    """Test creating table with error."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_database.list_collection_names = AsyncMock(return_value=[])
    mock_database.create_collection = AsyncMock(side_effect=Exception("Creation failed"))
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_error') as mock_log_error:
      with pytest.raises(Exception, match="Creation failed"):
        await adapter.create_table(mock_ingester)

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_success(self, mock_ingester):
    """Test inserting document successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.insert_one = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'):
      await adapter.insert(mock_ingester)

      mock_collection.insert_one.assert_called_once()
      insert_args = mock_collection.insert_one.call_args[0][0]
      assert "ts" in insert_args
      assert "meta" in insert_args
      assert "field1" in insert_args
      assert "field2" in insert_args
      assert "transient_field" not in insert_args  # Transient fields excluded

  @pytest.mark.asyncio
  async def test_insert_custom_table(self, mock_ingester):
    """Test inserting into custom table."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.insert_one = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'):
      await adapter.insert(mock_ingester, table="custom_table")

      mock_database.__getitem__.assert_called_with("custom_table")

  @pytest.mark.asyncio
  async def test_insert_collection_not_exists(self, mock_ingester):
    """Test inserting when collection doesn't exist."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()

    # First call fails, second succeeds
    mock_collection.insert_one = AsyncMock(side_effect=[
      Exception("collection does not exist"),
      None
    ])
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, 'create_table') as mock_create_table, \
         patch('src.adapters.mongodb.log_warn'):
      await adapter.insert(mock_ingester)

      mock_create_table.assert_called_once()
      assert mock_collection.insert_one.call_count == 2

  @pytest.mark.asyncio
  async def test_insert_other_error(self, mock_ingester):
    """Test inserting with other errors."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.insert_one = AsyncMock(side_effect=Exception("Other error"))
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_error') as mock_log_error:
      with pytest.raises(Exception, match="Other error"):
        await adapter.insert(mock_ingester)

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_no_database(self, mock_ingester):
    """Test inserting when database is None."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.database = None

    with patch.object(adapter, 'ensure_connected'):
      with pytest.raises(Exception, match="MongoDB database not connected"):
        await adapter.insert(mock_ingester)

  @pytest.mark.asyncio
  async def test_insert_many_success(self, mock_ingester):
    """Test inserting multiple documents successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.insert_many = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    test_values = [
      (datetime.now(timezone.utc), 100, "value1"),
      (datetime.now(timezone.utc), 200, "value2")
    ]

    with patch.object(adapter, 'ensure_connected'):
      await adapter.insert_many(mock_ingester, test_values)

      mock_collection.insert_many.assert_called_once()
      insert_docs = mock_collection.insert_many.call_args[0][0]
      assert len(insert_docs) == 2
      assert all("ts" in doc for doc in insert_docs)

  @pytest.mark.asyncio
  async def test_insert_many_timestamp_conversion(self, mock_ingester):
    """Test inserting many with timestamp conversion."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.insert_many = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    # Use timestamp as float
    test_values = [
      (1234567890.0, 100, "value1")
    ]

    with patch.object(adapter, 'ensure_connected'):
      await adapter.insert_many(mock_ingester, test_values)

      insert_docs = mock_collection.insert_many.call_args[0][0]
      assert isinstance(insert_docs[0]["ts"], datetime)

  @pytest.mark.asyncio
  async def test_insert_many_collection_not_exists(self, mock_ingester):
    """Test insert_many when collection doesn't exist."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.insert_many = AsyncMock(side_effect=[
      Exception("ns not found"),
      None
    ])
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    test_values = [(datetime.now(timezone.utc), 100, "value1")]

    with patch.object(adapter, 'ensure_connected'), \
         patch.object(adapter, 'create_table') as mock_create_table, \
         patch('src.adapters.mongodb.log_warn'):
      await adapter.insert_many(mock_ingester, test_values)

      mock_create_table.assert_called_once()
      assert mock_collection.insert_many.call_count == 2

  @pytest.mark.asyncio
  async def test_list_tables_success(self):
    """Test listing collections successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_database.list_collection_names = AsyncMock(return_value=["table1", "table2"])
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'):
      result = await adapter.list_tables()

      assert result == ["table1", "table2"]

  @pytest.mark.asyncio
  async def test_list_tables_no_database(self):
    """Test listing tables when database is None."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    adapter.database = None

    with patch.object(adapter, 'ensure_connected'):
      result = await adapter.list_tables()

      assert result == []

  @pytest.mark.asyncio
  async def test_list_tables_error(self):
    """Test listing tables with error."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_database.list_collection_names = AsyncMock(side_effect=Exception("List error"))
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.log_error') as mock_log_error:
      result = await adapter.list_tables()

      assert result == []
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_commit(self):
    """Test commit (no-op for MongoDB)."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    # Should not raise any errors
    await adapter.commit()

  @pytest.mark.asyncio
  async def test_fetchall_not_implemented(self):
    """Test fetchall raises NotImplementedError."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    with pytest.raises(NotImplementedError):
      await adapter.fetchall()

  @pytest.mark.asyncio
  async def test_fetch_success(self):
    """Test fetching data successfully."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_cursor = Mock()
    mock_cursor.to_list = AsyncMock(return_value=[
      {"ts": datetime.now(timezone.utc), "field1": 100, "field2": "value1"},
      {"ts": datetime.now(timezone.utc), "field1": 200, "field2": "value2"}
    ])
    mock_collection.aggregate.return_value = mock_cursor
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.now') as mock_now, \
         patch('src.adapters.mongodb.ago') as mock_ago:
      mock_now.return_value = datetime.now(timezone.utc)
      mock_ago.return_value = datetime.now(timezone.utc)

      columns, data = await adapter.fetch("test_table", columns=["field1", "field2"])

      assert "ts" in columns
      assert "field1" in columns
      assert "field2" in columns
      assert len(data) == 2

  @pytest.mark.asyncio
  async def test_fetch_no_results(self):
    """Test fetching when no results."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_cursor = Mock()
    mock_cursor.to_list = AsyncMock(return_value=[])
    mock_collection.aggregate.return_value = mock_cursor
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.now') as mock_now, \
         patch('src.adapters.mongodb.ago') as mock_ago:
      mock_now.return_value = datetime.now(timezone.utc)
      mock_ago.return_value = datetime.now(timezone.utc)

      columns, data = await adapter.fetch("test_table")

      assert columns == []
      assert data == []

  @pytest.mark.asyncio
  async def test_fetch_error(self):
    """Test fetch with error."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    mock_database = Mock()
    mock_collection = Mock()
    mock_collection.aggregate.side_effect = Exception("Fetch error")
    mock_database.__getitem__.return_value = mock_collection
    adapter.database = mock_database

    with patch.object(adapter, 'ensure_connected'), \
         patch('src.adapters.mongodb.now') as mock_now, \
         patch('src.adapters.mongodb.ago') as mock_ago, \
         patch('src.adapters.mongodb.log_error') as mock_log_error:
      mock_now.return_value = datetime.now(timezone.utc)
      mock_ago.return_value = datetime.now(timezone.utc)

      with pytest.raises(Exception, match="Fetch error"):
        await adapter.fetch("test_table")

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_fetch_batch_success(self):
    """Test fetching from multiple tables."""
    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")

    with patch.object(adapter, 'fetch') as mock_fetch, \
         patch('asyncio.gather') as mock_gather:
      mock_fetch.return_value = (["ts", "field1"], [(datetime.now(timezone.utc), 100)])
      mock_gather.return_value = [
        (["ts", "field1"], [(datetime.now(timezone.utc), 100)]),
        (["ts", "field1"], [(datetime.now(timezone.utc), 200)])
      ]

      columns, data = await adapter.fetch_batch(["table1", "table2"])

      assert len(data) == 2
      mock_gather.assert_called_once()

  def test_mongodb_inheritance(self):
    """Test that MongoDb inherits from Tsdb."""
    from src.model import Tsdb

    adapter = MongoDb(host="localhost", port=27017, db="test_db", user="test_user", password="test_pass")
    assert isinstance(adapter, Tsdb)
