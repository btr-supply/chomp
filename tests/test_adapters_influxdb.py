"""Tests for adapters.influxdb module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, timezone
from os import environ as env

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.deps import safe_import

# Check if InfluxDB dependencies are available
influxdb_client = safe_import("influxdb_client")
INFLUXDB_AVAILABLE = influxdb_client is not None

# Only import if dependencies are available
if INFLUXDB_AVAILABLE:
  from src.adapters.influxdb import InfluxDb, TYPES, INTERVALS


@pytest.mark.skipif(
    not INFLUXDB_AVAILABLE,
    reason="InfluxDB dependencies not available (influxdb-client)")
class TestInfluxDbAdapter:
  """Test InfluxDB adapter functionality."""

  def test_types_mapping(self):
    """Test InfluxDB type mapping."""
    assert TYPES["int32"] == "i"
    assert TYPES["float64"] == "f"
    assert TYPES["string"] == "s"
    assert TYPES["bool"] == "b"
    assert TYPES["uint32"] == "u"

  def test_intervals_mapping(self):
    """Test InfluxDB interval mapping."""
    assert INTERVALS["m5"] == "5m"
    assert INTERVALS["h1"] == "1h"
    assert INTERVALS["D1"] == "1d"
    assert INTERVALS["W1"] == "1w"
    assert INTERVALS["M1"] == "30d"

  def test_inheritance(self):
    """Test InfluxDb inherits from Tsdb."""
    from src.model import Tsdb
    assert issubclass(InfluxDb, Tsdb)

  def test_initialization(self):
    """Test InfluxDB adapter initialization."""
    adapter = InfluxDb(host="localhost",
                       port=8086,
                       db="test_bucket",
                       user="admin",
                       password="secret")

    assert adapter.host == "localhost"
    assert adapter.port == 8086
    assert adapter.db == "test_bucket"
    assert adapter.user == "admin"
    assert adapter.password == "secret"
    assert adapter._bucket == "test_bucket"
    assert adapter._influxdb_client is None

  def test_initialization_defaults(self):
    """Test InfluxDB adapter with defaults."""
    adapter = InfluxDb()

    assert adapter.host == "localhost"
    assert adapter.port == 8086
    assert adapter.db == "default"
    assert adapter.user == "admin"
    assert adapter.password == "password"

  def test_initialization_with_env_vars(self):
    """Test initialization with environment variables."""
    env_vars = {'INFLUXDB_ORG': 'test-org', 'INFLUXDB_TOKEN': 'test-token'}

    with patch.dict(env, env_vars):
      adapter = InfluxDb()

      assert adapter._org == "test-org"
      assert adapter._token == "test-token"

  def test_influxdb_client_property(self):
    """Test lazy loading of InfluxDB client."""
    adapter = InfluxDb()

    with patch('src.adapters.influxdb.lazy_import') as mock_lazy_import:
      mock_client_module = Mock()
      mock_lazy_import.return_value = mock_client_module

      client = adapter.influxdb_client

      assert client == mock_client_module
      assert adapter._influxdb_client == mock_client_module
      mock_lazy_import.assert_called_once_with("influxdb_client",
                                               "influxdb-client", "influxdb")

  @pytest.mark.asyncio
  async def test_connect_with_defaults(self):
    """Test connect with default parameters."""
    with patch.dict(env, {}, clear=True), \
         patch.object(InfluxDb, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      adapter = await InfluxDb.connect()

      assert adapter.host == "localhost"
      assert adapter.port == 8086
      assert adapter.db == "default"
      assert adapter.user == "admin"
      assert adapter.password == "password"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_env_vars(self):
    """Test connect with environment variables."""
    env_vars = {
        'INFLUXDB_HOST': 'influx-host',
        'INFLUXDB_PORT': '8087',
        'INFLUXDB_BUCKET': 'test-bucket',
        'DB_RW_USER': 'test-user',
        'DB_RW_PASS': 'test-pass'
    }

    with patch.dict(env, env_vars), \
         patch.object(InfluxDb, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      adapter = await InfluxDb.connect()

      assert adapter.host == "influx-host"
      assert adapter.port == 8087
      assert adapter.db == "test-bucket"
      assert adapter.user == "test-user"
      assert adapter.password == "test-pass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_parameters(self):
    """Test connect with explicit parameters."""
    with patch.object(InfluxDb, 'ensure_connected',
                      new_callable=AsyncMock) as mock_ensure:

      adapter = await InfluxDb.connect(host="custom-host",
                                       port=9999,
                                       db="custom-bucket",
                                       user="custom-user",
                                       password="custom-pass")

      assert adapter.host == "custom-host"
      assert adapter.port == 9999
      assert adapter.db == "custom-bucket"
      assert adapter.user == "custom-user"
      assert adapter.password == "custom-pass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test successful ping."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_buckets_api = Mock()
    mock_client.buckets_api.return_value = mock_buckets_api
    adapter.conn = mock_client

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      result = await adapter.ping()

      assert result is True
      mock_buckets_api.find_buckets.assert_called_once()

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test ping failure."""
    adapter = InfluxDb()

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock, side_effect=Exception("Connection failed")), \
         patch('src.adapters.influxdb.log_error') as mock_log_error:

      result = await adapter.ping()

      assert result is False
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_close(self):
    """Test closing connection."""
    adapter = InfluxDb()
    mock_client = Mock()
    adapter.conn = mock_client

    await adapter.close()

    mock_client.close.assert_called_once()
    assert adapter.conn is None

  @pytest.mark.asyncio
  async def test_ensure_connected_http(self):
    """Test connection establishment with HTTP."""
    adapter = InfluxDb(host="localhost", port=8086)
    adapter._token = "test-token"
    adapter._org = "test-org"

    mock_influx_client = Mock()
    mock_client_instance = Mock()
    mock_influx_client.InfluxDBClient.return_value = mock_client_instance

    with patch('src.adapters.influxdb.lazy_import', return_value=mock_influx_client), \
         patch('src.adapters.influxdb.log_info') as mock_log_info:

      await adapter.ensure_connected()

      mock_influx_client.InfluxDBClient.assert_called_once_with(
          url="http://localhost:8086",
          token="test-token",
          org="test-org",
          timeout=30000)
      assert adapter.client == mock_client_instance
      mock_log_info.assert_called_once()

  @pytest.mark.asyncio
  async def test_ensure_connected_https(self):
    """Test connection establishment with HTTPS."""
    adapter = InfluxDb(host="remote-host", port=443)
    adapter._token = "test-token"
    adapter._org = "test-org"

    mock_influx_client = Mock()
    mock_client_instance = Mock()
    mock_influx_client.InfluxDBClient.return_value = mock_client_instance

    with patch('src.adapters.influxdb.lazy_import',
               return_value=mock_influx_client):
      await adapter.ensure_connected()

      mock_influx_client.InfluxDBClient.assert_called_once_with(
          url="https://remote-host",
          token="test-token",
          org="test-org",
          timeout=30000)

  @pytest.mark.asyncio
  async def test_ensure_connected_failure(self):
    """Test connection failure."""
    adapter = InfluxDb()
    adapter._token = "test-token"
    adapter._org = "test-org"

    mock_influx_client = Mock()
    mock_influx_client.InfluxDBClient.side_effect = Exception(
        "Connection failed")

    with patch('src.adapters.influxdb.lazy_import', return_value=mock_influx_client), \
         patch('src.adapters.influxdb.log_error') as mock_log_error:

      with pytest.raises(Exception, match="Connection failed"):
        await adapter.ensure_connected()

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_db_new_bucket(self):
    """Test creating new bucket."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_buckets_api = Mock()
    mock_client.buckets_api.return_value = mock_buckets_api
    adapter.client = mock_client

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      await adapter.create_db("new-bucket")

      mock_buckets_api.create_bucket.assert_called_once_with(
          bucket_name="new-bucket", org=adapter._org, retention_rules=[])
      assert adapter.db == "new-bucket"

  @pytest.mark.asyncio
  async def test_create_db_with_retention(self):
    """Test creating bucket with retention policy."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_buckets_api = Mock()
    mock_client.buckets_api.return_value = mock_buckets_api
    adapter.client = mock_client

    options = {"retention": 86400}  # 1 day in seconds

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      await adapter.create_db("test-bucket", options)

      expected_retention_rules = [{"type": "expire", "everySeconds": 86400}]
      mock_buckets_api.create_bucket.assert_called_once_with(
          bucket_name="test-bucket",
          org=adapter._org,
          retention_rules=expected_retention_rules)

  @pytest.mark.asyncio
  async def test_create_db_force_recreate(self):
    """Test force recreating bucket."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_buckets_api = Mock()
    mock_client.buckets_api.return_value = mock_buckets_api
    adapter.client = mock_client

    # Mock existing bucket
    mock_existing_bucket = Mock()
    mock_buckets_api.find_bucket_by_name.return_value = mock_existing_bucket

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.log_info') as mock_log_info:

      await adapter.create_db("existing-bucket", force=True)

      # Should delete existing bucket first
      mock_buckets_api.delete_bucket.assert_called_once_with(
          mock_existing_bucket)
      # Then create new one
      mock_buckets_api.create_bucket.assert_called_once()
      mock_log_info.assert_called()

  @pytest.mark.asyncio
  async def test_create_db_failure(self):
    """Test bucket creation failure."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_buckets_api = Mock()
    mock_client.buckets_api.return_value = mock_buckets_api
    adapter.client = mock_client

    mock_buckets_api.create_bucket.side_effect = Exception("Creation failed")

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.log_error') as mock_log_error:

      with pytest.raises(Exception, match="Creation failed"):
        await adapter.create_db("test-bucket")

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_use_db(self):
    """Test switching to different bucket."""
    adapter = InfluxDb()

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.log_info') as mock_log_info:

      await adapter.use_db("new-bucket")

      assert adapter.db == "new-bucket"
      mock_log_info.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_table(self):
    """Test creating measurement (table equivalent)."""
    adapter = InfluxDb()
    mock_ingester = Mock()
    mock_ingester.name = "test_measurement"

    with patch('src.adapters.influxdb.log_info') as mock_log_info:
      await adapter.create_table(mock_ingester)

      mock_log_info.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_single_record(self):
    """Test inserting single record."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_write_api = Mock()
    mock_client.write_api.return_value = mock_write_api
    adapter.conn = mock_client

    # Create mock ingester with fields
    mock_ingester = Mock()
    mock_ingester.name = "test_measurement"

    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.type = "int32"
    mock_field1.value = 100
    mock_field1.transient = False

    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.type = "float64"
    mock_field2.value = 45.6
    mock_field2.transient = False

    mock_field3 = Mock()
    mock_field3.name = "transient_field"
    mock_field3.type = "string"
    mock_field3.value = "test"
    mock_field3.transient = True  # Should be excluded

    mock_ingester.fields = [mock_field1, mock_field2, mock_field3]

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.now') as mock_now:

      mock_now.return_value = datetime(2023,
                                       1,
                                       1,
                                       12,
                                       0,
                                       0,
                                       tzinfo=timezone.utc)

      await adapter.insert(mock_ingester)

      # Verify write was called
      mock_write_api.write.assert_called_once()
      call_args = mock_write_api.write.call_args

      # Check bucket parameter
      assert call_args[1]['bucket'] == adapter.db

      # Check that point was written
      assert 'record' in call_args[1]

  @pytest.mark.asyncio
  async def test_insert_with_none_values(self):
    """Test inserting record with None values."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_write_api = Mock()
    mock_client.write_api.return_value = mock_write_api
    adapter.conn = mock_client

    # Create mock ingester with None value field
    mock_ingester = Mock()
    mock_ingester.name = "test_measurement"

    mock_field = Mock()
    mock_field.name = "field1"
    mock_field.type = "int32"
    mock_field.value = None  # None value
    mock_field.transient = False

    mock_ingester.fields = [mock_field]

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.now') as mock_now:

      mock_now.return_value = datetime(2023,
                                       1,
                                       1,
                                       12,
                                       0,
                                       0,
                                       tzinfo=timezone.utc)

      await adapter.insert(mock_ingester)

      # Should still write, but without the None field
      mock_write_api.write.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_failure(self):
    """Test insert failure handling."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_write_api = Mock()
    mock_client.write_api.return_value = mock_write_api
    adapter.conn = mock_client

    mock_write_api.write.side_effect = Exception("Write failed")

    mock_ingester = Mock()
    mock_ingester.name = "test_measurement"
    mock_ingester.fields = []

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.log_error') as mock_log_error:

      with pytest.raises(Exception, match="Write failed"):
        await adapter.insert(mock_ingester)

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_insert_many(self):
    """Test inserting multiple records."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_write_api = Mock()
    mock_client.write_api.return_value = mock_write_api
    adapter.conn = mock_client

    # Create mock ingester
    mock_ingester = Mock()
    mock_ingester.name = "test_measurement"

    mock_field1 = Mock()
    mock_field1.name = "field1"
    mock_field1.type = "int32"
    mock_field1.transient = False

    mock_field2 = Mock()
    mock_field2.name = "field2"
    mock_field2.type = "float64"
    mock_field2.transient = False

    mock_ingester.fields = [mock_field1, mock_field2]

    values = [
        (datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 100, 45.6),
        (datetime(2023, 1, 1, 12, 1, 0, tzinfo=timezone.utc), 200, 67.8),
    ]

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      await adapter.insert_many(mock_ingester, values)

      # Should call write for batch
      mock_write_api.write.assert_called_once()
      call_args = mock_write_api.write.call_args

      # Check bucket parameter
      assert call_args[1]['bucket'] == adapter.db

      # Check that records were written
      assert 'record' in call_args[1]

  @pytest.mark.asyncio
  async def test_fetch_data(self):
    """Test fetching data from InfluxDB."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_query_api = Mock()
    mock_client.query_api.return_value = mock_query_api
    adapter.conn = mock_client

    # Mock query result
    mock_table = Mock()
    mock_table.records = [
        Mock(_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
             _value=100,
             _field="field1"),
        Mock(_time=datetime(2023, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
             _value=200,
             _field="field1"),
    ]
    mock_query_api.query.return_value = [mock_table]

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      columns, rows = await adapter.fetch(
          "test_measurement",
          from_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
          to_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
          aggregation_interval="m5",
          columns=["field1"])

      assert "ts" in columns
      assert "field1" in columns
      assert len(rows) == 2
      mock_query_api.query.assert_called_once()

  @pytest.mark.asyncio
  async def test_fetch_no_results(self):
    """Test fetching with no results."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_query_api = Mock()
    mock_client.query_api.return_value = mock_query_api
    adapter.conn = mock_client

    # Mock empty query result
    mock_query_api.query.return_value = []

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      columns, rows = await adapter.fetch("test_measurement")

      assert columns == []
      assert rows == []

  @pytest.mark.asyncio
  async def test_fetch_failure(self):
    """Test fetch failure handling."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_query_api = Mock()
    mock_client.query_api.return_value = mock_query_api
    adapter.conn = mock_client

    mock_query_api.query.side_effect = Exception("Query failed")

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.log_error') as mock_log_error:

      with pytest.raises(Exception, match="Query failed"):
        await adapter.fetch("test_measurement")

      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_fetch_batch(self):
    """Test fetching batch data."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_query_api = Mock()
    mock_client.query_api.return_value = mock_query_api
    adapter.conn = mock_client

    # Mock query results
    mock_query_api.query.return_value = []

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      columns, rows = await adapter.fetch_batch(
          ["measurement1", "measurement2"],
          from_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
          to_date=datetime(2023, 1, 2, tzinfo=timezone.utc))

      # Should execute query
      mock_query_api.query.assert_called_once()
      assert isinstance(columns, list)
      assert isinstance(rows, list)

  @pytest.mark.asyncio
  async def test_fetchall_not_implemented(self):
    """Test fetchall method (not implemented for InfluxDB)."""
    adapter = InfluxDb()

    with pytest.raises(NotImplementedError):
      await adapter.fetchall()

  @pytest.mark.asyncio
  async def test_commit_no_op(self):
    """Test commit method (no-op for InfluxDB)."""
    adapter = InfluxDb()

    # Should not raise any exceptions
    await adapter.commit()

  @pytest.mark.asyncio
  async def test_list_tables_success(self):
    """Test listing measurements (tables)."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_query_api = Mock()
    mock_client.query_api.return_value = mock_query_api
    adapter.conn = mock_client

    # Mock query result with measurement names
    mock_table = Mock()
    mock_record1 = Mock()
    mock_record1.values = {"_measurement": "measurement1"}
    mock_record2 = Mock()
    mock_record2.values = {"_measurement": "measurement2"}
    mock_table.records = [mock_record1, mock_record2]
    mock_query_api.query.return_value = [mock_table]

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock):
      tables = await adapter.list_tables()

      assert "measurement1" in tables
      assert "measurement2" in tables
      mock_query_api.query.assert_called_once()

  @pytest.mark.asyncio
  async def test_list_tables_failure(self):
    """Test list tables failure handling."""
    adapter = InfluxDb()
    mock_client = Mock()
    mock_query_api = Mock()
    mock_client.query_api.return_value = mock_query_api
    adapter.conn = mock_client

    mock_query_api.query.side_effect = Exception("Query failed")

    with patch.object(adapter, 'ensure_connected', new_callable=AsyncMock), \
         patch('src.adapters.influxdb.log_error') as mock_log_error:

      tables = await adapter.list_tables()

      assert tables == []
      mock_log_error.assert_called_once()
