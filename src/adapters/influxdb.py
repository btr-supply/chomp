from datetime import datetime, timezone
from os import environ as env
from typing import Optional, Tuple

from ..utils import log_error, log_info, Interval, ago, now
from ..models.base import Tsdb, FieldType
from ..models.ingesters import Ingester

from influxdb_client import InfluxDBClient, Point, WritePrecision  # happy mypy

UTC = timezone.utc

# InfluxDB data type mapping - maps to actual type categories for field processing
TYPES: dict[FieldType, str] = {
    "int8": "integer",
    "uint8": "integer",
    "int16": "integer",
    "uint16": "integer",
    "int32": "integer",
    "uint32": "integer",
    "int64": "integer",
    "uint64": "integer",
    "float32": "float",
    "ufloat32": "float",
    "float64": "float",
    "ufloat64": "float",
    "bool": "boolean",
    "timestamp": "integer",  # stored as nanoseconds since epoch
    "string": "string",
    "binary": "string",  # string representation
    "varbinary": "string",  # string representation
}

# InfluxDB interval mapping
INTERVALS: dict[Interval, str] = {
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
    "W2": "2w",
    "M1": "30d",  # approximate
    "M2": "60d",  # approximate
    "M3": "90d",  # approximate
    "M6": "180d",  # approximate
    "Y1": "365d",  # approximate
    "Y2": "730d",  # approximate
    "Y3": "1095d",  # approximate
}


class InfluxDb(Tsdb):
  """InfluxDB v2 adapter for time series data storage."""

  client: Optional[InfluxDBClient]

  def __init__(self,
               host: str = "localhost",
               port: int = 8086,
               db: str = "chomp",
               user: str = "",
               password: str = "",
               org: str = "chomp",
               token: str = ""):
    super().__init__(host, port, db, user, password)
    self._org = org
    self._token = token
    self.client = None

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "InfluxDb":
    """Factory method to create and connect to InfluxDB."""
    self = cls(host=host or env.get("INFLUXDB_HOST") or "localhost",
               port=int(port or env.get("INFLUXDB_PORT") or 8086),
               db=db or env.get("INFLUXDB_BUCKET") or "default",
               user=user or env.get("DB_RW_USER") or "admin",
               password=password or env.get("DB_RW_PASS") or "password")
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    """Test InfluxDB connectivity."""
    try:
      await self.ensure_connected()
      # Try to check bucket/database health
      conn = self.client
      if not conn:
        raise ValueError("InfluxDB client not connected")
      buckets_api = conn.buckets_api()
      buckets_api.find_buckets()  # Just check if the API call works
      return True
    except Exception as e:
      log_error("InfluxDB ping failed", e)
      return False

  async def close(self):
    """Close InfluxDB connection."""
    if self.client:
      self.client.close()
      self.client = None

  async def ensure_connected(self):
    """Ensure InfluxDB connection is established."""
    if not self.client:
      try:
        # Build connection URL
        if self.port == 443:
          url = f"https://{self.host}"
        else:
          url = f"http://{self.host}:{self.port}"

        # Create InfluxDB client for v2.x
        self.client = InfluxDBClient(
            url=url,
            token=self._token,
            org=self._org,
            timeout=30000  # 30 seconds
        )

        log_info(
            f"Connected to InfluxDB on {self.host}:{self.port}/{self.db} (org: {self._org})"
        )
      except Exception as e:
        log_error(
            f"Failed to connect to InfluxDB on {self.host}:{self.port}/{self.db}",
            e)
        raise e

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """Create InfluxDB bucket (database equivalent in v2.x)."""
    await self.ensure_connected()

    try:
      client = self.client
      if not client:
        raise ValueError("InfluxDB client not connected")
      buckets_api = client.buckets_api()

      if force:
        # Try to delete existing bucket
        try:
          existing_bucket = buckets_api.find_bucket_by_name(name)
          if existing_bucket:
            buckets_api.delete_bucket(existing_bucket)
            log_info(f"Dropped existing bucket {name}")
        except Exception:
          pass  # Bucket might not exist

      # Create new bucket
      retention_rules = []
      if "retention" in options:
        retention_rules = [{
            "type": "expire",
            "everySeconds": options["retention"]
        }]

      buckets_api.create_bucket(bucket_name=name,
                                org=self._org,
                                retention_rules=retention_rules)
      log_info(f"Created bucket {name}")

      # Switch to the new bucket
      db: str = name
      self.db = db

    except Exception as e:
      log_error(f"Failed to create bucket {name}", e)
      raise e

  async def use_db(self, db: str):
    """Switch to different InfluxDB bucket."""
    await self.ensure_connected()
    db_name: str = db
    self.db = db_name
    log_info(f"Switched to bucket {db_name}")

  async def create_table(self, ing: Ingester, name: str = ""):
    """InfluxDB doesn't require explicit table creation - measurements are created automatically."""
    measurement = name or ing.name
    log_info(
        f"Measurement {measurement} will be created automatically on first write"
    )

  async def insert(self, ing: Ingester, table: str = ""):
    """Insert data into InfluxDB measurement."""
    await self.ensure_connected()
    measurement = table or ing.name

    # Build InfluxDB point
    persistent_fields = [field for field in ing.fields if not field.transient]

    # Get timestamp from ts field
    ts_value = None
    for field in persistent_fields:
      if field.name == 'ts':
        ts_value = field.value
        break

    # Fallback to last_ingested if no ts field found
    if ts_value is None:
      ts_value = ing.last_ingested or now()

    # Create point with measurement name and timestamp
    point = Point(measurement)
    point.time(ts_value, WritePrecision.MS)

    # Add tags (metadata)
    point.tag("ingester", ing.name)
    if ing.tags:
      for tag in ing.tags:
        if "=" in tag:
          key, value = tag.split("=", 1)
          point.tag(key.strip(), value.strip())
        else:
          point.tag("tag", tag)

    # Add field values (excluding ts since it's handled as the time dimension)
    for field in persistent_fields:
      if field.name == 'ts':  # Skip ts field since it's used as time dimension
        continue

      field_type = TYPES.get(field.type, "string")

      if field_type == "integer":
        point.field(field.name,
                    int(field.value) if field.value is not None else 0)
      elif field_type == "float":
        point.field(field.name,
                    float(field.value) if field.value is not None else 0.0)
      elif field_type == "boolean":
        point.field(field.name,
                    bool(field.value) if field.value is not None else False)
      else:  # string, timestamp
        point.field(field.name,
                    str(field.value) if field.value is not None else "")

    try:
      # Write point to InfluxDB
      client = self.client
      if not client:
        raise ValueError("InfluxDB client not connected")
      write_api = client.write_api(write_options="synchronous")
      write_api.write(bucket=self.db, org=self._org, record=point)
    except Exception as e:
      log_error(f"Failed to insert data into {self.db}.{measurement}", e)
      raise e

  async def insert_many(self,
                        ing: Ingester,
                        values: list[tuple],
                        table: str = ""):
    """Insert multiple records into InfluxDB measurement."""
    await self.ensure_connected()
    measurement = table or ing.name

    persistent_fields = [field for field in ing.fields if not field.transient]

    # Find the ts field index
    ts_field_index = None
    for i, field in enumerate(persistent_fields):
      if field.name == 'ts':
        ts_field_index = i
        break

    # Build list of points
    points = []
    for value_tuple in values:
      # Get timestamp from appropriate position or first value
      if ts_field_index is not None and ts_field_index < len(value_tuple):
        timestamp = value_tuple[ts_field_index]
      else:
        timestamp = value_tuple[0]  # Fallback to first value

      if not isinstance(timestamp, datetime):
        timestamp = datetime.fromtimestamp(timestamp, UTC)

      # Create point
      point = Point(measurement)
      point.time(timestamp, WritePrecision.MS)

      # Add tags
      point.tag("ingester", ing.name)
      if ing.tags:
        for tag in ing.tags:
          if "=" in tag:
            key, value = tag.split("=", 1)
            point.tag(key.strip(), value.strip())
          else:
            point.tag("tag", tag)

      # Add field values (excluding ts since it's used as time dimension)
      for i, field in enumerate(persistent_fields):
        if field.name == 'ts':  # Skip ts field since it's used as time dimension
          continue

        if i < len(value_tuple):
          field_value = value_tuple[i]
          field_type = TYPES.get(field.type, "string")

          if field_type == "integer":
            point.field(field.name,
                        int(field_value) if field_value is not None else 0)
          elif field_type == "float":
            point.field(field.name,
                        float(field_value) if field_value is not None else 0.0)
          elif field_type == "boolean":
            point.field(
                field.name,
                bool(field_value) if field_value is not None else False)
          else:  # string, timestamp
            point.field(field.name,
                        str(field_value) if field_value is not None else "")

      points.append(point)

    try:
      # Write all points to InfluxDB
      client = self.client
      if not client:
        raise ValueError("InfluxDB client not connected")
      write_api = client.write_api(write_options="synchronous")
      write_api.write(bucket=self.db, org=self._org, record=points)
    except Exception as e:
      log_error(f"Failed to batch insert data into {self.db}.{measurement}", e)
      raise e

  async def fetch(self,
                  table: str,
                  from_date: Optional[datetime] = None,
                  to_date: Optional[datetime] = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    """Fetch data from InfluxDB measurement with aggregation."""
    await self.ensure_connected()

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    # Build Flux query for InfluxDB 2.x
    interval = INTERVALS.get(aggregation_interval, "5m")

    # Build field filters
    field_filter = ""
    if columns:
      field_conditions = [f'r["_field"] == "{col}"' for col in columns]
      field_filter = f'|> filter(fn: (r) => {" or ".join(field_conditions)})'

    # Build Flux query
    query = f'''
    from(bucket: "{self.db}")
      |> range(start: {from_date.isoformat()}, stop: {to_date.isoformat()})
      |> filter(fn: (r) => r["_measurement"] == "{table}")
      {field_filter}
      |> aggregateWindow(every: {interval}, fn: last, createEmpty: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
    '''

    try:
      # Execute query
      client = self.client
      if not client:
        raise ValueError("InfluxDB client not connected")
      query_api = client.query_api()
      tables = query_api.query(query=query, org=self._org)

      if not tables:
        return ([], [])

      # Convert result to expected format
      result_data = []
      result_columns = ["_time"]

      for result_table in tables:
        for record in result_table.records:
          # Extract columns from first record
          if not result_columns or len(result_columns) == 1:
            result_columns = ["_time"] + [
                key for key in record.values.keys() if key not in
                ["_time", "_start", "_stop", "_measurement", "ingester"]
            ]

          # Build row data
          row_data = [record.get_time()]
          for col in result_columns[1:]:
            row_data.append(record.values.get(col))

          result_data.append(tuple(row_data))

      # Replace _time column name with ts for consistency
      if result_columns and result_columns[0] == "_time":
        result_columns[0] = "ts"

      return (result_columns, result_data)

    except Exception as e:
      log_error(f"Failed to fetch data from {self.db}.{table}", e)
      raise e

  async def fetch_batch(
      self,
      tables: list[str],
      from_date: Optional[datetime] = None,
      to_date: Optional[datetime] = None,
      aggregation_interval: Interval = "m5",
      columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    """Fetch data from multiple InfluxDB measurements."""
    from asyncio import gather

    results = await gather(*[
        self.fetch(table, from_date, to_date, aggregation_interval, columns)
        for table in tables
    ])

    # Combine results from all measurements
    all_columns: list[str] = []
    all_data: list[Tuple] = []

    for columns_result, data in results:
      if not all_columns:
        all_columns = columns_result
      all_data.extend(data)

    return (all_columns, all_data)

  async def fetchall(self):
    """InfluxDB doesn't have a universal 'fetch all' - need measurement name."""
    raise NotImplementedError(
        "fetchall requires measurement name for InfluxDB")

  async def commit(self):
    """InfluxDB auto-commits by default."""
    pass

  async def list_tables(self) -> list[str]:
    """List InfluxDB measurements."""
    await self.ensure_connected()
    try:
      # Query to get all measurements using Flux
      query = f'''
      import "influxdata/influxdb/schema"
      schema.measurements(bucket: "{self.db}")
      '''

      if not self.client:
        raise ValueError("InfluxDB client not connected")
      query_api = self.client.query_api()
      tables = query_api.query(query=query, org=self._org)

      measurements = []
      for result_table in tables:
        for record in result_table.records:
          if record.get_value():
            measurements.append(record.get_value())

      return measurements
    except Exception as e:
      log_error(f"Failed to list measurements from {self.db}", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs using InfluxDB Flux queries for efficiency"""
    await self.ensure_connected()
    try:
      if not uids or not self.client:
        return []

      # Build Flux query with uid filter using contains() for multiple UIDs
      uid_conditions = " or ".join([f'r["uid"] == "{uid}"' for uid in uids])

      query = f'''
      from(bucket: "{self.db}")
        |> range(start: -10y)
        |> filter(fn: (r) => r["_measurement"] == "{table}")
        |> filter(fn: (r) => {uid_conditions})
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["updated_at"], desc: true)
      '''

      query_api = self.client.query_api()
      tables = query_api.query(query=query, org=self._org)

      if not tables:
        return []

      # Convert InfluxDB result to tuples
      results = []
      for result_table in tables:
        for record in result_table.records:
          # Extract values excluding internal InfluxDB fields
          row_data = []
          for key, value in record.values.items():
            if not key.startswith('_') or key == '_time':
              if key == '_time':
                row_data.append(record.get_time())
              else:
                row_data.append(value)
          results.append(tuple(row_data))

      return results
    except Exception as e:
      log_error(
          f"Failed to fetch batch records by IDs from {self.db}.{table}: {e}")
      return []
