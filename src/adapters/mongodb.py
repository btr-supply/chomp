from datetime import datetime, timezone
from os import environ as env
from typing import Optional, Any, Dict, List, Tuple
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from ..utils import log_error, log_info, log_warn, Interval, ago, now
from ..model import Ingester, Tsdb

UTC = timezone.utc

# MongoDB time series granularity mapping
GRANULARITY_MAP: Dict[Interval, str] = {
  "s1": "seconds", "s2": "seconds", "s5": "seconds", "s10": "seconds",
  "s15": "seconds", "s20": "seconds", "s30": "seconds",
  "m1": "minutes", "m2": "minutes", "m5": "minutes", "m10": "minutes",
  "m15": "minutes", "m30": "minutes",
  "h1": "hours", "h2": "hours", "h4": "hours", "h6": "hours",
  "h8": "hours", "h12": "hours",
  "D1": "hours", "D2": "hours", "D3": "hours",
  "W1": "hours", "M1": "hours", "Y1": "hours"
}

# MongoDB aggregation pipeline interval mapping
BUCKET_GRANULARITY: Dict[Interval, Any] = {
  "s1": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$ts"}},
  "s5": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": {"$dateTrunc": {"date": "$ts", "unit": "second", "binSize": 5}}}},
  "s10": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": {"$dateTrunc": {"date": "$ts", "unit": "second", "binSize": 10}}}},
  "s15": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": {"$dateTrunc": {"date": "$ts", "unit": "second", "binSize": 15}}}},
  "s30": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": {"$dateTrunc": {"date": "$ts", "unit": "second", "binSize": 30}}}},
  "m1": {"$dateToString": {"format": "%Y-%m-%d %H:%M", "date": {"$dateTrunc": {"date": "$ts", "unit": "minute"}}}},
  "m5": {"$dateToString": {"format": "%Y-%m-%d %H:%M", "date": {"$dateTrunc": {"date": "$ts", "unit": "minute", "binSize": 5}}}},
  "m15": {"$dateToString": {"format": "%Y-%m-%d %H:%M", "date": {"$dateTrunc": {"date": "$ts", "unit": "minute", "binSize": 15}}}},
  "m30": {"$dateToString": {"format": "%Y-%m-%d %H:%M", "date": {"$dateTrunc": {"date": "$ts", "unit": "minute", "binSize": 30}}}},
  "h1": {"$dateToString": {"format": "%Y-%m-%d %H", "date": {"$dateTrunc": {"date": "$ts", "unit": "hour"}}}},
  "h4": {"$dateToString": {"format": "%Y-%m-%d %H", "date": {"$dateTrunc": {"date": "$ts", "unit": "hour", "binSize": 4}}}},
  "h12": {"$dateToString": {"format": "%Y-%m-%d %H", "date": {"$dateTrunc": {"date": "$ts", "unit": "hour", "binSize": 12}}}},
  "D1": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$dateTrunc": {"date": "$ts", "unit": "day"}}}},
}

class MongoDb(Tsdb):
  """MongoDB adapter using time series collections for optimal time series performance."""

  client: Optional[AsyncIOMotorClient] = None
  database: Optional[AsyncIOMotorDatabase] = None

  @classmethod
  async def connect(
    cls,
    host: str | None = None,
    port: int | None = None,
    db: str | None = None,
    user: str | None = None,
    password: str | None = None
  ) -> "MongoDb":
    self = cls(
      host=host or env.get("MONGO_HOST") or "localhost",
      port=int(port or env.get("MONGO_PORT") or 27017),
      db=db or env.get("MONGO_DB") or "default",
      user=user or env.get("DB_RW_USER") or "admin",
      password=password or env.get("DB_RW_PASS") or "pass"
    )
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    """Test MongoDB connectivity."""
    try:
      await self.ensure_connected()
      if self.client is None:
        return False
      await self.client.admin.command('ping')
      return True
    except Exception as e:
      log_error("MongoDB ping failed", e)
      return False

  async def close(self):
    """Close MongoDB connection."""
    if self.client:
      self.client.close()
      self.client = None
      self.database = None

  async def ensure_connected(self):
    """Ensure MongoDB connection is established."""
    if not self.client:
      try:
        # Build connection string
        if self.user and self.password:
          connection_string = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}?authSource=admin"
        else:
          connection_string = f"mongodb://{self.host}:{self.port}/{self.db}"

        self.client = AsyncIOMotorClient(connection_string)
        self.database = self.client[self.db]
        log_info(f"Connected to MongoDB on {self.host}:{self.port}/{self.db}")
      except Exception as e:
        log_error(f"Failed to connect to MongoDB on {self.host}:{self.port}/{self.db}", e)
        raise e

  async def create_db(self, name: str, options: dict = {}, force: bool = False):
    """Create MongoDB database."""
    await self.ensure_connected()

    if force:
      if self.client is None:
        raise Exception("MongoDB client not connected")
      await self.client.drop_database(name)

    # MongoDB databases are created implicitly when collections are created
    if self.client is None:
      raise Exception("MongoDB client not connected")
    self.database = self.client[name]
    self.db = name
    log_info(f"Database {name} ready")

  async def use_db(self, db: str):
    """Switch to different MongoDB database."""
    await self.ensure_connected()
    if self.client is None:
      raise Exception("MongoDB client not connected")
    self.database = self.client[db]
    self.db = db
    log_info(f"Switched to database {db}")

  async def create_table(self, c: Ingester, name: str = ""):
    """Create MongoDB time series collection."""
    await self.ensure_connected()
    table = name or c.name

    # Check if collection already exists
    if self.database is None:
      raise Exception("MongoDB database not connected")
    collections = await self.database.list_collection_names()
    if table in collections:
      log_info(f"Time series collection {table} already exists")
      return

    # Get appropriate granularity based on ingester interval
    granularity = GRANULARITY_MAP.get(c.interval, "minutes")

    # Create time series collection
    try:
      if self.database is None:
        raise Exception("MongoDB database not connected")
      await self.database.create_collection(
        table,
        timeseries={
          "timeField": "ts",  # timestamp field
          "metaField": "meta",  # metadata field for tags/labels
          "granularity": granularity
        }
      )

      # Create index on timestamp for better query performance
      if self.database is None:
        raise Exception("MongoDB database not connected")
      collection = self.database[table]
      await collection.create_index("ts")

      log_info(f"Created time series collection {self.db}.{table} with granularity '{granularity}'")
    except Exception as e:
      log_error(f"Failed to create time series collection {self.db}.{table}", e)
      raise e

  async def insert(self, c: Ingester, table: str = ""):
    """Insert data into MongoDB time series collection."""
    await self.ensure_connected()
    table = table or c.name

    # Build document for time series collection
    persistent_fields = [field for field in c.fields if not field.transient]

    # Prepare document
    doc = {
      "ts": c.last_ingested,
      "meta": {
        "ingester": c.name,
        "tags": c.tags if c.tags else []
      }
    }

    # Add field values to document
    for field in persistent_fields:
      doc[field.name] = field.value

    try:
      if self.database is None:
        raise Exception("MongoDB database not connected")
      collection = self.database[table]
      await collection.insert_one(doc)
    except Exception as e:
      error_message = str(e).lower()
      if "collection does not exist" in error_message or "ns not found" in error_message:
        log_warn(f"Collection {table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        # Retry insert
        if self.database is None:
          raise Exception("MongoDB database not connected")
        collection = self.database[table]
        await collection.insert_one(doc)
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  async def insert_many(self, c: Ingester, values: List[Tuple], table: str = ""):
    """Insert multiple records into MongoDB time series collection."""
    await self.ensure_connected()
    table = table or c.name

    persistent_fields = [field for field in c.fields if not field.transient]

    # Build documents
    docs = []
    for value_tuple in values:
      timestamp = value_tuple[0]
      if not isinstance(timestamp, datetime):
        timestamp = datetime.fromtimestamp(timestamp, UTC)

      doc = {
        "ts": timestamp,
        "meta": {
          "ingester": c.name,
          "tags": c.tags if c.tags else []
        }
      }

      # Add field values
      for i, field in enumerate(persistent_fields):
        doc[field.name] = value_tuple[i + 1]  # Skip timestamp at index 0

      docs.append(doc)

    try:
      if self.database is None:
        raise Exception("MongoDB database not connected")
      collection = self.database[table]
      await collection.insert_many(docs)
    except Exception as e:
      error_message = str(e).lower()
      if "collection does not exist" in error_message or "ns not found" in error_message:
        log_warn(f"Collection {table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        # Retry insert
        if not self.database:
          raise Exception("Database not connected")
        if self.database is None:
          raise Exception("MongoDB database not connected")
        collection = self.database[table]
        await collection.insert_many(docs)
      else:
        log_error(f"Failed to batch insert data into {self.db}.{table}", e)
        raise e

  async def fetch(
    self,
    table: str,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    aggregation_interval: Interval = "m5",
    columns: list[str] = []
  ) -> tuple[list[str], list[tuple]]:
    """Fetch data from MongoDB time series collection with aggregation."""
    await self.ensure_connected()

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    if self.database is None:
      raise Exception("MongoDB database not connected")
    collection = self.database[table]

    # Build aggregation pipeline
    pipeline = [
      # Match time range
      {
        "$match": {
          "ts": {
            "$gte": from_date,
            "$lte": to_date
          }
        }
      }
    ]

    # If specific columns requested, project only those
    if columns:
      project_stage: Dict[str, Any] = {"ts": 1}
      for col in columns:
        project_stage[col] = 1
      pipeline.append({"$project": project_stage})

    # Add time bucketing for aggregation
    bucket_expr = BUCKET_GRANULARITY.get(aggregation_interval)
    if bucket_expr:
      group_fields: Dict[str, Any] = {col: {"$last": f"${col}"} for col in columns} if columns else {"data": {"$last": "$$ROOT"}}
      group_stage: Dict[str, Any] = {
        "$group": {
          "_id": bucket_expr,
          "ts": {"$last": "$ts"},
          **group_fields
        }
      }
      sort_stage: Dict[str, Any] = {"$sort": {"ts": -1}}
      pipeline.extend([group_stage, sort_stage])
    else:
      # No aggregation, just sort
      sort_stage_simple: Dict[str, Any] = {"$sort": {"ts": -1}}
      pipeline.append(sort_stage_simple)

    try:
      cursor = collection.aggregate(pipeline)
      results = await cursor.to_list(length=None)

      if not results:
        return ([], [])

      # Extract column names if not specified
      if not columns and results:
        # Get columns from first document (excluding _id, ts, meta)
        sample_doc = results[0].get("data", results[0])
        columns = [key for key in sample_doc.keys()
                  if key not in ["_id", "ts", "meta"]]

      result_columns = ["ts"] + columns
      result_data = []

      for result in results:
        if "data" in result:
          # Aggregated result with full document
          doc = result["data"]
          row = [doc["ts"]] + [doc.get(col) for col in columns]
        else:
          # Direct aggregated result
          row = [result["ts"]] + [result.get(col) for col in columns]

        result_data.append(tuple(row))

      return (result_columns, result_data)

    except Exception as e:
      log_error(f"Failed to fetch data from {self.db}.{table}", e)
      raise e

  async def fetch_batch(
    self,
    tables: list[str],
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    aggregation_interval: Interval = "m5",
    columns: list[str] = []
  ) -> tuple[list[str], list[tuple]]:
    """Fetch data from multiple MongoDB collections."""
    from asyncio import gather

    results = await gather(*[
      self.fetch(table, from_date, to_date, aggregation_interval, columns)
      for table in tables
    ])

    # Combine results from all collections
    all_columns: list[str] = []
    all_data: list[tuple] = []

    for columns_result, data in results:
      if not all_columns:
        all_columns = columns_result
      all_data.extend(data)

    return (all_columns, all_data)

  async def fetchall(self):
    """MongoDB doesn't have a universal 'fetch all' - need collection name."""
    raise NotImplementedError("fetchall requires collection name for MongoDB")

  async def commit(self):
    """MongoDB auto-commits by default."""
    pass

  async def list_tables(self) -> list[str]:
    """List MongoDB collections."""
    await self.ensure_connected()
    try:
      if self.database is None:
        return []
      collections = await self.database.list_collection_names()
      return collections
    except Exception as e:
      log_error(f"Failed to list collections from {self.db}", e)
      return []
