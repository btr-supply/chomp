from datetime import datetime, timezone
from os import environ as env
from typing import Any, Optional

from ..utils import log_error, log_info, Interval, ago, now
from ..models.base import Tsdb
from ..models.ingesters import Ingester, UpdateIngester

from motor.motor_asyncio import AsyncIOMotorClient

UTC = timezone.utc

# MongoDB time series granularity mapping
GRANULARITY_MAP: dict[Interval, str] = {
    "s1": "seconds",
    "s2": "seconds",
    "s5": "seconds",
    "s10": "seconds",
    "s15": "seconds",
    "s20": "seconds",
    "s30": "seconds",
    "m1": "minutes",
    "m2": "minutes",
    "m5": "minutes",
    "m10": "minutes",
    "m15": "minutes",
    "m30": "minutes",
    "h1": "hours",
    "h2": "hours",
    "h4": "hours",
    "h6": "hours",
    "h8": "hours",
    "h12": "hours",
    "D1": "hours",
    "D2": "hours",
    "D3": "hours",
    "W1": "hours",
    "M1": "hours",
    "Y1": "hours"
}

# MongoDB aggregation pipeline interval mapping
BUCKET_GRANULARITY: dict[Interval, Any] = {
    "s1": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M:%S",
            "date": "$ts"
        }
    },
    "s5": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M:%S",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "second",
                    "binSize": 5
                }
            }
        }
    },
    "s10": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M:%S",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "second",
                    "binSize": 10
                }
            }
        }
    },
    "s15": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M:%S",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "second",
                    "binSize": 15
                }
            }
        }
    },
    "s30": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M:%S",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "second",
                    "binSize": 30
                }
            }
        }
    },
    "m1": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "minute"
                }
            }
        }
    },
    "m5": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "minute",
                    "binSize": 5
                }
            }
        }
    },
    "m15": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "minute",
                    "binSize": 15
                }
            }
        }
    },
    "m30": {
        "$dateToString": {
            "format": "%Y-%m-%d %H:%M",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "minute",
                    "binSize": 30
                }
            }
        }
    },
    "h1": {
        "$dateToString": {
            "format": "%Y-%m-%d %H",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "hour"
                }
            }
        }
    },
    "h4": {
        "$dateToString": {
            "format": "%Y-%m-%d %H",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "hour",
                    "binSize": 4
                }
            }
        }
    },
    "h12": {
        "$dateToString": {
            "format": "%Y-%m-%d %H",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "hour",
                    "binSize": 12
                }
            }
        }
    },
    "D1": {
        "$dateToString": {
            "format": "%Y-%m-%d",
            "date": {
                "$dateTrunc": {
                    "date": "$ts",
                    "unit": "day"
                }
            }
        }
    },
}


class MongoDb(Tsdb):
  """MongoDB adapter for time series and update data storage."""

  client: Optional[Any] = None
  database: Optional[Any] = None

  def __init__(self,
               host: str = "localhost",
               port: int = 27017,
               db: str = "chomp",
               user: str = "",
               password: str = ""):
    super().__init__(host, port, db, user, password)

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "MongoDb":
    self = cls(host=host or env.get("MONGODB_HOST") or "localhost",
               port=int(port or env.get("MONGODB_PORT") or 27017),
               db=db or env.get("MONGODB_DB") or "chomp",
               user=user or env.get("DB_RW_USER") or "",
               password=password or env.get("DB_RW_PASS") or "")
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
        # Build connection URI
        if self.user and self.password:
          uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        else:
          uri = f"mongodb://{self.host}:{self.port}/"

        self.client = AsyncIOMotorClient(uri)
        self.database = self.client[self.db]

        # Test the connection
        await self.client.admin.command('ping')

        log_info(
            f"Connected to MongoDB on {self.host}:{self.port}/{self.db} as {self.user or 'anonymous'}"
        )
      except Exception as e:
        log_error(
            f"Failed to connect to MongoDB on {self.host}:{self.port}/{self.db}",
            e)
        raise e

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    """Create MongoDB database."""
    await self.ensure_connected()
    if self.client is None or self.database is None:
      raise RuntimeError("Database connection not established")

    try:
      # MongoDB creates databases implicitly when first collection is created
      # Just switch to the new database
      self.database = self.client[name]
      self.db = name

      # Create a dummy collection to ensure database exists
      await self.database.create_collection("_metadata")

      log_info(f"Created/switched to database {name}")
    except Exception as e:
      log_error(f"Failed to create database {name}", e)
      raise e

  async def use_db(self, db: str):
    """Switch to different MongoDB database."""
    await self.ensure_connected()
    if self.client is None:
      raise RuntimeError("Database connection not established")
    self.database = self.client[db]
    self.db = db
    log_info(f"Switched to database {db}")

  async def create_table(self, ing: Ingester, name: str = ""):
    """Create MongoDB collection (table equivalent)."""
    await self.ensure_connected()
    if self.database is None:
      raise RuntimeError("Database connection not established")

    table = name or ing.name

    try:
      if ing.resource_type == 'timeseries':
        # Create time series collection for time series data
        granularity = GRANULARITY_MAP.get(ing.interval, "seconds")

        await self.database.create_collection(table,
                                              timeseries={
                                                  "timeField": "ts",
                                                  "granularity": granularity
                                              })
        log_info(
            f"Created time series collection {table} with granularity {granularity}"
        )

      elif ing.resource_type == 'update':
        # Create regular collection with unique index on uid for update data
        collection = self.database[table]
        await collection.create_index("uid", unique=True)
        log_info(f"Created update collection {table} with unique uid index")

      else:
        # Create regular collection
        await self.database.create_collection(table)
        log_info(f"Created collection {table}")

    except Exception as e:
      if "already exists" in str(e).lower():
        log_info(f"Collection {table} already exists")
      else:
        log_error(f"Failed to create collection {table}", e)
        raise e

  async def insert(self, ing: Ingester, table: str = ""):
    """Insert single document into MongoDB collection."""
    await self.ensure_connected()
    if self.database is None:
      raise RuntimeError("Database connection not established")

    table = table or ing.name
    collection = self.database[table]

    # Build document from ingester fields
    document = {}

    # Add timestamp for time series
    if ing.resource_type == 'timeseries':
      document["ts"] = ing.ts or now()

    # Add field values
    for field in ing.fields:
      if not field.transient:
        document[field.name] = field.value

    try:
      result = await collection.insert_one(document)
      log_info(f"Inserted document with _id {result.inserted_id} into {table}")
    except Exception as e:
      log_error(f"Failed to insert document into {table}", e)
      raise e

  async def upsert(self, ing: UpdateIngester, table: str = "", uid: str = ""):
    """Upsert (update or insert) document in MongoDB collection."""
    await self.ensure_connected()
    if self.database is None:
      raise RuntimeError("Database connection not established")

    table = table or ing.name
    uid = uid or ing.uid
    collection = self.database[table]

    if not uid:
      raise ValueError("UID is required for upsert operations")

    # Build document from ingester fields
    document = {}
    for field in ing.fields:
      if not field.transient:
        document[field.name] = field.value

    # Ensure uid is in the document
    document["uid"] = uid

    try:
      result = await collection.replace_one({"uid": uid},
                                            document,
                                            upsert=True)

      if result.upserted_id:
        log_info(f"Inserted new document with uid {uid} into {table}")
      else:
        log_info(f"Updated existing document with uid {uid} in {table}")

    except Exception as e:
      log_error(f"Failed to upsert document with uid {uid} into {table}", e)
      raise e

  async def fetch_by_id(self, table: str, uid: str):
    """Fetch single document by UID from MongoDB collection."""
    await self.ensure_connected()
    if self.database is None:
      raise RuntimeError("Database connection not established")

    collection = self.database[table]

    try:
      document = await collection.find_one({"uid": uid})

      if document:
        # Remove MongoDB's _id field if present
        if "_id" in document:
          del document["_id"]
        return document
      return None

    except Exception as e:
      log_error(f"Failed to fetch document with uid {uid} from {table}", e)
      return None

  async def fetch(self,
                  table: str,
                  from_date: Optional[datetime] = None,
                  to_date: Optional[datetime] = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    """Fetch data from MongoDB time series collection with aggregation."""
    await self.ensure_connected()
    if self.database is None:
      raise RuntimeError("Database connection not established")

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

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
      project_stage: dict[str, Any] = {"ts": 1}
      for col in columns:
        project_stage[col] = 1
      pipeline.append({"$project": project_stage})

    # Add time bucketing for aggregation
    bucket_expr = BUCKET_GRANULARITY.get(aggregation_interval)
    if bucket_expr:
      group_fields: dict[str, Any] = {
          col: {
              "$last": f"${col}"
          }
          for col in columns
      } if columns else {
          "data": {
              "$last": "$$ROOT"
          }
      }
      group_stage: dict[str, Any] = {
          "$group": {
              "_id": bucket_expr,
              "ts": {
                  "$last": "$ts"
              },
              **group_fields
          }
      }
      sort_stage: dict[str, Any] = {"$sort": {"ts": -1}}
      pipeline.extend([group_stage, sort_stage])
    else:
      # No aggregation, just sort
      sort_stage_simple: dict[str, Any] = {"$sort": {"ts": -1}}
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
        columns = [
            key for key in sample_doc.keys()
            if key not in ["_id", "ts", "meta"]
        ]

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
      from_date: Optional[datetime] = None,
      to_date: Optional[datetime] = None,
      aggregation_interval: Interval = "m5",
      columns: list[str] = []) -> tuple[list[str], list[tuple]]:
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

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs using MongoDB $in operator for efficiency"""
    await self.ensure_connected()
    if self.database is None:
      return []

    try:
      if not uids:
        return []

      collection = self.database[table]

      # Use MongoDB's $in operator for efficient batch fetching
      cursor = collection.find({"uid": {"$in": uids}}).sort("updated_at", -1)
      documents = await cursor.to_list(length=None)

      if not documents:
        return []

      # Convert MongoDB documents to tuples (similar to other adapters)
      # We need to maintain consistent field ordering
      results = []
      for doc in documents:
        # Remove MongoDB's _id field and convert to tuple
        if "_id" in doc:
          del doc["_id"]

        # Convert document values to tuple (order should match field definitions)
        # For consistency, we'll return the document as a tuple of values
        # The calling code should handle field mapping
        result_tuple = tuple(doc.values())
        results.append(result_tuple)

      return results
    except Exception as e:
      log_error(
          f"Failed to fetch batch records by IDs from {self.db}.{table}: {e}")
      return []

  async def commit(self):
    """MongoDB auto-commits by default."""
    pass

  async def list_tables(self) -> list[str]:
    """List MongoDB collections."""
    await self.ensure_connected()
    if self.database is None:
      return []

    try:
      collections = await self.database.list_collection_names()
      return collections
    except Exception as e:
      log_error(f"Failed to list collections from {self.db}", e)
      return []

  async def _connect(self):
    """MongoDB-specific connection using motor."""
    try:
      client = AsyncIOMotorClient(
          f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
          if self.user and self.password else
          f"mongodb://{self.host}:{self.port}/")
      database = client[self.db]

      # Test the connection
      await database.list_collection_names()

      self.client = client
      self.database = database
      log_info(f"Connected to MongoDB at {self.host}:{self.port}")
      return {"client": client, "database": database}
    except Exception as e:
      log_error(f"Failed to connect to MongoDB: {e}")
      raise e

  async def _init_db(self):
    """Initialize MongoDB database."""
    try:
      client = AsyncIOMotorClient(
          f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
          if self.user and self.password else
          f"mongodb://{self.host}:{self.port}/")
      self.client = client
      self.database = client[self.db]

      # Test the connection
      await self.database.list_collection_names()

      log_info(f"Connected to MongoDB at {self.host}:{self.port}")
    except Exception as e:
      log_error("Failed to initialize MongoDB database", e)
      raise e

  @property
  def database_name(self) -> str:
    """Return the database name"""
    return self.db
