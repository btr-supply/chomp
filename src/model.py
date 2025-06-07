from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from aiocron import Cron
from asyncio import gather
import httpx
from socket import gethostname
from os import getpid
from typing import Literal, Optional, TypeVar, Any, get_args, cast, Union, Callable
from enum import Flag, auto
import time

from .utils.uid import generate_instance_name, get_instance_uid
from .utils import now, is_primitive, is_epoch, Interval, TimeUnit, extract_time_unit, interval_to_seconds, split_chain_addr, function_signature, selector_inputs, selector_outputs
from .utils.runtime import async_cache

T = TypeVar('T')

ResourceType = Literal[
  "value", # e.g., inplace document (json/text), binary, int, float, date, string...
  "series", # increment indexed values
  "timeseries" # time indexed values
]

IngesterType = Literal[
  "scrapper", "http_api", "ws_api", "fix_api", # web2
  "evm_caller", "evm_logger", # web3
  "svm_caller", "svm_logger",
  "sui_caller", "sui_logger",
  "aptos_caller", "aptos_logger",
  "ton_caller", "ton_logger",
  "processor", # post-processing
]

TsdbAdapter = Literal["tdengine", "timescale", "influx", "kx", "sqlite", "clickhouse", "duckdb", "mongodb", "opentsdb", "victoriametrics"]

FieldType = Literal[
  "int8", "uint8", # char, uchar
  "int16", "uint16", # short, ushort
  "int32", "uint32", # int, uint
  "int64", "uint64", # long, ulong
  "float32", "ufloat32", # float, ufloat
  "float64", "ufloat64", # double, udouble
  "bool", "timestamp", "string", "binary", "varbinary"]

HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

DataFormat = Literal[
  "py:row", "py:column", "np:row", "np:column", "polars", # python native
  "json", "json:row", "json:column", "row", "column", # json
  "csv", "tsv", "psv", "parquet", "pqt", "arrow", "feather", "orc", "avro" # other formats
]

FillMode = Literal["none", "back_fill", "forward_fill", "interpolate"] # same as polars for direct use

UNALIASED_FORMATS: dict[DataFormat, DataFormat] = {
  "json": "json:row",
  "column": "json:column",
  "row": "json:row",
  "json:column": "json:column",
  "json:row": "json:row",
  "csv": "csv",
  "tsv": "tsv",
  "psv": "psv",
  "parquet": "parquet",
  "pqt": "parquet",
  "arrow": "arrow",
  "feather": "feather",
  "orc": "orc", # not supported
  "avro": "avro",
  "np:column": "np:column",
  "py:column": "py:column",
  "np:row": "np:row",
  "py:row": "py:row",
  "polars": "polars",
}

class Scope(Flag):
  TRANSIENT = auto()      # 0b0001 == 1 << 0
  TARGET = auto()         # 0b0010 == 1 << 1
  SELECTOR = auto()       # 0b0100 == 1 << 2
  TRANSFORMERS = auto()   # 0b1000 == 1 << 3
  ALL = TRANSIENT | TARGET | SELECTOR | TRANSFORMERS
  DEFAULT = TARGET
  DETAILED = TARGET | SELECTOR | TRANSFORMERS
  DEBUG = ALL

SCOPE_ATTRS = {
  Scope.TARGET: "target",
  Scope.SELECTOR: "selector",
  Scope.TRANSFORMERS: "transformers"
}

def to_scope_mask(names: dict[str, bool]) -> Scope:
  mask = Scope(0)
  for name, enabled in names.items():
    if enabled and hasattr(Scope, name.upper()):
      mask |= getattr(Scope, name.upper())
  return mask if mask else Scope.DEFAULT

@dataclass
class RequestVitals:
  """Minimal container for request performance metrics"""
  instance_name: str = ""
  field_count: int = 0
  latency_ms: float = 0.0
  response_bytes: int = 0
  status_code: Optional[int] = None

@dataclass
class InstanceVitals:
  """Minimal container for instance performance metrics"""
  instance_name: str = ""
  resources_count: int = 0
  cpu_usage: float = 0.0 # percent of cpu or cpu count
  memory_usage: float = 0.0 # bytes
  disk_usage: float = 0.0 # bytes per second

class Monitor:
  """Minimal monitoring for ingester requests"""

  def __init__(self):
    self._start_time: float = 0.0

  def start_timer(self) -> None:
    """Start timing a request"""
    self._start_time = time.time()

  def stop_timer(self, response_bytes: int, status_code: Optional[int] = None) -> RequestVitals:
    """Stop timer and return metrics"""
    end_time = time.time()
    latency_ms = (end_time - self._start_time) * 1000 if self._start_time > 0 else 0.0

    # Store vitals on the monitor for later collection by store()
    self.vitals = RequestVitals(
      latency_ms=latency_ms,
      response_bytes=response_bytes,
      status_code=status_code
    )

    return self.vitals

@dataclass
class Targettable:
  name: str
  target: str = ""
  selector: str = ""
  selector_inputs: list[str] = field(default_factory=list)
  selector_outputs: list[str] = field(default_factory=list)
  method: HttpMethod = "GET"
  pre_transformer: str = ""
  headers: dict[str, str] = field(default_factory=dict)
  params: list[Any]|dict[str,Any] = field(default_factory=list)
  type: FieldType = "float64"
  handler: Union[str, Callable[..., Any]] = "" # for streams only (json ws, fix...)
  reducer: Union[str, Callable[..., Any]] = "" # for streams only (json ws, fix...)
  actions: list[Any] = field(default_factory=list) # for dynamic scrappers only
  transformers: list[str] = field(default_factory=list)
  tags: list[str] = field(default_factory=list)

  @property
  def target_id(self) -> str:
    return md5((self.target + self.selector + str(self.params) + str(self.actions) + function_signature(self.handler)).encode()).hexdigest()

  @property
  def output_count(self) -> int:
    return len(self.selector_outputs)

@dataclass
class ResourceField(Targettable):
  transient: bool = False
  value: Optional[Any] = None

  def signature(self) -> str:
    return f"{self.name}-{self.type}-{self.target}-{self.selector}-[{','.join(str(self.params))}]-[{','.join(self.transformers) if self.transformers else 'raw'}]"

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  def __hash__(self) -> int:
    return hash(self.id)

  def sql_escape(self) -> str:
    return f"'{self.value}'" if self.type in ["string", "binary", "varbinary"] else str(self.value)

  def chain_addr(self) -> tuple[str|int, str]:
    return split_chain_addr(self.target)

  @classmethod
  def from_dict(cls, d: dict) -> 'ResourceField':
    return cls(**d)

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    return {
      "type": self.type,
      "target": self.target if scope & Scope.TARGET else None,
      "selector": self.selector if scope & Scope.SELECTOR else None,
      "transformers": self.transformers if scope & Scope.TRANSFORMERS else None,
      "tags": self.tags,
      "transient": self.transient,
    }

@dataclass
class Resource:
  name: str
  resource_type: ResourceType = "timeseries"
  fields: list[ResourceField] = field(default_factory=list)
  data_by_field: dict[str, Any] = field(default_factory=dict)

  def field_by_name(self, include_transient=False) -> dict[str, ResourceField]:
    return {field.name: field for field in self.fields if not field.transient or include_transient}

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    return {
      "name": self.name,
      "type": self.resource_type,
      "fields": {
        field.name: field.to_dict(scope)
        for field in self.fields
        if scope & Scope.TRANSIENT or not field.transient
      }
    }

@dataclass
class Ingester(Resource, Targettable):
  interval: Interval = "h1"
  probablity: float = 1.0
  ingester_type: IngesterType = "evm_caller"
  started: datetime | None = None
  last_ingested: datetime | None = None
  cron: Optional[Cron] = None
  monitor: Optional[Monitor] = field(default_factory=lambda: Monitor())

  @classmethod
  def from_config(cls, d: dict):
    # Convert field dictionaries to ResourceField objects
    if 'fields' in d and isinstance(d['fields'], list):
      d['fields'] = [ResourceField.from_dict(field) if isinstance(field, dict) else field for field in d['fields']]

    r = cls(**d)
    r.started = now()

    for field_item in r.fields:
      if not field_item.tags and r.tags:
        field_item.tags = r.tags
      if not field_item.target.startswith("http") and r.target:
        field_item.target = r.target + field_item.target
      elif not field_item.target:
        field_item.target = r.target
      if not field_item.selector:
        field_item.selector = r.selector
      if not field_item.selector_inputs:
        field_item.selector_inputs = selector_inputs(field_item.selector)
      if not field_item.selector_outputs:
        field_item.selector_outputs = selector_outputs(field_item.selector)
      if not field_item.params:
        field_item.params = r.params
      if not field_item.type:
        field_item.type = r.type
      if not field_item.transformers and r.transformers:
        field_item.transformers = r.transformers  # TODO: assess default behaviour
      if field_item.params:
        if is_primitive(field_item.params):
          field_item.params = [field_item.params]
        for i, param in enumerate(field_item.params):
          if isinstance(param, dict):
            if is_epoch(param.get("value", param.get("default", ""))):
              param["value"] = param.get("value", param.get("default", ""))
            if is_epoch(param.get("default", "")):
              param["default"] = int(now().timestamp())
            if param.get("type") == "time" and not param.get("value") and not param.get("default"):
              param["default"] = round(now().timestamp())
          if isinstance(field_item.params, dict) and "time" in field_item.params:  # TODO: genericize to all time fields
            field_item.params["time"] = round(now().timestamp())
      if not field_item.handler:
        field_item.handler = r.handler
    return r

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    return {
      **super().to_dict(scope),
      "interval": self.interval,
      "probablity": self.probablity,
      "started": self.started,
      "last_ingested": self.last_ingested,
      "ingester_type": self.ingester_type,
    }

  def signature(self) -> str:
    return f"{self.name}-{self.resource_type}-{self.interval}-{self.ingester_type}"\
      + "-".join([field.id for field in self.fields])

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  @property
  def interval_sec(self) -> int:
    return interval_to_seconds(self.interval)

  @property
  def precision(self) -> TimeUnit:
    unit, _ = extract_time_unit(self.interval)
    return cast(TimeUnit, unit)

  def __hash__(self) -> int:
    return hash(self.id)

  def values(self):
    return [field.value for field in self.fields]

  def values_dict(self):
    d = {field.name: field.value for field in self.fields if not field.transient}
    d["date"] = self.last_ingested
    return d

  def load_values(self, values: list[Any]):
    for i, field_item in enumerate(self.fields):
      field_item.value = values[i]

  def load_values_dict(self, values: dict[str, Any]):
    for field_item in self.fields:
      field_item.value = values[field_item.name]

  def dependencies(self) -> list[str]:
    # extract name part of selector (eg. "BinanceFeeds.BTC" -> "BinanceFeeds") if any and not none
    return list({
      field.selector.split('.', 1)[0].strip()
      for field in self.fields
      if field.selector and field.selector[0] != '.' and '.' in field.selector
    })



class ConfigMeta(type):
  def __new__(cls, name, bases, dct):
    # iterate over all IngesterType
    for ingester_type in get_args(IngesterType):
      attr_name = ingester_type.lower()
      dct[attr_name] = field(default_factory=list) # add field default to the class
      dct.setdefault('__annotations__', {})[attr_name] = list[Ingester] # add type hint to the class
    return super().__new__(cls, name, bases, dct)

@dataclass
class Config(metaclass=ConfigMeta):
  # Explicitly define attributes for mypy (these are also added by the metaclass)
  scrapper: list[Ingester] = field(default_factory=list)
  http_api: list[Ingester] = field(default_factory=list)
  ws_api: list[Ingester] = field(default_factory=list)
  fix_api: list[Ingester] = field(default_factory=list)
  evm_caller: list[Ingester] = field(default_factory=list)
  evm_logger: list[Ingester] = field(default_factory=list)
  svm_caller: list[Ingester] = field(default_factory=list)
  svm_logger: list[Ingester] = field(default_factory=list)
  sui_caller: list[Ingester] = field(default_factory=list)
  sui_logger: list[Ingester] = field(default_factory=list)
  aptos_caller: list[Ingester] = field(default_factory=list)
  aptos_logger: list[Ingester] = field(default_factory=list)
  ton_caller: list[Ingester] = field(default_factory=list)
  ton_logger: list[Ingester] = field(default_factory=list)
  processor: list[Ingester] = field(default_factory=list)
  @classmethod
  def from_dict(cls, data: dict) -> 'Config':
    config_dict = {}
    for ingester_type in get_args(IngesterType):
      key = ingester_type.lower() # match yaml config e.g., scrapper, http_api, evm_caller...
      items = data.get(key, [])
      if not items:
        continue # skip missing categories
      # inject ingester_type into each to instantiate the correct ingester
      config_dict[key] = [Ingester.from_config({**item, 'ingester_type': ingester_type}) for item in items]
    return cls(**config_dict)

  @property
  def ingesters(self):
    return self.scrapper + self.http_api + self.ws_api \
      + self.evm_caller + self.evm_logger \
      + self.svm_caller + self.sui_caller + self.aptos_caller + self.ton_caller \
      + self.processor # + ...

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    return {
      r.name: r.to_dict(scope) for r in self.ingesters
    }

@dataclass
class Tsdb:
  host: str = "localhost"
  port: int = 40002 # 6030
  db: str = "default"
  user: str = "rw"
  password: str = "pass"
  conn: Any = None
  cursor: Any = None

  @classmethod
  async def connect(cls, host: str, port: int, db: str, user: str, password: str):
    raise NotImplementedError
  async def ping(self):
    raise NotImplementedError
  async def ensure_connected(self):
    raise NotImplementedError
  async def close(self):
    raise NotImplementedError
  async def create_db(self, name: str, options: dict, force=False):
    raise NotImplementedError
  async def use_db(self, db: str):
    raise NotImplementedError
  async def create_table(self, c: Ingester, name=""):
    raise NotImplementedError
  async def insert(self, c: Ingester, table=""):
    raise NotImplementedError
  async def insert_many(self, c: Ingester, values: list[tuple], table=""):
    raise NotImplementedError
  async def fetch(self, table: str, from_date: datetime, to_date: datetime, aggregation_interval: Interval, columns: list[str]):
    raise NotImplementedError
  async def fetchall(self):
    raise NotImplementedError
  async def fetch_batch(self, tables: list[str], from_date: datetime, to_date: datetime, aggregation_interval: Interval, columns: list[str]) -> tuple[list[str], list[tuple]]:
    raise NotImplementedError
  async def commit(self):
    raise NotImplementedError
  async def list_tables(self) -> list[str]:
    raise NotImplementedError

ServiceResponse = tuple[str, T]  # (error_message, result)

@dataclass
class Instance:
  """Represents an instance of chomp (either ingester or server)"""
  pid: int
  hostname: str
  uid: str = ""  # computed from launch command, config, hostname - internal use only
  ipv4: str = ""
  ipv6: str = ""
  name: str = ""  # human-friendly name from uid-masks
  mode: Literal["ingester", "server"] = "ingester"
  resources_count: int = 0
  monitored: bool = False
  started_at: datetime = field(default_factory=now)
  updated_at: datetime = field(default_factory=now)

  # Geolocation and ISP information (cached, transient)
  coordinates: str = ""  # "lat,lon" format
  timezone: str = ""
  country_code: str = ""
  location: str = ""  # "city, region, country" format
  isp: str = ""

  @property
  def id(self) -> str:
    """Use UID as primary identifier"""
    return self.uid

  def __hash__(self) -> int:
    """Make Instance hashable for cache keys using UID"""
    return hash(self.uid)

  def to_dict(self) -> dict:
    """Convert instance to dictionary for storage/serialization"""
    return {
      "uid": self.uid,
      "pid": self.pid,
      "hostname": self.hostname,
      "ipv4": self.ipv4,
      "ipv6": self.ipv6,
      "name": self.name,
      "mode": self.mode,
      "resources_count": self.resources_count,
      "monitored": self.monitored,
      "started_at": self.started_at.isoformat(),
      "updated_at": self.updated_at.isoformat() if self.updated_at else None,
      "coordinates": self.coordinates,
      "timezone": self.timezone,
      "country_code": self.country_code,
      "location": self.location,
      "isp": self.isp,
    }

  @classmethod
  async def from_dict(cls, data: dict) -> 'Instance':
    """Create instance from dictionary with async name generation"""
    # Handle datetime strings
    started_at = data.get('started_at') or now()
    if isinstance(started_at, str):
      started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))

    updated_at = data.get('updated_at') or now()
    if isinstance(updated_at, str):
      updated_at = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))

    uid = data.get('uid') or get_instance_uid()
    name = data.get('name') or await generate_instance_name()
    pid = data.get('pid') or getpid()
    hostname = data.get('hostname') or gethostname()
    instance = cls(
      uid=uid,
      pid=pid,
      hostname=hostname,
      name=name,
      mode=data.get('mode', 'ingester'),
      resources_count=data.get('resources_count', 0),
      monitored=data.get('monitored', False),
      started_at=started_at,
      updated_at=updated_at,
      coordinates=data.get('coordinates', ''),
      timezone=data.get('timezone', ''),
      country_code=data.get('country_code', ''),
      location=data.get('location', ''),
      isp=data.get('isp', ''),
    )
    await instance.get_ip_addresses()
    return instance

  @async_cache(ttl=21600, maxsize=1)
  async def get_ip_addresses(self) -> tuple[Optional[str], Optional[str]]:
    """Get instance IPv4 and IPv6 addresses with geolocation data, 6-hour caching"""

    async def get_ip(url: str) -> Optional[str]:
        try:
            response = await session.get(url)
            return response.json().get("ip") if response.status_code == 200 else None
        except Exception:
            return None

    async def get_geolocation(ip: str) -> dict:
        """Get geolocation and ISP data for an IP address"""
        try:
            response = await session.get(f"http://ip-api.com/json/{ip}")
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return data
        except Exception:
            pass
        return {}

    async with httpx.AsyncClient(timeout=3.0) as session:
        # Get IP addresses - split tuple unpacking to avoid mypy crash
        ip_results = await gather(
            get_ip("https://api4.ipify.org/?format=json"),
            get_ip("https://api6.ipify.org/?format=json")
        )
        self.ipv4 = ip_results[0] or ""
        self.ipv6 = ip_results[1] or ""

        # Get geolocation data for the primary IP (IPv4 preferred, fallback to IPv6)
        primary_ip = self.ipv4 or self.ipv6
        if primary_ip:
            geo_data = await get_geolocation(primary_ip)
            if geo_data:
                # Extract and format the data as requested
                lat = geo_data.get("lat", "")
                lon = geo_data.get("lon", "")
                self.coordinates = f"{lat},{lon}" if lat and lon else ""

                city = geo_data.get("city", "")
                region = geo_data.get("regionName", "")
                country = geo_data.get("country", "")
                location_parts = [part for part in [city, region, country] if part]
                self.location = ", ".join(location_parts)

                self.timezone = geo_data.get("timezone", "")
                self.country_code = geo_data.get("countryCode", "")
                self.isp = geo_data.get("isp", "")

    return self.ipv4, self.ipv6
