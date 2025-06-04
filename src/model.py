from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from aiocron import Cron
from typing import Literal, Optional, TypeVar, Any, get_args, cast, Union, Callable
from enum import Flag, auto

from .utils import now, is_primitive, is_epoch, Interval, TimeUnit, extract_time_unit, interval_to_seconds, split_chain_addr, function_signature, selector_inputs, selector_outputs

T = TypeVar('T')

ResourceType = Literal[
  "value", # e.g., inplace document (json/text), binary, int, float, date, string...
  "series", # increment indexed values
  "timeseries" # time indexed values
]

IngesterType = Literal[
  "scrapper", "http_api", "ws_api", "fix_api", # web2
  "evm_caller", "evm_logger", # web3
  "solana_caller", "solana_logger",
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

  @classmethod
  def from_config(cls, d: dict):
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
      + self.solana_caller + self.sui_caller + self.aptos_caller + self.ton_caller \
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
