from dataclasses import dataclass, field, fields
from datetime import datetime
from hashlib import md5
from os import getpid
from socket import gethostname
from typing import Literal, Any, Union, Callable, Optional, Final
from enum import Flag, auto

from ..utils.types import safe_field_value
from ..utils.date import now, parse_date, Interval
from ..utils.format import function_signature, split_chain_addr, selector_inputs, selector_outputs
from ..utils.reflexion import DictMixin
from ..utils.decorators import cache
from ..utils.uid import get_instance_uid, generate_instance_name
from ..utils import log_error, log_warn

SYS_FIELDS = {'ts', 'uid', 'created_at', 'updated_at'}  # standard fields

# Type aliases that need to be defined here to avoid circular imports
TimeUnit = str

ResourceType = Literal[
    "update",  # e.g., upsert documents (users, configs, etc.)
    "series",  # increment indexed values
    "timeseries"  # time indexed values
]

IngesterType = Literal[
    "scrapper",
    "http_api",
    "ws_api",
    "fix_api",  # web2
    "evm_caller",
    "evm_logger",  # web3
    "svm_caller",
    "svm_logger",
    "sui_caller",
    "sui_logger",
    "aptos_caller",
    "aptos_logger",
    "ton_caller",
    "ton_logger",
    "processor",  # post-processing
]

AuthMethod = Literal[
    "static",  # Pre-defined token
    "email",  # Email-based authentication
    "oauth2_x",  # X OAuth2 authentication
    "oauth2_github",  # Github OAuth2 authentication
    "evm",  # Ethereum wallet authentication
    "svm",  # Solana wallet authentication
    "sui"  # Sui wallet authentication
]

UserStatus = Literal[
    "public",  # Regular user
    "admin",  # Admin user
    "banned"  # Banned user
]

TsdbAdapter = Literal["tdengine", "timescale", "influx", "kx", "sqlite",
                      "clickhouse", "duckdb", "mongodb",
                      "victoriametrics"]

FieldType = Literal[
    "int8",
    "uint8",  # char, uchar
    "int16",
    "uint16",  # short, ushort
    "int32",
    "uint32",  # int, uint
    "int64",
    "uint64",  # long, ulong
    "float32",
    "ufloat32",  # float, ufloat
    "float64",
    "ufloat64",  # double, udouble
    "bool",
    "timestamp",
    "string",
    "binary",
    "varbinary"]

HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

DataFormat = Literal[
    "py:row",
    "py:column",
    "np:row",
    "np:column",
    "polars",  # python native
    "json",
    "json:row",
    "json:column",
    "row",
    "column",  # json
    "csv",
    "tsv",
    "psv",
    "parquet",
    "pqt",
    "arrow",
    "feather",
    "orc",
    "avro"  # other formats
]

FillMode = Literal["none", "back_fill", "forward_fill",
                   "interpolate"]  # same as polars for direct use

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
    "orc": "orc",  # not supported
    "avro": "avro",
    "np:column": "np:column",
    "py:column": "py:column",
    "np:row": "np:row",
    "py:row": "py:row",
    "polars": "polars",
}


class Scope(Flag):
  # single scopes
  TRANSIENT = auto()  # 0b0001 == 1 << 0
  TARGET = auto()  # 0b0010 == 1 << 1
  SELECTOR = auto()  # 0b0100 == 1 << 2
  METHOD = auto()  # 0b1000 == 1 << 3
  TRANSFORMERS = auto()  # 0b10000 == 1 << 4
  PRE_TRANSFORMER = auto()  # 0b100000 == 1 << 5
  PARAMS = auto()  # 0b1000000 == 1 << 6
  PROTECTED = auto()  # 0b10000000 == 1 << 7
  # merged scopes
  ALL = TRANSIENT | TARGET | SELECTOR | METHOD | TRANSFORMERS | PRE_TRANSFORMER | PARAMS | PROTECTED
  DEFAULT = TARGET
  DETAILED = TRANSIENT | TARGET | SELECTOR
  DEBUG = ALL


SCOPES = {
    Scope.TARGET: "target",
    Scope.SELECTOR: "selector",
    Scope.METHOD: "method",
    Scope.TRANSFORMERS: "transformers",
    Scope.PRE_TRANSFORMER: "pre_transformer",
    Scope.PARAMS: "params",
    Scope.TRANSIENT: "transient",
    Scope.PROTECTED: "protected",
    Scope.ALL: "all",
    Scope.DEFAULT: "default",
    Scope.DETAILED: "detailed"
}


def to_scope_mask(names: dict[str, bool]) -> Scope:
  mask = Scope(0)
  for name, enabled in names.items():
    if enabled and hasattr(Scope, name.upper()):
      mask |= getattr(Scope, name.upper())
  return mask if mask else Scope.DEFAULT


def _infer_field_type(value: Any) -> FieldType:
  """Infer field type from value"""
  if value is None:
    return "string"

  # Type mapping for common types
  type_map: dict[type, FieldType] = {
    bool: "bool",
    int: "int64",
    float: "float64",
    str: "string",
    datetime: "timestamp",
  }

  # Check direct type match first
  value_type = type(value)
  if value_type in type_map:
    return type_map[value_type]
  elif isinstance(value, (list, tuple, dict)):
    return "string"  # Convert to JSON string representation
  else:
    return "string"


@dataclass
class RateLimitConfig(DictMixin):
  """Rate limiting configuration for requests and input validation"""
  rpm: int = 60
  rph: int = 1800
  rpd: int = 21600
  spm: int = 100_000_000  # 100MB per minute
  sph: int = 3_000_000_000  # 3GB per hour
  spd: int = 36_000_000_000  # 36GB per day
  ppm: int = 120  # points per minute
  pph: int = 3600  # points per hour
  ppd: int = 43200  # points per day


@dataclass
class InputRateLimitConfig(DictMixin):
  """Rate limiting configuration for input validation (failed auth attempts, etc.)"""
  start: int = 50  # Starting delay in seconds
  factor: float = 6.0  # Exponential backoff factor
  max: int = 345600  # Maximum delay (4 days in seconds)


@dataclass
class Targettable(DictMixin):
  name: str
  target: str = ""
  selector: str = ""
  method: HttpMethod = "GET"
  pre_transformer: str = ""
  headers: dict[str, str] = field(default_factory=dict)
  params: Union[list[Any], dict[str, Any]] = field(default_factory=list)
  type: FieldType = "float64"
  handler: Union[str, Callable[...,
                               Any]] = ""  # for streams only (json ws, fix...)
  reducer: Union[str, Callable[...,
                               Any]] = ""  # for streams only (json ws, fix...)
  actions: list[Any] = field(
      default_factory=list)  # for dynamic scrappers only
  transformers: list[str] = field(default_factory=list)
  tags: list[str] = field(default_factory=list)

  @property
  def target_id(self) -> str:
    return md5(
        f"{self.target or ''}{self.selector or ''}{self.params or ''}{self.actions or ''}{function_signature(self.handler)}".encode()
    ).hexdigest()

  @property
  def output_count(self) -> int:
    return len(self.selector_outputs)


@dataclass
class ResourceField(Targettable, DictMixin):
  transient: bool = False
  protected: bool = False
  value: Optional[Any] = None

  def signature(self) -> str:
    return f"{self.name}-{self.type}-{self.target}-{self.selector}-[{','.join(str(self.params))}]-[{','.join(self.transformers) if self.transformers else 'raw'}]"

  def __hash__(self) -> int:
    return hash(md5(self.signature().encode()).hexdigest())

  def sql_escape(self) -> str:
    return safe_field_value(self.value, self.type)

  def chain_addr(self) -> tuple[Union[str, int], str]:
    return split_chain_addr(self.target)

  @property
  def selector_inputs(self) -> list[str]:
    """Get selector inputs from the selector string"""
    return selector_inputs(self.selector) if self.selector else []

  @property
  def selector_outputs(self) -> list[str]:
    """Get selector outputs from the selector string"""
    return selector_outputs(self.selector) if self.selector else []

  @classmethod
  def from_dict(cls, d: dict) -> 'ResourceField':
    return cls(**d)

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    """Custom to_dict with scope filtering for ResourceField"""

    # Always include basic fields
    result = {"name": self.name, "type": self.type, "tags": self.tags}

    # Add scoped fields based on the scope mask
    result.update({
        attr: getattr(self, attr)
        for flag, attr in SCOPES.items() if scope & flag
    })

    # Always include transient and value fields when appropriate
    if self.value is not None:
      result["value"] = self.value

    return result


@dataclass
class Resource(DictMixin):
  name: str
  resource_type: ResourceType = "timeseries"
  protected: bool = False
  fields: list[ResourceField] = field(default_factory=list)
  field_by_name: dict[str, ResourceField] = field(default_factory=dict)

  def __post_init__(self):
    """Ensure field_by_name is properly populated after initialization"""
    # Always rebuild field_by_name dictionary from fields list
    self.field_by_name = {f.name: f for f in self.fields}

  def add_field(self, field_obj: ResourceField):
    """Add a field to the fields list"""
    self.fields.append(field_obj)
    self.field_by_name[field_obj.name] = field_obj

  def get_field(self, name_or_index: Union[str, int]) -> Optional[ResourceField]:
    """Get field by name or index"""
    if isinstance(name_or_index, str):
      try:
        return self.field_by_name.get(name_or_index)
      except AttributeError:
        return None
    elif 0 <= name_or_index < len(self.fields):
      return self.fields[name_or_index]
    return None

  def set_field(self, name_or_index: Union[str, int], value: Any) -> bool:
    """Set field value by name or index. Returns True if successful."""
    field_obj = self.get_field(name_or_index)
    if field_obj:
      field_obj.value = value
      return True
    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name_or_index}'")

  def get_field_values(self, include_transient: bool = True) -> dict[str, Any]:
    """Get all field values as a dictionary"""
    if include_transient:
      return {f.name: f.value for f in self.fields}
    return {f.name: f.value for f in self.fields if not f.transient}

  def set_field_values(self, values: dict[str, Any]) -> None:
    """Set multiple field values from dictionary"""
    for name, value in values.items():
      self.set_field(name, value)

  def __getattr__(self, name: str) -> Any:
    """Fallback getter to access field values when attribute doesn't exist"""
    if name.startswith('_') or name == 'field_by_name':
      raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    field_obj = self.get_field(name)
    if field_obj is not None:
      return field_obj.value

    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

  def __setattr__(self, name: str, value: Any) -> None:
    """Fallback setter to update field values when attribute doesn't exist"""
    # Handle core attributes and private attributes normally
    if name.startswith('_') or name in {'name', 'resource_type', 'protected', 'fields', 'field_by_name'}:
      super().__setattr__(name, value)
      return

    # Try to set field value first
    field_obj = self.get_field(name)
    if field_obj:
      field_obj.value = value
      return

    # If not a field, set as normal attribute
    super().__setattr__(name, value)

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    """Custom to_dict with scope filtering for Resource"""
    if scope == Scope.DEFAULT:
      return super().to_dict()

    return {
        "name": self.name,
        "type": self.resource_type,
        "protected": self.protected,
        "fields": {
            f.name: f.to_dict(scope) for f in self.fields
            if scope & Scope.TRANSIENT or not f.transient
        }
    }

  def get_persistent_fields(self) -> list[ResourceField]:
    """Get only non-transient fields"""
    return [field for field in self.fields if not field.transient]

  def get_persistent_field_names(self) -> list[str]:
    """Get names of non-transient fields"""
    return [field.name for field in self.fields if not field.transient]

  def get_persistent_field_values(self) -> list[Any]:
    """Get values of non-transient fields"""
    return [field.value for field in self.fields if not field.transient]

  @classmethod
  def from_record(cls, record: Any, resource_name: str = "", resource_type: ResourceType = "timeseries") -> 'Resource':
    """Convert any record type (dict, User, UpdateIngester, etc.) to Resource format"""
    if record is None:
      return cls(name=resource_name, resource_type=resource_type)

    # If it's already a Resource, return it
    if isinstance(record, cls):
      return record

    # If it has a to_dict method, use it
    if hasattr(record, 'to_dict') and callable(record.to_dict):
      try:
        record_dict = record.to_dict()
      except Exception:
        # If to_dict fails, fallback to direct conversion
        record_dict = getattr(record, '__dict__', {})
    elif isinstance(record, dict):
      record_dict = record
    else:
      # Convert object attributes to dict
      record_dict = getattr(record, '__dict__', {})

    # Extract resource info from the record if available
    name = record_dict.get('name', resource_name or 'unknown')
    r_type = record_dict.get('resource_type', resource_type)

    # Create ResourceFields from the record data
    fields = []
    for key, value in record_dict.items():
      if key not in ['name', 'resource_type', 'fields']:  # Skip meta fields
        field_type = _infer_field_type(value)
        field_data = {
          'name': key,
          'type': field_type,
          'value': value,
          'transient': False
        }
        fields.append(ResourceField(**field_data))

    return cls(
      name=name,
      resource_type=r_type,
      fields=fields
    )


@dataclass
class Tsdb:
  host: str = "localhost"
  port: int = 40002  # 6030
  db: str = "default"
  user: str = "rw"
  password: str = "pass"
  conn: Any = None
  cursor: Any = None

  @classmethod
  async def connect(cls, host: str, port: int, db: str, user: str,
                    password: str):
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

  async def create_table(self, ing: 'Ingester', name=""):  # type: ignore  # noqa: F821
    raise NotImplementedError

  async def insert(self, ing: 'Ingester', table=""):  # type: ignore  # noqa: F821
    raise NotImplementedError

  async def upsert(self, ing: 'UpdateIngester', table="", uid=""):  # type: ignore  # noqa: F821
    raise NotImplementedError

  async def fetch_by_id(self, table: str, uid: str):
    raise NotImplementedError

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs in a single query for efficiency

    Args:
      table: Table name to query
      uids: List of UID strings to fetch

    Returns:
      List of tuples representing the records (empty list if none found)
    """
    raise NotImplementedError

  async def fetchall(self):
    raise NotImplementedError

  async def insert_many(self, ing: 'Ingester', values: list[tuple], table=""):  # type: ignore  # noqa: F821
    raise NotImplementedError

  async def fetch(self, table: str, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None,
                  aggregation_interval: Interval = "m5", columns: list[str] = []):
    raise NotImplementedError

  async def fetch_batch(self, tables: list[str], from_date: Optional[datetime] = None,
                        to_date: Optional[datetime] = None, aggregation_interval: Interval = "m5",
                        columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    raise NotImplementedError

  async def commit(self):
    raise NotImplementedError

  async def list_tables(self) -> list[str]:
    raise NotImplementedError


@dataclass
class Instance(DictMixin):
  """Represents an instance of chomp (either ingester or server)"""
  pid: Final[int]
  hostname: Final[str]
  uid: Final[str]  # computed from launch command, config, hostname - internal use only
  name: Final[str]  # human-friendly name from uid-masks
  ipv4: str = ""
  ipv6: str = ""
  mode: Literal["ingester", "server"] = "ingester"
  resources_count: int = 0
  monitored: bool = False
  started_at: datetime = field(default_factory=now)
  updated_at: datetime = field(default_factory=now)
  args: dict[str, Any] = field(default_factory=dict)  # CLI/env arguments

  # Geolocation and ISP information (cached, transient)
  coordinates: str = ""  # "lat,lon" format
  timezone: str = ""
  country_code: str = ""
  location: str = ""  # "city, region, country" format
  isp: str = ""

  def __hash__(self) -> int:
    """Make Instance hashable for cache keys using UID"""
    return hash(self.uid)

  @classmethod
  async def from_dict(cls, data: dict) -> 'Instance':
    """Create instance from dictionary with async name generation"""
    # Handle special computed fields
    data['uid'] = data.get('uid') or get_instance_uid()
    data['name'] = data.get('name') or await generate_instance_name(data['uid'])
    data['pid'] = data.get('pid') or getpid()
    data['hostname'] = data.get('hostname') or gethostname()

    # Handle datetime fields
    for date_field in ['started_at', 'updated_at']:
      if date_field in data:
        parsed_date = parse_date(data[date_field]) if data[date_field] is not None else now()
        data[date_field] = parsed_date or now()
      else:
        data[date_field] = now()

    # Filter to only include valid dataclass fields
    field_names = {f.name for f in fields(cls)}
    filtered_data = {k: v for k, v in data.items() if k in field_names}

    instance = cls(**filtered_data)
    await instance.get_ip_addresses()
    return instance

  @cache(ttl=21600, maxsize=1)
  async def get_ip_addresses(self) -> tuple[Optional[str], Optional[str]]:
    """Get instance IPv4 and IPv6 addresses with geolocation data, Redis cached for 1 week"""

    # Get IP addresses with Redis caching (1 week TTL) - handle failures gracefully
    ipv4_result = None
    ipv6_result = None

    from ..utils.http import cached_http_get
    ipv4_result = await cached_http_get(
        "https://api4.ipify.org/?format=json",
        f"ipv4:{self.uid}",
        ttl=604800)

    ipv6_result = await cached_http_get(
        "https://api6.ipify.org/?format=json",
        f"ipv6:{self.uid}",
        ttl=604800)

    # Extract IP addresses from responses
    self.ipv4 = str(ipv4_result.get("ip", "")) if ipv4_result else ""
    self.ipv6 = str(ipv6_result.get("ip", "")) if ipv6_result else ""

    # Ensure we have at least one IP address
    if not self.ipv4 and not self.ipv6:
      log_error("Failed to obtain both IPv4 and IPv6 addresses")
      return None, None

    # Get geolocation data for the primary IP (IPv4 preferred, fallback to IPv6)
    primary_ip = self.ipv4 or self.ipv6
    if primary_ip:
      # Cache key for geolocation (cached per instance UID as requested)
      try:
        geo_data = await cached_http_get(
            f"http://ip-api.com/json/{primary_ip}",
            f"geoip:{primary_ip}",
            ttl=604800)

        if geo_data and geo_data.get("status") == "success":
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
      except Exception as e:
        log_warn(f"Failed to get geolocation data for {primary_ip}: {e}")

    return self.ipv4, self.ipv6
