from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from aiocron import Cron
from typing import Optional, Any, cast

from .base import (
    SYS_FIELDS,
    Resource,
    ResourceType,
    Targettable,
    ResourceField,
    Scope,
    IngesterType,
    Interval,
    FieldType
)
from ..utils.date import now, floor_utc, interval_to_seconds, extract_time_unit
from ..utils import log_error, log_warn, log_debug
from ..utils.deps import safe_import


def split_init_data(cls, kwargs: dict) -> tuple[dict, dict]:
  """
  Separates kwargs into dataclass fields and dynamic resource fields.
  This is a critical helper to ensure safe initialization of Ingester subclasses.
  """
  import dataclasses

  # Get all unique dataclass field names from the class's entire method resolution order (MRO)
  cls_fields = {
      f.name
      for c in cls.__mro__ if dataclasses.is_dataclass(c)
      for f in dataclasses.fields(c)
  }

  init_data = {k: v for k, v in kwargs.items() if k in cls_fields}
  dynamic_data = {k: v for k, v in kwargs.items() if k not in cls_fields}

  return init_data, dynamic_data


@dataclass
class Ingester(Resource, Targettable):
  """Base class for all simple, non-timeseries ingesters"""

  interval: Interval = "h1"
  probablity: float = 1.0
  ingester_type: IngesterType = "evm_caller"
  started: Optional[datetime] = None
  last_ingested: Optional[datetime] = None
  cron: Optional[Cron] = None
  monitor: Optional['ResourceMonitor'] = None  # type: ignore  # noqa: F821
  _response_timestamp: Optional[float] = field(default=None, init=False)
  resource_type: ResourceType = "timeseries"

  def __post_init__(self):
    """
    Initialize fields, inheriting defaults from the parent ingester.
    This method is concise and maintains the original inheritance logic.
    """
    # First, ensure fields are proper ResourceField objects
    if self.fields and isinstance(self.fields[0], dict):
      self.fields = [ResourceField.from_dict(f) for f in self.fields]

    # Then, run the parent post_init to populate field_by_name
    super().__post_init__()

    # Define which Targettable attributes are inheritable and their "empty" values
    inheritable_attrs = {
        'target': "", 'selector': "", 'method': "GET", 'pre_transformer': "",
        'headers': {}, 'params': [], 'type': "float64", 'handler': "", 'reducer': "",
        'actions': [], 'transformers': [], 'tags': []
    }

    for field in self.fields:
      for attr, empty_value in inheritable_attrs.items():
        field_value = getattr(field, attr)

        # Check if the field's value is the default/empty one
        is_empty = (field_value == empty_value)
        if isinstance(empty_value, (list, dict)) and not field_value:
          is_empty = True

        if is_empty:
          parent_value = getattr(self, attr)
          if parent_value is not None and parent_value != empty_value:
            setattr(field, attr, parent_value)

    # Special case for target concatenation
    if self.target:
      for field in self.fields:
        if field.target and not field.target.startswith(('http', '/')):
          field.target = self.target + field.target
        elif not field.target:
          field.target = self.target

  async def _pre_ingest(self):
    """Clear field values before ingestion"""
    pass # implement in subclasses

  async def pre_ingest(self):
    """Reset cache, claim task, and start monitoring"""
    from ..cache import ensure_claim_task
    await ensure_claim_task(self)
    # Clear all field values except read-only fields
    readonly_fields = self._get_readonly_fields()
    for field_item in self.fields:
      if field_item.name not in readonly_fields:
        field_item.value = None
    await self._pre_ingest()
    if self.monitor:
      self.monitor.start_timer()

  async def _post_ingest(self, response_data=None, status_code=200, table="",
                        publish=True, jsonify=False, monitor=True):
    """Update timestamps, transform, and store data"""
    pass # implement in subclasses

  async def post_ingest(self, response_data=None, status_code=200, table="",
                        publish=True, jsonify=False, monitor=True):
    """Stop monitoring, transform, cache, and store data"""

    self._update_timestamps()
    if self.monitor:
      response_size = len(str(response_data)) * 2 if response_data else 0
      self._response_timestamp = self.monitor.stop_timer(response_size, status_code)
    else:
      self._response_timestamp = now().timestamp()

    # Lazy import using safe_import to break circular dependency
    actions_store = safe_import("src.actions.store")
    state = safe_import("src.state")

    if actions_store and state:
      await actions_store.transform_and_store(self, table, publish, jsonify, monitor)
      if state.args.verbose:
        log_debug(f"Ingested {self.name} -> {self.get_field_values()}")

    await self._post_ingest(response_data, status_code, table, publish, jsonify, monitor)

  def compile_transformers(self):
    """Pre-compile all transformer expressions for optimal performance"""
    actions_transform = safe_import("src.actions.transform")

    if actions_transform:
      for field_item in self.fields:
        if field_item.transformers:
          for transformer in field_item.transformers:
            # Pre-compile each transformer to populate the cache
            actions_transform.compile_transformer(transformer)

  def _get_readonly_fields(self) -> set[str]:
    """Get list of read-only field names that shouldn't be cleared"""
    return set()

  def _update_timestamps(self):
    """Update timestamp fields - to be overridden by subclasses"""
    pass

  def _populate_fields(self, definitions: list, source_kwargs: Optional[dict] = None):
    """
    Generic helper to populate instance fields from a list of definitions.

    Each definition is a tuple: (name, type, default, is_transient[Optional])
    Values from source_kwargs will override defaults if provided.
    """
    kwargs = source_kwargs or {}
    for definition in definitions:
      name, field_type, default, *rest = definition
      transient = rest[0] if rest else False
      value = kwargs.get(name, default)
      self.add_field(ResourceField(
          name=name,
          type=cast(FieldType, field_type),
          value=value,
          transient=transient
      ))

  @classmethod
  def from_config(cls, d: dict, instance: Optional[Any] = None, **kwargs):
    """
    Unified factory to create any Ingester instance from a dictionary.
    Now acts as a simple wrapper around the robust __init__ methods.
    """
    ingester = cls(**d)
    # Pre-compile transformers for optimal performance
    ingester.compile_transformers()
    return ingester

  def to_dict(self, scope: Scope = Scope.DEFAULT) -> dict:
    """Custom to_dict with scope filtering for Ingester"""
    if scope == Scope.DEFAULT:
      return super().to_dict()

    return {
        **super().to_dict(scope),
        "interval": self.interval,
        "probablity": self.probablity,
        "started": self.started,
        "last_ingested": self.last_ingested,
        "ingester_type": self.ingester_type,
    }

  def log_resource_not_found(self, resource_type: str, resource_id: str,
                             field_names: list[str], endpoint: str,
                             retries_exhausted: bool = False, **context) -> None:
    """Log resource not found error with ingester context"""
    configured_targets = {f.target for f in self.fields if f.target}
    is_configured = resource_id in configured_targets

    ctx_parts = [f"{k.upper()}: {v}" for k, v in context.items()] if context else []
    ctx_str = " | " + " | ".join(ctx_parts) if ctx_parts else ""

    log_error(
        f"{resource_type} '{resource_id}' not found | "
        f"INGESTER: {self.name} | FIELDS: {field_names} | "
        f"ENDPOINT: {endpoint} | RETRIES_EXHAUSTED: {retries_exhausted}{ctx_str}"
    )

    if not is_configured:
      log_warn(f"{resource_type} '{resource_id}' missing from ingester '{self.name}' config")
    elif retries_exhausted:
      log_warn(f"{resource_type} '{resource_id}' configured but unreachable via '{endpoint}'")

  def signature(self) -> str:
    field_hashes = [md5(field_item.signature().encode()).hexdigest() for field_item in self.fields]
    return (f"{self.name}-{self.resource_type}-{self.interval}-{self.ingester_type}-" +
            "-".join(field_hashes))

  @property
  def interval_sec(self) -> int:
    return interval_to_seconds(self.interval)

  @property
  def precision(self):
    unit, _ = extract_time_unit(self.interval)
    return unit

  @property
  def id(self) -> str:
    """Unique identifier for the ingester based on its signature"""
    return md5(self.signature().encode()).hexdigest()

  def __hash__(self) -> int:
    return hash(md5(self.signature().encode()).hexdigest())

  def values(self):
    return [field_item.value for field_item in self.fields]

  def load_values(self, values: list[Any]):
    for i, value in enumerate(values):
      self.set_field(i, value)

  def load_from_dict(self, values: dict[str, Any]):
    """Load field values from dictionary"""
    self.set_field_values(values)

  def dependencies(self) -> list[str]:
    """Extract dependency names from field selectors"""
    return list({
        field_item.selector.split('.', 1)[0].strip()
        for field_item in self.fields
        if (field_item.selector and field_item.selector[0] != '.' and '.' in field_item.selector)
    })

  def process_batch_results(self, batch_results: dict[str, Any]) -> None:
    """Process batch results and update field values"""
    for field_item in self.fields:
      try:
        if field_item.target in batch_results:
          field_item.value = batch_results[field_item.target]
      except Exception as e:
        log_error(f"Error processing field '{field_item.name}' for ingester '{self.name}': {str(e)}")


@dataclass
class UpdateIngester(Ingester):
  """Ingester for update/upsert data with standard fields: created_at, updated_at, uid"""
  uid: str = ""

  def __post_init__(self):
    self.resource_type = 'update'

    """Ensure standard fields are always first: created_at (0), updated_at (1), uid (2)"""
    # Remove existing standard fields if they exist
    self.fields = [f for f in self.fields if f.name not in SYS_FIELDS]

    # Add standard fields at the beginning in correct order
    current_time = now()
    standard_fields = [
        ResourceField(name='created_at', type='timestamp', value=current_time),
        ResourceField(name='updated_at', type='timestamp', value=current_time),
        ResourceField(name='uid', type='string', value=self.uid),
    ]
    self.fields = standard_fields + self.fields

    # Call parent post_init after field setup to handle inheritance and field_by_name
    super().__post_init__()

  def _get_readonly_fields(self) -> set[str]:
    """uid and created_at are read-only after instantiation"""
    return {'uid', 'created_at'} # part of SYS_FIELDS

  def _update_timestamps(self):
    """Update updated_at timestamp using floor_utc"""
    self.updated_at = floor_utc(self.interval)

  def from_dict(cls, data: dict, **kwargs) -> 'UpdateIngester':
    """Alias for from_config for compatibility."""
    return cls(**data)


@dataclass
class TimeSeriesIngester(Ingester):
  """Ingester for time series data with standard field: ts (0)"""

  def __init__(self, **kwargs):
    kwargs['resource_type'] = 'timeseries'
    super().__init__(**kwargs)
    self._ensure_ts_field()

  def _ensure_ts_field(self):
    """Ensure ts field is always first (index 0)"""
    # Remove existing ts field
    self.fields = [f for f in self.fields if f.name != 'ts']

    # Add ts field at the beginning
    ts_field = ResourceField(name='ts', type='timestamp', value=now(), transformers=[])
    self.fields.insert(0, ts_field)

    # Sync field_by_name after field modification
    self.field_by_name = {f.name: f for f in self.fields}

  def _update_timestamps(self):
    """Update ts timestamp using floor_utc"""
    self.ts = floor_utc(self.interval)

  @classmethod
  def from_config(cls, d: dict, instance: Optional[Any] = None, **kwargs):
    return super(TimeSeriesIngester, cls).from_config(d, instance, **kwargs)
