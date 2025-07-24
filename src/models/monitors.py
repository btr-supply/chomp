from dataclasses import dataclass, field
from typing import Optional

from .base import Instance
from .ingesters import Ingester, TimeSeriesIngester, split_init_data
from ..utils.date import now


@dataclass
class ResourceMonitor(TimeSeriesIngester):
  """Monitor ingester resource performance metrics"""
  instance_name: str = ""
  field_count: int = 0
  latency_ms: float = 0.0
  response_bytes: int = 0
  status_code: Optional[int] = None
  _start_time: float = field(default_factory=lambda: 0.0, init=False)

  def __init__(self, ing: Ingester, instance: Instance, **kwargs):
    init_data, _ = split_init_data(self.__class__, kwargs)

    # Set required base values
    init_data['name'] = f"sys.{ing.name}.ing.monitor"
    init_data.setdefault('resource_type', "timeseries")
    init_data.setdefault('ingester_type', "processor")
    init_data.setdefault('interval', ing.interval)
    init_data.setdefault('protected', True)
    init_data.setdefault('tags', ing.tags)

    super().__init__(**init_data)

    # Populate instance-specific values and monitor fields
    self.instance_name = instance.name
    self.field_count = len(ing.fields)
    self._add_monitor_fields()

  def _add_monitor_fields(self):
    """Add monitoring fields after ts field."""
    field_definitions = [
        # (name, type, default)
        ("instance_name", "string", self.instance_name),
        ("field_count", "int32", self.field_count),
        ("latency_ms", "float64", 0.0),
        ("response_bytes", "int64", 0),
        ("status_code", "int32", None),
    ]
    self._populate_fields(field_definitions)

  def start_timer(self) -> None:
    """Start timing a request"""
    self._start_time = now().timestamp()

  def stop_timer(self, response_bytes: int, status_code: Optional[int] = None) -> float:
    """Stop timer and update metrics"""
    end_time = now().timestamp()
    self.latency_ms = (end_time - self._start_time) * 1000 if self._start_time > 0 else 0.0
    self.response_bytes = response_bytes
    self.status_code = status_code
    self._update_field_values()
    return end_time

  def _update_field_values(self) -> None:
    """Update field values from current metrics"""
    field_values = {
        "instance_name": self.instance_name,
        "field_count": self.field_count,
        "latency_ms": self.latency_ms,
        "response_bytes": self.response_bytes,
        "status_code": self.status_code,
    }
    self.set_field_values(field_values)


@dataclass
class InstanceMonitor(TimeSeriesIngester):
  """Monitor instance system performance metrics"""
  resources_count: int = field(default=0)
  cpu_usage: float = field(default=0.0)
  memory_usage: float = field(default=0.0)
  disk_usage: float = field(default=0.0)
  network_usage: float = field(default=0.0)
  instance: Optional[Instance] = field(default=None)

  def __init__(self, instance: Instance, **kwargs):
    init_data, _ = split_init_data(self.__class__, kwargs)

    # Set required base values
    init_data['name'] = f"sys.{instance.name}.ins.monitor"
    init_data.setdefault('resource_type', "timeseries")
    init_data.setdefault('ingester_type', "processor")
    init_data.setdefault('interval', "s30")
    init_data.setdefault('protected', True)

    super().__init__(**init_data)

    # Populate instance-specific values and monitor fields
    self.instance = instance
    self._add_system_fields()

  def _add_system_fields(self):
    """Add system monitoring fields after ts field."""
    field_definitions = [
        # System metrics fields (name, type, default)
        ("resources_count", "int32", 0),
        ("cpu_usage", "float64", 0.0),
        ("memory_usage", "float64", 0.0),
        ("disk_usage", "float64", 0.0),
        # Geolocation fields (name, type, default, is_transient)
        ("coordinates", "string", "", True),
        ("timezone", "string", "", True),
        ("country_code", "string", "", True),
        ("location", "string", "", True),
        ("isp", "string", "", True),
    ]
    self._populate_fields(field_definitions)

  def update_metrics(self, resources_count: int = 0, cpu_usage: float = 0.0,
                     memory_usage: float = 0.0, disk_usage: float = 0.0) -> None:
    """Update system metrics and field values"""
    self.resources_count = resources_count
    self.cpu_usage = cpu_usage
    self.memory_usage = memory_usage
    self.disk_usage = disk_usage

    # Update field values including instance geolocation
    field_values = {
        "resources_count": self.resources_count,
        "cpu_usage": self.cpu_usage,
        "memory_usage": self.memory_usage,
        "disk_usage": self.disk_usage,
        "coordinates": self.instance.coordinates if self.instance else "",
        "timezone": self.instance.timezone if self.instance else "",
        "country_code": self.instance.country_code if self.instance else "",
        "location": self.instance.location if self.instance else "",
        "isp": self.instance.isp if self.instance else "",
    }
    self.set_field_values(field_values)
