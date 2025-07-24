from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class RouteMeta:
  endpoint: str
  points: int = 1
  protected: bool = False
  router_prefix: str = ""

  @property
  def full_endpoint(self) -> str:
    """Get the full endpoint including router prefix"""
    if self.router_prefix and not self.endpoint.startswith(self.router_prefix):
      return f"{self.router_prefix}{self.endpoint}"
    return self.endpoint


class Route(Enum):
  """Complete route definitions with metadata for all API endpoints"""

  # Root/Retriever routes (no prefix)
  ROOT = RouteMeta(endpoint="/", points=1, protected=False)
  INFO = RouteMeta(endpoint="/info", points=1, protected=False)
  DOCS = RouteMeta(endpoint="/docs", points=1, protected=False)
  PING = RouteMeta(endpoint="/ping", points=1, protected=False)
  STATUS = RouteMeta(endpoint="/status", points=1, protected=False)

  # Schema and data retrieval
  SCHEMA = RouteMeta(endpoint="/schema", points=1, protected=False)
  SCHEMA_WITH_RESOURCES = RouteMeta(endpoint="/schema/{resources:path}", points=1, protected=False)
  LAST = RouteMeta(endpoint="/last", points=1, protected=False)
  LAST_WITH_RESOURCES = RouteMeta(endpoint="/last/{resources:path}", points=1, protected=False)
  HISTORY = RouteMeta(endpoint="/history", points=5, protected=False)
  HISTORY_WITH_RESOURCES = RouteMeta(endpoint="/history/{resources:path}", points=5, protected=False)

  # Analysis endpoints
  ANALYSIS = RouteMeta(endpoint="/analysis", points=15, protected=False)
  ANALYSIS_WITH_RESOURCES = RouteMeta(endpoint="/analysis/{resources:path}", points=15, protected=False)
  VOLATILITY = RouteMeta(endpoint="/volatility", points=10, protected=False)
  VOLATILITY_WITH_RESOURCES = RouteMeta(endpoint="/volatility/{resources:path}", points=10, protected=False)
  TREND = RouteMeta(endpoint="/trend", points=10, protected=False)
  TREND_WITH_RESOURCES = RouteMeta(endpoint="/trend/{resources:path}", points=10, protected=False)
  MOMENTUM = RouteMeta(endpoint="/momentum", points=10, protected=False)
  MOMENTUM_WITH_RESOURCES = RouteMeta(endpoint="/momentum/{resources:path}", points=10, protected=False)
  OPRANGE = RouteMeta(endpoint="/oprange", points=10, protected=False)
  OPRANGE_WITH_RESOURCES = RouteMeta(endpoint="/oprange/{resources:path}", points=10, protected=False)

  # Conversion endpoints
  CONVERT = RouteMeta(endpoint="/convert", points=2, protected=False)
  CONVERT_WITH_PAIR = RouteMeta(endpoint="/convert/{pair:path}", points=2, protected=False)
  PEGCHECK = RouteMeta(endpoint="/pegcheck", points=2, protected=False)
  PEGCHECK_WITH_PAIR = RouteMeta(endpoint="/pegcheck/{pair:path}", points=2, protected=False)

  # Utility endpoints
  LIMITS = RouteMeta(endpoint="/limits", points=1, protected=False)

  # Resource management
  LIST_RESOURCE = RouteMeta(endpoint="/list/{resource}", points=2, protected=False)
  LIST_RESOURCES = RouteMeta(endpoint="/list", points=2, protected=False)
  RESOURCE_BY_UID = RouteMeta(endpoint="/{resource}/{uid}", points=2, protected=False)
  RESOURCE_BY_UID_QUERY = RouteMeta(endpoint="/{resource}", points=2, protected=False)

  # Authentication routes (with /auth prefix)
  AUTH_LOGIN = RouteMeta(endpoint="/login", points=2, protected=False, router_prefix="/auth")
  AUTH_AUTHENTICATE = RouteMeta(endpoint="/authenticate", points=2, protected=False, router_prefix="/auth")
  AUTH_CHALLENGE_CREATE = RouteMeta(endpoint="/challenge/create", points=2, protected=False, router_prefix="/auth")
  AUTH_CHALLENGE_AUTH = RouteMeta(endpoint="/challenge/authenticate", points=2, protected=False, router_prefix="/auth")
  AUTH_CHALLENGE_STATUS = RouteMeta(endpoint="/challenge/{challenge_id}", points=1, protected=False, router_prefix="/auth")
  AUTH_CHALLENGE_CANCEL = RouteMeta(endpoint="/challenge/{challenge_id}", points=1, protected=False, router_prefix="/auth")
  AUTH_LOGOUT = RouteMeta(endpoint="/logout", points=1, protected=False, router_prefix="/auth")
  AUTH_PROFILE = RouteMeta(endpoint="/profile", points=1, protected=False, router_prefix="/auth")
  AUTH_STATUS = RouteMeta(endpoint="/status", points=1, protected=False, router_prefix="/auth")
  AUTH_HEALTH = RouteMeta(endpoint="/health", points=1, protected=False, router_prefix="/auth")

  # Configuration routes (with /config prefix)
  CONFIG_INGESTER_GET = RouteMeta(endpoint="/ingester", points=1, protected=True, router_prefix="/config")
  CONFIG_INGESTER_VALIDATE = RouteMeta(endpoint="/ingester/validate", points=2, protected=True, router_prefix="/config")
  CONFIG_INGESTER_UPDATE = RouteMeta(endpoint="/ingester/update", points=5, protected=True, router_prefix="/config")
  CONFIG_INGESTER_UPLOAD = RouteMeta(endpoint="/ingester/upload", points=5, protected=True, router_prefix="/config")
  CONFIG_INGESTER_HISTORY = RouteMeta(endpoint="/ingester/history", points=1, protected=True, router_prefix="/config")
  CONFIG_SERVER_GET = RouteMeta(endpoint="/server", points=1, protected=True, router_prefix="/config")
  CONFIG_SERVER_VALIDATE = RouteMeta(endpoint="/server/validate", points=2, protected=True, router_prefix="/config")
  CONFIG_SERVER_UPDATE = RouteMeta(endpoint="/server/update", points=5, protected=True, router_prefix="/config")
  CONFIG_SERVER_UPLOAD = RouteMeta(endpoint="/server/upload", points=5, protected=True, router_prefix="/config")
  CONFIG_SERVER_HISTORY = RouteMeta(endpoint="/server/history", points=1, protected=True, router_prefix="/config")
  CONFIG_ROLLBACK = RouteMeta(endpoint="/rollback", points=10, protected=True, router_prefix="/config")
  CONFIG_HISTORY = RouteMeta(endpoint="/history", points=1, protected=True, router_prefix="/config")

  # Admin routes (with /admin prefix)
  ADMIN_TEST = RouteMeta(endpoint="/test", points=1, protected=True, router_prefix="/admin")
  ADMIN_INGESTERS = RouteMeta(endpoint="/ingesters", points=1, protected=True, router_prefix="/admin")
  ADMIN_INGESTER_DETAILS = RouteMeta(endpoint="/ingesters/{ingester_name}", points=1, protected=True, router_prefix="/admin")
  ADMIN_INGESTERS_CONTROL = RouteMeta(endpoint="/ingesters/control", points=10, protected=True, router_prefix="/admin")
  ADMIN_INGESTER_RESTART = RouteMeta(endpoint="/ingesters/{ingester_name}/restart", points=5, protected=True, router_prefix="/admin")
  ADMIN_SYSTEM_LOGS = RouteMeta(endpoint="/system/logs", points=2, protected=True, router_prefix="/admin")
  ADMIN_DATABASE_STATUS = RouteMeta(endpoint="/database/status", points=1, protected=True, router_prefix="/admin")
  ADMIN_DATABASE_TABLES = RouteMeta(endpoint="/database/tables", points=1, protected=True, router_prefix="/admin")
  ADMIN_CACHE_STATUS = RouteMeta(endpoint="/cache/status", points=1, protected=True, router_prefix="/admin")
  ADMIN_CACHE_CLEAR = RouteMeta(endpoint="/cache/clear", points=5, protected=True, router_prefix="/admin")
  ADMIN_REGISTRY = RouteMeta(endpoint="/registry/{registry_type}", points=1, protected=True, router_prefix="/admin")
  ADMIN_USERS = RouteMeta(endpoint="/users", points=2, protected=True, router_prefix="/admin")
  ADMIN_USERS_STATUS = RouteMeta(endpoint="/users/status", points=5, protected=True, router_prefix="/admin")
  ADMIN_USERS_SUMMARY = RouteMeta(endpoint="/users/stats/summary", points=2, protected=True, router_prefix="/admin")

  # WebSocket route
  WS_SUBSCRIBE = RouteMeta(endpoint="/ws/subscribe", points=10, protected=False)

  @property
  def endpoint(self) -> str:
    return self.value.endpoint

  @property
  def points(self) -> int:
    return self.value.points

  @property
  def protected(self) -> bool:
    return self.value.protected

  @property
  def router_prefix(self) -> str:
    return self.value.router_prefix

  @property
  def full_endpoint(self) -> str:
    return self.value.full_endpoint


# Route mapping for user usage tracking
ROUTE_USAGE_MAP = {
    "schema": ("schema_count", "schema_bytes"),
    "resources": ("schema_count", "schema_bytes"),
    "last": ("last_count", "last_bytes"),
    "history": ("history_count", "history_bytes"),
    "analysis": ("analysis_count", "analysis_bytes"),
    "volatility": ("analysis_count", "analysis_bytes"),
    "trend": ("analysis_count", "analysis_bytes"),
    "momentum": ("analysis_count", "analysis_bytes"),
    "oprange": ("analysis_count", "analysis_bytes"),
}


def get_usage_tracking_fields(endpoint: str) -> Optional[tuple[str, str]]:
  """Get usage tracking field names for an endpoint"""
  try:
    key = endpoint.split('/')[1]
    return ROUTE_USAGE_MAP.get(key)
  except IndexError:
    return None


def apply_route_overrides(routes_config: Dict[str, RouteMeta]) -> None:
  """Apply route configuration overrides to Route enum values"""
  for route_name, override_meta in routes_config.items():
    try:
      route = Route[route_name.upper()]
      route._value_ = override_meta
    except (KeyError, AttributeError):
      # Route name not found or can't modify - skip
      pass
