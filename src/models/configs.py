from dataclasses import dataclass, field, fields
from typing import get_args, Any, Optional
from .base import (
    DictMixin, IngesterType, AuthMethod, RateLimitConfig,
    InputRateLimitConfig
)

# Type alias for any ingester type


class IngesterConfigMeta(type):

  def __new__(cls, name, bases, dct):
    # iterate over all IngesterType
    for ingester_type in get_args(IngesterType):
      attr_name = ingester_type.lower()
      dct[attr_name] = field(
          default_factory=list)  # add field default to the class
      dct.setdefault(
          '__annotations__',
          {})[attr_name] = list[Any]  # add type hint to the class
    return super().__new__(cls, name, bases, dct)


@dataclass
class IngesterConfig(DictMixin, metaclass=IngesterConfigMeta):
  # Explicitly define attributes for mypy (these are also added by the metaclass)
  scrapper: list[Any] = field(default_factory=list)
  http_api: list[Any] = field(default_factory=list)
  ws_api: list[Any] = field(default_factory=list)
  fix_api: list[Any] = field(default_factory=list)
  evm_caller: list[Any] = field(default_factory=list)
  evm_logger: list[Any] = field(default_factory=list)
  svm_caller: list[Any] = field(default_factory=list)
  svm_logger: list[Any] = field(default_factory=list)
  sui_caller: list[Any] = field(default_factory=list)
  sui_logger: list[Any] = field(default_factory=list)
  aptos_caller: list[Any] = field(default_factory=list)
  aptos_logger: list[Any] = field(default_factory=list)
  ton_caller: list[Any] = field(default_factory=list)
  ton_logger: list[Any] = field(default_factory=list)
  processor: list[Any] = field(default_factory=list)

  @classmethod
  def from_dict(cls,
                data: dict,
                instance: Optional[Any] = None) -> 'IngesterConfig':
    config_dict = {}
    for ingester_type in get_args(IngesterType):
      key = ingester_type.lower(
      )  # match yaml config e.g., scrapper, http_api, evm_caller...
      items = data.get(key, [])
      if not items:
        continue  # skip missing categories
      # inject ingester_type into each to instantiate the correct ingester
      ingesters = []
      from .ingesters import Ingester, UpdateIngester, TimeSeriesIngester
      for item in items:
        item_config = {**item, 'ingester_type': ingester_type}

        # Determine which ingester class to use based on resource_type
        resource_type = item_config.get('resource_type', 'timeseries')
        if resource_type == 'update':
          ingester = UpdateIngester.from_config(item_config, instance)
        elif resource_type == 'timeseries':
          ingester = TimeSeriesIngester.from_config(item_config, instance)
        else:
          # Default to base Ingester for other types
          ingester = Ingester.from_config(item_config, instance)

        ingesters.append(ingester)

      config_dict[key] = ingesters

    # Group ingesters by instance and resource name
    grouped = []
    for key, ingesters_list in config_dict.items():
      for ingester in ingesters_list:
        grouped.append(ingester)

    # Ensure all required fields are present with empty lists as defaults
    final_config = {}
    for ingester_type in get_args(IngesterType):
      key = ingester_type.lower()
      final_config[key] = config_dict.get(key, [])

    return cls(**final_config)  # type: ignore

  @property
  def ingesters(self):
    return self.scrapper + self.http_api + self.ws_api \
      + self.evm_caller + self.evm_logger \
      + self.svm_caller + self.sui_caller + self.aptos_caller + self.ton_caller \
      + self.processor # + ...

  def to_dict(self, scope = None) -> dict:
    """Custom to_dict for IngesterConfig with scope filtering"""
    from .base import Scope
    if scope is None:
      scope = Scope.DEFAULT

    if scope == Scope.DEFAULT:
      return super().to_dict()

    return {r.name: r.to_dict(scope) for r in self.ingesters}


@dataclass
class WSConfig(DictMixin):
  """WebSocket configuration for subscriptions and connections"""
  # Subscription costs
  subscription_base_cost: int = 10
  subscription_per_topic_cost: int = 2

  # Connection limits
  max_topics_per_connection: int = 50
  max_connections_per_user: int = 5

  # Message filtering and security
  filter_protected_fields: bool = True
  allowed_topics_pattern: str = "chomp:*"


@dataclass
class ServerConfig(DictMixin):
  """Configuration for API server/admin"""
  # Server information
  name: str = "Chomp Data Ingester"
  description: str = "High-performance data ingestion framework"

  # Server runtime settings
  host: str = "127.0.0.1"
  port: int = 40004
  ws_ping_interval: int = 30
  ws_ping_timeout: int = 20
  allow_origin_regex: str = r"^https?://localhost(:[0-9]+)?$|^https?://127\.0\.0\.1(:[0-9]+)?$|^wss?://localhost(:[0-9]+)?$|^wss?://127\.0\.0\.1(:[0-9]+)?$|^https?://cho\.mp$|^wss?://cho\.mp$"

  # Authentication settings
  auth_methods: list[AuthMethod] = field(
      default_factory=lambda:
      ["static", "evm", "svm", "sui", "oauth2_github", "oauth2_x"])
  static_auth_token: Optional[str] = None

  # OAuth2 configuration
  oauth2_app_ids: dict[str, str] = field(default_factory=dict)
  oauth2_client_ids: dict[str, str] = field(default_factory=dict)
  oauth2_client_secrets: dict[str, str] = field(default_factory=dict)
  callback_base_url: str = "https://cho.mp"

  # Session management
  auth_flow_expiry: int = 600  # 10 minutes for OAuth2 flows
  session_ttl: int = 86400  # 24 hours in seconds
  auto_renew_session: bool = True
  default_rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
  input_rate_limits: InputRateLimitConfig = field(
      default_factory=InputRateLimitConfig)
  jwt_secret_key: Optional[str] = None
  jwt_expires_hours: int = 24
  protected_routes: list[str] = field(default_factory=lambda: ["/admin/*"])

  # Rate limiting settings
  whitelist: list[str] = field(
      default_factory=lambda: ["127.0.0.1", "localhost"])
  blacklist: list[str] = field(default_factory=list)

  # User tracking settings
  persist_user_debounce: int = 30  # Debounce user database updates by 30 seconds

  # Route configuration overrides
  routes_config: dict[str, Any] = field(default_factory=dict)

  # WebSocket configuration
  ws_config: Optional['WSConfig'] = field(default_factory=lambda: None)

  @classmethod
  def from_dict(cls, data: dict) -> 'ServerConfig':
    """Create ServerConfig from dictionary with nested object handling"""
    # Handle nested rate limit configs
    if 'default_rate_limits' in data and isinstance(data['default_rate_limits'], dict):
      data['default_rate_limits'] = RateLimitConfig(**data['default_rate_limits'])

    if 'input_rate_limits' in data and isinstance(data['input_rate_limits'], dict):
      data['input_rate_limits'] = InputRateLimitConfig(**data['input_rate_limits'])

    # Handle WebSocket config
    if 'ws_config' in data and isinstance(data['ws_config'], dict):
      data['ws_config'] = WSConfig(**data['ws_config'])

    # Handle routes_config - convert dict to RouteMeta objects and apply overrides
    if 'routes_config' in data and isinstance(data['routes_config'], dict):
      from ..server.routes import Route, RouteMeta, apply_route_overrides

      routes_config = {}
      for route_name, route_data in data['routes_config'].items():
        if isinstance(route_data, dict):
          # Try to find the default route
          try:
            default_route = Route[route_name.upper()]
            default_meta = default_route.value
            # Merge default with overrides
            merged_data = {
              'endpoint': default_meta.endpoint,
              'points': default_meta.points,
              'protected': default_meta.protected,
              'router_prefix': default_meta.router_prefix,
              **route_data  # Override with config values
            }
            routes_config[route_name] = RouteMeta(**merged_data)
          except (KeyError, ValueError):
            # Route not found, create new RouteMeta from config data
            routes_config[route_name] = RouteMeta(**route_data)

      # Apply the overrides to the Route enum
      apply_route_overrides(routes_config)
      data['routes_config'] = routes_config

    # Filter to only include valid dataclass fields
    field_names = {f.name for f in fields(cls)}
    filtered_data = {k: v for k, v in data.items() if k in field_names}

    return cls(**filtered_data)

  @property
  def auth_enabled(self) -> bool:
    """Check if any authentication methods are configured"""
    return bool(self.auth_methods)
