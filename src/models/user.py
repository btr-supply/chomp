from dataclasses import dataclass

from .base import RateLimitConfig
from .ingesters import UpdateIngester, split_init_data


@dataclass
class User(UpdateIngester):
  """User model for authentication, rate limits, and API usage tracking"""

  def __init__(self, **kwargs):
    # Separate dataclass fields from dynamic data before calling super().__init__
    init_data, dynamic_data = split_init_data(self.__class__, kwargs)

    # Set required base values
    init_data.setdefault('name', "sys.users")
    init_data.setdefault('ingester_type', "processor")
    init_data.setdefault('interval', "h1")
    init_data.setdefault('protected', True)

    super().__init__(**init_data)
    self._add_user_fields(**dynamic_data)

    # Set rate limits from server config if available
    from .. import state
    if hasattr(state, 'server_config') and state.server_config:
      self.rate_limits = state.server_config.default_rate_limits

  def _add_user_fields(self, **kwargs):
    """Add user-specific fields after standard fields."""
    field_definitions = [
        # Identity fields (name, type, default)
        ("ipv4", "string", ""),
        ("ipv6", "string", ""),
        ("alias", "string", ""),
        ("status", "string", "public"),
        # Usage counters
        ("total_count", "int64", 0),
        ("schema_count", "int64", 0),
        ("last_count", "int64", 0),
        ("history_count", "int64", 0),
        ("analysis_count", "int64", 0),
        # Byte counters
        ("total_bytes", "int64", 0),
        ("schema_bytes", "int64", 0),
        ("last_bytes", "int64", 0),
        ("history_bytes", "int64", 0),
        ("analysis_bytes", "int64", 0),
        # Transient fields (name, type, default, is_transient)
        ("rate_limits", "string", RateLimitConfig(), True),
        ("jwt_token", "string", None, True),
        ("session_expires_at", "timestamp", None, True),
    ]
    self._populate_fields(field_definitions, kwargs)

  def update_usage(self, endpoint: str, response_bytes: int) -> None:
    """Update user usage statistics using the centralized route mapping"""
    self.total_count: int = (self.total_count or 0) + 1
    self.total_bytes: int = (self.total_bytes or 0) + response_bytes

    # Use the centralized route usage mapping from routes.py
    from ..server.routes import get_usage_tracking_fields

    if attrs := get_usage_tracking_fields(endpoint):
      count_attr, bytes_attr = attrs
      setattr(self, count_attr, (getattr(self, count_attr, 0) or 0) + 1)
      setattr(self, bytes_attr, (getattr(self, bytes_attr, 0) or 0) + response_bytes)

  def is_admin(self) -> bool:
    """Check if user is admin"""
    return self.status == "admin"

  @classmethod
  def from_dict(cls, data: dict, **kwargs) -> 'User':
    """Creates a User instance from a dictionary."""
    return cls(**data)
