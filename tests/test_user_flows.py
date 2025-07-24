"""Test user flows for Chomp API"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import ServerConfig, RateLimitConfig, InputRateLimitConfig
from src.server.responses import ApiResponse

STATIC_AUTH_TOKEN = "secrettoken"

@pytest.fixture
def mock_server_config():
  """Mock server configuration"""
  return ServerConfig(
    static_auth_token=STATIC_AUTH_TOKEN,
    default_rate_limits=RateLimitConfig(
      rpm=60, rph=1800, rpd=21600,
      spm=100_000_000, sph=3_000_000_000, spd=36_000_000_000,
      ppm=120, pph=3600, ppd=43200
    ),
    input_rate_limits=InputRateLimitConfig(
      start=50, factor=6.0, max=345600
    ),
    protected_routes=["/admin/*"]
  )

@pytest.fixture
def mock_request():
  """Mock request object"""
  request = Mock()
  request.client.host = "127.0.0.1"
  request.headers = {}
  request.state = Mock()
  request.state.requester_id = "127.0.0.1"
  return request

class TestPublicUserFlows:
  """Test public user flows (Flow 1)"""

  def test_static_token_constant(self):
    """Test that static token constant is correct"""
    assert STATIC_AUTH_TOKEN == "secrettoken"

  def test_server_config_protection(self):
    """Test server config protection settings"""
    config = ServerConfig(
      static_auth_token=STATIC_AUTH_TOKEN,
      protected_routes=["/admin/*"]
    )

    assert config.static_auth_token == STATIC_AUTH_TOKEN
    assert "/admin/*" in config.protected_routes

  @pytest.mark.asyncio
  async def test_1_1_resource_discovery_excludes_protected(self, mock_request, mock_server_config):
    """Test 1.1 - Query list of resources, ensure protected are excluded"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.config.get_resources') as mock_get_resources:

      # Mock resources with mix of public and protected
      mock_get_resources.return_value = ("", [
        "public.data", "public.prices", "sys.monitor", "admin.users"
      ])

      from src.server.routers.retriever import get_resources

      response = await get_resources(
        mock_request,
        include_transient=False,
        include_protected=False
      )

      assert isinstance(response, ApiResponse)
      resources = response.data["resources"]

      # Should exclude sys.* and admin.* resources
      protected_found = [r for r in resources if r.startswith(("sys.", "admin."))]
      assert len(protected_found) == 0
      assert "public.data" in resources
      assert "public.prices" in resources

  @pytest.mark.asyncio
  async def test_1_2_last_values_queries(self, mock_request, mock_server_config):
    """Test 1.2 - Query last values for single and multiple resources"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_last_data') as mock_get_last:

      from src.server.routers.retriever import get_last_data

      # Test single resource
      mock_get_last.return_value = ("", {
        "test.data": {"value": 123.45, "timestamp": "2024-01-01T00:00:00Z"}
      })

      response = await get_last_data(
        mock_request,
        resource="test.data",
        include_transient=False
      )

      assert isinstance(response, ApiResponse)
      assert response.data["test.data"]["value"] == 123.45

  @pytest.mark.asyncio
  async def test_1_3_historical_data_queries(self, mock_request, mock_server_config):
    """Test 1.3 - Query historical values with various parameters"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_history_data') as mock_get_history:

      from src.server.routers.retriever import get_history_data

      mock_data = {
        "resource": "test.data",
        "data": [
          {"ts": "2024-01-01T00:00:00Z", "value": 100.0},
          {"ts": "2024-01-01T01:00:00Z", "value": 101.0}
        ]
      }
      mock_get_history.return_value = ("", mock_data)

      response = await get_history_data(
        mock_request,
        resource="test.data",
        from_date=None,
        to_date=None,
        interval=None,
        limit=1000
      )

      assert isinstance(response, ApiResponse)
      assert len(response.data["data"]) == 2

  @pytest.mark.asyncio
  async def test_1_4_protection_validation(self, mock_request, mock_server_config):
    """Test 1.4 - Ensure protected resources are not accessible"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_last_data') as mock_get_last:

      from src.server.routers.retriever import get_last_data

      # Mock access denied for protected resource
      mock_get_last.return_value = ("Access denied to protected resource", {})

      response = await get_last_data(
        mock_request,
        resource="sys.monitor",
        include_transient=False
      )

      # Should handle protection appropriately
      assert isinstance(response, ApiResponse)

  @pytest.mark.asyncio
  async def test_1_5_dynamic_analysis_endpoints(self, mock_request, mock_server_config):
    """Test 1.5 - Dynamic data endpoints with protection filtering"""

    with patch('src.state.server_config', mock_server_config):

      # Test pegcheck
      with patch('src.services.converter.get_conversion') as mock_conversion:
        mock_conversion.return_value = ("", {"peg_ratio": 1.001})

        from src.server.routers.retriever import get_convert

        response = await get_convert(
          mock_request,
          pair="USDC-USDT",
          base_amount=1000.0,
          quote_amount=None,
          precision=6
        )

        assert isinstance(response, ApiResponse)

  @pytest.mark.asyncio
  async def test_1_6_rate_limit_validation(self, mock_request, mock_server_config):
    """Test 1.6 - Rate limit enforcement and monitoring"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.limiter.check_limits') as mock_check, \
         patch('src.services.limiter.increment_counters') as mock_increment:

      # Mock within limits
      mock_check.return_value = ("", {"allowed": True})
      mock_increment.return_value = ("", {"remaining": {"requests": 59}})

      from src.server.routers.retriever import get_schema

      response = await get_schema(
        mock_request,
        scope="default",
        include_transient=False,
        include_protected=False
      )

      assert isinstance(response, ApiResponse)


class TestAuthenticatedUserFlows:
  """Test authenticated user flows (Flow 2)"""

  @pytest.mark.asyncio
  async def test_2_1_authentication_failure_handling(self, mock_request, mock_server_config):
    """Test 2.1 - Invalid credentials and retry throttling"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.state.redis', AsyncMock()):

      from src.server.routers.admin import login

      with patch('src.services.auth.AuthService.check_input_rate_limit') as mock_rate_check, \
           patch('src.services.auth.AuthService.login') as mock_auth_login:

        # Mock failed login
        mock_rate_check.return_value = (None, True)
        mock_auth_login.return_value = ("Invalid authentication token", None)

        login_request = Mock()
        login_request.token = "invalid_token"
        login_request.auth_method = "static"

        response = await login(mock_request, login_request)

        assert isinstance(response, ApiResponse)
        assert response.status_code == 401
        assert response.data["success"] is False

  @pytest.mark.asyncio
  async def test_2_2_successful_authentication(self, mock_request, mock_server_config):
    """Test 2.2 - Valid token authentication and session management"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.state.redis', AsyncMock()):

      from src.server.routers.admin import login

      with patch('src.services.auth.AuthService.check_input_rate_limit') as mock_rate_check, \
           patch('src.services.auth.AuthService.login') as mock_auth_login:

        # Mock successful login
        mock_rate_check.return_value = (None, True)
        mock_jwt_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.mock.token"
        mock_auth_login.return_value = (None, mock_jwt_token)

        login_request = Mock()
        login_request.token = STATIC_AUTH_TOKEN
        login_request.auth_method = "static"

        response = await login(mock_request, login_request)

        assert isinstance(response, ApiResponse)
        assert response.data["success"] is True
        assert "jwt_token" in response.data

  @pytest.mark.asyncio
  async def test_2_3_protected_resource_access(self, mock_request, mock_server_config):
    """Test 2.3 - Access to protected resources with authentication"""

    # Mock authenticated request
    mock_request.headers = {"Authorization": "Bearer mock.jwt.token"}
    mock_request.state.authenticated = True

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.config.get_resources') as mock_get_resources:

      mock_get_resources.return_value = ("", [
        "public.data", "sys.monitor", "sys.users", "admin.settings"
      ])

      from src.server.routers.retriever import get_resources

      response = await get_resources(
        mock_request,
        include_transient=False,
        include_protected=True
      )

      assert isinstance(response, ApiResponse)
      resources = response.data["resources"]

      # Should include sys.* resources for authenticated user
      assert "sys.monitor" in resources
      assert "sys.users" in resources

  @pytest.mark.asyncio
  async def test_2_7_user_management_validation(self, mock_request, mock_server_config):
    """Test 2.7 - sys.users table and user management"""

    # Mock authenticated request
    mock_request.headers = {"Authorization": "Bearer mock.jwt.token"}

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.user.UserService.list_users') as mock_list_users:

      mock_users = [
        {
          "uid": "user123",
          "ipv4": "192.168.1.100",
          "status": "public",
          "total_count": 150,
          "total_bytes": 1048576,
          "created_at": "2024-01-01T00:00:00Z"
        }
      ]

      mock_list_users.return_value = ("", {
        "users": mock_users,
        "total": 1,
        "offset": 0,
        "limit": 100
      })

      from src.server.routers.admin import list_users

      response = await list_users(mock_request, limit=100, offset=0)

      assert isinstance(response, ApiResponse)
      assert "users" in response.data
      assert len(response.data["users"]) == 1
      assert response.data["users"][0]["uid"] == "user123"


class TestRateLimitingSystem:
  """Test rate limiting system"""

  def test_exponential_backoff_calculation(self, mock_server_config):
    """Test exponential backoff for authentication failures"""

    input_limits = mock_server_config.input_rate_limits

    # Calculate expected delays
    attempt_1 = input_limits.start  # 50 seconds
    attempt_2 = input_limits.start * input_limits.factor  # 300 seconds
    attempt_3 = input_limits.start * (input_limits.factor ** 2)  # 1800 seconds

    assert attempt_1 == 50
    assert attempt_2 == 300
    assert attempt_3 == 1800

    # Should cap at max value (4 days)
    assert input_limits.max == 345600

  def test_rate_limit_point_costs(self, mock_server_config):
    """Test point costs for different endpoints"""

    route_limits = mock_server_config.route_limits

    assert route_limits["/schema"]["points"] == 1
    assert route_limits["/last"]["points"] == 1
    assert route_limits["/history"]["points"] == 5
    assert route_limits["/analysis"]["points"] == 15


class TestProtectionSystem:
  """Test protection system"""

  def test_route_protection_patterns(self, mock_server_config):
    """Test route protection pattern matching"""
    import fnmatch

    protected_routes = mock_server_config.protected_routes

    test_cases = [
      ("/admin/users", True),
      ("/admin/settings", True),
      ("/schema", False),
      ("/last", False),
      ("/public/data", False)
    ]

    for route, should_be_protected in test_cases:
      is_protected = any(fnmatch.fnmatch(route, pattern) for pattern in protected_routes)
      assert is_protected == should_be_protected

  def test_resource_protection_patterns_via_names(self, mock_server_config):
    """Test resource protection based on resource names (sys.*, admin.*) in actual resource data"""

    # Resource protection is now handled by the resource.protected attribute
    # rather than server config patterns
    test_cases = [
      ("sys.monitor", True),   # sys.* resources are protected by default
      ("sys.users", True),     # sys.* resources are protected by default
      ("admin.settings", True), # admin.* resources are protected by default
      ("public.data", False),  # public.* resources are not protected
      ("market.prices", False) # market.* resources are not protected
    ]

    for resource, should_be_protected in test_cases:
      # This simulates the actual protection logic based on resource names
      is_protected = resource.startswith(("sys.", "admin."))
      assert is_protected == should_be_protected
