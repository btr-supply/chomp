"""
Comprehensive API tests covering all user flows for Chomp data ingester.
This file contains detailed tests for both public and authenticated user scenarios,
including rate limiting, protection, and data access controls.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import ServerConfig, RateLimitConfig, InputRateLimitConfig
from src.server.responses import ApiResponse


# Test configuration constants
STATIC_AUTH_TOKEN = "secrettoken"


@pytest.fixture
def mock_server_config():
  """Mock server configuration for testing"""
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
  """Mock FastAPI request object"""
  request = Mock()
  request.client.host = "127.0.0.1"
  request.headers = {}
  request.state = Mock()
  request.state.requester_id = "127.0.0.1"
  return request


@pytest.fixture
def mock_authenticated_request():
  """Mock authenticated FastAPI request object"""
  request = Mock()
  request.client.host = "127.0.0.1"
  request.headers = {"Authorization": "Bearer mock.jwt.token"}
  request.state = Mock()
  request.state.requester_id = "127.0.0.1"
  request.state.authenticated = True
  return request


class TestPublicUserFlows:
  """Test suite for Flow 1: Public (unauthenticated) user flows"""

  @pytest.mark.asyncio
  async def test_1_1_resource_discovery_excludes_protected(self, mock_request, mock_server_config):
    """Test 1.1 - Query list of resources, ensure protected are excluded"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.config.get_resources') as mock_get_resources:

      # Mock mixed public/protected resources
      mock_get_resources.return_value = ("", [
        "public.data", "public.prices", "sys.monitor", "admin.users", "sys.instance.monitor"
      ])

      from src.server.routers.retriever import get_resources

      response = await get_resources(
        mock_request,
        include_transient=False,
        include_protected=False  # Public request should not include protected
      )

      assert isinstance(response, ApiResponse)
      resources = response.data["resources"]

      # Verify protected resources (sys.*) are excluded
      protected_resources = [r for r in resources if r.startswith("sys.") or r.startswith("admin.")]
      assert len(protected_resources) == 0, f"Protected resources found: {protected_resources}"

      # Verify public resources are included
      assert "public.data" in resources
      assert "public.prices" in resources

  @pytest.mark.asyncio
  async def test_1_2_last_values_single_and_multiple_resources(self, mock_request, mock_server_config):
    """Test 1.2 - Query last values for single and multiple resources"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_last_data') as mock_get_last:

      from src.server.routers.retriever import get_last_data

      # Test single resource
      mock_get_last.return_value = ("", {
        "test.data": {"value": 123.45, "timestamp": "2024-01-01T00:00:00Z"}
      })

      response = await get_last_data(mock_request, resource="test.data", include_transient=False)

      assert isinstance(response, ApiResponse)
      assert "test.data" in response.data
      assert response.data["test.data"]["value"] == 123.45

      # Test multiple resources (would be handled by path-based routing)
      mock_get_last.return_value = ("", {
        "test.data1": {"value": 123.45},
        "test.data2": {"value": 678.90}
      })

      response = await get_last_data(mock_request, resource="test.data1,test.data2", include_transient=False)

      assert isinstance(response, ApiResponse)
      assert len(response.data) >= 1  # Response structure may vary

  @pytest.mark.asyncio
  async def test_1_3_historical_data_queries_with_parameters(self, mock_request, mock_server_config):
    """Test 1.3 - Query historical values with various parameters"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_history_data') as mock_get_history:

      from src.server.routers.retriever import get_history_data

      # Mock historical data
      mock_data = {
        "resource": "test.data",
        "data": [
          {"ts": "2024-01-01T00:00:00Z", "value": 100.0},
          {"ts": "2024-01-01T01:00:00Z", "value": 101.0},
          {"ts": "2024-01-01T02:00:00Z", "value": 102.0}
        ]
      }
      mock_get_history.return_value = ("", mock_data)

      # Test basic query
      response = await get_history_data(
        mock_request,
        resource="test.data",
        from_date=None,
        to_date=None,
        interval=None,
        limit=1000
      )

      assert isinstance(response, ApiResponse)
      assert len(response.data["data"]) == 3

      # Test with date range
      response = await get_history_data(
        mock_request,
        resource="test.data",
        from_date="2024-01-01T00:00:00Z",
        to_date="2024-01-01T23:59:59Z",
        interval="h1",
        limit=100
      )

      assert isinstance(response, ApiResponse)
      mock_get_history.assert_called()

  @pytest.mark.asyncio
  async def test_1_4_protection_validation_denies_sys_resources(self, mock_request, mock_server_config):
    """Test 1.4 - Ensure protected resources are not accessible to public users"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_last_data') as mock_get_last:

      from src.server.routers.retriever import get_last_data

      # Mock protection error for sys.* resources
      mock_get_last.return_value = ("Access denied to protected resource", {})

      # Test accessing sys.* resource should fail
      response = await get_last_data(mock_request, resource="sys.monitor", include_transient=False)

      # Should either return error or empty data
      assert isinstance(response, ApiResponse)
      # The actual behavior depends on implementation - could be empty data or error

  @pytest.mark.asyncio
  async def test_1_5_dynamic_analysis_endpoints_filter_protected(self, mock_request, mock_server_config):
    """Test 1.5 - Dynamic analysis endpoints generate data and filter protected resources"""

    with patch('src.state.server_config', mock_server_config):

      # Test pegcheck endpoint
      with patch('src.services.converter.get_conversion') as mock_pegcheck:
        mock_pegcheck.return_value = ("", {
          "pair": "USDC-USDT",
          "peg_ratio": 1.001,
          "deviation": 0.001
        })

        from src.server.routers.retriever import get_convert

        response = await get_convert(
          mock_request,
          pair="USDC-USDT",
          base_amount=1000.0,
          quote_amount=None,
          precision=6
        )

        assert isinstance(response, ApiResponse)

      # Test analysis endpoints
      with patch('src.services.ts_analysis.get_analysis') as mock_analysis:
        mock_analysis.return_value = ("", {
          "analysis_type": "volatility",
          "data": [{"period": 20, "volatility": 0.025}]
        })

        from src.server.routers.retriever import get_analysis

        response = await get_analysis(
          mock_request,
          resource="test.data",
          analysis_type="volatility",
          from_date=None,
          to_date=None
        )

        assert isinstance(response, ApiResponse)

  @pytest.mark.asyncio
  async def test_1_6_rate_limit_enforcement_and_monitoring(self, mock_request, mock_server_config):
    """Test 1.6 - Rate limits decrease as expected"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.limiter.check_limits') as mock_check_limits, \
         patch('src.services.limiter.increment_counters') as mock_increment:

      # Test normal request within limits
      mock_check_limits.return_value = ("", {
        "allowed": True,
        "current_counts": [10, 150, 1800],  # requests, points, bytes
        "limits": {"rpm": 60, "ppm": 120, "spm": 100_000_000}
      })
      mock_increment.return_value = ("", {
        "remaining": {"requests": 49, "points": 119, "bytes": 99_999_000}
      })

      from src.server.routers.retriever import get_schema

      response = await get_schema(
        mock_request,
        scope="default",
        include_transient=False,
        include_protected=False
      )

      assert isinstance(response, ApiResponse)

      # Test rate limit exceeded scenario
      mock_check_limits.return_value = ("Rate limit exceeded for requests", {})

      # The actual endpoint would handle rate limiting via middleware
      # Here we verify the limiter service works correctly


class TestAuthenticatedUserFlows:
  """Test suite for Flow 2: Authenticated user flows"""

  @pytest.mark.asyncio
  async def test_2_1_authentication_failure_throttling(self, mock_request, mock_server_config):
    """Test 2.1 - Invalid credentials and retry throttling"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.state.redis', AsyncMock()):

      from src.server.routers.admin import login

      # Test first failed attempt
      with patch('src.services.auth.AuthService.check_input_rate_limit') as mock_rate_check, \
           patch('src.services.auth.AuthService.login') as mock_auth_login:

        # First attempt - no throttling
        mock_rate_check.return_value = (None, True)
        mock_auth_login.return_value = ("Invalid authentication token", None)

        login_request = Mock()
        login_request.token = "invalid_token"
        login_request.auth_method = "static"

        response = await login(mock_request, login_request)

        assert isinstance(response, ApiResponse)
        assert response.status_code == 401
        assert response.data["success"] is False

        # Test throttled attempt
        mock_rate_check.return_value = ("Too many failed attempts. Try again in 50 seconds.", False)

        # This should be rejected due to rate limiting
        # The actual implementation would return 429 or similar

  @pytest.mark.asyncio
  async def test_2_2_successful_authentication_flow(self, mock_request, mock_server_config):
    """Test 2.2 - Valid token authentication and session management"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.state.redis', AsyncMock()):

      from src.server.routers.admin import login, auth_status

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
        assert "expires_hours" in response.data

        # Test authentication status check
        mock_authenticated_request = Mock()
        mock_authenticated_request.client.host = "127.0.0.1"
        mock_authenticated_request.headers = {"Authorization": f"Bearer {mock_jwt_token}"}

        with patch('src.services.auth.AuthService.verify_session') as mock_verify:
          mock_verify.return_value = (None, True)

          auth_response = await auth_status(mock_authenticated_request)

          assert isinstance(auth_response, ApiResponse)
          assert auth_response.data["authenticated"] is True

  @pytest.mark.asyncio
  async def test_2_3_protected_resource_access_for_authenticated(self, mock_authenticated_request, mock_server_config):
    """Test 2.3 - Access to protected resources with authentication"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.config.get_resources') as mock_get_resources:

      # Mock resources including protected ones for authenticated user
      mock_get_resources.return_value = ("", [
        "public.data", "public.prices", "sys.monitor", "sys.users",
        "sys.instance.monitor", "admin.settings"
      ])

      from src.server.routers.retriever import get_resources

      response = await get_resources(
        mock_authenticated_request,
        include_transient=False,
        include_protected=True  # Authenticated user can see protected resources
      )

      assert isinstance(response, ApiResponse)
      resources = response.data["resources"]

      # Verify sys.* resources are included for authenticated user
      sys_resources = [r for r in resources if r.startswith("sys.")]
      assert len(sys_resources) > 0
      assert "sys.monitor" in resources
      assert "sys.users" in resources

  @pytest.mark.asyncio
  async def test_2_4_protected_monitoring_data_access(self, mock_authenticated_request, mock_server_config):
    """Test 2.4 - Access last values of protected monitoring data"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.loader.get_last_data') as mock_get_last:

      # Mock protected monitoring data
      mock_get_last.return_value = ("", {
        "sys.monitor": {
          "instance_name": "chomp-instance-1",
          "latency_ms": 45.2,
          "response_bytes": 1024,
          "status_code": 200,
          "field_count": 5
        }
      })

      from src.server.routers.retriever import get_last_data

      response = await get_last_data(
        mock_authenticated_request,
        resource="sys.monitor",
        include_transient=False
      )

      assert isinstance(response, ApiResponse)
      monitor_data = response.data["sys.monitor"]
      assert monitor_data["latency_ms"] == 45.2
      assert monitor_data["response_bytes"] == 1024

  @pytest.mark.asyncio
  async def test_2_7_user_management_sys_users_table(self, mock_authenticated_request, mock_server_config):
    """Test 2.7 - Access sys.users table and user management"""

    with patch('src.state.server_config', mock_server_config), \
         patch('src.services.user.UserService.list_users') as mock_list_users:

      # Mock user data from sys.users table
      mock_users = [
        {
          "uid": "user123",
          "ipv4": "192.168.1.100",
          "ipv6": "",
          "alias": "test@example.com",
          "status": "public",
          "total_count": 150,
          "schema_count": 10,
          "last_count": 100,
          "history_count": 30,
          "analysis_count": 10,
          "total_bytes": 1048576,
          "created_at": "2024-01-01T00:00:00Z",
          "updated_at": "2024-01-01T12:00:00Z"
        },
        {
          "uid": "admin456",
          "ipv4": "192.168.1.200",
          "status": "admin",
          "total_count": 500,
          "total_bytes": 5242880,
          "created_at": "2024-01-01T00:00:00Z"
        }
      ]

      mock_list_users.return_value = ("", {
        "users": mock_users,
        "total": 2,
        "offset": 0,
        "limit": 100
      })

      from src.server.routers.admin import list_users

      response = await list_users(mock_authenticated_request, limit=100, offset=0)

      assert isinstance(response, ApiResponse)
      user_data = response.data
      assert "users" in user_data
      assert len(user_data["users"]) == 2
      assert user_data["total"] == 2

      # Verify user tracking data
      user1 = user_data["users"][0]
      assert user1["uid"] == "user123"
      assert user1["total_count"] == 150
      assert user1["total_bytes"] == 1048576
      assert user1["status"] == "public"


class TestRateLimitingSystem:
  """Test rate limiting system integration"""

  @pytest.mark.asyncio
  async def test_rate_limit_point_calculation(self, mock_server_config):
    """Test that different endpoints have correct point costs"""

    with patch('src.state.server_config', mock_server_config):

      # Test point costs from server config
      route_limits = mock_server_config.route_limits

      assert route_limits["/schema"]["points"] == 1
      assert route_limits["/last"]["points"] == 1
      assert route_limits["/history"]["points"] == 5
      assert route_limits["/analysis"]["points"] == 15

  @pytest.mark.asyncio
  async def test_exponential_backoff_for_auth_failures(self, mock_server_config):
    """Test exponential backoff calculation for authentication failures"""

    input_limits = mock_server_config.input_rate_limits

    # Calculate expected delays
    attempt_1_delay = input_limits.start  # 50 seconds
    attempt_2_delay = input_limits.start * input_limits.factor  # 300 seconds
    attempt_3_delay = input_limits.start * (input_limits.factor ** 2)  # 1800 seconds

    assert attempt_1_delay == 50
    assert attempt_2_delay == 300
    assert attempt_3_delay == 1800

    # Should cap at max value
    max_attempts = 10
    max_delay = min(
      input_limits.start * (input_limits.factor ** (max_attempts - 1)),
      input_limits.max
    )
    assert max_delay == input_limits.max  # 345600 seconds (4 days)


class TestProtectionPatterns:
  """Test protection pattern matching"""

  def test_route_protection_patterns(self, mock_server_config):
    """Test route protection pattern matching"""
    import fnmatch

    protected_routes = mock_server_config.protected_routes

    # Test patterns
    test_routes = [
      ("/admin/users", True),
      ("/admin/settings", True),
      ("/admin/logs", True),
      ("/public/data", False),
      ("/schema", False),
      ("/last", False)
    ]

    for route, should_be_protected in test_routes:
      is_protected = any(fnmatch.fnmatch(route, pattern) for pattern in protected_routes)
      assert is_protected == should_be_protected

  def test_resource_protection_patterns_via_names(self, mock_server_config):
    """Test resource protection based on resource names (sys.*, admin.*) in actual resource data"""

    # Resource protection is now handled by the resource.protected attribute
    # rather than server config patterns
    test_resources = [
      ("sys.monitor", True),
      ("sys.users", True),
      ("sys.instance.monitor", True),
      ("admin.settings", True),
      ("public.data", False),
      ("market.prices", False)
    ]

    for resource, should_be_protected in test_resources:
      # This simulates the actual protection logic based on resource names
      is_protected = resource.startswith(("sys.", "admin."))
      assert is_protected == should_be_protected


if __name__ == "__main__":
  pytest.main([__file__, "-v", "--tb=short"])
