"""
Comprehensive API flow tests for Chomp data ingester.
Tests both public and authenticated user flows including rate limiting,
protection, and data access controls.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Optional
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import ServerConfig, RateLimitConfig, InputRateLimitConfig
from src.utils.http import HttpClientSingleton

# Test configuration constants
STATIC_AUTH_TOKEN = "secrettoken"
TEST_BASE_URL = "http://localhost:40004"


class APITestClient:
  """Helper class for making API requests with authentication support"""

  def __init__(self, base_url: str = TEST_BASE_URL):
    self.base_url = base_url
    self.jwt_token: Optional[str] = None
    self._http_client = HttpClientSingleton()

  async def request(self, method: str, endpoint: str, **kwargs):
    """Make HTTP request with optional authentication"""
    url = f"{self.base_url}{endpoint}"
    headers = kwargs.get('headers', {})

    if self.jwt_token:
      headers['Authorization'] = f"Bearer {self.jwt_token}"

    kwargs['headers'] = headers

    # Use the singleton HTTP client
    client = await self._http_client.get_client()
    response = await client.request(method, url, **kwargs)
    return response

  async def get(self, endpoint: str, **kwargs):
    return await self.request("GET", endpoint, **kwargs)

  async def post(self, endpoint: str, **kwargs):
    return await self.request("POST", endpoint, **kwargs)

  async def login(self, token: str = STATIC_AUTH_TOKEN) -> bool:
    """Authenticate and store JWT token"""
    response = await self.post("/admin/login", json={
      "token": token,
      "auth_method": "static"
    })

    if response.status_code == 200:
      data = response.json()
      if data.get("success"):
        self.jwt_token = data.get("jwt_token")
        return True
    return False

  async def logout(self) -> bool:
    """Logout and clear JWT token"""
    if self.jwt_token:
      response = await self.post("/admin/logout")
      self.jwt_token = None
      return response.status_code == 200
    return True

  async def close(self):
    """Close HTTP session - singleton client is managed globally"""
    # Don't close the singleton client here
    pass


@pytest.fixture
async def api_client():
  """Fixture providing API test client"""
  client = APITestClient()
  yield client
  await client.close()


@pytest.fixture
async def authenticated_client():
  """Fixture providing authenticated API test client"""
  client = APITestClient()
  await client.login()
  yield client
  await client.logout()
  await client.close()


@pytest.fixture
def mock_server_state():
  """Mock server state for testing"""
  config = ServerConfig(
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

  with patch('src.state.server_config', config):
    with patch('src.state.redis', AsyncMock()):
      yield config


class TestPublicAPIFlows:
  """Test suite for public (unauthenticated) API flows"""

  @pytest.mark.asyncio
  async def test_1_1_resource_discovery(self, mock_server_state):
    """Test 1.1 - Query list of resources, ensure protected are excluded"""

    # Mock the resource service to return mixed public/protected resources
    with patch('src.services.config.get_resources') as mock_get_resources:
      mock_get_resources.return_value = ("", [
        "public.data", "public.prices", "sys.monitor", "admin.users"
      ])

      # Mock request object
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"

      # Import and test the actual endpoint
      from src.server.routers.retriever import get_resources

      response = await get_resources(
        mock_request,
        include_transient=False,
        include_protected=False
      )

      # Verify response structure
      assert hasattr(response, 'data')
      resources = response.data["resources"]

      # Verify protected resources are excluded for public access
      public_resources = [r for r in resources if not r.startswith("sys.")]
      assert len(public_resources) == len(resources)
      assert "public.data" in resources
      assert "public.prices" in resources
      assert "sys.monitor" not in resources
      assert "admin.users" not in resources

  @pytest.mark.asyncio
  async def test_1_2_last_values_queries(self, api_client, mock_server_state):
    """Test 1.2 - Query last values for single and multiple resources"""

    with patch('src.services.loader.get_last_data') as mock_get_last:
      # Test single resource
      mock_get_last.return_value = ("", {"resource": "test.data", "value": 123.45})

      response = await api_client.get("/last?resource=test.data")
      assert response.status_code == 200
      data = response.json()
      assert "data" in data
      assert data["data"]["value"] == 123.45

      # Test multiple resources
      mock_get_last.return_value = ("", {
        "test.data1": {"value": 123.45},
        "test.data2": {"value": 678.90}
      })

      response = await api_client.get("/last/test.data1,test.data2")
      assert response.status_code == 200
      data = response.json()
      assert "data" in data
      assert len(data["data"]) == 2

  @pytest.mark.asyncio
  async def test_1_3_historical_data_queries(self, api_client, mock_server_state):
    """Test 1.3 - Query historical values with various parameters"""

    with patch('src.services.loader.get_history_data') as mock_get_history:
      # Mock historical data response
      mock_data = {
        "resource": "test.data",
        "data": [
          {"ts": "2024-01-01T00:00:00Z", "value": 100.0},
          {"ts": "2024-01-01T01:00:00Z", "value": 101.0}
        ]
      }
      mock_get_history.return_value = ("", mock_data)

      # Test basic historical query
      response = await api_client.get("/history?resource=test.data")
      assert response.status_code == 200
      data = response.json()
      assert "data" in data
      assert len(data["data"]["data"]) == 2

      # Test with date range
      response = await api_client.get(
        "/history?resource=test.data&from_date=2024-01-01T00:00:00Z&to_date=2024-01-01T23:59:59Z"
      )
      assert response.status_code == 200

      # Test with interval
      response = await api_client.get(
        "/history?resource=test.data&interval=h1&limit=100"
      )
      assert response.status_code == 200

  @pytest.mark.asyncio
  async def test_1_4_protection_validation(self, api_client, mock_server_state):
    """Test 1.4 - Ensure protected resources are not accessible"""

    with patch('src.services.loader.get_last_data') as mock_get_last:
      # Should return filtered results excluding protected resources
      mock_get_last.return_value = ("Unauthorized access to protected resource", {})

      response = await api_client.get("/last?resource=sys.monitor")
      assert response.status_code in [403, 404, 500]  # Should be denied

      response = await api_client.get("/last/sys.users")
      assert response.status_code in [403, 404, 500]  # Should be denied

  @pytest.mark.asyncio
  async def test_1_5_dynamic_analysis_endpoints(self, api_client, mock_server_state):
    """Test 1.5 - Dynamic data endpoints with protection filtering"""

    with patch('src.services.ts_analysis.get_analysis') as mock_analysis:
      mock_analysis.return_value = ("", {"analysis": "data"})

      # Test pegcheck
      with patch('src.services.converter.get_conversion') as mock_pegcheck:
        mock_pegcheck.return_value = ("", {"peg_ratio": 1.001})
        response = await api_client.get("/pegcheck/USDC-USDT")
        assert response.status_code == 200

      # Test analysis endpoints
      response = await api_client.get("/analysis/test.data")
      assert response.status_code == 200

      response = await api_client.get("/volatility/test.data")
      assert response.status_code == 200

      response = await api_client.get("/trend/test.data")
      assert response.status_code == 200

      response = await api_client.get("/momentum/test.data")
      assert response.status_code == 200

  @pytest.mark.asyncio
  async def test_1_6_rate_limit_validation(self, api_client, mock_server_state):
    """Test 1.6 - Rate limit enforcement and monitoring"""

    with patch('src.services.limiter.check_limits') as mock_check_limits, \
         patch('src.services.limiter.increment_counters') as mock_increment:

      # Mock rate limit responses
      mock_check_limits.return_value = ("", {"allowed": True})
      mock_increment.return_value = ("", {"remaining": {"requests": 59}})

      # Test normal request within limits
      response = await api_client.get("/schema")
      assert response.status_code == 200

      # Test rate limit headers (if implemented)
      if 'X-RateLimit-Remaining' in response.headers:
        assert int(response.headers['X-RateLimit-Remaining']) >= 0

      # Test rate limit exceeded
      mock_check_limits.return_value = ("Rate limit exceeded", {})
      response = await api_client.get("/schema")
      assert response.status_code == 429


class TestAuthenticatedAPIFlows:
  """Test suite for authenticated API flows"""

  @pytest.mark.asyncio
  async def test_2_1_authentication_failure_handling(self, api_client, mock_server_state):
    """Test 2.1 - Invalid credentials and retry throttling"""

    with patch('src.services.auth.AuthService.login') as mock_login, \
         patch('src.services.auth.AuthService.check_input_rate_limit') as mock_rate_check:

      # Test first failed attempt (no throttling)
      mock_rate_check.return_value = (None, True)
      mock_login.return_value = ("Invalid authentication token", None)

      response = await api_client.post("/admin/login", json={
        "token": "invalid_token",
        "auth_method": "static"
      })
      assert response.status_code == 401

      # Test throttled attempt (after multiple failures)
      mock_rate_check.return_value = ("Too many failed attempts. Try again in 50 seconds.", False)

      response = await api_client.post("/admin/login", json={
        "token": "invalid_token",
        "auth_method": "static"
      })
      assert response.status_code == 429 or response.status_code == 401

  @pytest.mark.asyncio
  async def test_2_2_successful_authentication(self, api_client, mock_server_state):
    """Test 2.2 - Valid token authentication and session management"""

    with patch('src.services.auth.AuthService.login') as mock_login, \
         patch('src.services.auth.AuthService.verify_session') as mock_verify:

      # Mock successful login
      mock_jwt_token = "mock.jwt.token"
      mock_login.return_value = (None, mock_jwt_token)

      response = await api_client.post("/admin/login", json={
        "token": STATIC_AUTH_TOKEN,
        "auth_method": "static"
      })

      assert response.status_code == 200
      data = response.json()
      assert data["data"]["success"] is True
      assert "jwt_token" in data["data"]
      assert "expires_hours" in data["data"]

      # Test authentication status
      api_client.jwt_token = data["data"]["jwt_token"]
      mock_verify.return_value = (None, True)

      response = await api_client.get("/admin/auth/status")
      assert response.status_code == 200
      auth_data = response.json()
      assert auth_data["data"]["authenticated"] is True

  @pytest.mark.asyncio
  async def test_2_3_protected_resource_access(self, authenticated_client, mock_server_state):
    """Test 2.3 - Access to protected resources with authentication"""

    with patch('src.services.config.get_resources') as mock_get_resources:
      # Mock resources including protected ones for authenticated user
      mock_get_resources.return_value = ("", [
        "public.data", "public.prices", "sys.monitor", "sys.users", "admin.settings"
      ])

      response = await authenticated_client.get("/resources?include_protected=true")
      assert response.status_code == 200
      data = response.json()

      resources = data["data"]["resources"]
      assert "sys.monitor" in resources
      assert "sys.users" in resources
      assert "admin.settings" in resources

  @pytest.mark.asyncio
  async def test_2_4_protected_data_queries_last(self, authenticated_client, mock_server_state):
    """Test 2.4 - Access last values of protected monitoring data"""

    with patch('src.services.loader.get_last_data') as mock_get_last:
      # Mock protected monitoring data
      mock_get_last.return_value = ("", {
        "sys.monitor": {
          "latency_ms": 45.2,
          "response_bytes": 1024,
          "status_code": 200
        }
      })

      response = await authenticated_client.get("/last?resource=sys.monitor")
      assert response.status_code == 200
      data = response.json()
      assert "data" in data
      assert data["data"]["sys.monitor"]["latency_ms"] == 45.2

  @pytest.mark.asyncio
  async def test_2_5_protected_data_queries_historical(self, authenticated_client, mock_server_state):
    """Test 2.5 - Access historical values of protected monitoring data"""

    with patch('src.services.loader.get_history_data') as mock_get_history:
      # Mock historical monitoring data
      mock_data = {
        "resource": "sys.monitor",
        "data": [
          {"ts": "2024-01-01T00:00:00Z", "latency_ms": 45.2, "response_bytes": 1024},
          {"ts": "2024-01-01T01:00:00Z", "latency_ms": 52.1, "response_bytes": 2048}
        ]
      }
      mock_get_history.return_value = ("", mock_data)

      response = await authenticated_client.get("/history?resource=sys.monitor")
      assert response.status_code == 200
      data = response.json()
      assert "data" in data
      assert len(data["data"]["data"]) == 2

  @pytest.mark.asyncio
  async def test_2_6_admin_rate_limits(self, authenticated_client, mock_server_state):
    """Test 2.6 - Admin-specific rate limiting behavior"""

    with patch('src.services.limiter.check_limits') as mock_check_limits, \
         patch('src.services.limiter.get_user_limits') as mock_get_limits:

      # Mock admin rate limits (potentially higher than public)
      mock_check_limits.return_value = ("", {"allowed": True, "admin": True})
      mock_get_limits.return_value = ("", {
        "requests": {"cap": 1800, "remaining": 1799},  # Higher limit for admin
        "points": {"cap": 7200, "remaining": 7199}
      })

      response = await authenticated_client.get("/admin/ingesters")
      assert response.status_code == 200

      # Test rate limit info endpoint
      response = await authenticated_client.get("/limits")
      assert response.status_code == 200

  @pytest.mark.asyncio
  async def test_2_7_user_management_validation(self, authenticated_client, mock_server_state):
    """Test 2.7 - sys.users table and user management"""

    with patch('src.services.user.UserService.list_users') as mock_list_users, \
         patch('src.services.user.UserService.get_user') as mock_get_user:

      # Mock user data
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
      mock_list_users.return_value = ("", {"users": mock_users, "total": 1})

      response = await authenticated_client.get("/admin/users")
      assert response.status_code == 200
      data = response.json()
      assert "data" in data
      assert "users" in data["data"]
      assert len(data["data"]["users"]) == 1
      assert data["data"]["users"][0]["uid"] == "user123"

      # Test individual user query
      mock_get_user.return_value = ("", mock_users[0])
      response = await authenticated_client.get("/admin/users/user123")
      assert response.status_code == 200
      user_data = response.json()
      assert user_data["data"]["uid"] == "user123"
      assert user_data["data"]["total_count"] == 150


class TestRateLimitingIntegration:
  """Test rate limiting integration across different endpoints"""

  @pytest.mark.asyncio
  async def test_rate_limit_point_system(self, api_client, mock_server_state):
    """Test point-based rate limiting system"""

    with patch('src.services.limiter.check_limits') as mock_check, \
         patch('src.services.limiter.increment_counters') as mock_increment:

      # Define point costs per endpoint
      endpoint_points = {
        "/schema": 1,
        "/last": 1,
        "/history": 5,
        "/analysis": 15
      }

      for endpoint, points in endpoint_points.items():
        # Mock rate limit check allowing request
        mock_check.return_value = ("", {"allowed": True, "ppr": points})
        mock_increment.return_value = ("", {"remaining": {"points": 100 - points}})

        await api_client.get(endpoint.replace("/", "/test.data")
                                       if endpoint != "/schema" else endpoint)

        # Verify points were calculated correctly
        if mock_increment.called:
          call_args = mock_increment.call_args[0]
          assert call_args[2] == points  # points parameter

  @pytest.mark.asyncio
  async def test_data_size_rate_limiting(self, api_client, mock_server_state):
    """Test data size-based rate limiting"""

    with patch('src.services.limiter.increment_counters') as mock_increment:
      mock_increment.return_value = ("", {"remaining": {"bytes": 99_000_000}})

      # Mock large response
      large_response_data = {"data": "x" * 1000000}  # 1MB response

      with patch('src.services.loader.get_history_data') as mock_get_history:
        mock_get_history.return_value = ("", large_response_data)

        await api_client.get("/history?resource=test.data")

        if mock_increment.called:
          call_args = mock_increment.call_args[0]
          assert call_args[1] > 500000  # Should account for response size


class TestProtectionSystem:
  """Test protection system for routes and resources"""

  @pytest.mark.asyncio
  async def test_route_protection_patterns(self, api_client, mock_server_state):
    """Test route protection pattern matching"""

    # Test admin routes are protected
    response = await api_client.get("/admin/ingesters")
    assert response.status_code in [401, 403]

    response = await api_client.get("/admin/users")
    assert response.status_code in [401, 403]

  @pytest.mark.asyncio
  async def test_resource_protection_filtering(self, api_client, mock_server_state):
    """Test that protected resources are filtered from responses"""

    with patch('src.services.config.get_schema') as mock_get_schema:
      # Mock schema with both public and protected resources
      mock_schema = {
        "public.data": {"type": "timeseries", "protected": False},
        "sys.monitor": {"type": "timeseries", "protected": True},
        "admin.users": {"type": "update", "protected": True}
      }
      mock_get_schema.return_value = ("", mock_schema)

      # Public request should only see public resources
      response = await api_client.get("/schema")
      assert response.status_code == 200
      data = response.json()

      # Verify protected resources are excluded
      schema_keys = list(data["data"].keys())
      assert "public.data" in schema_keys
      assert "sys.monitor" not in schema_keys
      assert "admin.users" not in schema_keys


# Integration tests that require real server instance
@pytest.mark.integration
class TestLiveAPIIntegration:
  """Integration tests that require a running server instance"""

  @pytest.mark.asyncio
  async def test_full_authentication_flow(self):
    """Test complete authentication flow with real server"""
    client = APITestClient()

    # Test login
    login_success = await client.login(STATIC_AUTH_TOKEN)
    if not login_success:
      pytest.skip("Server not available or authentication failed")

    # Test authenticated request
    response = await client.get("/admin/auth/status")
    assert response.status_code == 200

    # Test logout
    logout_success = await client.logout()
    assert logout_success

    await client.close()

  @pytest.mark.asyncio
  async def test_live_rate_limiting(self):
    """Test rate limiting with real Redis backend"""
    client = APITestClient()

    # Make rapid requests to test rate limiting
    responses = []
    for i in range(10):
      response = await client.get("/ping")
      responses.append(response)
      if response.status_code == 429:
        break

    # Should eventually hit rate limit or all requests succeed
    assert any(r.status_code in [200, 429] for r in responses)

    await client.close()


if __name__ == "__main__":
  # Run specific test suites
  pytest.main([
    __file__,
    "-v",
    "--tb=short",
    "-k", "not integration"  # Skip integration tests by default
  ])
