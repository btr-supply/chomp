"""
Purpose: Test suite for services/limiter module
Tests the rate limiting functionality using Redis for storage
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, Mock, AsyncMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.limiter import RateLimiter
from src.models import User, RateLimitConfig


class TestOptimizedRateLimiter:
  """Test the new optimized RateLimiter class."""

  def setup_method(self):
    """Set up test fixtures."""
    # Mock state and config
    self.mock_state = Mock()
    self.mock_config = Mock()
    self.mock_config.route_limits = {
        "/schema": {
            "points": 1
        },
        "/history": {
            "points": 5
        },
        "/admin/*": {
            "points": 1
        }
    }
    self.mock_config.whitelist = ["admin123"]
    self.mock_config.blacklist = ["banned456"]

    self.mock_state.server_config = self.mock_config

    # Mock user with rate limits
    self.test_user = User(uid="test123",
                          rate_limits=RateLimitConfig(rpm=10,
                                                      rph=100,
                                                      rpd=1000,
                                                      spm=1000000,
                                                      sph=10000000,
                                                      spd=100000000,
                                                      ppm=20,
                                                      pph=200,
                                                      ppd=2000),
                          status="public")

  @pytest.mark.asyncio
  async def test_get_route_points_caching(self):
    """Test route points caching mechanism."""
    with patch('src.services.limiter.state', self.mock_state):
      # First call should cache
      points1 = RateLimiter.get_route_points("/schema")
      assert points1 == 1

      # Second call should use cache
      points2 = RateLimiter.get_route_points("/schema")
      assert points2 == 1

      # Test pattern matching
      points3 = RateLimiter.get_route_points("/admin/users")
      assert points3 == 1  # matches /admin/* pattern

      # Test default
      points4 = RateLimiter.get_route_points("/unknown")
      assert points4 == 10

  @pytest.mark.asyncio
  async def test_check_and_increment_whitelist(self):
    """Test whitelist bypass functionality."""
    whitelisted_user = User(uid="admin123", status="admin")

    with patch('src.services.limiter.state', self.mock_state):
      err, result = await RateLimiter.check_and_increment(
          whitelisted_user, "/schema", 1000)

      assert err == ""
      assert result["bypass"]

  @pytest.mark.asyncio
  async def test_check_and_increment_blacklist(self):
    """Test blacklist blocking functionality."""
    blacklisted_user = User(uid="banned456")

    with patch('src.services.limiter.state', self.mock_state):
      err, result = await RateLimiter.check_and_increment(
          blacklisted_user, "/schema", 1000)

      assert "blacklisted" in err
      assert result == {}

  @pytest.mark.asyncio
  async def test_check_and_increment_within_limits(self):
    """Test successful request within limits."""
    mock_redis = AsyncMock()
    mock_pipeline = AsyncMock()

    # Mock Redis responses - user is within limits
    mock_pipeline.execute.side_effect = [
        [
            "5", "50", "500", "500000", "5000000", "50000000", "10", "100",
            "1000"
        ],  # current values
        []  # increment results
    ]

    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipeline
    mock_redis.pipeline.return_value.__aexit__.return_value = None

    self.mock_state.redis = mock_redis

    with patch('src.services.limiter.state', self.mock_state):
      err, result = await RateLimiter.check_and_increment(
          self.test_user, "/schema", 1000)

      assert err == ""
      assert not result["limited"]
      assert "remaining" in result

  @pytest.mark.asyncio
  async def test_check_and_increment_rate_limited(self):
    """Test rate limit exceeded scenario."""
    mock_redis = AsyncMock()
    mock_pipeline = AsyncMock()

    # Mock Redis responses - user exceeds rpm limit (10)
    mock_pipeline.execute.side_effect = [
        [
            "10", "90", "900", "900000", "9000000", "90000000", "19", "190",
            "1900"
        ],  # at limit
        []
    ]

    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipeline
    mock_redis.pipeline.return_value.__aexit__.return_value = None

    self.mock_state.redis = mock_redis

    with patch('src.services.limiter.state', self.mock_state):
      err, result = await RateLimiter.check_and_increment(
          self.test_user, "/schema", 1000)

      assert "Rate limit exceeded" in err
      assert result["limited"]
      assert result["retry_after"] == 60  # rpm limit TTL

  @pytest.mark.asyncio
  async def test_check_and_increment_points_limit(self):
    """Test points-based rate limiting."""
    mock_redis = AsyncMock()
    mock_pipeline = AsyncMock()

    # Mock Redis responses - user exceeds ppm limit with high-point request
    mock_pipeline.execute.side_effect = [
        [
            "5", "50", "500", "500000", "5000000", "50000000", "18", "180",
            "1800"
        ],  # close to ppm limit (20)
        []
    ]

    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipeline
    mock_redis.pipeline.return_value.__aexit__.return_value = None

    self.mock_state.redis = mock_redis

    with patch('src.services.limiter.state', self.mock_state):
      err, result = await RateLimiter.check_and_increment(
          self.test_user,
          "/history",
          1000  # /history costs 5 points, would exceed 20 limit
      )

      assert "Rate limit exceeded" in err
      assert "ppm" in err
      assert result["limited"]


class TestLimiterService:
  """Test rate limiting service functionality."""

  @pytest.mark.asyncio
  async def test_check_limits_blacklisted_user(self):
    """Test that blacklisted users are rejected."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = ["baduser"]
      mock_limiter.whitelist = []
      mock_state.server.limiter = mock_limiter

      from src.services.limiter import check_limits
      error, data = await check_limits("baduser")

      assert error == "User is blacklisted"
      assert data == {}

  @pytest.mark.asyncio
  async def test_check_limits_whitelisted_user(self):
    """Test that whitelisted users are allowed."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = ["gooduser"]
      mock_state.server.limiter = mock_limiter

      # Create mock user for testing
      mock_user = Mock()
      mock_user.uid = "gooduser"

      error, data = await RateLimiter.check_and_increment(mock_user, "/", 1)

      assert error == ""
      assert data == {"whitelisted": True}

  @pytest.mark.asyncio
  async def test_check_limits_under_limit(self):
    """Test user under rate limits."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (60, 100)}
      mock_limiter.ppr = {}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=["50"])

      # Create mock user for testing
      mock_user = Mock()
      mock_user.uid = "testuser"

      error, data = await RateLimiter.check_and_increment(mock_user, "/", 1)

      assert error == ""
      assert "current_counts" in data
      assert data["current_counts"] == [50]

  @pytest.mark.asyncio
  async def test_check_limits_over_limit(self):
    """Test user over rate limits."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (60, 100)}
      mock_limiter.ppr = {}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=["150"])

      # Create mock user for testing
      mock_user = Mock()
      mock_user.uid = "testuser"

      error, data = await RateLimiter.check_and_increment(mock_user, "/", 1)

      assert "Rate limit exceeded" in error
      assert data == {}

  @pytest.mark.asyncio
  async def test_get_user_limits_success(self):
    """Test getting user limits successfully."""
    with patch('src.services.limiter.state') as mock_state, \
         patch('src.services.limiter.secs_to_ceil_date') as mock_secs, \
         patch('src.services.limiter.fmt_date') as mock_fmt:

      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (60, 100)}
      mock_state.server.limiter = mock_limiter

      # Mock pipeline
      mock_pipe = AsyncMock()
      mock_pipe.get = Mock()
      mock_pipe.ttl = Mock()
      mock_pipe.execute = AsyncMock(return_value=["50", "3600"])
      mock_state.redis.pipeline.return_value.__aenter__ = AsyncMock(
          return_value=mock_pipe)
      mock_state.redis.pipeline.return_value.__aexit__ = AsyncMock(
          return_value=None)

      mock_secs.return_value = 3600
      mock_fmt.return_value = "2023-12-01T12:00:00Z"

      from src.services.limiter import get_user_limits
      error, data = await get_user_limits("testuser")

      assert error == ""
      assert "requests" in data
      assert data["requests"]["cap"] == 100
      assert data["requests"]["remaining"] == 50

  @pytest.mark.asyncio
  async def test_get_user_limits_error(self):
    """Test get_user_limits error handling."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (60, 100)}
      mock_state.server.limiter = mock_limiter

      mock_state.redis.pipeline.side_effect = Exception("Redis error")

      from src.services.limiter import get_user_limits
      error, data = await get_user_limits("testuser")

      assert "Error fetching limits" in error
      assert data == {}

  @pytest.mark.asyncio
  async def test_increment_counters_success(self):
    """Test incrementing counters successfully."""
    with patch('src.services.limiter.state') as mock_state, \
         patch('src.services.limiter.secs_to_ceil_date') as mock_secs:

      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (60, 100)}
      mock_state.server.limiter = mock_limiter

      # Mock pipeline
      mock_pipe = AsyncMock()
      mock_pipe.incrby = Mock()
      mock_pipe.expire = Mock()
      mock_pipe.execute = AsyncMock(return_value=[])
      mock_state.redis.pipeline.return_value.__aenter__ = AsyncMock(
          return_value=mock_pipe)
      mock_state.redis.pipeline.return_value.__aexit__ = AsyncMock(
          return_value=None)
      mock_state.redis.get = AsyncMock(return_value="50")

      mock_secs.return_value = 3600

      from src.services.limiter import increment_counters
      error, data = await increment_counters("testuser", 1024, 1)

      assert error == ""
      assert "limits" in data
      assert "remaining" in data

  @pytest.mark.asyncio
  async def test_increment_counters_error(self):
    """Test increment_counters error handling."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (60, 100)}
      mock_state.server.limiter = mock_limiter

      mock_state.redis.pipeline.side_effect = Exception("Redis error")

      from src.services.limiter import increment_counters
      error, data = await increment_counters("testuser", 1024, 1)

      assert "Error incrementing counters" in error
      assert data == {}

  @pytest.mark.asyncio
  async def test_check_limits_with_path_ppr(self):
    """Test check_limits with path-specific points per request."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (60, 100)}
      mock_limiter.ppr = {"/api/heavy": 5}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=["50", "10"])

      from src.services.limiter import check_limits
      error, data = await check_limits("testuser", "/api/heavy")

      assert error == ""
      assert data["ppr"] == 5

  @pytest.mark.asyncio
  async def test_check_limits_no_redis_data(self):
    """Test check_limits when Redis returns None values."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (60, 100)}
      mock_limiter.ppr = {}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=[None])

      from src.services.limiter import check_limits
      error, data = await check_limits("testuser")

      assert error == ""
      assert data["current_counts"] == [0]
