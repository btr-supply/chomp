"""
Comprehensive tests for the unified authentication flow.
Covers challenge-response (Web3) and direct (static token) auth.
"""

import sys
import os
import pytest
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Mock web3 before importing
mock_web3 = Mock()
mock_web3.Web3 = Mock()
mock_web3.Web3.to_checksum_address = lambda x: x
sys.modules['web3'] = mock_web3

# Mock hexbytes
mock_hexbytes = Mock()
mock_hexbytes.HexBytes = lambda x: x
sys.modules['hexbytes'] = mock_hexbytes

# Import after mocking is set up
from src.server import create_app  # noqa: E402
from src.models import ServerConfig  # noqa: E402

# Test setup
@pytest.fixture(scope="module")
def client():
  """Create a test client for the FastAPI application."""
  # Patch server config to enable all auth methods
  with patch('chomp.src.state.server_config', ServerConfig(
    auth_methods=['static', 'evm', 'svm', 'sui'],
    static_auth_token='test_token',
    auth_flow_expiry=60
  )):
    app = create_app()
    with TestClient(app) as test_client:
      yield test_client

# === TEST CASES ===

def test_get_auth_methods(client: TestClient):
  """Test that the /auth/methods endpoint returns the correct list of enabled methods."""
  response = client.get("/auth/methods")
  assert response.status_code == 200
  data = response.json()
  assert "available_methods" in data
  assert set(data["available_methods"]) == {'static', 'evm', 'svm', 'sui'}

# --- Web3 Challenge-Response Flow ---

@pytest.mark.parametrize("auth_method", ["evm", "svm", "sui"])
def test_web3_challenge_creation(client: TestClient, auth_method: str):
  """Test the creation of a Web3 authentication challenge."""
  response = client.post("/auth/challenge", json={
    "auth_method": auth_method,
    "identifier": "0x1234567890123456789012345678901234567890"
  })
  assert response.status_code == 200
  data = response.json()
  assert "challenge_id" in data
  assert "message" in data
  assert "expires_at" in data
  assert "Address: 0x1234567890123456789012345678901234567890" in data["message"]

def test_web3_challenge_verification_success(client: TestClient):
  """Test successful verification of a Web3 challenge."""
  # 1. Create challenge
  challenge_response = client.post("/auth/challenge", json={
    "auth_method": "evm",
    "identifier": "0x1234567890123456789012345678901234567890"
  })
  challenge_id = challenge_response.json()["challenge_id"]

  # 2. Verify challenge
  verify_response = client.post("/auth/verify", json={
    "challenge_id": challenge_id,
    "credentials": {
      "address": "0x1234567890123456789012345678901234567890",
      "signature": "0x_fake_signature"
    }
  })
  assert verify_response.status_code == 200
  data = verify_response.json()
  assert "access_token" in data
  assert data["token_type"] == "bearer"
  assert data["user_id"] == "0x1234567890123456789012345678901234567890"
  assert data["provider"] == "evm"

def test_web3_challenge_verification_failure_bad_signature(client: TestClient):
  """Test failed verification due to a mismatched signature."""
  # Mock the signature verification to fail
  mock_web3.Web3.recover_message.return_value = "0x_different_address"

  # 1. Create challenge
  challenge_response = client.post("/auth/challenge", json={
    "auth_method": "evm",
    "identifier": "0x1234567890123456789012345678901234567890"
  })
  challenge_id = challenge_response.json()["challenge_id"]

  # 2. Verify challenge
  verify_response = client.post("/auth/verify", json={
    "challenge_id": challenge_id,
    "credentials": {
      "address": "0x1234567890123456789012345678901234567890",
      "signature": "0x_fake_signature_that_will_fail"
    }
  })
  assert verify_response.status_code == 401
  assert "Signature does not match address" in verify_response.json()["detail"]

  # Reset mock
  mock_web3.Web3.recover_message.return_value = "0x1234567890123456789012345678901234567890"

def test_web3_challenge_reuse_fails(client: TestClient):
  """Test that a challenge cannot be used more than once."""
  # 1. Create challenge
  challenge_response = client.post("/auth/challenge", json={"auth_method": "evm", "identifier": "0x1"})
  challenge_id = challenge_response.json()["challenge_id"]

  # 2. Use it once (successfully)
  client.post("/auth/verify", json={
    "challenge_id": challenge_id,
    "credentials": {"address": "0x1234567890123456789012345678901234567890", "signature": "0x_sig"}
  })

  # 3. Try to use it again
  verify_response = client.post("/auth/verify", json={
    "challenge_id": challenge_id,
    "credentials": {"address": "0x1234567890123456789012345678901234567890", "signature": "0x_sig"}
  })
  assert verify_response.status_code == 401
  assert "Invalid or expired" in verify_response.json()["detail"]


# --- Direct Authentication Flow (Static Token) ---

def test_direct_auth_static_token_success(client: TestClient):
  """Test successful direct authentication with a static token."""
  response = client.post("/auth/direct", json={
    "auth_method": "static",
    "credentials": {
      "token": "test_token"
    }
  })
  assert response.status_code == 200
  data = response.json()
  assert "access_token" in data
  assert data["user_id"] == "static_admin"
  assert data["provider"] == "static"

def test_direct_auth_static_token_failure(client: TestClient):
  """Test failed direct authentication with an invalid static token."""
  response = client.post("/auth/direct", json={
    "auth_method": "static",
    "credentials": {
      "token": "wrong_token"
    }
  })
  assert response.status_code == 401
  assert "Invalid token" in response.json()["detail"]
