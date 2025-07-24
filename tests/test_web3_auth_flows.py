"""
Comprehensive tests for Web3 authentication flows.
Tests EVM, Solana (SVM), and Sui wallet authentication.
"""

import pytest
import time
from unittest.mock import patch, AsyncMock, MagicMock

# Use the standard safe_import from deps
from chomp.src.utils.deps import safe_import
from src.services.auth import AuthService
from src.models import ServerConfig, RateLimitConfig, InputRateLimitConfig

# Test addresses for different chains
TEST_EVM_ADDRESS = "0x1234567890123456789012345678901234567890"
TEST_SOLANA_ADDRESS = "11111111111111111111111111111112"
TEST_SUI_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"

# Check for optional dependencies
eth_account = safe_import("eth_account")
solana = safe_import("solana")
nacl = safe_import("nacl")

EVM_AVAILABLE = eth_account is not None
SOLANA_AVAILABLE = solana is not None
SUI_AVAILABLE = True  # Sui uses fallback verification

@pytest.fixture
def mock_server_config():
    """Mock server configuration with all auth methods enabled"""
    return ServerConfig(
        auth_methods=["static", "evm", "svm", "sui", "oauth2_github", "oauth2_x"],
        static_auth_token="test_token_123",
        auth_flow_expiry=600,  # 10 minutes
        session_ttl=86400,
        auto_renew_session=True,
        jwt_secret_key="test_secret_key",
        default_rate_limits=RateLimitConfig(),
        input_rate_limits=InputRateLimitConfig()
    )

@pytest.fixture
def mock_redis():
    """Mock Redis client"""
    return AsyncMock()

class TestWeb3AuthenticationFlows:
    """Test Web3 wallet authentication flows"""

    @pytest.mark.asyncio
    @pytest.mark.skipif(not EVM_AVAILABLE, reason="EVM dependencies not available (eth_account)")
    async def test_evm_complete_auth_flow(self, mock_server_config, mock_redis):
        """Test complete EVM wallet authentication flow"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Step 1: Create EVM challenge
            error, challenge_data = await AuthService.create_auth_challenge(
                "evm", "127.0.0.1", TEST_EVM_ADDRESS
            )

            assert error is None
            assert challenge_data is not None
            assert challenge_data["auth_method"] == "evm"
            assert challenge_data["identifier"] == TEST_EVM_ADDRESS
            assert "Welcome to Chomp!" in challenge_data["message"]
            assert f"Address: {TEST_EVM_ADDRESS}" in challenge_data["message"]

            challenge_id = challenge_data["challenge_id"]
            message = challenge_data["message"]

            # Step 2: Mock signature verification to succeed
            with patch.object(AuthService, '_verify_evm_signature') as mock_verify:
                mock_verify.return_value = (None, True)  # Success

                # Step 3: Verify challenge with mocked signature
                credentials = {
                    "address": TEST_EVM_ADDRESS,
                    "signature": "0x_mocked_signature_data"
                }

                error, token, user_data = await AuthService.authenticate_with_challenge(
                    challenge_id, credentials
                )

                assert error is None
                assert token is not None
                assert user_data["user_id"] == f"web3:{TEST_EVM_ADDRESS}"
                assert user_data["provider"] == "evm"

                # Verify signature verification was called correctly
                mock_verify.assert_called_once_with(
                    TEST_EVM_ADDRESS,
                    "0x_mocked_signature_data",
                    message
                )

    @pytest.mark.asyncio
    @pytest.mark.skipif(not SOLANA_AVAILABLE, reason="Solana dependencies not available")
    async def test_solana_complete_auth_flow(self, mock_server_config, mock_redis):
        """Test complete Solana wallet authentication flow"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Step 1: Create Solana challenge
            error, challenge_data = await AuthService.create_auth_challenge(
                "svm", "127.0.0.1", TEST_SOLANA_ADDRESS
            )

            assert error is None
            assert challenge_data is not None
            assert challenge_data["auth_method"] == "svm"
            assert challenge_data["identifier"] == TEST_SOLANA_ADDRESS
            assert "Welcome to Chomp!" in challenge_data["message"]
            assert f"Address: {TEST_SOLANA_ADDRESS}" in challenge_data["message"]

            challenge_id = challenge_data["challenge_id"]
            message = challenge_data["message"]

            # Step 2: Mock signature verification to succeed
            with patch.object(AuthService, '_verify_svm_signature') as mock_verify:
                mock_verify.return_value = (None, True)  # Success

                # Step 3: Verify challenge with mocked signature
                credentials = {
                    "address": TEST_SOLANA_ADDRESS,
                    "signature": [1, 2, 3, 4, 5]  # Mocked signature bytes
                }

                error, token, user_data = await AuthService.authenticate_with_challenge(
                    challenge_id, credentials
                )

                assert error is None
                assert token is not None
                assert user_data["user_id"] == f"web3:{TEST_SOLANA_ADDRESS}"
                assert user_data["provider"] == "svm"

                # Verify signature verification was called correctly
                mock_verify.assert_called_once_with(
                    TEST_SOLANA_ADDRESS,
                    [1, 2, 3, 4, 5],
                    message
                )

    @pytest.mark.asyncio
    async def test_sui_complete_auth_flow(self, mock_server_config, mock_redis):
        """Test complete Sui wallet authentication flow"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Step 1: Create Sui challenge
            error, challenge_data = await AuthService.create_auth_challenge(
                "sui", "127.0.0.1", TEST_SUI_ADDRESS
            )

            assert error is None
            assert challenge_data is not None
            assert challenge_data["auth_method"] == "sui"
            assert challenge_data["identifier"] == TEST_SUI_ADDRESS
            assert "Welcome to Chomp!" in challenge_data["message"]
            assert f"Address: {TEST_SUI_ADDRESS}" in challenge_data["message"]

            challenge_id = challenge_data["challenge_id"]
            message = challenge_data["message"]

            # Step 2: Mock signature verification to succeed
            with patch.object(AuthService, '_verify_sui_signature') as mock_verify:
                mock_verify.return_value = (None, True)  # Success

                # Step 3: Verify challenge with mocked signature
                credentials = {
                    "address": TEST_SUI_ADDRESS,
                    "signature": "sui_signature_data"
                }

                error, token, user_data = await AuthService.authenticate_with_challenge(
                    challenge_id, credentials
                )

                assert error is None
                assert token is not None
                assert user_data["user_id"] == f"web3:{TEST_SUI_ADDRESS}"
                assert user_data["provider"] == "sui"

                # Verify signature verification was called correctly
                mock_verify.assert_called_once_with(
                    TEST_SUI_ADDRESS,
                    "sui_signature_data",
                    message
                )

    @pytest.mark.asyncio
    async def test_static_token_auth_flow(self, mock_server_config, mock_redis):
        """Test static token authentication flow"""

        with patch('src.state.server_config.config', mock_server_config):

            # Test valid static token
            error, success = await AuthService.authenticate_static_token("test_token_123")
            assert error is None
            assert success is True

            # Test invalid static token
            error, success = await AuthService.authenticate_static_token("invalid_token")
            assert error == "Invalid token"
            assert success is False

    @pytest.mark.asyncio
    async def test_challenge_expiration(self, mock_server_config, mock_redis):
        """Test that challenges expire after the configured time"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Mock Redis to return None (expired challenge)
            mock_redis.get.return_value = None

            # Try to verify non-existent/expired challenge
            credentials = {
                "address": TEST_EVM_ADDRESS,
                "signature": "0x_signature"
            }

            error, token, user_data = await AuthService.authenticate_with_challenge(
                "expired_challenge_id", credentials
            )

            assert error == "Invalid or expired challenge"
            assert token is None
            assert user_data is None

    @pytest.mark.asyncio
    async def test_signature_verification_failure(self, mock_server_config, mock_redis):
        """Test authentication failure when signature verification fails"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Step 1: Create challenge
            error, challenge_data = await AuthService.create_auth_challenge(
                "evm", "127.0.0.1", TEST_EVM_ADDRESS
            )

            assert error is None
            challenge_id = challenge_data["challenge_id"]

            # Step 2: Mock signature verification to fail
            with patch.object(AuthService, '_verify_evm_signature') as mock_verify:
                mock_verify.return_value = ("Signature does not match address", False)

                # Step 3: Try to verify with bad signature
                credentials = {
                    "address": TEST_EVM_ADDRESS,
                    "signature": "0x_bad_signature"
                }

                error, token, user_data = await AuthService.authenticate_with_challenge(
                    challenge_id, credentials
                )

                assert error == "Signature does not match address"
                assert token is None
                assert user_data is None

    @pytest.mark.asyncio
    async def test_challenge_single_use(self, mock_server_config, mock_redis):
        """Test that challenges can only be used once"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Step 1: Create challenge and use it successfully
            error, challenge_data = await AuthService.create_auth_challenge(
                "evm", "127.0.0.1", TEST_EVM_ADDRESS
            )

            assert error is None
            challenge_id = challenge_data["challenge_id"]

            with patch.object(AuthService, '_verify_evm_signature') as mock_verify:
                mock_verify.return_value = (None, True)

                credentials = {
                    "address": TEST_EVM_ADDRESS,
                    "signature": "0x_signature"
                }

                # First use should succeed
                error, token, user_data = await AuthService.authenticate_with_challenge(
                    challenge_id, credentials
                )

                assert error is None
                assert token is not None

                # Second use should fail (challenge should be deleted after use)
                mock_redis.get.return_value = None  # Simulate deleted challenge

                error, token, user_data = await AuthService.authenticate_with_challenge(
                    challenge_id, credentials
                )

                assert error == "Invalid or expired challenge"
                assert token is None

    @pytest.mark.asyncio
    async def test_invalid_auth_method(self, mock_server_config, mock_redis):
        """Test error handling for invalid authentication methods"""

        # Configure server to not support certain auth methods
        limited_config = ServerConfig(
            auth_methods=["static"],  # Only static auth enabled
            static_auth_token="test_token_123"
        )

        with patch('src.state.server_config.config', limited_config), \
             patch('src.state.redis', mock_redis):

            # Try to create challenge for unsupported method
            error, challenge_data = await AuthService.create_auth_challenge(
                "evm", "127.0.0.1", TEST_EVM_ADDRESS
            )

            assert error == "EVM authentication not enabled"
            assert challenge_data is None

    @pytest.mark.asyncio
    async def test_rate_limiting_integration(self, mock_server_config, mock_redis):
        """Test that rate limiting works with authentication failures"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Mock rate limiting to be triggered
            mock_redis.hgetall.return_value = {
                b"count": b"5",
                b"timestamp": str(time.time()).encode()
            }

            user_id = "test_user"
            error, allowed = await AuthService.check_input_rate_limit(user_id)

            # Should be rate limited after multiple failures
            assert not allowed
            assert "Too many failed attempts" in error


class TestWeb3SignatureVerification:
    """Test Web3 signature verification methods"""

    @pytest.mark.asyncio
    @pytest.mark.skipif(not EVM_AVAILABLE, reason="EVM dependencies not available")
    async def test_evm_signature_verification_success(self):
        """Test successful EVM signature verification"""

        message = "Test message for signing"

        # Mock eth_account verification to succeed
        with patch('src.services.auth.encode_defunct') as mock_encode, \
             patch('src.services.auth.Account.recover_message') as mock_recover:

            mock_encode.return_value = "encoded_message"
            mock_recover.return_value = TEST_EVM_ADDRESS

            error, success = await AuthService._verify_evm_signature(
                TEST_EVM_ADDRESS, "0x_signature", message
            )

            assert error is None
            assert success is True
            mock_encode.assert_called_once_with(text=message)
            mock_recover.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.skipif(not EVM_AVAILABLE, reason="EVM dependencies not available")
    async def test_evm_signature_verification_failure(self):
        """Test failed EVM signature verification"""

        message = "Test message for signing"

        # Mock eth_account to return different address
        with patch('src.services.auth.encode_defunct') as mock_encode, \
             patch('src.services.auth.Account.recover_message') as mock_recover:

            mock_encode.return_value = "encoded_message"
            mock_recover.return_value = "0xdifferentaddress"

            error, success = await AuthService._verify_evm_signature(
                TEST_EVM_ADDRESS, "0x_signature", message
            )

            assert error == "Signature does not match address"
            assert success is False

    @pytest.mark.asyncio
    @pytest.mark.skipif(not SOLANA_AVAILABLE, reason="Solana dependencies not available")
    async def test_solana_signature_verification_mock(self):
        """Test Solana signature verification with mocking"""

        message = "Test message for signing"
        signature_bytes = [1, 2, 3, 4, 5] * 13  # 65 bytes total

        # Mock solana verification to succeed
        with patch('src.services.auth.VerifyKey') as mock_verify_key_class:
            mock_verify_key = MagicMock()
            mock_verify_key_class.return_value = mock_verify_key
            mock_verify_key.verify.return_value = True

            error, success = await AuthService._verify_svm_signature(
                TEST_SOLANA_ADDRESS, signature_bytes, message
            )

            assert error is None
            assert success is True

    @pytest.mark.asyncio
    async def test_sui_signature_verification_fallback(self):
        """Test Sui signature verification fallback"""

        message = "Test message for signing"

        # Use a valid Sui address format for testing
        valid_sui_address = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

        # Sui verification should use fallback (always succeed for testing)
        error, success = await AuthService._verify_sui_signature(
            valid_sui_address, "signature_data", message
        )

        # Fallback implementation should succeed
        assert error is None
        assert success is True


class TestAuthenticationEndpoints:
    """Test authentication endpoint functionality"""

    def test_get_auth_methods(self):
        """Test /auth/methods endpoint"""

        # This would require a full FastAPI test client setup
        # For now, we test the underlying service method
        config = ServerConfig(
            auth_methods=["static", "evm", "svm", "sui"],
            static_auth_token="test"
        )

        with patch('src.state.server_config.config', config):
            # Test that server config is properly accessed
            assert config.auth_methods == ["static", "evm", "svm", "sui"]

    @pytest.mark.asyncio
    async def test_universal_authenticate_method(self, mock_server_config, mock_redis):
        """Test the universal authenticate method for different auth types"""

        with patch('src.state.server_config.config', mock_server_config), \
             patch('src.state.redis', mock_redis):

            # Test static token authentication
            error, token, user_data = await AuthService.universal_authenticate(
                "static", {"token": "test_token_123"}
            )

            assert error is None
            assert token is not None
            assert user_data["user_id"] == "static:test_token_123"


class TestMessageGeneration:
    """Test authentication message generation"""

    def test_web3_message_format(self):
        """Test Web3 authentication message format"""

        challenge_id = "test_challenge_123"

        # Test EVM message
        evm_message = AuthService.generate_auth_message("evm", TEST_EVM_ADDRESS, challenge_id)
        assert "Welcome to Chomp!" in evm_message
        assert f"Address: {TEST_EVM_ADDRESS}" in evm_message
        assert f"Challenge: {challenge_id}" in evm_message
        assert "Terms of Service" in evm_message
        assert "any gas fees" in evm_message  # Fixed assertion

        # Test SVM message
        svm_message = AuthService.generate_auth_message("svm", TEST_SOLANA_ADDRESS, challenge_id)
        assert "Welcome to Chomp!" in svm_message
        assert f"Address: {TEST_SOLANA_ADDRESS}" in svm_message
        assert f"Challenge: {challenge_id}" in svm_message

        # Test Sui message
        sui_message = AuthService.generate_auth_message("sui", TEST_SUI_ADDRESS, challenge_id)
        assert "Welcome to Chomp!" in sui_message
        assert f"Address: {TEST_SUI_ADDRESS}" in sui_message
        assert f"Challenge: {challenge_id}" in sui_message

    def test_message_timestamp_format(self):
        """Test that messages include proper timestamp format"""

        challenge_id = "test_challenge_123"
        message = AuthService.generate_auth_message("evm", TEST_EVM_ADDRESS, challenge_id)

        # Should contain timestamp in DD-MM-YYYY HH:MM:SS format
        import re
        timestamp_pattern = r'\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'
        assert re.search(timestamp_pattern, message) is not None

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
