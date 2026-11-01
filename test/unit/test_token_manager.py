"""
Unit tests for TokenManager.
Tests token caching, TTL expiration, and async lock mechanism.
"""
# endregion Import Modules...
import time
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.connection.token_manager import TokenManager
# endregion


class TestTokenManagerInitialization:
    """Test TokenManager initialization and configuration."""

    def test_token_manager_initialization(self):
        """Test that TokenManager initializes with correct attributes."""
        tm = TokenManager(
            base_url="http://test-api.example.com/",
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )
        
        assert tm.base_url == "http://test-api.example.com"
        assert tm.username == "test_user"
        assert tm._password == "test_password"
        assert tm._ttl == 300
        assert tm._token is None
        assert tm._fetched_at == 0.0
        assert isinstance(tm._lock, asyncio.Lock)


class TestTokenValidation:
    """Test token validity checking logic."""

    def test_is_valid_returns_false_when_token_is_none(self, token_manager):
        """Test that _is_valid returns False when no token is cached."""
        assert token_manager._is_valid() is False

    def test_is_valid_returns_false_when_token_is_expired(self, token_manager):
        """Test that _is_valid returns False when token is past TTL."""
        token_manager._token = "valid.token"
        token_manager._fetched_at = time.time() - 1000  # 1000 seconds ago
        token_manager._ttl = 300  # 300 second TTL
        
        assert token_manager._is_valid() is False

    def test_is_valid_returns_true_when_token_is_within_ttl(self, token_manager):
        """Test that _is_valid returns True when token is within TTL."""
        token_manager._token = "valid.token"
        token_manager._fetched_at = time.time() - 100  # 100 seconds ago
        token_manager._ttl = 300  # 300 second TTL
        
        assert token_manager._is_valid() is True


class TestTokenCache:
    """Test token caching behavior."""

    @pytest.mark.asyncio
    async def test_get_valid_token_returns_cached_token_when_valid(
        self, token_manager, sample_token
    ):
        """Test that cached token is returned without API call when valid."""
        token_manager._token = sample_token
        token_manager._fetched_at = time.time() - 100
        token_manager._ttl = 300
        
        # Mock client should not be called
        mock_client = MagicMock(spec=MagicMock)
        
        result = await token_manager.get_valid_token(mock_client)
        
        assert result == sample_token
        # Verify _login was not called (no network request)

    @pytest.mark.asyncio
    async def test_token_refresh_when_expired(self, token_manager, sample_token):
        """Test that token is refreshed when expired."""
        token_manager._token = "expired.token"
        token_manager._fetched_at = time.time() - 1000
        token_manager._ttl = 300
        
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access": sample_token}
        mock_client.post.return_value = mock_response
        
        result = await token_manager.get_valid_token(mock_client)
        
        assert result == sample_token
        assert token_manager._token == sample_token
        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_refresh_when_none(self, token_manager, sample_token):
        """Test that token is fetched when no token exists."""
        token_manager._token = None
        
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access": sample_token}
        mock_client.post.return_value = mock_response
        
        result = await token_manager.get_valid_token(mock_client)
        
        assert result == sample_token
        assert token_manager._token == sample_token


class TestLockMechanism:
    """Test async lock mechanism for thread-safe token refresh."""

    @pytest.mark.asyncio
    async def test_double_checked_locking_prevents_duplicate_login(
        self, token_manager, sample_token
    ):
        """Test that double-checked locking prevents concurrent login requests."""
        token_manager._token = None
        
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access": sample_token}
        mock_client.post.return_value = mock_response
        
        # Create multiple concurrent tasks
        tasks = [
            token_manager.get_valid_token(mock_client) for _ in range(5)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All tasks should return the same token
        assert all(result == sample_token for result in results)
        
        # Login should only be called once despite 5 concurrent requests
        assert mock_client.post.call_count == 1

    @pytest.mark.asyncio
    async def test_lock_is_released_after_token_refresh(
        self, token_manager, sample_token
    ):
        """Test that lock is released after token refresh completes."""
        token_manager._token = None
        
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access": sample_token}
        mock_client.post.return_value = mock_response
        
        await token_manager.get_valid_token(mock_client)
        
        # Lock should be free to acquire again
        assert token_manager._lock.locked() is False


class TestLoginMethod:
    """Test the _login method behavior."""

    @pytest.mark.asyncio
    async def test_login_success(self, token_manager, sample_token):
        """Test successful login and token storage."""
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access": sample_token}
        mock_client.post.return_value = mock_response
        
        await token_manager._login(mock_client)
        
        assert token_manager._token == sample_token
        assert token_manager._fetched_at > 0
        mock_client.post.assert_called_once_with(
            f"{token_manager.base_url}/jwt_login/",
            json={"username": "test_user", "password": "test_password"},
            timeout=10.0,
        )

    @pytest.mark.asyncio
    async def test_login_raises_on_non_200_status(self, token_manager):
        """Test that RuntimeError is raised on non-200 status code."""
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_client.post.return_value = mock_response
        
        with pytest.raises(RuntimeError) as exc_info:
            await token_manager._login(mock_client)
        
        assert "jwt_login failed: 401" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_login_raises_on_missing_access_token(self, token_manager):
        """Test that RuntimeError is raised when response lacks 'access' field."""
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "invalid credentials"}
        mock_client.post.return_value = mock_response
        
        with pytest.raises(RuntimeError) as exc_info:
            await token_manager._login(mock_client)
        
        assert "missing 'access' in response" in str(exc_info.value)


class TestTokenExpirationEdgeCases:
    """Test edge cases around token expiration."""

    @pytest.mark.asyncio
    async def test_token_exactly_at_ttl_boundary(self, token_manager, sample_token):
        """Test token validation exactly at TTL boundary."""
        token_manager._token = sample_token
        token_manager._fetched_at = time.time() - 300
        token_manager._ttl = 300
        
        # At exact TTL boundary, should be considered expired
        assert token_manager._is_valid() is False

    @pytest.mark.asyncio
    async def test_token_just_before_ttl_boundary(self, token_manager, sample_token):
        """Test token validation just before TTL boundary."""
        token_manager._token = sample_token
        token_manager._fetched_at = time.time() - 299
        token_manager._ttl = 300
        
        # Just before TTL boundary, should be valid
        assert token_manager._is_valid() is True
