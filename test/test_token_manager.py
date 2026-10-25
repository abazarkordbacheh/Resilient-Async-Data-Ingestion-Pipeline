# ------------------------ Modules ------------------------
# Libraries=>
import asyncio
import time
import pytest
import httpx
# Modules=>
from src.connection.token_manager import TokenManager

# ------------------------ test ------------------------

@pytest.mark.asyncio
async def test_token_manager_happy_path():
    """Test successful login and token caching."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/jwt_login/"
        return httpx.Response(200, json={"access": "fake-jwt-token-123"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        manager = TokenManager("http://testserver", "admin", "secret", token_ttl_seconds=60)

        # First call should trigger login
        token = await manager.get_valid_token(client)
        assert token == "fake-jwt-token-123"
        assert manager._is_valid() is True


@pytest.mark.asyncio
async def test_token_manager_cache_reuse(monkeypatch):
    """Test that valid cached token is reused without making new HTTP requests."""
    login_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal login_count
        login_count += 1
        return httpx.Response(200, json={"access": f"token-{login_count}"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        manager = TokenManager("http://testserver", "admin", "secret", token_ttl_seconds=60)

        t1 = await manager.get_valid_token(client)
        t2 = await manager.get_valid_token(client)

        assert t1 == "token-1"
        assert t2 == "token-1"
        assert login_count == 1  # Only one login request made


@pytest.mark.asyncio
async def test_token_manager_expiration(monkeypatch):
    """Test token expiration and re-login when TTL passes."""
    login_count = 0
    current_time = 1000.0

    monkeypatch.setattr(time, "time", lambda: current_time)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal login_count
        login_count += 1
        return httpx.Response(200, json={"access": f"token-{login_count}"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        manager = TokenManager("http://testserver", "admin", "secret", token_ttl_seconds=10)

        # Initial login at t=1000
        t1 = await manager.get_valid_token(client)
        assert t1 == "token-1"

        # Advance time past TTL (1000 + 15 > 10)
        current_time = 1015.0
        t2 = await manager.get_valid_token(client)

        assert t2 == "token-2"
        assert login_count == 2


@pytest.mark.asyncio
async def test_token_manager_login_failure():
    """Test handling of non-200 HTTP status code during login."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="Unauthorized")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        manager = TokenManager("http://testserver", "admin", "wrong", token_ttl_seconds=60)

        with pytest.raises(RuntimeError, match="jwt_login failed with status 401"):
            await manager.get_valid_token(client)


@pytest.mark.asyncio
async def test_token_manager_missing_access_key():
    """Test handling of 200 OK response lacking 'access' key."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"token": "wrong-key-name"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        manager = TokenManager("http://testserver", "admin", "secret", token_ttl_seconds=60)

        with pytest.raises(RuntimeError, match="missing 'access' key"):
            await manager.get_valid_token(client)


@pytest.mark.asyncio
async def test_token_manager_concurrency():
    """Test concurrent token requests to ensure double-checked locking prevents duplicate logins."""
    login_count = 0

    async def async_handler(request: httpx.Request) -> httpx.Response:
        nonlocal login_count
        await asyncio.sleep(0.05)  # Simulate network latency
        login_count += 1
        return httpx.Response(200, json={"access": "concurrent-token"})

    transport = httpx.ASGITransport(app=None)  # fallback to custom transport via AsyncClient

    # Using MockTransport with async callback
    class AsyncMockTransport(httpx.BaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return await async_handler(request)

    client = httpx.AsyncClient(transport=AsyncMockTransport(), base_url="http://testserver")
    manager = TokenManager("http://testserver", "admin", "secret", token_ttl_seconds=60)

    # Fire 10 concurrent requests
    results = await asyncio.gather(*(manager.get_valid_token(client) for _ in range(10)))

    await client.aclose()

    assert all(t == "concurrent-token" for t in results)
    assert login_count == 1  # Lock ensured login was called exactly once
