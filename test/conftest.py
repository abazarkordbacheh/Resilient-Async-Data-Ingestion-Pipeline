"""
Shared fixtures for pytest test suite.
Provides common test utilities, mock server client, and sample data.
"""
# endregion Import Modules...
import httpx
import pytest
import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

from src.connection.token_manager import TokenManager
from src.connection.db_handeler import SQLServerConnection
from pipline.retry_worker import RetryWorker
# endregion


# region Pytest Configuration
pytest_plugins = ["pytest_asyncio"]


@pytest.fixture(scope="session")
def event_loop_policy():
    """
    Use the default event loop policy for async tests.
    """
    return asyncio.DefaultEventLoopPolicy()


# endregion


# region Mock Fixtures
@pytest.fixture
def sample_token() -> str:
    """
    Provides a sample JWT token for testing.
    """
    return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.token"


@pytest.fixture
def sample_batch_data() -> dict:
    """
    Provides sample batch response data from the API.
    """
    return {
        "batch_id": 123,
        "records": [
            {"id": 1, "name": "Record 1", "value": 100},
            {"id": 2, "name": "Record 2", "value": 200},
            {"id": 3, "name": "Record 3", "value": 300},
        ],
    }


@pytest.fixture
def sample_empty_batch() -> dict:
    """
    Provides an empty batch response (end of stream).
    """
    return {
        "batch_id": None,
        "records": [],
    }


@pytest.fixture
def sample_lag_response() -> dict:
    """
    Provides sample lag monitoring response.
    """
    return {
        "lag": 1500,
    }


# endregion


# region HTTP Client Fixtures
@pytest.fixture
async def mock_http_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """
    Provides an httpx.AsyncClient for testing HTTP requests.
    Uses AsyncMock to avoid actual network calls.
    """
    async with httpx.AsyncClient() as client:
        yield client


@pytest.fixture
def mock_http_response():
    """
    Provides a factory for creating mock httpx.Response objects.
    """
    def _create_response(status_code: int, json_data: dict = None):
        response = MagicMock(spec=httpx.Response)
        response.status_code = status_code
        response.json.return_value = json_data or {}
        response.text = str(json_data) if json_data else ""
        return response
    return _create_response


# endregion


# region Token Manager Fixtures
@pytest.fixture
def token_manager() -> TokenManager:
    """
    Provides a TokenManager instance with test credentials.
    """
    return TokenManager(
        base_url="http://test-api.example.com",
        username="test_user",
        password="test_password",
        token_ttl_seconds=300,
    )


@pytest.fixture
def expired_token_manager() -> TokenManager:
    """
    Provides a TokenManager with an expired token for testing refresh logic.
    """
    tm = TokenManager(
        base_url="http://test-api.example.com",
        username="test_user",
        password="test_password",
        token_ttl_seconds=1,  # Very short TTL
    )
    # Manually set an expired token
    tm._token = "expired.token"
    tm._fetched_at = 0.0  # Epoch time, definitely expired
    return tm


# endregion


# region Database Fixtures
@pytest.fixture
def mock_db_connection():
    """
    Provides a mock SQLServerConnection for testing.
    """
    db = MagicMock(spec=SQLServerConnection)
    db.insert_batch = AsyncMock()
    db.connect = MagicMock()
    db.disconnect = MagicMock()
    return db


# endregion


# region Retry Worker Fixtures
@pytest.fixture
def retry_worker():
    """
    Provides a RetryWorker instance with mocked dependencies.
    """
    token_getter = AsyncMock(return_value="test.token")
    insert_func = AsyncMock()
    
    worker = RetryWorker(
        base_url="http://test-api.example.com",
        token_getter=token_getter,
        insert_func=insert_func,
        max_attempts=3,
        base_delay=0.1,  # Short delay for testing
    )
    return worker


# endregion


# region Mock Server Fixtures
@pytest.fixture
def mock_server_url() -> str:
    """
    Provides the base URL for the mock server.
    """
    return "http://localhost:8000"


# endregion
