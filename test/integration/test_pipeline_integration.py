"""
Integration tests for the pipeline with mock server.
Tests end-to-end interaction between Pipeline, RetryWorker, and Mock Server.
"""
# endregion Import Modules...import pytest
import os
import sys
import time
import httpx
import pytest
import asyncio
import subprocess
from unittest.mock import AsyncMock, patch

# Add test directory to path for mock_server import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.connection.token_manager import TokenManager
from src.connection.db_handeler import SQLServerConnection
from pipline.retry_worker import RetryWorker
from mock_server import app, reset_state


# endregion

# region Mock Server Fixture
@pytest.fixture(scope="session")
def mock_server():
    """
    Start the mock FastAPI server for integration testing.
    Runs on http://localhost:8000
    """
    # Start the mock server in a subprocess
    server_process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "test.mock_server:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to start
    time.sleep(2)

    yield "http://127.0.0.1:8000"

    # Cleanup: terminate server
    server_process.terminate()
    server_process.wait(timeout=5)


# endregion


# region Integration Tests
class TestTokenManagerWithMockServer:
    """Integration tests for TokenManager with mock server."""

    @pytest.mark.asyncio
    async def test_token_manager_authenticates_with_mock_server(self, mock_server):
        """Test that TokenManager successfully authenticates with mock server."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            token = await token_mgr.get_valid_token(client)

            assert token == "mock.jwt.token"
            assert token_mgr._token == token

    @pytest.mark.asyncio
    async def test_token_manager_handles_invalid_credentials(self, mock_server):
        """Test that TokenManager handles invalid credentials from mock server."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="invalid_user",
            password="wrong_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            with pytest.raises(RuntimeError) as exc_info:
                await token_mgr.get_valid_token(client)

            assert "jwt_login failed: 401" in str(exc_info.value)


class TestRetryWorkerWithMockServer:
    """Integration tests for RetryWorker with mock server."""

    @pytest.mark.asyncio
    async def test_retry_worker_resets_pointer_and_refetches(self, mock_server):
        """Test that RetryWorker successfully resets pointer and refetches from mock server."""
        reset_state()

        # Setup environment variables for retry worker
        os.environ["RESET_URL"] = f"{mock_server}/reset_pointer/"
        os.environ["FETCH_URL"] = f"{mock_server}/getnextbatch/"

        token_getter = AsyncMock(return_value="mock.jwt.token")
        insert_func = AsyncMock()

        retry_worker = RetryWorker(
            base_url=mock_server,
            token_getter=token_getter,
            insert_func=insert_func,
            max_attempts=3,
            base_delay=0.1,
        )

        async with httpx.AsyncClient() as client:
            # Process batch_id 1
            await retry_worker._process_one(client, batch_id=1)

            # Verify insert was called
            insert_func.assert_called_once()

            # Verify records were fetched
            call_args = insert_func.call_args
            records = call_args[0][0]
            assert len(records) > 0

    @pytest.mark.asyncio
    async def test_retry_worker_handles_mock_server_errors(self, mock_server):
        """Test that RetryWorker handles errors from mock server with backoff."""
        reset_state()

        os.environ["RESET_URL"] = f"{mock_server}/reset_pointer/"
        os.environ["FETCH_URL"] = f"{mock_server}/getnextbatch/"

        token_getter = AsyncMock(return_value="mock.jwt.token")
        insert_func = AsyncMock()

        retry_worker = RetryWorker(
            base_url=mock_server,
            token_getter=token_getter,
            insert_func=insert_func,
            max_attempts=2,
            base_delay=0.1,
        )

        async with httpx.AsyncClient() as client:
            # Use invalid batch_id to trigger error
            with pytest.raises(RuntimeError):
                await retry_worker._process_one(client, batch_id=999)


class TestPipelineEndToEnd:
    """End-to-end integration tests for the pipeline."""

    @pytest.mark.asyncio
    async def test_pipeline_fetches_batches_from_mock_server(self, mock_server):
        """Test that pipeline successfully fetches batches from mock server."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            # Fetch first batch
            token = await token_mgr.get_valid_token(client)
            headers = {"Authorization": f"Bearer {token}"}
            response = await client.get(
                f"{mock_server}/getnextbatch/?count=2",
                headers=headers,
                timeout=30.0,
            )

            assert response.status_code == 200
            data = response.json()
            assert "batch_id" in data
            assert "records" in data
            assert len(data["records"]) == 2

    @pytest.mark.asyncio
    async def test_pipeline_handles_empty_response_from_mock_server(self, mock_server):
        """Test that pipeline handles empty batch response (end of stream)."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            # Fetch all batches until empty
            token = await token_mgr.get_valid_token(client)
            headers = {"Authorization": f"Bearer {token}"}

            batch_count = 0
            while True:
                response = await client.get(
                    f"{mock_server}/getnextbatch/?count=2",
                    headers=headers,
                    timeout=30.0,
                )

                data = response.json()
                if not data.get("records"):
                    break

                batch_count += 1
                if batch_count > 10:  # Safety limit
                    break

            assert batch_count >= 2  # Should have fetched at least 2 batches

    @pytest.mark.asyncio
    async def test_pipeline_lag_monitoring_with_mock_server(self, mock_server):
        """Test that lag monitoring works with mock server."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            token = await token_mgr.get_valid_token(client)
            headers = {"Authorization": f"Bearer {token}"}

            # Get lag
            response = await client.post(
                f"{mock_server}/getlag/",
                headers=headers,
                json={"function": "getnextbatch"},
                timeout=30.0,
            )

            assert response.status_code == 200
            data = response.json()
            assert "lag" in data
            assert isinstance(data["lag"], int)


class TestQueueHandling:
    """Test queue handling in integration context."""

    @pytest.mark.asyncio
    async def test_retry_queue_integration_with_mock_server(self, mock_server):
        """Test that failed batches are queued and retried with mock server."""
        reset_state()

        os.environ["RESET_URL"] = f"{mock_server}/reset_pointer/"
        os.environ["FETCH_URL"] = f"{mock_server}/getnextbatch/"

        token_getter = AsyncMock(return_value="mock.jwt.token")

        # Mock insert to fail first time, succeed second time
        insert_call_count = 0

        async def mock_insert(records):
            nonlocal insert_call_count
            insert_call_count += 1
            if insert_call_count == 1:
                raise Exception("Database error")

        insert_func = AsyncMock(side_effect=mock_insert)

        retry_worker = RetryWorker(
            base_url=mock_server,
            token_getter=token_getter,
            insert_func=insert_func,
            max_attempts=3,
            base_delay=0.1,
        )

        async with httpx.AsyncClient() as client:
            # Enqueue a failed batch
            await retry_worker.enqueue(1)

            # Process it (will fail first attempt)
            with pytest.raises(RuntimeError):
                await retry_worker._process_one(client, batch_id=1)


class TestConcurrentOperations:
    """Test concurrent operations with mock server."""

    @pytest.mark.asyncio
    async def test_concurrent_token_requests_with_mock_server(self, mock_server):
        """Test that concurrent token requests are handled correctly."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            # Make concurrent requests
            tasks = [token_mgr.get_valid_token(client) for _ in range(5)]
            tokens = await asyncio.gather(*tasks)

            # All should return the same token
            assert all(token == "mock.jwt.token" for token in tokens)

    @pytest.mark.asyncio
    async def test_concurrent_batch_fetches_with_mock_server(self, mock_server):
        """Test that concurrent batch fetches work correctly."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async def fetch_batch(client):
            token = await token_mgr.get_valid_token(client)
            headers = {"Authorization": f"Bearer {token}"}
            response = await client.get(
                f"{mock_server}/getnextbatch/?count=2",
                headers=headers,
                timeout=30.0,
            )
            return response.json()

        async with httpx.AsyncClient() as client:
            # Fetch batches concurrently
            tasks = [fetch_batch(client) for _ in range(3)]
            results = await asyncio.gather(*tasks)

            # All should succeed
            assert all("batch_id" in result for result in results)


class TestErrorScenarios:
    """Test error scenarios with mock server."""

    @pytest.mark.asyncio
    async def test_pipeline_handles_mock_server_unavailability(self, mock_server):
        """Test that pipeline handles mock server being unavailable."""
        reset_state()

        token_mgr = TokenManager(
            base_url="http://localhost:9999",  # Wrong port
            username="test_user",
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            with pytest.raises(RuntimeError):
                await token_mgr.get_valid_token(client)

    @pytest.mark.asyncio
    async def test_pipeline_handles_mock_server_500_errors(self, mock_server):
        """Test that pipeline handles 500 errors from mock server."""
        reset_state()

        token_mgr = TokenManager(
            base_url=mock_server,
            username="invalid_user",  # Triggers 500
            password="test_password",
            token_ttl_seconds=300,
        )

        async with httpx.AsyncClient() as client:
            with pytest.raises(RuntimeError) as exc_info:
                await token_mgr.get_valid_token(client)

            assert "jwt_login failed: 500" in str(exc_info.value)

# endregion
