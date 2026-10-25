# ------------------------ Modules ------------------------
# Libraries =>
import asyncio
import pytest
import httpx
from unittest.mock import AsyncMock
# Modules =>
from pipeline.retry_worker import RetryWorker

# ------------------------ Test ------------------------
@pytest.mark.asyncio
async def test_retry_worker_success_first_attempt():
    """Test successful recovery of a failed batch on the first try."""
    requests_log = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_log.append(request.url.path)
        if request.url.path == "/resettobatchid/":
            return httpx.Response(200, json={"status": "ok"})
        elif request.url.path == "/getnextbatch/":
            return httpx.Response(200, json={"batch_id": 99, "records": [{"id": 10}]})
        return httpx.Response(404)

    mock_insert = AsyncMock()
    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        worker = RetryWorker(
            base_url="http://testserver",
            token_getter=AsyncMock(return_value="test-token"),
            insert_func=mock_insert,
            max_attempts=3,
            base_delay=0.01
        )

        await worker.enqueue(99)
        # Run worker for one task and cancel immediately after
        task = asyncio.create_task(worker.run_forever(client))

        # Wait until queue is fully processed
        await worker.queue.join()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert "/resettobatchid/" in requests_log
        assert "/getnextbatch/" in requests_log
        mock_insert.assert_awaited_once_with([{"id": 10}])


@pytest.mark.asyncio
async def test_retry_worker_exhausted_attempts(monkeypatch):
    """Test that RetryWorker raises and logs permanent failure after max attempts."""
    # Speed up sleeps during test
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    mock_insert = AsyncMock()
    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        worker = RetryWorker(
            base_url="http://testserver",
            token_getter=AsyncMock(return_value="test-token"),
            insert_func=mock_insert,
            max_attempts=2,
            base_delay=0.1
        )

        await worker.enqueue(123)
        task = asyncio.create_task(worker.run_forever(client))

        await worker.queue.join()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # insert_func should not be called because HTTP requests failed
        mock_insert.assert_not_awaited()
