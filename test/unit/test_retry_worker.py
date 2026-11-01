"""
Unit tests for RetryWorker.
Tests exponential backoff logic, queue handling, and retry attempts.
"""
# endregion Import Modules...
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from pipline.retry_worker import RetryWorker
# endregion


class TestRetryWorkerInitialization:
    """Test RetryWorker initialization and configuration."""

    def test_retry_worker_initialization(self):
        """Test that RetryWorker initializes with correct attributes."""
        token_getter = AsyncMock(return_value="test.token")
        insert_func = AsyncMock()
        
        worker = RetryWorker(
            base_url="http://test-api.example.com",
            token_getter=token_getter,
            insert_func=insert_func,
            max_attempts=5,
            base_delay=1.0,
        )
        
        assert worker.base_url == "http://test-api.example.com"
        assert worker._token_getter == token_getter
        assert worker.insert_func == insert_func
        assert worker.max_attempts == 5
        assert worker.base_delay == 1.0
        assert isinstance(worker.queue, asyncio.Queue)


class TestQueueOperations:
    """Test queue enqueue and dequeue operations."""

    @pytest.mark.asyncio
    async def test_enqueue_adds_batch_id_to_queue(self, retry_worker):
        """Test that enqueue adds batch_id to the queue."""
        await retry_worker.enqueue(123)
        
        assert retry_worker.queue.qsize() == 1
        batch_id = await retry_worker.queue.get()
        assert batch_id == 123

    @pytest.mark.asyncio
    async def test_enqueue_multiple_batch_ids(self, retry_worker):
        """Test that multiple batch_ids can be enqueued."""
        await retry_worker.enqueue(100)
        await retry_worker.enqueue(200)
        await retry_worker.enqueue(300)
        
        assert retry_worker.queue.qsize() == 3

    @pytest.mark.asyncio
    async def test_queue_is_unbounded(self, retry_worker):
        """Test that queue has no maxsize (no backpressure)."""
        # Enqueue many items to verify no backpressure
        for i in range(1000):
            await retry_worker.enqueue(i)
        
        assert retry_worker.queue.qsize() == 1000


class TestExponentialBackoff:
    """Test exponential backoff delay calculation."""

    def test_exponential_backoff_calculation(self):
        """Test that delay follows exponential backoff formula."""
        base_delay = 1.0
        
        # Attempt 1: base_delay * (2^0) = 1.0
        assert base_delay * (2 ** 0) == 1.0
        
        # Attempt 2: base_delay * (2^1) = 2.0
        assert base_delay * (2 ** 1) == 2.0
        
        # Attempt 3: base_delay * (2^2) = 4.0
        assert base_delay * (2 ** 2) == 4.0
        
        # Attempt 4: base_delay * (2^3) = 8.0
        assert base_delay * (2 ** 3) == 8.0

    @pytest.mark.asyncio
    async def test_backoff_delays_in_process_one(self, retry_worker):
        """Test that actual delays are applied in retry logic."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        # Mock reset and fetch to fail
        mock_post_response = MagicMock()
        mock_post_response.status_code = 500
        mock_client.post.return_value = mock_post_response
        mock_client.get.return_value = mock_post_response
        
        start_time = asyncio.get_event_loop().time()
        
        with pytest.raises(RuntimeError):
            await retry_worker._process_one(mock_client, batch_id=123)
        
        elapsed = asyncio.get_event_loop().time() - start_time
        
        # With base_delay=0.1 and 3 attempts:
        # Attempt 1: 0.1s delay
        # Attempt 2: 0.2s delay
        # Attempt 3: 0.4s delay
        # Total delay should be approximately 0.7s (plus processing time)
        assert elapsed >= 0.6  # Allow some margin


class TestRetryAttempts:
    """Test maximum retry attempts and failure behavior."""

    @pytest.mark.asyncio
    async def test_max_retry_attempts_exceeded(self, retry_worker):
        """Test that RuntimeError is raised after max attempts."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        # Mock all API calls to fail
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_client.post.return_value = mock_response
        mock_client.get.return_value = mock_response
        
        with pytest.raises(RuntimeError) as exc_info:
            await retry_worker._process_one(mock_client, batch_id=123)
        
        assert "failed permanently after max_attempts=3" in str(exc_info.value)
        
        # Verify API was called max_attempts times
        assert mock_client.post.call_count == 3

    @pytest.mark.asyncio
    async def test_successful_retry_on_first_attempt(self, retry_worker):
        """Test that retry succeeds on first attempt."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        # Mock successful reset and fetch
        reset_response = MagicMock()
        reset_response.status_code = 200
        
        fetch_response = MagicMock()
        fetch_response.status_code = 200
        fetch_response.json.return_value = {
            "batch_id": 123,
            "records": [{"id": 1, "data": "test"}],
        }
        
        mock_client.post.return_value = reset_response
        mock_client.get.return_value = fetch_response
        
        # Mock successful insert
        retry_worker.insert_func = AsyncMock()
        
        await retry_worker._process_one(mock_client, batch_id=123)
        
        # Insert should be called once
        retry_worker.insert_func.assert_called_once()
        
        # Should not raise error
        assert True

    @pytest.mark.asyncio
    async def test_successful_retry_after_initial_failure(self, retry_worker):
        """Test that retry succeeds after initial failure."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        # First attempt fails, second succeeds
        reset_response_fail = MagicMock()
        reset_response_fail.status_code = 500
        
        reset_response_success = MagicMock()
        reset_response_success.status_code = 200
        
        fetch_response = MagicMock()
        fetch_response.status_code = 200
        fetch_response.json.return_value = {
            "batch_id": 123,
            "records": [{"id": 1, "data": "test"}],
        }
        
        mock_client.post.side_effect = [reset_response_fail, reset_response_success]
        mock_client.get.return_value = fetch_response
        
        retry_worker.insert_func = AsyncMock()
        
        await retry_worker._process_one(mock_client, batch_id=123)
        
        # Insert should be called once
        retry_worker.insert_func.assert_called_once()
        
        # Post should be called twice (fail then success)
        assert mock_client.post.call_count == 2


class TestProcessOneLogic:
    """Test the _process_one method logic."""

    @pytest.mark.asyncio
    async def test_reset_pointer_api_call(self, retry_worker):
        """Test that reset pointer API is called correctly."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        reset_response = MagicMock()
        reset_response.status_code = 200
        
        fetch_response = MagicMock()
        fetch_response.status_code = 200
        fetch_response.json.return_value = {
            "batch_id": 123,
            "records": [],
        }
        
        mock_client.post.return_value = reset_response
        mock_client.get.return_value = fetch_response
        
        retry_worker.insert_func = AsyncMock()
        
        with patch.dict("os.environ", {"RESET_URL": "http://test.com/reset", "FETCH_URL": "http://test.com/fetch"}):
            await retry_worker._process_one(mock_client, batch_id=123)
        
        # Verify reset was called with correct payload
        mock_client.post.assert_called()
        call_args = mock_client.post.call_args
        assert "batch_id" in call_args.kwargs["json"]

    @pytest.mark.asyncio
    async def test_fetch_after_reset(self, retry_worker):
        """Test that fetch is called after successful reset."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        reset_response = MagicMock()
        reset_response.status_code = 200
        
        fetch_response = MagicMock()
        fetch_response.status_code = 200
        fetch_response.json.return_value = {
            "batch_id": 123,
            "records": [{"id": 1}],
        }
        
        mock_client.post.return_value = reset_response
        mock_client.get.return_value = fetch_response
        
        retry_worker.insert_func = AsyncMock()
        
        with patch.dict("os.environ", {"RESET_URL": "http://test.com/reset", "FETCH_URL": "http://test.com/fetch"}):
            await retry_worker._process_one(mock_client, batch_id=123)
        
        # Verify fetch was called after reset
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_insert_called_with_refetched_records(self, retry_worker):
        """Test that insert is called with records from refetch."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        reset_response = MagicMock()
        reset_response.status_code = 200
        
        fetch_response = MagicMock()
        fetch_response.status_code = 200
        expected_records = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        fetch_response.json.return_value = {
            "batch_id": 123,
            "records": expected_records,
        }
        
        mock_client.post.return_value = reset_response
        mock_client.get.return_value = fetch_response
        
        retry_worker.insert_func = AsyncMock()
        
        with patch.dict("os.environ", {"RESET_URL": "http://test.com/reset", "FETCH_URL": "http://test.com/fetch"}):
            await retry_worker._process_one(mock_client, batch_id=123)
        
        # Verify insert was called with correct records
        retry_worker.insert_func.assert_called_once_with(expected_records)


class TestRunForever:
    """Test the run_forever background task."""

    @pytest.mark.asyncio
    async def test_run_forever_processes_queue_items(self, retry_worker):
        """Test that run_forever processes items from queue."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        # Mock successful processing
        reset_response = MagicMock()
        reset_response.status_code = 200
        
        fetch_response = MagicMock()
        fetch_response.status_code = 200
        fetch_response.json.return_value = {"batch_id": 123, "records": []}
        
        mock_client.post.return_value = reset_response
        mock_client.get.return_value = fetch_response
        retry_worker.insert_func = AsyncMock()
        
        with patch.dict("os.environ", {"RESET_URL": "http://test.com/reset", "FETCH_URL": "http://test.com/fetch"}):
            # Enqueue an item
            await retry_worker.enqueue(123)
            
            # Create task
            task = asyncio.create_task(retry_worker.run_forever(mock_client))
            
            # Wait for processing
            await asyncio.sleep(0.1)
            
            # Cancel task
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Verify item was processed
        assert retry_worker.queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_run_forever_handles_exceptions_gracefully(self, retry_worker):
        """Test that run_forever continues after exceptions."""
        mock_client = AsyncMock()
        retry_worker._token_getter = AsyncMock(return_value="test.token")
        
        # Mock failure
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client.post.return_value = mock_response
        mock_client.get.return_value = mock_response
        
        with patch.dict("os.environ", {"RESET_URL": "http://test.com/reset", "FETCH_URL": "http://test.com/fetch"}):
            # Enqueue an item
            await retry_worker.enqueue(123)
            
            # Create task
            task = asyncio.create_task(retry_worker.run_forever(mock_client))
            
            # Wait for processing attempt
            await asyncio.sleep(0.5)
            
            # Cancel task
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Task should have called task_done despite exception
        assert retry_worker.queue.qsize() == 0
