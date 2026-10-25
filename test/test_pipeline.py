# ------------------------ Modules ------------------------
# Libraries = >
import asyncio
import pytest
import httpx
from unittest.mock import AsyncMock, patch
# Modules =>
from pipeline.pipeline import fetch_next_batch, main


@pytest.mark.asyncio
async def test_fetch_next_batch():
    """Test fetch_next_batch constructs correct URL and authorization headers."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/getnextbatch/"
        assert request.url.params["count"] == "100"
        assert request.headers["authorization"] == "Bearer test-jwt-token"
        return httpx.Response(200, json={"batch_id": 1, "records": [{"id": 1}]})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await fetch_next_batch(client, "http://testserver", "test-jwt-token", 100)
        assert response.status_code == 200
        data = response.json()
        assert data["batch_id"] == 1


@pytest.mark.asyncio
async def test_main_pipeline_execution(monkeypatch):
    """Test main loop fetches batches, inserts into DB, and terminates on empty records."""
    monkeypatch.setenv("BASE_URL", "http://testserver")
    monkeypatch.setenv("BATCH_COUNT", "100")

    mock_db_instance = AsyncMock()
    mock_retry_worker_instance = AsyncMock()

    # Mock TokenManager.get_valid_token
    with patch("pipeline.TokenManager") as MockTokenMgr, \
            patch("pipeline.SQLConnection", return_value=mock_db_instance) as MockSQLConn, \
            patch("pipeline.RetryWorker", return_value=mock_retry_worker_instance) as MockRetryWorker, \
            patch("pipeline.monitor_lag_forever", new_callable=AsyncMock) as mock_monitor, \
            patch("pipeline.fetch_next_batch", side_effect=[
                # First call returns batch with 1 record
                httpx.Response(200, json={"batch_id": 42, "records": [{"col": "val"}]}),
                # Second call returns empty records to trigger graceful break
                httpx.Response(200, json={"batch_id": 43, "records": []})
            ]):
        # Run main and let it terminate normally via empty records
        await main()

        # Verify DB interactions
        mock_db_instance.start.assert_awaited_once()
        mock_db_instance.insert_batch.assert_awaited_once_with([{"col": "val"}])
        mock_db_instance.close.assert_awaited_once()
