# ------------------------ Modules ------------------------
# Libraries =>
import asyncio
import pytest
import httpx
from unittest.mock import AsyncMock
# Modules =>
from pipeline.monitor_lag import monitor_lag_forever

# ------------------------ Test ------------------------
@pytest.mark.asyncio
async def test_monitor_lag_success(caplog):
    """Test monitor_lag_forever sends correct POST request and logs lag successfully."""
    requests_log = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_log.append({
            "path": request.url.path,
            "method": request.method,
            "auth": request.headers.get("authorization"),
        })
        return httpx.Response(200, json={"status": "Success", "lag": "47673892"})

    transport = httpx.MockTransport(handler)
    mock_token_getter = AsyncMock(return_value="test-jwt-token")

    # Mock asyncio.sleep to raise CancelledError after 1 iteration to break infinite loop
    async def fake_sleep(delay):
        raise asyncio.CancelledError()

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        with pytest.raises(asyncio.CancelledError), \
                pytest.MonkeyPatch.context() as mp:
            mp.setattr(asyncio, "sleep", fake_sleep)

            await monitor_lag_forever(
                base_url="http://testserver/",
                token_getter=mock_token_getter,
                poll_seconds=5.0,
                client=client
            )

        assert len(requests_log) == 1
        assert requests_log[0]["path"] == "/getlag/"
        assert requests_log[0]["method"] == "POST"
        assert requests_log[0]["auth"] == "Bearer test-jwt-token"
        assert "remaining=47673892" in caplog.text
