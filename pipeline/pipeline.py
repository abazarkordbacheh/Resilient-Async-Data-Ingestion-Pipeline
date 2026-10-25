# ------------------------ Modules ------------------------
# Libraries =>
import asyncio
import logging
import os
import httpx
# Modules =>
from src.connection.token_manager import TokenManager
from src.connection.db_handler import SQLConnection
from retry_worker import RetryWorker
from monitor_lag import monitor_lag_forever

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def fetch_next_batch(client: httpx.AsyncClient, base_url: str, token: str, count: int) -> httpx.Response:
    """
    Fetches the next batch of records from the target ingestion web service.

    Args:
        client: Active `httpx.AsyncClient` instance.
        base_url: Base URL of the web service.
        token: Valid JWT access token for authorization.
        count: Number of records to request in this batch.

    Returns:
        The raw `httpx.Response` object from the server.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url.rstrip('/')}/getnextbatch/?count={count}"
    return await client.get(url, headers=headers, timeout=30.0)


async def main() -> None:
    """
    Main asynchronous orchestration loop for the Resilient-Async-Ingestor pipeline.

    Initializes database connections, token management, background retry workers,
    and lag monitors. Continuously pulls batches, inserts them into SQL Server,
    and enqueues failed batches for background recovery without blocking ingestion.
    """
    # Load environment configurations with defaults and proper type conversion
    base_url = os.getenv("BASE_URL", "http://localhost:8000")
    username = os.getenv("USERNAME", "admin")
    password = Password = os.getenv("PASSWORD", "secret")
    token_ttl = int(os.getenv("TOKEN_TTL_SECONDS", "300"))

    sql_dsn = os.getenv("SQL_DSN", "mssql+pymssql://user:pass@localhost/db")
    sql_table = os.getenv("SQL_TABLE", "IngestedData")

    retry_max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
    retry_base_delay = float(os.getenv("RETRY_BASE_DELAY_SECONDS", "2.0"))
    lag_poll_seconds = float(os.getenv("LAG_POLL_SECONDS", "10.0"))
    batch_count = int(os.getenv("BATCH_COUNT", "100"))

    logger.info("Initializing TokenManager and database connection...")
    token_mgr = TokenManager(base_url, username, password, token_ttl_seconds=token_ttl)
    db = SQLConnection(dsn=sql_dsn, table=sql_table)
    await db.start()

    async with httpx.AsyncClient() as client:
        retry_worker = RetryWorker(
            base_url=base_url,
            token_getter=token_mgr.get_valid_token,
            insert_func=db.insert_batch,
            max_attempts=retry_max_attempts,
            base_delay=retry_base_delay
        )

        # Start background workers
        retry_task = asyncio.create_task(retry_worker.run_forever(client))
        lag_task = asyncio.create_task(
            monitor_lag_forever(base_url, token_mgr.get_valid_token, lag_poll_seconds, client)
        )

        logger.info("Starting main ingestion loop...")
        try:
            while True:
                token = await token_mgr.get_valid_token(client)
                response = await fetch_next_batch(client, base_url, token, batch_count)

                if response.status_code != 200:
                    logger.warning("getnextbatch failed with status %d: %s", response.status_code, response.text)
                    await asyncio.sleep(0.5)
                    continue

                data = response.json()
                records = data.get("records", []) or []
                batch_id = data.get("batch_id")

                if not records:
                    logger.info("No more records received from server. Stopping main loop.")
                    break

                if batch_id is None:
                    logger.warning("Response missing 'batch_id'; skipping insert to prevent tracking ambiguity.")
                    continue

                try:
                    await db.insert_batch(records)
                    logger.info("[MAIN] Successfully inserted batch_id=%s (%d records)", batch_id, len(records))
                except Exception as db_err:
                    logger.error("Database insertion failed for batch_id=%s: %s -> Enqueuing for retry.", batch_id,
                                 db_err)
                    await retry_worker.enqueue(int(batch_id))

        except asyncio.CancelledError:
            logger.info("Pipeline main loop was cancelled.")
        finally:
            logger.info("Shutting down pipeline workers and database connections...")
            lag_task.cancel()
            retry_task.cancel()

            # Await task cancellation suppression
            await asyncio.gather(lag_task, retry_task, return_exceptions=True)
            await db.close()
            logger.info("Pipeline shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
