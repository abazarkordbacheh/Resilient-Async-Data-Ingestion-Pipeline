# ------------------------ Modules ------------------------
# Libraries=>
import asyncio
import logging
import os
import httpx
# Modules=>
from src.connection.token_manager import TokenManager
from src.connection.db_handler import SQLConnection
from retry_worker import RetryWorker
from monitor_lag import monitor_lag_forever


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def fetch_next_batch(
    client: httpx.AsyncClient,
    base_url: str,
    token: str,
    count: int,
) -> httpx.Response:
    """Fetch the next batch of records from the source API."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url.rstrip('/')}/getnextbatch/?count={count}"
    return await client.get(url, headers=headers, timeout=30.0)


async def main() -> None:
    # ------------------------ eading settings directly from environment variables (.env) ------------------------
    base_url = os.getenv("BASE_URL", "http://localhost:8000")
    username = os.getenv("USERNAME", "admin")
    password = os.getenv("PASSWORD", "secret")
    token_ttl_seconds = int(os.getenv("TOKEN_TTL_SECONDS", "270"))

    sql_host = os.getenv("SQL_HOST", "localhost")
    sql_port = int(os.getenv("SQL_PORT", "1433"))
    sql_user = os.getenv("SQL_USER", "sa")
    sql_password = os.getenv("SQL_PASSWORD", "")
    sql_database = os.getenv("SQL_DATABASE", "master")
    sql_table = os.getenv("SQL_TABLE", "IngestedRecords")

    retry_max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
    retry_base_delay_seconds = float(os.getenv("RETRY_BASE_DELAY_SECONDS", "1.0"))
    lag_poll_seconds = float(os.getenv("LAG_POLL_SECONDS", "5.0"))
    batch_count = int(os.getenv("BATCH_COUNT", "100"))

    # ------------------------ Launching TokenManager ------------------------
    token_mgr = TokenManager(
        base_url=base_url,
        username=username,
        password=password,
        token_ttl_seconds=token_ttl_seconds,
    )
    # ------------------------ Database setup ------------------------
    db = SQLConnection(
        host=sql_host,
        port=sql_port,
        user=sql_user,
        password=sql_password,
        database=sql_database,
        table=sql_table,
        windows_authentication=False,
    )
    db.connect()

    try:
        async with httpx.AsyncClient() as client:
            retry_worker = RetryWorker(
                base_url=base_url,
                token_getter=token_mgr.get_valid_token,
                insert_func=db.insert_batch,
                max_attempts=retry_max_attempts,
                base_delay=retry_base_delay_seconds,
            )
            # ------------------------ TaskGroup ------------------------
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(retry_worker.run_forever(client), name="retry-worker")
                    tg.create_task(
                        monitor_lag_forever(
                            base_url=base_url,
                            token_getter=token_mgr.get_valid_token,
                            poll_seconds=int(lag_poll_seconds),
                            client=client,
                        ),
                        name="lag-monitor",
                    )

                    # ------------------------ The main ring of the pipeline ------------------------
                    while True:
                        token = await token_mgr.get_valid_token(client)
                        response = await fetch_next_batch(
                            client=client,
                            base_url=base_url,
                            token=token,
                            count=batch_count,
                        )

                        if response.status_code != 200:
                            logger.warning(
                                "fetch_next_batch returned status=%s; retrying shortly.",
                                response.status_code,
                            )
                            await asyncio.sleep(0.5)
                            continue

                        data = response.json()
                        records = data.get("records", []) or []

                        if not records:
                            logger.info("No more records returned by source API. Stopping pipeline.")
                            break

                        batch_id = data.get("batch_id")
                        if batch_id is None:
                            logger.warning("Response does not contain 'batch_id'; skipping batch.")
                            continue

                        try:
                            await db.insert_batch(records)
                        except Exception as exc:
                            logger.error(
                                "insert_batch failed for batch_id=%s: %s",
                                batch_id,
                                exc,
                            )
                            await retry_worker.enqueue(int(batch_id))

            except* Exception as eg:
                logger.critical("Critical error inside TaskGroup execution: %s", eg)
                raise

    finally:
        db.disconnect()
        logger.info("Pipeline shut down gracefully.")


if __name__ == "__main__":
    asyncio.run(main())
