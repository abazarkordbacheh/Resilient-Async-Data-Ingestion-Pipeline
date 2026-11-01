# region Import Modules...
import os
import httpx
import dotenv
import asyncio
import logging

from pipline import TokenManager, SQLServerConnection, RetryWorker

# endregion

# region Configure Logger...
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# endregion

# region Fetch Next Batch...
async def fetch_next_batch(
        client: httpx.AsyncClient,
        base_url: str,
        token: str,
        count: int
) -> httpx.Response:
    """
    Fetch the next batch of records from the source API
    :param client: https.AsyncClient
    :param base_url: Base Url
    :param token: Token
    :param count: Count of records to fetch
    :return: httpx.Response
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url.rstrip('/')}/getnextbatch/?count={count}"
    return await client.get(url, headers=headers, timeout=30.0)


# endregion


# region Main...
async def main() -> None:
    # Read from .env file
    dotenv.load_dotenv()
    base_url = os.getenv("BASE_URL")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    token_ttl_seconds = os.getenv("TOKEN_TTL_SECONDS")

    sql_host = os.getenv("SQL_HOST")
    sql_user = os.getenv("SQL_USER")
    sql_password = os.getenv("SQL_PASSWORD")
    sql_db = os.getenv("SQL_DB")
    sql_port = int(os.getenv("SQL_PORT"))
    sql_schema = os.getenv("SQL_SCHEMA")
    sql_table = os.getenv("SQL_TABLE")

    retry_max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS"))
    retry_base_delay_seconds = float(os.getenv("RETRY_BASE_DELAY"))
    lag_poll_seconds = float(os.getenv("LAG_POLL_SECONDS"))
    batch_count = int(os.getenv("BATCH_COUNT"))

    # Launching TokenManager
    token_mgr = TokenManager(
        base_url=base_url,
        username=username,
        password=password,
        token_ttl_seconds=int(token_ttl_seconds)
    )

    # Database setup
    db = SQLServerConnection(
        host=sql_host,
        port=sql_port,
        user=sql_user,
        password=sql_password,
        database=sql_db,
        windows_authentication=True
    )
    db.connect()

    # Client
    try:
        async with httpx.AsyncClient() as client:
            retry_worker = RetryWorker(
                base_url=base_url,
                token_getter=token_mgr.get_valid_token,
                insert_func=db.insert_batch,
                max_attempts=retry_max_attempts,
                base_delay=retry_base_delay_seconds
            )
            # TaskGroup
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(retry_worker.run_forever(client), name="retry_worker")
                    tg.create_task(
                        monitor_lag_forever(
                            base_url=base_url,
                            token_getter=token_mgr.get_valid_token,
                            poll_seconds=int(lag_poll_seconds),
                            client=client,
                        ),
                        name="lag-monitor",
                    )
                # The main ring of the pipeline
                while True:
                    token = await token_mgr.get_valid_token(client)
                    response = await fetch_next_batch(
                        client=client,
                        base_url=base_url,
                        token=token,
                        count=batch_count
                    )
                    # Check response Status Code
                    if response.status_code != 200:
                        logger.warning(
                            "fetch_next_batch returned status=%s; retrying shortly.",
                            response.status_code,
                        )
                        await asyncio.sleep(2)
                        continue

                    # Check response Data
                    data = response.json()
                    records = data.get("records", []) or []
                    if not records:
                        logger.info("No more records returned by source API. Stopping pipeline.")
                        break

                    # Get Batch ID
                    batch_id = data.get("batch_id")
                    if batch_id is None:
                        logger.warning("Response does not contain 'batch_id'; skipping batch.")
                        continue

                    # Insert Batch
                    try:
                        await db.insert_batch(records,sql_table,sql_schema)
                    # Retry on failure =>> Send to enqueue
                    except Exception as exc:
                        logger.error(
                            "insert_batch failed for batch_id=%s: %s",
                            batch_id,
                            exc,
                        )
                        await retry_worker.enqueue(int(batch_id))
            except Exception as eg:
                logger.critical("Critical error inside TaskGroup execution: %s", eg)
                raise
    finally:
        db.disconnect()
        logger.info("Pipeline shut down gracefully.")

# endregion

# region Run Main...
if __name__ == "__main__":
    asyncio.run(main())
# endregion