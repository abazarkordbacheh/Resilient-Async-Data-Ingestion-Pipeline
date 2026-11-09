# region Import Modules...
import os
import httpx
import dotenv
import asyncio
import logging
from pipline.monitor_lag import monitoring_lag_forever
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
        count: int,
        last_successful_batch_id: int | None = None,
        max_retries: int = 5
) -> httpx.Response:
    """
    Fetch the next batch of records from the source API
    :param client: https.AsyncClient
    :param base_url: Base Url
    :param token: Token
    :param count: Count of records to fetch
    :param last_successful_batch_id: Last batch_id
    :param max_retries: Maximum retries
    :return: httpx.Response
    """
    for attempt in range(1,max_retries+1):
        try:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{base_url.rstrip('/')}/getnextbatch/?count={count}"
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            if isinstance(data,dict) and "records" in data:
                return data
            else:
                raise ValueError("Structure is invalid!")
        except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as err:
            delay = 2 ** attempt
            logger.warning(
                f"[FETCH ERROR] Attempt {attempt} of {max_retries} failed: {err}."
                f"Retrying in {delay} seconds..."
            )
            await asyncio.sleep(delay)
            if last_successful_batch_id is not None:
                try:
                    reset_url = f"{base_url.rstrip('/')}/resettobatchid/"
                    headers = {"Authorization": f"Bearer {token}"}
                    payload = {"function": "getnextbatch", "batch_id": last_successful_batch_id}

                    reset_response = await client.post(reset_url, headers=headers, json=payload, timeout=10.0)
                    reset_response.raise_for_status()
                    logger.info(f"[POINTER RESET] The server pointer was successfully reset on secure batch {last_successful_batch_id}.")
                except Exception as reset_err:
                    logger.error(f"[POINTER RESET FAILED] Pointer reset encountered an error: {reset_err}")
        raise RuntimeError(f"Critical error: Batch could not be received after {max_retries} consecutive attempts.")


# endregion


# region Main...
async def main() -> None:
    # Read from .env file
    dotenv.load_dotenv()
    base_url = os.getenv("BASE_URL")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    token_ttl_seconds = os.getenv("TOKEN_TTL_SECONDS")
    # SQLConnection
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
    logger.info("Initializing TokenManager and Database Connection...")
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
                    monitoring_lag_forever(
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
                batch_data  = await fetch_next_batch(
                    client=client,
                    base_url=base_url,
                    token=token,
                    count=batch_count
                )
                records = batch_data.get("records", []) or []
                batch_id = batch_data.get("batch_id")

                if not records:
                    logger.info("No more records received from source API. Stopping main loop.")
                    break

                if batch_id is None:
                    logger.warning("Response missing 'batch_id'; skipping insert to prevent tracking ambiguity.")
                    continue
                try:
                    # Insert to Database
                    await db.insert_batch(records)
                    current_last_batch_id = int(batch_id)
                    logger.info("[MAIN] Successfully inserted batch_id=%d (%d records)", current_last_batch_id,
                                len(records))

                except Exception as db_err:
                    logger.error("[MAIN] Database insertion failed for batch_id=%s: %s -> Enqueuing for retry.", batch_id, db_err)
                    await retry_worker.enqueue(int(batch_id))
        except Exception as eg:
            logger.critical("Critical error encountered inside task group: %s", eg)
            raise
        finally:
            logger.info("Shutting down database connection and releasing resources...")
            db.disconnect()
            logger.info("Pipeline shutdown complete.")

# endregion

# region Run Main...
if __name__ == "__main__":
    asyncio.run(main())
# endregion
