# ------------------------ Modules ------------------------
# Libraries =>
import asyncio
import logging
import httpx
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)


class RetryWorker:
    """
    Background worker responsible for handling failed database batch insertions.

    Utilizes an asynchronous queue to process failed batch IDs in the background
    without blocking the main ingestion pipeline. It resets the remote batch pointer
    via `/resettobatchid/`, refetches the records, and retries insertion with
    exponential backoff.
    """

    def __init__(
        self,
        base_url: str,
        token_getter: Callable[[httpx.AsyncClient], Awaitable[str]],
        insert_func: Callable[[list[dict]], Awaitable[None]],
        max_attempts: int = 7,
        base_delay: float = 1.0,
    ) -> None:
        """Initializes the RetryWorker.

        Args:
            base_url: Base URL of the ingestion web service.
            token_getter: Async callable to fetch a valid JWT token.
            insert_func: Async callable to insert a list of records into the SQL database.
            max_attempts: Maximum retry attempts for a failed batch (default: 7).
            base_delay: Base delay in seconds for exponential backoff (default: 1.0s).
        """
        self.base_url: str = base_url.rstrip("/")
        self.token_getter = token_getter
        self.insert_func = insert_func
        self.max_attempts: int = max_attempts
        self.base_delay: float = base_delay
        self.queue: asyncio.Queue[int] = asyncio.Queue()

    async def enqueue(self, batch_id: int) -> None:
        """Enqueues a failed batch ID for background retry processing.

        Args:
            batch_id: The unique identifier of the failed batch.
        """
        await self.queue.put(batch_id)

    async def run_forever(self, client: httpx.AsyncClient) -> None:
        """Continuously runs in the background, consuming failed batch IDs from the queue.

        Args:
            client: An active `httpx.AsyncClient` instance for making HTTP requests.
        """
        while True:
            batch_id = await self.queue.get()
            try:
                await self._process_one(client, batch_id)
            except Exception as e:
                logger.exception(
                    "[RetryWorker] Unrecoverable error processing batch_id=%d: %s",
                    batch_id,
                    e,
                )
            finally:
                self.queue.task_done()

    async def _process_one(self, client: httpx.AsyncClient, batch_id: int) -> None:
        """Processes a single failed batch with exponential backoff retry logic.

        Resets the server's batch pointer using `/resettobatchid/`, fetches the records
        via `/getnextbatch/`, and attempts to insert them into the database.

        Args:
            client: Active `httpx.AsyncClient` instance.
            batch_id: The batch ID to recover.

        Raises:
            RuntimeError: If all retry attempts are exhausted without success.
        """
        reset_url = f"{self.base_url}/resettobatchid/"
        fetch_url = f"{self.base_url}/getnextbatch/?count=100"

        for attempt in range(1, self.max_attempts + 1):
            try:
                token = await self.token_getter(client)
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

                # Step 1: Reset batch pointer to target batch_id
                reset_payload = {"function": "getnextbatch", "batch_id": batch_id}
                reset_res = await client.post(reset_url, json=reset_payload, headers=headers, timeout=10.0)

                if reset_res.status_code != 200:
                    raise RuntimeError(f"resettobatchid failed: {reset_res.status_code} {reset_res.text}")

                # Step 2: Refetch the batch records
                fetch_res = await client.get(fetch_url, headers=headers, timeout=30.0)

                if fetch_res.status_code != 200:
                    raise RuntimeError(f"getnextbatch(after reset) failed: {fetch_res.status_code} {fetch_res.text}")

                data = fetch_res.json()
                records = data.get("records", []) or []
                got_batch_id = data.get("batch_id")

                logger.info(
                    "[RetryWorker] reset(%d) -> refetched batch_id=%s, records_count=%d",
                    batch_id,
                    got_batch_id,
                    len(records),
                )

                # Step 3: Retry database insertion
                await self.insert_func(records)
                logger.info("[RetryWorker] Batch_id=%d recovered successfully on attempt %d.", batch_id, attempt)
                return

            except Exception as err:
                delay = self.base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "[RetryWorker] Attempt %d/%d failed for batch_id=%d: %s. Retrying in %.1fs...",
                    attempt,
                    self.max_attempts,
                    batch_id,
                    err,
                    delay,
                )
                await asyncio.sleep(delay)

        raise RuntimeError(f"[RetryWorker] batch_id={batch_id} failed permanently after max_attempts={self.max_attempts}")
