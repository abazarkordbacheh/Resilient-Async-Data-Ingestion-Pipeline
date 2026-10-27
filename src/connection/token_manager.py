# ------------------------ Modules ------------------------
# Libraries=>
import asyncio
import time
import httpx
import logging


# ------------------------ Class Token Manager ------------------------
class TokenManager:
    """
    Proactive JWT Token Manager with TTL caching and asynchronous thread-safety.
    Manages authentication lifecycle against the `/jwt_login/` endpoint, caching
    the access token and refreshing it proactively before TTL expiration using
    double-checked locking pattern (`asyncio.Lock`).
    """

    def __init__(self, base_url: str, username: str, password: str, token_ttl_seconds: int):
        """
        Initializes the TokenManager.

        Args:
        base_url: Base URL of the target web service (trailing slashes are stripped).
        username: Username for authentication.
        password: Password for authentication.
        token_ttl_seconds: Time-to-live for the cached token in seconds (default: 300s).
        """
        self.base_url = base_url.rstrip("/")
        self.username = username
        self._password = password

        self._token: str | None = None
        self._fetched_at: float = 0.0
        self._ttl = token_ttl_seconds

        self._lock = asyncio.Lock()

    def _is_valid(self) -> bool:
        """
        Checks whether the currently cached token exists and has not expired.

        Returns:
        True if the token is present and within TTL, False otherwise.
        """
        if not self._token:
            return False
        return (time.time() - self._fetched_at) < self._ttl

    async def get_valid_token(self, client: httpx.AsyncClient) -> str:
        """
        Retrieves a valid JWT access token, utilizing cache or refreshing if needed.

       Thread-safe / async-safe using double-checked locking to prevent duplicate
       simultaneous login requests.

       Args:
       client: An active `httpx.AsyncClient` instance for making requests.

       Returns:
       A valid JWT access token string.

       Raises:
       RuntimeError: If authentication fails or response is malformed.
       """
        if self._is_valid():
            return self._token  # type: ignore

        async with self._lock:
            # double-check inside lock
            if self._is_valid():
                return self._token  # type: ignore

            await self._login(client)
            return self._token  # type: ignore

    async def _login(self, client: httpx.AsyncClient) -> None:
        """
        Authenticates against the `/jwt_login/` endpoint and updates the token cache.

        Args:
        client: An active `httpx.AsyncClient` instance.

        Raises:
        RuntimeError: If HTTP status code is not 200 or response lacks 'access' token.
        """
        url = f"{self.base_url}/jwt_login/"
        payload = {"username": self.username, "password": self._password}

        r = await client.post(url, json=payload, timeout=10.0)
        if r.status_code != 200:
            raise RuntimeError(f"jwt_login failed: {r.status_code} {r.text}")

        data = r.json()
        token = data.get("access")
        if not token:
            raise RuntimeError(f"jwt_login: missing 'access' in response: {data}")

        self._token = token
        self._fetched_at = time.time()
        logging.info("TokenManager: token refreshed.")
