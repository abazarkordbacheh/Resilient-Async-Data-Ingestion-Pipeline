# monitor_lag.py
import asyncio
import logging
import httpx

async def monitor_lag_forever(base_url: str, token_getter, poll_seconds: int, client: httpx.AsyncClient):
    base_url = base_url.rstrip("/")
    while True:
        try:
            token = await token_getter(client)
            headers = {"Authorization": f"Bearer {token}"}
            payload = {"function": "getnextbatch"}

            r = await client.post(f"{base_url}/getlag/", json=payload, headers=headers, timeout=10.0)
            if r.status_code == 200:
                data = r.json()
                lag = data.get("lag")
                logging.info(f"[LAG] remaining={lag}")
            else:
                logging.warning(f"[LAG] getlag failed: {r.status_code} {r.text}")

        except Exception as e:
            logging.warning(f"[LAG] monitor error: {e}")

        await asyncio.sleep(poll_seconds)
