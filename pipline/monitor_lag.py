# region Import Modules...
import httpx
import asyncio
import logging
from typing import Callable
# endregion

# region Import Monitoring ...
async def monitoring_lag_forever(
        base_url:str,
        token_getter:Callable,
        poll_seconds:int,
        client:httpx.AsyncClient
) -> None:
    base_url = base_url.rstrip('/')
    while True:
        try:
            token = await token_getter(client)
            headers = {"Authorization": f"Bearer {token}"}
            payload = {"function": "getnextbatch"}

            rsp = await client.post(f"{base_url}/getlag/", headers=headers, json=payload)

            if rsp.status_code == 200:
                data = rsp.json()
                lag = data.get("lag")
                logging.info(f"[LAG] remaining={lag}")
            else:
                logging.warning(f"[LAG] getlag failed: {rsp.status_code} {rsp.text}")
        except Exception as e:
            logging.warning(f"[LAG] monitor error: {e}")
            await asyncio.sleep(poll_seconds)


# endregion
