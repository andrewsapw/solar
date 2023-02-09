import logging
from typing import Optional

import aiohttp
import orjson

# requests.packages.urllib3.disable_warnings()  # type: ignore
logger = logging.getLogger("root")


class ApiEngine:
    def __init__(
        self,
        base_url: str,
        collection: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        id_col: str = "id",
    ) -> None:
        self.base_url = base_url
        self.collection = collection
        self.username = username
        self.password = password
        self.id_col = id_col
        self.client = None

    async def build_client(self):
        if self.password is not None and self.username is not None:
            auth = aiohttp.BasicAuth(login=self.username, password=self.password)
        else:
            auth = None

        self.client: Optional[aiohttp.ClientSession] = aiohttp.ClientSession(
            auth=auth, connector=aiohttp.TCPConnector(verify_ssl=False)
        )

    async def close_client(self):
        if self.client is None:
            return

        await self.client.close()

    async def api_request(
        self,
        *,
        path: str,
        params: dict = {},
        method: str = "GET",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Сначала нужно вызвать .build_client()")

        async with self.client.request(
            method=method,
            url=self.base_url + path,
            params=params,
            **kwargs,
        ) as resp:
            if resp.status != 200:
                text = resp.text
                logger.error(f"{method} - {resp.url} - {text}")
                logger.error(resp.text)
                return None

            if method.lower() == "get":
                content = await resp.text(encoding="UTF-8")
                data = orjson.loads(content)
                return data
            else:
                return resp
