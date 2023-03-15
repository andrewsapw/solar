import logging
from typing import List, Optional

import aiohttp
import orjson

# requests.packages.urllib3.disable_warnings()  # type: ignore
logger = logging.getLogger("root")


class ApiEngine:
    """Base class for working with API Solr"""

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
        """
        aiohttp client initialization.
        Have to used before calling .api_request
        """

        if self.password is not None and self.username is not None:
            auth = aiohttp.BasicAuth(login=self.username, password=self.password)
        else:
            auth = None

        self.client: Optional[aiohttp.ClientSession] = aiohttp.ClientSession(
            auth=auth, connector=aiohttp.TCPConnector(verify_ssl=False)
        )

    async def close_client(self):
        """Close aiohttp client and free resources"""

        if self.client is None:
            return

        await self.client.close()

    async def _fetch_ids(self, query: str = "*:*") -> Optional[List[str]]:
        """Fetch collection documents IDs

        Args:
            query (str, optional): `query` parameter for Solr. Useful for filtering docs
                Defaults to "*:*".

        Returns:
            Optional[List[str]]: array of documents IDs
        """
        url_path = f"/solr/{self.collection}/export"
        content = await self.api_request(
            method="GET",
            path=url_path,
            params={"q": query, "fl": self.id_col, "sort": f"{self.id_col} desc"},
        )
        if content is None:
            return

        data = orjson.loads(content)

        body = data["response"]
        ids = [i["id"] for i in body["docs"]]
        return ids

    async def api_request(
        self,
        *,
        path: str,
        params: dict = {},
        method: str = "GET",
        **kwargs,
    ) -> Optional[str]:
        """Create request to Solr API

        Args:
            path (str): URL path
            params (dict, optional): query params.
                Defaults to {}.
            method (str, optional): request method (GET, POST, etc...).
                Defaults to "GET".

        Raises:
            ValueError: .build_client() is not called before request

        Returns:
            Optional[aiohttp.ClientResponse]: Solr response.
                If None - response status code is >= 400
        """
        if self.client is None:
            raise ValueError(".build_client() have to be called before request")

        async with self.client.request(
            method=method,
            url=self.base_url + path,
            params=params,
            **kwargs,
        ) as resp:
            if resp.status == 401:
                raise ValueError("Auth error :(")

            if resp.status != 200:
                text = await resp.text()
                logger.error(f"{method} - {resp.url} - {text}")
                return None

            try:
                return await resp.text(encoding="UTF-8")
            except UnicodeDecodeError as e:
                print("Can't decode answer :( {e}")
                return None
