import json
import logging

import requests

requests.packages.urllib3.disable_warnings()  # type: ignore
logger = logging.getLogger("root")


class ApiEngine:
    def __init__(
        self,
        base_url: str,
        collection: str | None = None,
        username: str | None = None,
        password: str | None = None,
        id_col: str = "id",
    ) -> None:
        self.base_url = base_url
        self.collection = collection
        self.username = username
        self.password = password
        self.id_col = id_col

        self.session = requests.Session()
        if password is not None and username is not None:
            self.session.auth = (username, password)

    def api_request(self, path: str, params: dict = {}, method: str = "GET", **kwargs):
        resp = self.session.request(
            method=method,
            url=self.base_url + path,
            params=params,
            verify=False,
            **kwargs,
        )

        if resp.status_code != 200:
            text = resp.text
            logger.error(f"{method} - {resp.url} - {text}")
            logger.error(resp.reason)
            return None

        if method.lower() == "get":
            content = resp.text
            data = json.loads(content)
            return data
        else:
            return resp
