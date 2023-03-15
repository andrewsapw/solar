import asyncio
import datetime
import logging
import pathlib
from typing import List, Optional, Union

import orjson
from rich import print
from rich.progress import Progress

from solar.base import ApiEngine

logger = logging.getLogger("root")


class Exporter(ApiEngine):
    def __init__(
        self,
        base_url: str,
        collection: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        id_col: str = "id",
    ) -> None:
        super().__init__(base_url, collection, username, password, id_col)

    async def _get_documents(
        self, query: str, start_row: int, rows: int, nested: bool
    ) -> List[dict]:
        """Fetch documents by `query`

        Args:
            query (List[dict]): `query` for Solr
            start_row (int): index of first document to fetch
            rows (int): number of documents to fetch
            nested (bool): fetch nested documents or not
                if True - adds `fl=*, [child limit=-1]` to query params

        Raises:
            ValueError: Error fetching documents

        Returns:
            List[dict]: array of collection documents
        """
        url_path = f"/solr/{self.collection}/select"
        params = {
            "q": query,
            "q.op": "OR",
            "start": start_row,
            "rows": rows,
        }
        if nested:
            params["fl"] = "*, [child limit=-1]"

        content = await self.api_request(path=url_path, params=params, method="GET")

        if content is None:
            print(f"[red]Error fetching document {id} ({self.collection=})")
            raise ValueError(f"Error fetching document {id} ({self.collection=})")

        data = orjson.loads(content)  # type: ignore
        doc = data["response"]["docs"]
        return doc

    async def _export_to_path(
        self,
        query: str,
        path: str,
        nested: bool,
        batch_size: int = 100,
    ) -> Optional[pathlib.Path]:
        """Export documents to .json file in `path`

        Args:
            query (str): `q` parameter to fetch docs from Solr
            path (str): path to save result `.json` file
            batch_size (int, optional): batch size.
                Specifies how many documents will be fetched by one request
                Defaults to 10.

        Returns:
            pathlib.Path: путь до итогового файла
        """
        today = datetime.datetime.now()
        today_str = today.strftime("%d-%m-%Y")
        data = dict(
            collection=self.collection,
            solr_url=self.base_url,
            date=today_str,
            docs=[],
        )

        ids = await self._fetch_ids(query=query)
        if ids is None:
            print("[red]Error fetching documents IDs")
            return

        num_ids = len(ids)
        with Progress() as progress:
            task = progress.add_task("Downloading...", total=num_ids)
            for from_idx in range(0, num_ids, batch_size):
                docs = await self._get_documents(
                    start_row=from_idx, rows=batch_size, query=query, nested=nested
                )
                data["docs"] += docs  # type: ignore
                progress.update(task, advance=batch_size)

        file_directory = pathlib.Path(path)
        if not file_directory.exists():
            file_directory.mkdir()

        filepath = file_directory / f"{self.collection}_{today_str}.json"
        with open(filepath, "w", encoding="UTF-8") as f:
            f.write(orjson.dumps(data).decode("utf-8"))  # type: ignore

        return filepath

    async def export_data(
        self, path: str, query: str = "*:*", nested: bool = False
    ) -> Optional[pathlib.Path]:
        """Export Solr collection to `path`

        Args:
            path (str): path to save result `.json` file
            query (str, optional): `q` parameter to fetch docs from Solr
                Defaults to "*:*".

        Returns:
            Optional[pathlib.Path]: result `.json` path
        """
        print(f"Export nested documents: {nested}")

        filepath = await self._export_to_path(path=path, query=query, nested=nested)

        return filepath

    async def export_config(self, path: Union[str, pathlib.Path]):
        """Export configs from Solr

        Args:
            path (Union[str, pathlib.Path]): folder path to save configs to.
        """
        if isinstance(path, str):
            path = pathlib.Path(path)

        if not path.exists():
            path.mkdir()

        url = "/solr/admin/zookeeper?detail=true&path=/configs/&wt=json"
        resp: dict = await self.api_request(path=url)  # type: ignore
        if resp is None:
            print("[red]Error fetching configs :(")
            return

        tree = resp["tree"][0]["children"]

        await self._parse_tree(tree=tree, folder=path)

    async def _parse_tree(self, tree: list, folder: pathlib.Path):
        """Recursive Zookeeper file tree parsing"""
        for el in tree:
            if isinstance(el, list):
                await self._parse_tree(el, folder=folder)
            elif isinstance(el, dict) and "children" in el:
                folder_name: str = el["text"]
                childrens = el["children"]

                folder_path: pathlib.Path = folder / folder_name

                if not folder_path.exists():
                    folder_path.mkdir()
                await self._parse_tree(tree=childrens, folder=folder_path)

            elif isinstance(el, dict) and "children" not in el:
                filename = el["text"]

                content_url = f'/solr/{el["a_attr"]["href"]}'
                resp = await self.api_request(
                    path=content_url,
                )
                if resp is None:
                    raise ValueError(
                        f"Error fetching document body: {filename} [{content_url}] ({el})"
                    )

                assert isinstance(resp, dict)
                content: str = resp["znode"]["data"]  # type: ignore
                self._write_file(path=folder / filename, content=content)
            elif isinstance(el, str):
                continue

    def _write_file(self, path: pathlib.Path, content: str):
        with open(path, "w", encoding="UTF-8") as f:
            f.write(content)


if __name__ == "__main__":
    exporter = Exporter(
        base_url="http://10.113.18.48:8983",
        collection="portal",
    )
    import asyncio

    asyncio.run(exporter.export_data(path=r"C:\projects\solrdumper\data\0302"))
