import asyncio
import datetime
import logging
import pathlib
from typing import List, Optional, Union

import orjson
from rich import print
from rich.progress import Progress

from solar.api.base import ApiEngine
from solar.types.config_files import ConfigFiles

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
        path: Union[str, pathlib.Path],
        nested: bool,
        batch_size: int = 10,
        name: Optional[str] = None,
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

        if isinstance(path, str):
            file_directory = pathlib.Path(path)
        else:
            file_directory = path

        if not file_directory.exists():
            file_directory.mkdir()

        if name is None:
            filepath = file_directory / f"{self.collection}_{today_str}.json"
        else:
            filepath = file_directory / name

        with open(filepath, "w", encoding="UTF-8") as f:
            f.write(orjson.dumps(data).decode("utf-8"))  # type: ignore

        return filepath

    async def export_data(
        self,
        path: Union[str, pathlib.Path],
        query: str = "*:*",
        nested: bool = False,
        name: Optional[str] = None,
        batch_size: Optional[int] = 50,
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
        print(f"Batch size: {batch_size}")

        filepath = await self._export_to_path(
            path=path, query=query, nested=nested, name=name, batch_size=batch_size
        )

        return filepath

    async def export_config(
        self, path: Union[str, pathlib.Path], collection_name: str
    ) -> Optional[pathlib.Path]:
        """Export configs from Solr

        Args:
            path (Union[str, pathlib.Path]): folder path to save configs to.
            config_name (Optional[str]): config name to export.
                If None - export all configs
        """
        if isinstance(path, str):
            path = pathlib.Path(path)

        if not path.exists():
            path.mkdir()

        # url = "/solr/admin/zookeeper?detail=true&path=/configs/&wt=json"
        url = f"/solr/{collection_name}/admin/file?wt=json"

        resp: Optional[str] = await self.api_request(path=url)
        if resp is None:
            raise ValueError("Error fetching config info :(")

        resp_json: dict = orjson.loads(resp)

        config_files = ConfigFiles(**resp_json)

        await self._parse_tree(
            tree=config_files, folder=path, config_name=collection_name
        )
        return path

    async def _parse_tree(
        self, tree: ConfigFiles, folder: pathlib.Path, config_name: Optional[str]
    ):
        """Recursive Zookeeper file tree parsing"""
        for filename, fileinfo in tree.files.items():
            if fileinfo.directory:
                url = f"/solr/{config_name}/admin/file?file={filename}&wt=json"
                resp = await self.api_request(path=url)
                if resp is None:
                    raise ValueError(f"Error fetching dir {filename} files")

                data: dict = orjson.loads(resp)
                dir_files = ConfigFiles(**data)
                dir_folder = folder / filename
                if not dir_folder.exists():
                    dir_folder.mkdir()

                dir_files.path += f"{filename}/"

                await self._parse_tree(
                    tree=dir_files, folder=dir_folder, config_name=config_name
                )

            else:
                file_path: pathlib.Path = folder / filename

                if tree.path:
                    url = f"/solr/{config_name}/admin/file?file={tree.path}{filename}&wt=json"
                else:
                    url = f"/solr/{config_name}/admin/file?file={filename}&wt=json"

                resp: Optional[str] = await self.api_request(
                    path=url,
                )
                if resp is None:
                    raise ValueError(f"Error fetching document body: {filename}")

                self._write_file(path=folder / filename, content=resp)

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
