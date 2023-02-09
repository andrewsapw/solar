import asyncio
import datetime
import logging
import pathlib
from typing import List, Optional, Union

import orjson
import tqdm.asyncio

from solrdumper.base import ApiEngine

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

    async def fetch_ids(self, query: str = "*:*"):
        url_path = f"/solr/{self.collection}/export"
        data = await self.api_request(
            path=url_path,
            params={"q": query, "fl": self.id_col, "sort": f"{self.id_col} desc"},
        )
        if data is None:
            logger.error("Ошибка при получении ID документов")
            return

        body = data["response"]  # type: ignore
        num_found = body["numFound"]
        ids = [i["id"] for i in body["docs"]]
        return ids

    async def get_documents(self, ids: List[str]):
        # http://10.113.18.48:8983/solr/all/select?indent=true&q.op=OR&q=id%3Anpp_ev_196513

        query = "\n".join([f"{self.id_col}:{i}" for i in ids])
        url_path = f"/solr/{self.collection}/select"
        params = {"q": query, "q.op": "OR", "rows": len(ids)}
        resp = await self.api_request(path=url_path, params=params, method="GET")
        if resp is None:
            logger.error(f"Error fetching document {id} ({self.collection=})")
            raise ValueError("Ошибка при получении документов")

        doc = resp["response"]["docs"]  # type: ignore
        return doc

    async def save_json(
        self,
        ids: List[str],
        path: str,
        batch_size: int = 10,
    ):
        today = datetime.datetime.now()
        today_str = today.strftime("%d-%m-%Y")
        data = dict(
            collection=self.collection,
            solr_url=self.base_url,
            date=today_str,
            docs=[],
        )
        num_ids = len(ids)

        logger.info("Scrapping docs...")
        tasks = []
        for from_idx in range(0, num_ids, batch_size):
            to_idx = min(from_idx + batch_size, num_ids)
            batch_ids = ids[from_idx:to_idx]

            docs = self.get_documents(ids=batch_ids)
            tasks.append(docs)

        for r in tqdm.asyncio.tqdm.as_completed(tasks):
            batch_data = await r
            data["docs"] += batch_data

        file_directory = pathlib.Path(path)
        if not file_directory.exists():
            file_directory.mkdir()

        filepath = file_directory / f"{self.collection}_{today_str}.json"
        with open(filepath, "w", encoding="UTF-8") as f:
            f.write(orjson.dumps(data).decode("utf-8"))

        return filepath

    async def export(self, path: str, query: str = "*:*"):
        ids = await self.fetch_ids(query=query)
        if ids is None:
            raise ValueError("Ошибка при получении индексов документов")

        print(f"Num found: {len(ids)}")
        filepath = await self.save_json(ids=ids, path=path)

        return filepath

    async def export_config(self, path: Union[str, pathlib.Path]):
        if isinstance(path, str):
            path = pathlib.Path(path)

        if not path.exists():
            path.mkdir()

        url = "/solr/admin/zookeeper?detail=true&path=/configs/&wt=json"
        resp: dict = await self.api_request(path=url)  # type: ignore
        if resp is None:
            logger.error("Ошибка при получении списка конфигов")
            return

        tree = resp["tree"][0]["children"]

        await self._parse_tree(tree=tree, folder=path)

    async def _parse_tree(self, tree: list, folder: pathlib.Path):
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
                        f"Ошибка во время получения тела файла: {filename} [{content_url}] ({el})"
                    )

                assert isinstance(resp, dict)
                content = resp["znode"]["data"]
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

    asyncio.run(exporter.export(path=r"C:\projects\solrdumper\data\0302"))
