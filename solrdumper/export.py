import asyncio
import datetime
import logging
import pathlib
from typing import List, Optional, Union

import orjson
from rich import print
from rich.progress import Progress

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

    async def _get_documents(
        self, query: str, start_row: int, rows: int, nested: bool
    ) -> List[dict]:
        """Получение документов по списку их ID

        Args:
            ids (List[str]): массив ID документов коллекции

        Raises:
            ValueError: Ошибка при получении документов

        Returns:
            List[dict]: массив документов
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
            logger.error(f"Error fetching document {id} ({self.collection=})")
            raise ValueError("Ошибка при получении документов")

        data = orjson.loads(content)  # type: ignore
        doc = data["response"]["docs"]
        return doc

    async def _export_ids(
        self,
        query: str,
        ids: List[str],
        path: str,
        nested: bool,
        batch_size: int = 100,
    ) -> pathlib.Path:
        """Сохраняет документы с нужными ID (`ids`) в путь `path`

        Args:
            ids (List[str]): массив ID документов коллекции
            path (str): путь, в который запишется итоговый .json файл с документами
            batch_size (int, optional): размер батча, с которым буду скачиваться документы из Solr
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
        num_ids = len(ids)

        logger.info("Scrapping docs...")
        with Progress() as progress:
            task = progress.add_task("Скачивание...", total=num_ids)
            for from_idx in range(0, num_ids, batch_size):
                docs = await self._get_documents(
                    start_row=from_idx, rows=batch_size, query=query, nested=nested
                )
                data["docs"] += docs
                progress.update(task, advance=batch_size)

            # tasks.append(docs)

        # for r in tqdm.asyncio.tqdm.as_completed(tasks):
        #     batch_data = await r
        #     data["docs"] += batch_data

        file_directory = pathlib.Path(path)
        if not file_directory.exists():
            file_directory.mkdir()

        filepath = file_directory / f"{self.collection}_{today_str}.json"
        with open(filepath, "w", encoding="UTF-8") as f:
            f.write(orjson.dumps(data).decode("utf-8"))  # type: ignore

        return filepath

    async def export_data(
        self, path: str, query: str = "*:*", nested: bool = False
    ) -> pathlib.Path:
        """Экспорт данных из Solr в заданный путь (`path`),
        фильтруя по `query`

        Args:
            path (str): путь, в который запишется итоговый файл с данными
            query (str, optional): query запрос в SOlr, с помощью которого соберутся ID документов
                Defaults to "*:*".

        Raises:
            ValueError: Ошибка при получении индексов документов

        Returns:
            pathlib.Path: путь до итогового файла
        """
        ids = await self._fetch_ids(query=query)
        if ids is None:
            raise ValueError("Ошибка при получении индексов документов")

        print(f"Количество документов для экспорта: {len(ids)}")
        filepath = await self._export_ids(
            ids=ids, path=path, query=query, nested=nested
        )

        return filepath

    async def export_config(self, path: Union[str, pathlib.Path]):
        """Экспорт конфигов из Solr

        Args:
            path (Union[str, pathlib.Path]): путь до папки, в которую запишутся конфиги
        """
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
        """Рекурсивный парсинг дерева файлов Zookeeper"""
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
