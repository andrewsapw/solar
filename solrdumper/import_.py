import json
import logging
import pathlib
import time
from typing import List, Optional, Union

from rich import print
from tqdm import trange

from solrdumper.base import ApiEngine

logger = logging.getLogger("root")


class Importer(ApiEngine):
    def __init__(
        self,
        base_url: str,
        collection: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        id_col: str = "id",
    ) -> None:
        super().__init__(
            base_url=base_url, username=username, password=password, id_col=id_col
        )
        self.collection = collection

    def load_json(self, path: str):
        with open(path, "r", encoding="UTF-8") as f:
            data = json.load(f)

        if self.collection is None:
            self.collection = data["collection"]
        return data

    async def post_documents(self, docs: List[dict]):
        curr_time = round(time.time() * 1000)
        for doc in docs:
            doc.pop("_version_", None)

        url_path = f"/solr/{self.collection}/update"
        headers = {
            "Content-type": "application/json",
        }

        params = {
            "_": curr_time,
            "commitWithin": "1000",
            "overwrite": "true",
            "wt": "json",
        }

        doc_binary = json.dumps(docs, ensure_ascii=False).encode("utf-8")
        res = await self.api_request(
            method="POST",
            path=url_path,
            data=doc_binary,
            headers=headers,
            params=params,
        )
        if res is None:
            raise ValueError(res)

    async def import_json(self, path: str, batch_size: int = 50):
        data = self.load_json(path)

        print("Запуск импорта с параметрами:")
        print(f"Импорт из: [bold]{path}[/]")
        print(f"Размер батча: [bold]{batch_size}[/]")

        num_docs = len(data["docs"])

        RPS = 10
        request_ts = time.monotonic()
        for from_idx in trange(0, num_docs, batch_size):
            to_idx = min(from_idx + batch_size, num_docs)
            batch_docs = data["docs"][from_idx:to_idx]
            await self.post_documents(docs=batch_docs)
            request_ts += 1.0 / RPS
            now = time.monotonic()
            if now < request_ts:
                time.sleep(request_ts - now)

    async def import_configs(
        self,
        configs_path: Union[str, pathlib.Path],
        overwrite: bool = False,
        name: Optional[str] = None,
    ):
        import io
        import zipfile

        overwrite_str = "true" if overwrite else "false"

        if isinstance(configs_path, str):
            configs_path = pathlib.Path(configs_path)

        if name is None:
            name = configs_path.name

        print("Параметры импорта:")
        print(f"Конфиг: [bold]{name}[/bold]")
        print(f"Перезапись: [bold]{overwrite_str}[/bold]")
        print(f"Путь импорта: [bold]{configs_path.absolute().__str__()}[/bold]")

        confirm = input("Все верно? (y/n)")
        if confirm.lower() != "y":
            return

        upload_url = "/solr/admin/configs"
        params = dict(action="UPLOAD", name=name, overwrite=overwrite_str)
        with io.BytesIO() as f:
            with zipfile.ZipFile(f, "w") as zf:
                for path in configs_path.rglob("*"):
                    rel_path = path.relative_to(configs_path)
                    zf.write(path, rel_path)

            f.seek(0)

            resp = await self.api_request(
                method="POST", path=upload_url, params=params, data=f
            )
            if resp is None:
                raise ValueError("Ошибка при отправке файла :(")
