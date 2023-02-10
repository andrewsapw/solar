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
    """Класс, определяющий методы для работы с импортом данных (документов и конфигов)
    в Solr
    """

    def _load_json(self, path: str):
        """Загрузка json. Если поле коллекции не указано при создании
        экземпляра класса - берется из .json файла
        """

        with open(path, "r", encoding="UTF-8") as f:
            data = json.load(f)

        if self.collection is None:
            self.collection = data["collection"]
        return data

    async def _post_documents(self, docs: List[dict]):
        """Отправить документы (docs) в Solr.

        Args:
            docs (List[dict]): массив документов для отправки

        Raises:
            ValueError: ошибка при отправке документов
        """
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

    async def import_data(self, path: str, batch_size: int = 50):
        """Импорт данных, хранящихся в виде `json`

        Args:
            path (str): путь до `.json` файла с данными
            batch_size (int, optional): Размер батча данных при импорте.
                Defaults to 50.
        """
        data = self._load_json(path)

        print("Запуск импорта с параметрами:")
        print(f"Импорт из: [bold]{path}")
        print(f"Размер батча: [bold]{batch_size}")
        print(f"Коллекция: [bold]{self.collection}")

        confirm = input("Всё верно? (y/n)").lower() == "y"
        if not confirm:
            print("[red]Отмена...")
            return

        num_docs = len(data["docs"])

        RPS = 10
        request_ts = time.monotonic()
        for from_idx in trange(0, num_docs, batch_size):
            to_idx = min(from_idx + batch_size, num_docs)
            batch_docs = data["docs"][from_idx:to_idx]
            await self._post_documents(docs=batch_docs)
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
        """Импорт конфига

        Args:
            configs_path (Union[str, pathlib.Path]): папка в которой находятся файлы конфига
            overwrite (bool, optional): нужно ли переписывать уже существующий конфиг с таким именем
                Defaults to False.
            name (Optional[str], optional): имя конфига для создания.
                Если None - будет использовано название папки
                Defaults to None.

        Raises:
            ValueError: Ошибка импорта конфига
        """
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
            print("[red]Отмена...")
            return
        
        print("Запуск импорта...")

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
        
        print("[bold green]Импорт успешно завершен!")
