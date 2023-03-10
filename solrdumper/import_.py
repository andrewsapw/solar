import json
import logging
import pathlib
import time
from typing import List, Optional, Union

from more_itertools import chunked
from rich import print
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from solrdumper.base import ApiEngine

logger = logging.getLogger("root")


def recursive_field_remove(doc: dict, field: str):
    if not isinstance(doc, (dict, list)):
        return doc
    if isinstance(doc, list):
        return [recursive_field_remove(v, field) for v in doc]
    return {k: recursive_field_remove(v, field) for k, v in doc.items() if k != field}


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
            doc = recursive_field_remove(doc, "_root_")
            # doc = recursive_field_remove(doc, "_version_")
            # print(doc)

        url_path = f"/solr/{self.collection}/update"
        headers = {
            "Content-type": "application/json",
        }

        params = {
            "_": curr_time,
            "commitWithin": "5000",
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
        return res

    async def import_data(
        self, path: str, batch_size: int = 50, overwrite: bool = True
    ):
        """Импорт данных, хранящихся в виде `json`

        Args:
            path (str): путь до `.json` файла с данными
            batch_size (int, optional): Размер батча данных при импорте.
                Defaults to 50.
        """
        data = self._load_json(path)
        docs = data["docs"]
        docs_ids = set([i[self.id_col] for i in docs])

        print(f"Количество докуметов: {len(docs_ids)}")
        if not overwrite:
            num_before = len(docs)
            print("Получения ID существующих документов...")
            existing_ids = await self._fetch_ids(query="*:*")
            if existing_ids is None:
                raise ValueError("Ошибка при получении индексов документов")

            print(f"Количество документов в базе: {len(existing_ids)}")
            print("Фильтрация...")

            existing_ids = set(existing_ids)
            upload_ids = docs_ids - existing_ids

            print(f"Будет загружено: {len(upload_ids)} документов")
            seen = set()
            upload_docs = []
            for doc in docs:
                doc_id = doc[self.id_col]
                if doc_id not in seen and doc_id in upload_ids:
                    upload_docs.append(doc)
                    seen.add(doc_id)
        else:
            upload_docs = docs

        del docs
        del data

        if len(upload_docs) == 0:
            print("Нет документов :(")
            return

        print("Запуск импорта с параметрами:")
        print(f"Импорт из: [bold]{path}")
        print(f"Размер батча: [bold]{batch_size}")
        print(f"Коллекция: [bold]{self.collection}")

        confirm = input("Всё верно? (y/n)").lower() == "y"
        if not confirm:
            print("[red]Отмена...")
            return

        num_docs = len(upload_docs)

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("({task.completed})"),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Загрузка...", total=num_docs)

            for batch_docs in chunked(upload_docs, batch_size):
                try:
                    await self._post_documents(docs=batch_docs)
                except Exception:

                    print("[red]Ошибка :)")

                progress.update(task, advance=batch_size)

    async def _remove_config(self, name: str):
        url = f"/api/cluster/configs/{name}?omitHeader=true"
        r = await self.api_request(method="DELETE", path=url)
        if r is None:
            raise ValueError("Ошибка во время удаления конфига :(")

        print(r)

    async def _remove_config(self, name: str):
        url = f"/api/cluster/configs/{name}?omitHeader=true"
        r = await self.api_request(method="DELETE", path=url)
        if r is None:
            raise ValueError("Ошибка во время удаления конфига :(")

        print(r)

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
        cleanup_str = overwrite_str

        if isinstance(configs_path, str):
            configs_path = pathlib.Path(configs_path)

        if name is None:
            name = configs_path.name

        print("Параметры импорта:")
        print(f"Конфиг: [bold]{name}[/bold]")
        print(f"Перезапись: [bold]{overwrite}[/bold]")
        print(f"Путь импорта: [bold]{configs_path.absolute().__str__()}[/bold]")

        confirm = input("Все верно? (y/n)")
        if confirm.lower() != "y":
            print("[red]Отмена...")
            return

        print("Запуск импорта...")

        if overwrite:
            print("Удаление старого конфига...")
            await self._remove_config(name=name)

        print("Загрузка нового...")
        upload_url = "/solr/admin/configs"
        params = dict(
            action="UPLOAD", name=name, overwrite=overwrite_str, cleanup=cleanup_str
        )
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
