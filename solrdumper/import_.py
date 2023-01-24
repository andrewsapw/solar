import json
import logging
import pathlib
import time

from tqdm import tqdm

from solrdumper.base import ApiEngine

logger = logging.getLogger("root")


class Importer(ApiEngine):
    def __init__(
        self,
        base_url: str,
        collection: str | None = None,
        username: str | None = None,
        password: str | None = None,
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

    def post_documents(self, docs: list[dict]):
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
        res = self.api_request(
            method="POST",
            path=url_path,
            data=doc_binary,
            headers=headers,
            params=params,
        )
        if res is None:
            raise ValueError(res)

    def import_json(self, path: str, batch_size: int = 50):
        data = self.load_json(path)
        logger.info("Importing docs...")

        num_docs = len(data["docs"])
        bar = tqdm(total=num_docs)
        for from_idx in range(0, num_docs, batch_size):
            to_idx = min(from_idx + batch_size, num_docs)
            batch_docs = data["docs"][from_idx:to_idx]
            self.post_documents(docs=batch_docs)
            bar.update(batch_size)

    def import_configs(self, configs_path: str | pathlib.Path):
        import io
        import zipfile

        if isinstance(configs_path, str):
            configs_path = pathlib.Path(configs_path)

        upload_url = "/solr/admin/configs"
        params = dict(action="UPLOAD", name=configs_path.name, overwrite="false")
        with io.BytesIO() as f:
            with zipfile.ZipFile(f, "w") as zf:
                for path in configs_path.rglob("*"):
                    rel_path = path.relative_to(configs_path)
                    zf.write(path, rel_path)

            f.seek(0)

            resp = self.api_request(
                method="POST", path=upload_url, params=params, data=f
            )
            if resp is None:
                raise ValueError("Ошибка при отправке файла :(")

            print(resp.json())
