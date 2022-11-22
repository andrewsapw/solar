import logging
import datetime
import json
import pathlib

from tqdm import tqdm
import aiohttp

from solrdumper.base import ApiEngine


logger = logging.getLogger("root")


class Exporter(ApiEngine):
    def __init__(
        self,
        base_url: str,
        collection: str,
        username: str | None = None,
        password: str | None = None,
        id_col: str = "id",
    ) -> None:
        super().__init__(base_url, collection, username, password, id_col)

    def fetch_ids(self, query: str = "*:*"):
        # get documents ids
        url_path = f"/solr/{self.collection}/export"
        data = self.api_request(
            path=url_path,
            params={"q": query, "fl": self.id_col, "sort": f"{self.id_col} desc"},
        )
        if data is None:
            logger.error("Ошибка при получении ID документов")
            return

        body = data["response"]
        num_found = body["numFound"]
        ids = [i["id"] for i in body["docs"]]
        return ids

    def get_documents(self, ids: list[str]):
        # http://10.113.18.48:8983/solr/all/select?indent=true&q.op=OR&q=id%3Anpp_ev_196513

        query = "\n".join([f"{self.id_col}:{i}" for i in ids])
        url_path = f"/solr/{self.collection}/select"
        params = {"q": query, "q.op": "OR", "rows": len(ids)}
        resp = self.api_request(path=url_path, params=params, method="GET")
        if resp is None:
            logger.error(f"Error fetching document {id} ({self.collection=})")
            return None

        doc = resp["response"]["docs"]
        return doc

    def save_json(self, ids: list[str], path: str, batch_size: int = 50):
        today = datetime.datetime.now()
        today_str = today.strftime("%d-%M-%Y")
        data = dict(
            collection=self.collection,
            solr_url=self.base_url,
            date=today_str,
            docs=[],
        )
        num_ids = len(ids)

        logger.info("Scrapping docs...")
        bar = tqdm(total=num_ids)
        for from_idx in range(0, num_ids, batch_size):
            to_idx = min(from_idx + batch_size, num_ids)
            batch_ids = ids[from_idx:to_idx]

            docs = self.get_documents(ids=batch_ids)
            data["docs"] += docs

            bar.update(batch_size)

        file_directory = pathlib.Path(path)
        if not file_directory.exists():
            file_directory.mkdir()

        filepath = file_directory / f"{self.collection}_{today_str}.json"
        with open(filepath, "w", encoding="UTF-8") as f:
            json.dump(data, f, ensure_ascii=False)

        return filepath

    def export(self, path: str, query: str = "*:*"):
        ids = self.fetch_ids(query=query)
        print(f"Num found: {len(ids)}")
        filepath = self.save_json(ids=ids, path=path)
        return filepath
