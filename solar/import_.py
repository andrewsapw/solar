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

from solar.base import ApiEngine

logger = logging.getLogger("root")


def recursive_field_remove(doc: dict, field: str):
    if not isinstance(doc, (dict, list)):
        return doc
    if isinstance(doc, list):
        return [recursive_field_remove(v, field) for v in doc]
    return {k: recursive_field_remove(v, field) for k, v in doc.items() if k != field}


class Importer(ApiEngine):
    """Class with *import* methods"""

    def _load_json(self, path: str):
        """Load source `.json` file"""

        with open(path, "r", encoding="UTF-8") as f:
            data = json.load(f)

        if self.collection is None:
            self.collection = data["collection"]
        return data

    async def _post_documents(self, docs: List[dict]):
        """Send documents to Solr.

        Args:
            docs (List[dict]): array of docs

        Raises:
            ValueError: error sending docs
        """
        curr_time = round(time.time() * 1000)
        for doc in docs:
            doc.pop("_version_", None)
            doc = recursive_field_remove(doc, "_root_")

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
        """Import data, saved as `.json` file

        Args:
            path (str): source `.json` path
            batch_size (int, optional): how many documents will be sent by one request.
                Defaults to 50.
        """
        data = self._load_json(path)
        docs = data["docs"]
        docs_ids = set([i[self.id_col] for i in docs])

        print(f"Number of docs: {len(docs_ids)}")
        if not overwrite:
            print("Fetching IDs of collection documents...")
            existing_ids = await self._fetch_ids(query="*:*")
            if existing_ids is None:
                raise ValueError("Error fetching collection IDs :(")

            print(f"Number of docs found: {len(existing_ids)}")
            existing_ids = set(existing_ids)
            upload_ids = docs_ids - existing_ids

            print(f"{len(upload_ids)} docs will be created")
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
            print("No documents to import :(")
            return

        print("Begin import with params:")
        print(f"Source file: [bold]{path}")
        print(f"Batch size: [bold]{batch_size}")
        print(f"Collection: [bold]{self.collection}")

        confirm = input("Correct? (y/n)").lower() == "y"
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
            task = progress.add_task("Uploading...", total=num_docs)

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
            raise ValueError("Error deleting config :(")

        print("[green]Ok")

    async def import_configs(
        self,
        configs_path: Union[str, pathlib.Path],
        overwrite: bool = False,
        name: Optional[str] = None,
    ):
        """Import config

        Args:
            configs_path (Union[str, pathlib.Path]): folder with config root
            overwrite (bool, optional): overwrite currently existing config with the same name
                Defaults to False.
            name (Optional[str], optional): create config with this name
                if `None` - source folder name will be used
                Defaults to None.

        Raises:
            ValueError: Error importing config
        """
        import io
        import zipfile

        overwrite_str = "true" if overwrite else "false"
        cleanup_str = overwrite_str

        if isinstance(configs_path, str):
            configs_path = pathlib.Path(configs_path)

        if name is None:
            name = configs_path.name

        print("Import params:")
        print(f"Source config: [bold]{name}[/bold]")
        print(f"Overwrite: [bold]{overwrite}[/bold]")

        confirm = input("Correct? (y/n)")
        if confirm.lower() != "y":
            print("[red]Stopping...")
            return

        if overwrite:
            print("Removing old config...", end=" ")
            await self._remove_config(name=name)

        print("Creating new config...")
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
                raise ValueError("Error sending config .zip archive :(")

        print("[bold green]Done!")
