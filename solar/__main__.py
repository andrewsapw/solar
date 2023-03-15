import asyncio
import urllib.parse
from functools import wraps

import click
from rich import print

from solar.export import Exporter
from solar.import_ import Importer


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))

    return wrapper


@click.group()
@click.option("-q", "--query", help="Solr query", default="*:*")
@click.argument("URL", nargs=1)
@click.option("-c", "--collection", default=None)
@click.pass_context
def cli(ctx, query: str, collection: str, url: str):
    url_parsed = urllib.parse.urlparse(url)
    if url_parsed.scheme not in ("http", "https"):
        print(f"[red]Unknown scheme {url_parsed.scheme}")
        return

    username = url_parsed.username
    password = url_parsed.password

    url_str = f"{url_parsed.scheme}://{url_parsed.hostname}{url_parsed.path}"

    ctx.ensure_object(dict)
    ctx.obj["query"] = query
    ctx.obj["url"] = url_str
    ctx.obj["collection"] = collection
    ctx.obj["username"] = username
    ctx.obj["password"] = password


@cli.command(name="remove-config")
@click.argument("name")
@click.pass_context
@coro
async def remove_config(ctx, name):
    ctx.ensure_object(dict)
    importer = Importer(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )
    try:
        confirm = input(f"Вы точно хотите удалить конфиг {name}").lower() == "y"
        if not confirm:
            return

        await importer.build_client()
        await importer._remove_config(name=name)
    finally:
        await importer.close_client()


@cli.command(name="import")
@click.argument("filepath")
@click.option("--batch", default=50)
@click.pass_context
@coro
async def import_data(ctx, filepath, batch):
    print(f"Importing data from [bold]{filepath}[/bold]")
    ctx.ensure_object(dict)
    importer = Importer(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )
    try:
        await importer.build_client()
        await importer.import_data(path=filepath, batch_size=batch)
    finally:
        await importer.close_client()


@cli.command(name="import-config")
@click.argument("directory")
@click.option(
    "--overwrite",
    is_flag=True,
    show_default=True,
    default=False,
    help="Перезаписать, если конфиг существует",
)
@click.pass_context
@click.option("--name", default=None)
@coro
async def import_config(ctx, directory, overwrite, name):
    ctx.ensure_object(dict)
    importer = Importer(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )
    try:
        await importer.build_client()
        await importer.import_configs(
            configs_path=directory, overwrite=overwrite, name=name
        )
    finally:
        await importer.close_client()


@cli.command(name="export")
@click.argument("directory")
@click.option(
    "--nested",
    is_flag=True,
    show_default=True,
    default=False,
    help="Загрузить вложенные документы",
)
@click.pass_context
@coro
async def export_data(ctx, directory, nested: bool):
    print(f"Экспорт данных в [bold]{directory}[/bold]")
    ctx.ensure_object(dict)
    if ctx.obj["collection"] is None:
        print("[bold red]Параметр collection должен быть задан")
        return

    exporter = Exporter(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )
    try:
        await exporter.build_client()
        await exporter.export_data(
            path=directory, query=ctx.obj["query"], nested=nested
        )
    finally:
        await exporter.close_client()


@cli.command(name="export-configs")
@click.argument("directory")
@click.pass_context
@coro
async def export_configs(ctx, directory):
    print(f"Экспорт конфигов в [bold]{directory}[/bold]")
    ctx.ensure_object(dict)
    exporter = Exporter(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )
    try:
        await exporter.build_client()
        await exporter.export_config(path=directory)
    finally:
        await exporter.close_client()


if __name__ == "__main__":
    cli()  # type: ignore
