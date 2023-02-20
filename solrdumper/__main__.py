import asyncio
from functools import wraps

import click
from rich import print

from solrdumper.export import Exporter
from solrdumper.import_ import Importer


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))

    return wrapper


@click.group()
@click.option("-q", "--query", help="Solr query", default="*:*")
@click.option("-u", "--username", help="Solr username", default=None)
@click.option(
    "-p",
    "--password",
    help="Solr user password",
    default=None,
)
@click.argument("URL", nargs=1)
@click.option("-c", "--collection", default=None)
@click.pass_context
def cli(ctx, query: str, username: str, password: str, collection: str, url: str):
    ctx.ensure_object(dict)
    ctx.obj["query"] = query
    ctx.obj["url"] = url
    ctx.obj["password"] = password
    ctx.obj["username"] = username
    ctx.obj["collection"] = collection


@cli.command(name="import")
@click.argument("filepath")
@click.pass_context
@coro
async def import_data(ctx, filepath):
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
        await importer.import_data(path=filepath)
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
        await exporter.export_data(path=directory, query=ctx.obj["query"])
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
