import click
from rich import print

from solar.api.export import Exporter
from solar.cli import cli, coro


@cli.command(name="export")
@click.argument("directory")
@click.option(
    "--nested",
    is_flag=True,
    show_default=True,
    default=False,
    help="Export nested documents. Default: False",
)
@click.pass_context
@coro
async def export_data(ctx, directory, nested: bool):
    """Export data from Solr"""
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


@cli.command(name="export-config")
@click.argument("directory")
@click.pass_context
@coro
async def export_configs(ctx, directory):
    """Export config from Solr"""
    if ctx.obj["collection"] is None:
        print("[red]-c is required")
        return

    print(f"Export config to [bold]{directory}")
    ctx.ensure_object(dict)
    exporter = Exporter(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )
    try:
        await exporter.build_client()
        await exporter.export_config(
            path=directory, collection_name=ctx.obj["collection"]
        )
        print("[green]Done!")
    finally:
        await exporter.close_client()
