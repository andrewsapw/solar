import click

from solar.api.import_ import Importer
from solar.cli import cli, coro


@cli.command(name="import")
@click.argument("filepath")
@click.option("--batch", help="Batch size to import docs with. Default: 50", default=50)
@click.pass_context
@coro
async def import_data(ctx, filepath, batch):
    """Import data to Solr"""
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
    except Exception:
        return
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
    """Import config to Solr"""
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
    except Exception:
        return
    finally:
        await importer.close_client()
