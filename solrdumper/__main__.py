import click

from solrdumper.export import Exporter
from solrdumper.import_ import Importer


@click.group()
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
def cli(ctx, username: str, password: str, collection: str, url: str):
    ctx.ensure_object(dict)
    ctx.obj["url"] = url
    ctx.obj["password"] = password
    ctx.obj["username"] = username
    ctx.obj["collection"] = collection
    print(ctx.obj)


@cli.command(name="import")
@click.argument("filepath")
@click.pass_context
def import_data(ctx, filepath):
    click.echo(f"Importing data from {filepath}")
    ctx.ensure_object(dict)
    importer = Importer(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )

    importer.import_json(path=filepath)


@cli.command(name="import-config")
@click.argument("directory")
@click.pass_context
def import_config(ctx, directory):
    click.echo(f"Importing config from {directory}")
    ctx.ensure_object(dict)
    importer = Importer(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )

    importer.import_configs(configs_path=directory)


@cli.command(name="export")
@click.argument("directory")
@click.pass_context
def export_data(ctx, directory):
    click.echo(f"Exporting data to {directory}")
    ctx.ensure_object(dict)
    if ctx.obj["collection"] is None:
        raise ValueError("Параметр collection должен быть задан")
    exporter = Exporter(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )

    exporter.export(path=directory)


@cli.command(name="export-configs")
@click.argument("directory")
@click.pass_context
def export_configs(ctx, directory):
    click.echo(f"Exporting data to {directory}")
    ctx.ensure_object(dict)
    exporter = Exporter(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )

    exporter.export_config(path=directory)


if __name__ == "__main__":
    cli()
