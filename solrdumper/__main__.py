import click

from solrdumper.import_ import Importer
from solrdumper.export import Exporter


@click.group()
@click.option("-u", "--username", help="Solr username", default=None)
@click.option(
    "-p",
    "--password",
    # prompt="Solr user password",
    help="Solr user password",
    default=None,
)
@click.argument("URL", nargs=1)
@click.argument("COLLECTION")
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


@cli.command(name="export")
@click.argument("directory")
@click.pass_context
def export_data(ctx, directory):
    click.echo(f"Exporting data to {directory}")
    ctx.ensure_object(dict)
    exporter = Exporter(
        base_url=ctx.obj["url"],
        collection=ctx.obj["collection"],
        username=ctx.obj["username"],
        password=ctx.obj["password"],
    )

    exporter.export(path=directory)


if __name__ == "__main__":
    cli()
