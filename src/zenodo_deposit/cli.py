import logging.config
import click
import json
import zenodo_deposit.api
import zenodo_deposit.config
from zenodo_deposit.api import (
    zenodo_url,
    access_token,
)
import os
import logging
import zenodo_deposit.metadata
from rich.logging import RichHandler


def flatten(lists):
    def _flatten(lyst):
        for el in lyst:
            if isinstance(el, list):
                yield from _flatten(el)
            else:
                yield el

    return list(_flatten(lists))


def hide_access_token(token):
    return token[:4] + "*" * (len(token) - 4)


def get_unique_dicts(dict_list):
    # Convert each dictionary to a frozenset of items and use a set to remove duplicates
    unique_dicts = {frozenset(d.items()): d for d in dict_list}.values()
    # Convert the frozensets back to dictionaries
    return list(unique_dicts)


DEFAULT_USE_SANDBOX = True

rich_handler = RichHandler(rich_tracebacks=True)
rich_handler.console.stderr = (
    True  # zenodo_deposit emits json to stdout, so we want to keep it clean
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[rich_handler],
)

logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--sandbox/--production",
    "--dev/--prod",
    is_flag=True,
    default=DEFAULT_USE_SANDBOX,
    help="Set Zenodo environment to sandbox or production",
)
@click.option(
    "--config-file",
    default=None,
    help="Path to the configuration file",
    type=click.Path(),
)
@click.option(
    "--log-level",
    default=None,
    help="Set the log level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
@click.pass_context
def cli(ctx, sandbox, config_file, log_level):
    global logger
    if log_level:
        logging.getLogger().setLevel(log_level)

    logger.debug("Configuration loaded")
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj["SANDBOX"] = sandbox

    if config_file:
        logging.info(f"Loading configuration from {config_file}")
    config = zenodo_deposit.config.zenodo_config(config_file=config_file)

    try:
        zenodo_deposit.config.validate_zenodo_config(config, use_sandbox=sandbox)
    except ValueError as e:
        raise click.ClickException("Invalid configuration: " + str(e))

    # set all values in the config as attributes of the context object
    for key, value in config.items():
        logger.debug(f"Setting {key} to {value}")
        ctx.obj[key] = value


@cli.command(help="Retrieve deposition details")
@click.argument("deposition_id", type=int)
@click.pass_context
def retrieve(ctx, deposition_id):
    logging.info(f"Retrieving details for deposition: {deposition_id}")
    results = zenodo_deposit.api.get_deposition(
        deposition_id, config=ctx.obj, sandbox=ctx.obj["SANDBOX"]
    )
    print(json.dumps(results))


@cli.command(help="Deposit a file")
@click.option("--title", required=False, help="Title of the deposition")
@click.option(
    "--type",
    required=False,
    help="Upload type",
    type=click.Choice(zenodo_deposit.metadata.upload_types),
)
@click.option(
    "--keywords",
    "-k",
    required=False,
    help="Keyword(s) for the deposition",
    multiple=True,
)
@click.option(
    "--name",
    required=False,
    type=str,
    help="Name of the depositor in last,first format",
    default=None,
)
@click.option(
    "--affiliation",
    required=False,
    type=str,
    help="Affiliation of the depositor",
    default=None,
)
@click.option(
    "--metadata",
    default=None,
    help="Path to the metadata file",
    type=click.Path(),
)
@click.argument("file", type=click.Path())
@click.pass_context
def deposit(ctx, file, title, type, keywords, name, affiliation, metadata):
    path = os.path.abspath(file)
    ctx.obj["title"] = title
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in flatten([k.split(",") for k in keywords])]
    ctx.obj["name"] = name
    ctx.obj["affiliation"] = affiliation
    logging.info(f"Depositing file: {path}")
    logging.debug(f"Title: {title}")
    logging.debug(f"Type: {type}")
    logging.debug(f"Keywords: {keywords}")
    # Create a metatdata dictionary
    if metadata:
        metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
        ctx.obj["metadata"] = metadata_object


def debug(ctx, func):
    logging.info(f"Running {func.name}")


@cli.command(help="Create a new deposition, without uploading a file")
@click.pass_context
def create(ctx):
    debug(ctx, create)


@cli.command(help="Publish the deposition")
@click.pass_context
def publish(ctx):
    debug(ctx, publish)


@cli.command(help="Delete the deposition")
@click.pass_context
def delete(ctx):
    debug(ctx, delete)


@cli.command(help="Update the metadata of the deposition")
@click.pass_context
def update_metadata(ctx):
    debug(ctx, update_metadata)


@cli.command(help="Add metadata to the deposition")
@click.pass_context
def add_metadata(ctx):
    debug(ctx, add_metadata)


@cli.command(help="Upload one or more files, with metadata, creating a new deposit")
@click.option("--title", required=False, help="Title of the deposition")
@click.option("--description", required=False, help="Description of the deposition")
@click.option(
    "--type",
    required=False,
    help="Upload type",
    type=click.Choice(zenodo_deposit.metadata.upload_types),
)
@click.option(
    "--keywords",
    "-k",
    required=False,
    help="Keyword(s) for the deposition",
    multiple=True,
)
@click.option(
    "--name",
    required=False,
    type=str,
    help="Name of the depositor in last,first format",
    default=None,
)
@click.option(
    "--affiliation",
    required=False,
    type=str,
    help="Affiliation of the depositor",
    default=None,
)
@click.option(
    "--metadata",
    default=None,
    help="Path to the metadata file",
    type=click.Path(),
)
@click.option(
    "--publish/--no-publish",
    default=True,
    help="Publish the deposition after uploading",
)
@click.argument("files", type=click.Path(), nargs=-1)
@click.pass_context
def upload(
    ctx, files, title, description, type, keywords, name, affiliation, metadata, publish
):
    ctx.obj["title"] = title
    ctx.obj["description"] = description
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in flatten([k.split(",") for k in keywords])]
    ctx.obj["name"] = name
    ctx.obj["affiliation"] = affiliation
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    logging.info(
        f"Uploading files: {files} to {zenodo_url(ctx.obj['SANDBOX'])} using token {hide_access_token(token)}"
    )
    logging.debug(f"Title: {title}")
    logging.debug(f"Type: {type}")
    logging.debug(f"Keywords: {keywords}")
    # Create a metatdata dictionary
    metadata_object = None
    if metadata:
        metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
    else:
        metadata_object = {}

    if title:
        metadata_object["title"] = title
    if description:
        metadata_object["description"] = description
    if type:
        metadata_object["upload_type"] = type
    if keywords:
        current_keywords = metadata_object.get("keywords", [])
        metadata_object["keywords"] = list(current_keywords) + list(keywords)
    if name or affiliation:
        current_creators = metadata_object.get("creators", [])
        new_creator = {"name": name, "affiliation": affiliation}
        new_creators = current_creators + [new_creator]
        # make sure the creators are unique
        metadata_object["creators"] = get_unique_dicts(new_creators)

    # validate
    if not metadata_object.get("title"):
        raise ValueError("Title is required")
    if not metadata_object.get("creators"):
        raise ValueError("Creators are required")
    if not metadata_object.get("upload_type"):
        raise ValueError("Upload type is required")
    logging.debug(f"Metadata: {metadata_object}")
    results = zenodo_deposit.api.upload(
        files=files,
        metadata=metadata_object,
        config=ctx.obj,
        sandbox=ctx.obj["SANDBOX"],
        publish=publish,
    )
    if publish:
        logging.info(f"Deposition published with ID: {results['id']}")
    else:
        logging.info(f"Deposition created with ID: {results['id']}")
    print(json.dumps(results))


@cli.command(
    help="Create a new version of a deposition, with additional or updated files"
)
@click.option(
    "--publish/--no-publish",
    default=True,
    help="Publish the deposition after uploading",
)
@click.argument("deposition_id", type=int)
@click.argument("files", type=click.Path(), nargs=-1)
@click.pass_context
def new_version(ctx, deposition_id, files, publish):
    logger.critical("Not implemented")


if __name__ == "__main__":
    cli()
