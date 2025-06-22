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
    unique_dicts = {frozenset(d.items()): d for d in dict_list}.values()
    return list(unique_dicts)


DEFAULT_USE_SANDBOX = False  # Changed to default to production

rich_handler = RichHandler(rich_tracebacks=True)
rich_handler.console.stderr = True
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[rich_handler],
)

logger = logging.getLogger(__name__)

@click.group(context_settings={"show_default": True})
@click.version_option()
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
    """Zenodo Deposit CLI for uploading and managing depositions.

    Note: Fixed token reading bug (Issue #1) in config.py by using copy.deepcopy(default_zenodo)
    to prevent overriding default configuration, ensuring ZENODO_ACCESS_TOKEN and
    ZENODO_SANDBOX_ACCESS_TOKEN are correctly loaded from environment variables.
    """
    global logger
    if log_level:
        logging.getLogger().setLevel(log_level)

    logger.debug(f"Configuration loaded with sandbox={sandbox}")
    ctx.ensure_object(dict)
    ctx.obj["SANDBOX"] = sandbox

    if config_file:
        logging.info(f"Loading configuration from {config_file}")
    config = zenodo_deposit.config.zenodo_config(config_file=config_file)

    try:
        zenodo_deposit.config.validate_zenodo_config(config, use_sandbox=sandbox)
    except ValueError as e:
        raise click.ClickException("Invalid configuration: " + str(e))

    for key, value in config.items():
        logger.debug(f"Setting {key} to {hide_access_token(value)}")
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
    if metadata:
        metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
        ctx.obj["metadata"] = metadata_object


def debug(ctx, func):
    logging.info(f"Running {func.__name__}")


@cli.command(help="Create a new deposition, without uploading a file")
@click.option(
    "-m",
    "--metadata",
    default=None,
    help="Path to the metadata file",
    type=click.Path(),
)
@click.pass_context
def create(ctx, metadata):
    sandbox = ctx.obj["SANDBOX"]
    base_url = zenodo_url(sandbox)
    if metadata:
        metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
        ctx.obj["metadata"] = metadata_object
    results = zenodo_deposit.api.create_deposition(
        base_url,
        {
            "metadata": metadata_object,
            "config": ctx.obj,
        },
    )
    logging.info(f"Deposition created with ID: {results['id']}")
    print(json.dumps(results))


@cli.command(
    help="Upload one or more files, with metadata, creating a new deposit",
)
@click.option("--title", required=False, help="Title of the deposition")
@click.option("--description", required=False, help="Description of the deposition")
@click.option(
    "--variable",
    "-v",
    required=False,
    help="Variables for metadata, format: key:value",
    multiple=True,
)
@click.option(
    "--type",
    required=False,
    help="Upload type",
    type=click.Choice(zenodo_deposit.metadata.upload_types),
    default="dataset",
)
@click.option(
    "--keywords",
    "-k",
    required=False,
    help="Keyword(s) for the deposition",
    multiple=True,
)
@click.option(
    "--metadata",
    "-m",
    required=True,
    help="Path to the metadata file",
    type=click.Path(),
)
@click.option(
    "--publish/--no-publish",
    default=False,
    help="Publish the deposition after uploading",
)
@click.option(
    "--zip/--no-zip",
    default=False,
    help="Zip any directory before uploading",
    type=bool,
)
@click.argument("files", type=click.Path(), nargs=-1)
@click.pass_context
def upload(
    ctx, files, title, description, variable, type, keywords, metadata, publish, zip
):
    logger.debug(f"Upload command with sandbox={ctx.obj['SANDBOX']}")
    ctx.obj["title"] = title
    ctx.obj["description"] = description
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in flatten([k.split(",") for k in keywords])]
    for var in variable:
        key, value = var.split(":")
        ctx.obj[key] = value
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    logging.info(
        f"Uploading files: {files} to {zenodo_url(ctx.obj['SANDBOX'])} using token {hide_access_token(token)}"
    )
    logging.debug(f"Title: {title}")
    logging.debug(f"Type: {type}")
    logging.debug(f"Keywords: {keywords}")
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

    if not metadata_object.get("title"):
        raise ValueError("Title is required")
    if not metadata_object.get("creators"):
        raise ValueError("Creators are required")
    if not metadata_object.get("upload_type"):
        raise ValueError("Upload type is required")
    logging.debug(f"Metadata: {metadata_object}")
    results = zenodo_deposit.api.upload(
        paths=files,
        metadata=metadata_object,
        config=ctx.obj,
        sandbox=ctx.obj["SANDBOX"],
        publish=publish,
        zip=zip,
    )
    if publish:
        logging.info(f"Deposition published with ID: {results['id']}")
    else:
        logging.info(f"Deposition created with ID: {results['id']}")
    print(json.dumps(results))


@cli.command(help="Search for depositions")
@click.option("--query", required=True, help="Search query")
@click.option("--size", default=10, help="Number of results to return")
@click.option("--page", default=1, help="Page number")
@click.option("--sort", default="mostrecent", help="Sort order")
@click.option(
    "--status", default="all", help="Limit to depositions with a specific status"
)
@click.pass_context
def search(ctx, query, size, page, sort, status):
    results = zenodo_deposit.api.search(
        query=query,
        size=size,
        page=page,
        sort=sort,
        status=status,
        config=ctx.obj,
        sandbox=ctx.obj["SANDBOX"],
    )
    print(json.dumps(results))


if __name__ == "__main__":
    cli()