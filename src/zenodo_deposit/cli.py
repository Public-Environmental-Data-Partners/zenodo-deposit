import logging
import click
import json
import requests
import zenodo_deposit.api
import zenodo_deposit.config
from zenodo_deposit.api import zenodo_url, access_token
import os
import zenodo_deposit.metadata
from rich.logging import RichHandler

logger = logging.getLogger(__name__)

def flatten(lists):
    """
    Flatten a list of lists into a single list.

    Args:
        lists: A list of lists to flatten.

    Returns:
        List: A flattened list.
    """
    def _flatten(lyst):
        for el in lyst:
            if isinstance(el, list):
                yield from _flatten(el)
            else:
                yield el

    return list(_flatten(lists))


def hide_access_token(token):
    """
    Hide all but the first 4 characters of an access token for logging.

    Args:
        token: The access token to hide.

    Returns:
        str: The hidden token.
    """
    return token[:4] + "*" * (len(token) - 4) if token else str(None)


def get_unique_dicts(dict_list):
    """
    Remove duplicate dictionaries from a list.

    Args:
        dict_list: List of dictionaries.

    Returns:
        List: List of unique dictionaries.
    """
    unique_dicts = {frozenset(d.items()): d for d in dict_list}.values()
    # Convert the frozensets back to dictionaries
    return list(unique_dicts)


DEFAULT_USE_SANDBOX = True

rich_handler = RichHandler(rich_tracebacks=True)
rich_handler.console.stderr = True
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[rich_handler],
)


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
    """
    Zenodo Deposit CLI for uploading and managing depositions.

    Args:
        ctx: Click context object to store configuration.
        sandbox: Flag to use Zenodo sandbox or production (default: sandbox).
        config_file: Path to the configuration file.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Raises:
        click.ClickException: If the configuration is invalid.
    """
    global logger
    if log_level:
        logging.getLogger().setLevel(log_level)

    logger.debug(f"Configuration loaded with sandbox={sandbox}")
    ctx.ensure_object(dict)
    ctx.obj["SANDBOX"] = sandbox

    if config_file:
        logger.info(f"Loading configuration from {config_file}")
    config = zenodo_deposit.config.zenodo_config(config_file=config_file)

    try:
        zenodo_deposit.config.validate_zenodo_config(config, use_sandbox=sandbox)
    except ValueError as e:
        raise click.ClickException(f"Invalid configuration: {str(e)}")

    for key, value in config.items():
        logger.debug(f"Setting {key} to {hide_access_token(value)}")
        ctx.obj[key] = value


@cli.command(help="Retrieve deposition details")
@click.argument("deposition_id", type=int)
@click.pass_context
def retrieve(ctx, deposition_id):
    """
    Retrieve details of a Zenodo deposition by ID.

    Args:
        ctx: Click context object containing configuration.
        deposition_id: The ID of the deposition to retrieve.

    Raises:
        click.ClickException: If the access token is missing or the API request fails.
    """
    logger.info(f"Retrieving details for deposition: {deposition_id}")
    try:
        results = zenodo_deposit.api.get_deposition(
            deposition_id=deposition_id, config=ctx.obj, sandbox=ctx.obj["SANDBOX"]
        )
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to retrieve deposition: {error_msg}")


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
    default="",
    help="Keyword(s) for the deposition",
)
@click.option(
    "--name",
    required=False,
    default="",
    type=str,
    help="Name of the depositor in last, first format",
)
@click.option(
    "--affiliation",
    required=False,
    default="",
    type=str,
    help="Type of the depositor",
)
@click.option(
    "--metadata",
    default=None,
    help="Path to the metadata file",
    type=click.Path(),
)
@click.argument("file", type=click.Path(exists=True))
@click.pass_context
def deposit(ctx, title, type, keywords, name, affiliation, metadata):
    """
    Deposit a file to a new Zenodo deposition with metadata.

    Args:
        ctx: Click context object containing configuration.
        file: The path to the file to upload.
        title: Title of the deposition.
        type: Upload type (e.g., dataset, publication).
        keywords: Keyword(s) for the deposition.
        name: Name of the depositor in last, first format.
        affiliation: Type of the depositor.
        metadata: Path to the metadata TOML file.

    Raises:
        click.ClickException: If the file is invalid or the API request fails.
    """
    path = os.path.abspath(file) # noqa: F821
    ctx.obj["title"] = title
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in keywords.split(",") if x.strip()]
    ctx.obj["name"] = name
    ctx.obj["affiliation"] = affiliation
    logger.info(f"Depositing file: {path}")
    logger.debug(f"Title: {title}")
    logger.debug(f"Type: {type}")
    logger.debug(f"Keywords: {keywords}")
    if metadata:
        metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
        ctx.obj["metadata"] = metadata_object

@cli.command(help="Create a new deposition without uploading a file")
@click.option("--title", required=False, help="Title of the deposition (overrides metadata)")
@click.option("--description", required=False, default="", help="Description of the deposition")
@click.option(
    "--variable",
    "-v",
    multiple=True,
    help="Variables for metadata substitution, format: key=value or key:val",
)
@click.option(
    "--type",
    required=False,
    default="dataset",
    help="Upload type",
    type=click.Choice(zenodo_deposit.metadata.upload_types),
)
@click.option(
    "--keywords",
    "-k",
    multiple=True,
    help="Keyword(s) for the deposition",
)
@click.option(
    "--metadata",
    "-m",
    default=None,
    help="Path to metadata file",
    type=click.Path(exists=True),
)
@click.pass_context
def create(ctx, title, description, variable, type, keywords, metadata):
    """
    Create a new Zenodo deposition without uploading a file, with optional metadata.

    Args:
        ctx: The context object containing configuration.
        title: The title for the deposition (overrides metadata).
        description: The description for the deposition.
        variable: Variables for metadata substitution (format: key=value or key:val).
        type: Upload type (e.g., dataset, publication).
        keywords: List of keywords for the deposition.
        metadata: Path to the metadata TOML file.

    Raises:
        click.ClickException: If the token is missing, metadata is invalid, or the API request fails.
    """
    logger.info("Creating new deposition")
    sandbox = ctx.obj["SANDBOX"]
    base_url = zenodo_url(sandbox)
    token = access_token(ctx.obj, sandbox)
    if not token:
        raise click.ClickException("Access token is missing")
    params = {"access_token": token}
    ctx.obj["title"] = title
    ctx.obj["description"] = description
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in keywords]
    try:
        for var in variable:
            # Try splitting on '=' first, then ':'
            if '=' in var:
                key, value = var.split("=", 1)
            elif ':' in var:
                key, value = var.split(":", 1)
            else:
                raise ValueError(f"Invalid variable format: {var}")
            logger.debug(f"Variable {key} = {value}")
            ctx.obj[key] = value
    except ValueError as e:
        raise click.ClickException(f"Invalid variable format, expected 'key=value' or 'key:val': {str(e)}")
    metadata_object = {}
    if metadata:
        metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
    if title:
        metadata_object["title"] = title
    if description:
        metadata_object["description"] = description
    if type:
        metadata_object["upload_type"] = type
    if keywords:
        current_keywords = metadata_object.get("keywords", [])
        metadata_object["keywords"] = list(set(current_keywords + list(keywords)))
    if not metadata_object.get("title"):
        raise click.ClickException("Metadata must include title, either via --title or metadata file")
    if not metadata_object.get("creators"):
        raise click.ClickException("Metadata must include creators")
    try:
        results = zenodo_deposit.api.create_deposition(base_url, params)
        if metadata_object:
            results = zenodo_deposit.api.add_metadata(base_url, results["id"], metadata_object, params)
        logger.info(f"Deposition created with ID: {results['id']}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to create deposition: {error_msg}")

@cli.command(help="Publish an existing deposition")
@click.argument("deposition_id", type=int)
@click.pass_context
def publish(ctx, deposition_id):
    """
    Publish a Zenodo deposition by ID.

    Args:
        ctx: The context object containing configuration.
        deposition_id: The ID of the deposition to publish.

    Raises:
        click.ClickException: If the token is missing or the API request fails.
    """
    logger.info(f"Publishing deposition: {deposition_id}")
    base_url = zenodo_url(ctx.obj["SANDBOX"])
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    if not token:
        raise click.ClickException("Access token is missing")
    params = {"access_token": token}
    try:
        results = zenodo_deposit.api.publish_deposition(base_url, deposition_id, params)
        logger.info(f"Deposition published with ID: {deposition_id}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to publish: {error_msg}")

@cli.command(help="Delete a draft deposition")
@click.argument("deposition_id", type=int)
@click.pass_context
def delete(ctx, deposition_id):
    """
    Delete a Zenodo draft deposition by ID.

    Args:
        ctx: The context object containing configuration.
        deposition_id: The ID of the deposition to delete.

    Raises:
        click.ClickException: If the token is missing or the API request fails.
    """
    logger.debug(f"Deleting deposition: {deposition_id}")
    base_url = zenodo_url(ctx.obj["SANDBOX"])
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    if not token:
        raise click.ClickException("Token missing")
    params = {"access_token": token}
    try:
        results = zenodo_deposit.api.delete_deposition(base_url, deposition_id, params)
        logger.debug(f"Successfully deleted deposition with ID: {deposition_id}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to delete: {error_msg}")

@cli.command("update_metadata", help="Update metadata for an existing deposition")
@click.argument("deposition_id", type=int)
@click.option(
    "-m",
    "--metadata",
    required=True,
    help="Path to metadata file",
    type=click.Path(exists=True),
)
@click.pass_context
def update_metadata(ctx, deposition_id, metadata):
    """
    Update metadata for a Zenodo deposition by ID, overwriting existing metadata.

    Args:
        ctx: The context object containing configuration.
        deposition_id: The ID of the deposition to update.
        metadata: Path to the metadata TOML file.

    Raises:
        click.ClickException: If the token is missing, metadata is invalid, or the API request fails.
    """
    logger.debug(f"Updating metadata for deposition: {deposition_id}")
    base_url = zenodo_url(ctx.obj["SANDBOX"])
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    if not token:
        raise click.ClickException("Token missing")
    params = {"access_token": token}
    metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
    if not metadata_object.get("title"):
        raise click.ClickException("Metadata must include title")
    if not metadata_object.get("creators"):
        raise click.ClickException("Metadata must include creators")
    try:
        results = zenodo_deposit.api.update_metadata(base_url, deposition_id, metadata_object, params)
        logger.debug(f"Metadata updated for deposition ID: {deposition_id}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to update metadata: {error_msg}")

@cli.command("add_metadata", help="Add metadata to an existing deposition, without overwriting existing metadata")
@click.argument("deposition_id", type=int)
@click.option(
    "-m",
    "--metadata",
    required=True,
    help="Path to metadata file",
    type=click.Path(exists=True),
)
@click.pass_context
def add_metadata(ctx, deposition_id, metadata):
    """
    Add metadata to a Zenodo deposition by ID, merging with existing metadata.

    Args:
        ctx: The context object containing configuration.
        deposition_id: The ID of the deposition to update.
        metadata: Path to the metadata TOML file.

    Raises:
        click.ClickException: If the token is missing, metadata is invalid, or the API request fails.
    """
    logger.debug(f"Adding metadata to deposition {deposition_id}")
    base_url = zenodo_url(ctx.obj["SANDBOX"])
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    if not token:
        raise click.ClickException("Access token missing")
    params = {"access_token": token}
    metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
    if not metadata_object.get("title"):
        raise click.ClickException("Metadata must include title")
    if not metadata_object.get("creators"):
        raise click.ClickException("Metadata must include creators")
    try:
        results = zenodo_deposit.api.add_metadata(base_url, deposition_id, metadata_object, params)
        logger.debug(f"Metadata added to deposition ID: {deposition_id}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to add metadata: {error_msg}")

@cli.command(help="Upload one or more files, creating a new deposition with metadata")
@click.option("--title", required=False, help="Title of the deposition")
@click.option("--description", required=False, help="Description of the deposition")
@click.option(
    "--variable",
    "-v",
    multiple=True,
    help="Variables for metadata, format: key=value or key:val",
)
@click.option(
    "--type",
    required=False,
    default="dataset",
    help="Upload type",
    type=click.Choice(zenodo_deposit.metadata.upload_types),
)
@click.option(
    "--keywords",
    "-k",
    multiple=True,
    help="Keyword(s) for the deposition",
)
@click.option(
    "--metadata",
    "-m",
    required=True,
    help="Path to metadata file",
    type=click.Path(exists=True),
)
@click.option(
    "--publish/--no-publish",
    default=False,
    is_flag=True,
    help="Publish after uploading",
)
@click.option(
    "--zip/--no-zip",
    default=False,
    is_flag=True,
    help="Zip directories before uploading",
)
@click.argument("files", type=click.Path(exists=True, file_okay=True, dir_okay=True), nargs=-1)
@click.pass_context
def upload(ctx, title, description, variable, type, keywords, metadata, publish, zip, files):
    """
    Upload one or more files to a new Zenodo deposition with metadata.

    Args:
        ctx: The context object containing configuration.
        files: List of paths to files to upload.
        title: Title of the deposition.
        description: Description of the deposition.
        variable: Variables for metadata substitution (format: key=value or key:val).
        type: Upload type (e.g., dataset, publication).
        keywords: List of keywords for the deposition.
        metadata: Path to the metadata file.
        publish: Flag to publish after uploading.
        zip: Flag to zip directories before uploading.

    Raises:
        click.ClickException: If no files are provided, metadata is invalid, or the API request fails.
    """
    if not files:
        raise click.ClickException("At least one file must be specified")
    logger.debug(f"Uploading files with sandbox={ctx.obj['SANDBOX']} {ctx.obj}")
    ctx.obj["title"] = title
    ctx.obj["description"] = description
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in keywords]
    try:
        for var in variable:
            # Try splitting on '=' first, then ':'
            if '=' in var:
                key, value = var.split("=", 1)
            elif ':' in var:
                key, value = var.split(":", 1)
            else:
                raise ValueError(f"Invalid variable format: {var}")
            logger.debug(f"Variable {key} = {value}")
            ctx.obj[key] = value
    except ValueError as e:
        raise click.ClickException(f"Invalid variable format, expected 'key=value' or 'key:val': {str(e)}")
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    logger.info(
        f"Uploading {files} to {zenodo_url(ctx.obj['SANDBOX'])} using token {hide_access_token(token)}"
    )
    logger.debug(f"Metadata: {title}")
    logger.debug(f"Type: {type}")
    logger.debug(f"Keywords: {keywords}")
    metadata_object = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
    if title:
        metadata_object["title"] = title
    if description:
        metadata_object["description"] = description
    if type:
        metadata_object["upload_type"] = type
    if keywords:
        current_keywords = metadata_object.get("keywords", [])
        metadata_object["keywords"] = list(set(current_keywords + list(keywords)))
    if not metadata_object.get("title"):
        raise click.ClickException("Metadata must include title")
    if not metadata_object.get("creators"):
        raise click.ClickException("Metadata must include creators")
    if not metadata_object.get("upload_type"):
        raise click.ClickException("Metadata must include upload_type")
    logger.debug(f"Metadata object: {metadata_object}")
    try:
        results = zenodo_deposit.api.upload(
            paths=files,
            metadata=metadata_object,
            config=ctx.obj,
            sandbox=ctx.obj["SANDBOX"],
            publish=publish,
            zip=zip,
        )
        if publish:
            logger.info(f"Deposition published with ID: {results['id']}")
        else:
            logger.info(f"Deposition created with ID: {results['id']}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to upload files: {error_msg}")

@cli.command("new_version", help="Create a new version of an existing deposition")
@click.argument("deposition_id", type=int)
@click.option("--title", required=False, default=None, help="Title of the new version")
@click.option("--description", required=False, default=None, help="Description of the new version")
@click.option(
    "--variable",
    "-v",
    multiple=True,
    help="Variables for metadata, format: key=value or key:val",
)
@click.option(
    "--type",
    required=False,
    default="dataset",
    help="Upload type",
    type=click.Choice(zenodo_deposit.metadata.upload_types),
)
@click.option(
    "--keywords",
    "-k",
    multiple=True,
    help="Keyword(s) for the new version",
)
@click.option(
    "--metadata",
    "-m",
    default=None,
    help="Path to metadata file",
    type=click.Path(exists=True),
)
@click.option(
    "--publish/--no-publish",
    default=False,
    help="Publish after uploading",
)
@click.option(
    "--zip/--no-zip",
    default=False,
    help="Zip directories before uploading",
)
@click.argument("files", type=click.Path(exists=True, file_okay=True, dir_okay=True), nargs=-1)
@click.pass_context
def new_version(ctx, deposition_id, title, description, variable, type, keywords, metadata, publish, zip, files):
    """
    Create a new version of an existing Zenodo deposition, uploading additional or updated files.

    Args:
        ctx: The context object containing configuration.
        deposition_id: The ID of the existing deposition.
        files: List of paths to files to upload.
        title: Title of the new version.
        description: Description of the new version.
        variable: Variables for metadata substitution (format: key=value or key:val).
        type: Upload type (e.g., dataset, publication).
        keywords: List of keywords for the new version.
        metadata: Path to the metadata TOML file.
        publish: Flag to publish after uploading.
        zip: Flag to zip directories before uploading.

    Raises:
        click.ClickException: If the token is missing, deposition_id is invalid, or the API request fails.
    """
    if not files:
        raise click.ClickException("At least one file must be specified for new version")
    logger.debug(f"Creating new version for deposition: {deposition_id}")
    base_url = zenodo_url(ctx.obj["SANDBOX"])
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    if not token:
        raise click.ClickException("Access token missing")
    params = {"access_token": token}
    try:
        base_deposition = zenodo_deposit.api.get_deposition(deposition_id, ctx.obj, ctx.obj["SANDBOX"])
        base_metadata = base_deposition.get("metadata", {})
        logger.debug(f"Base deposition metadata: {base_metadata}")
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to retrieve base deposition: {error_msg}")
    if not base_metadata.get("title"):
        raise click.ClickException("Base deposition must have title")
    if not base_metadata.get("creators"):
        raise click.ClickException("Base deposition must have creators")
    if not base_metadata.get("upload_type"):
        raise click.ClickException("Base deposition must have an upload type")
    ctx.obj["title"] = title
    ctx.obj["description"] = description
    ctx.obj["upload_type"] = type
    ctx.obj["keywords"] = [x.strip() for x in keywords]
    try:
        for var in variable:
            # Try splitting on '=' first, then ':'
            if '=' in var:
                key, value = var.split("=", 1)
            elif ':' in var:
                key, value = var.split(":", 1)
            else:
                raise ValueError(f"Invalid variable format: {var}")
            logger.debug(f"Variable {key} = {value}")
            ctx.obj[key] = value
    except ValueError as e:
        raise click.ClickException(f"Invalid variable format, expected 'key=value' or 'key:val': {str(e)}")
    metadata_object = base_metadata.copy()
    if metadata:
        new_metadata = zenodo_deposit.metadata.metadata_from_toml(metadata, ctx.obj)
        metadata_object.update(new_metadata)
    if title:
        metadata_object["title"] = title
    if description:
        metadata_object["description"] = description
    if type:
        metadata_object["upload_type"] = type
    if keywords:
        current_keywords = metadata_object.get("keywords", [])
        metadata_object["keywords"] = list(set(current_keywords + list(keywords)))
    if not metadata_object.get("title"):
        raise click.ClickException("Title required")
    if not metadata_object.get("creators"):
        raise click.ClickException("Creators required")
    if not metadata_object.get("upload_type"):
        raise click.ClickException("Upload type required")
    logger.debug(f"New version metadata: {metadata_object}")
    try:
        new_version_data = zenodo_deposit.api.create_new_version(
            base_url, deposition_id, params, ctx.obj, ctx.obj["SANDBOX"], files_to_add=files, zip=zip
        )
        new_deposition_id = new_version_data["links"]["latest_draft"].split("/")[-1]
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to create new version: {error_msg}")
    try:
        zenodo_deposit.api.update_metadata(base_url, new_deposition_id, metadata_object, params)
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to update metadata: {error_msg}")
    try:
        if publish:
            results = zenodo_deposit.api.publish_deposition(base_url, new_deposition_id, params)
            logger.info(f"New version published with ID: {new_deposition_id}")
        else:
            results = zenodo_deposit.api.get_deposition(
                deposition_id=int(new_deposition_id), config=ctx.obj, sandbox=ctx.obj["SANDBOX"]
            )
            logger.info(f"New version created as draft with ID: {new_deposition_id}")
        print(json.dumps(results))
    except requests.exceptions.HTTPError as e:
        error_msg = e.response.json().get("message", str(e)) if e.response else str(e)
        raise click.ClickException(f"Failed to finalize operation: {error_msg}")



@cli.command(help="Add tags to an existing deposition")
@click.argument("deposition_id", type=int)
@click.option(
    "-k",
    "--keywords",
    required=True,
    multiple=True,
    help="Keyword(s) to add to the deposition",
)
@click.pass_context
def tag(ctx, deposition_id, keywords):
    """
    Add tags (keywords) to a Zenodo deposition by ID.

    Args:
        ctx: Click context object containing configuration.
        deposition_id (int): The ID of the deposition.
        keywords (tuple): Keywords to add to the deposition.

    Returns:
        None: Outputs the updated deposition metadata as JSON.
    """
    logging.info(f"Adding tags to deposition: {deposition_id}")
    base_url = zenodo_url(ctx.obj["SANDBOX"])
    token = access_token(ctx.obj, ctx.obj["SANDBOX"])
    if not token:
        raise click.ClickException("Access token is missing in the configuration")
    params = {"access_token": token}
    deposition = zenodo_deposit.api.get_deposition(deposition_id, ctx.obj, ctx.obj["SANDBOX"])
    metadata = deposition.get("metadata", {})
    current_keywords = metadata.get("keywords", [])
    metadata["keywords"] = list(set(current_keywords + list(keywords)))
    results = zenodo_deposit.api.update_metadata(base_url, deposition_id, metadata, params)
    logging.info(f"Tags added to deposition ID: {deposition_id}")
    print(json.dumps(results))


@cli.command(help="Search depositions based on a query string")
@click.argument("query", required=True)
@click.option("--size", default=10, help="Number of results to return")
@click.option("--page", default=1, help="Page number for pagination")
@click.option("--sort", default="mostrecent", help="Sort order (e.g., mostrecent, bestmatch)")
@click.option(
    "--status",
    default="all",
    help="Filter by deposition status (e.g., draft, published, all)",
)
@click.pass_context
def search(ctx, query, size, page, sort, status):
    """
    Search depositions based on a query string.

    Args:
        ctx: The context object containing configuration.
        query: Search query string (required).
        size: Number of results to return.
        page: Page number for pagination.
        sort: Sort order (e.g., mostrecent, bestmatch).
        status: Filter by deposition status (e.g., draft, published, all).

    Raises:
        click.ClickException: If the token is missing or the API request fails.
    """
    try:
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
    except (requests.exceptions.HTTPError, ValueError) as e:
        error_msg = e.response.json().get("message", str(e)) if hasattr(e, 'response') and e.response else str(e)
        raise click.ClickException(f"Failed to search depositions: {error_msg}")

if __name__ == "__main__":
    cli()