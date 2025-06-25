import tomllib as toml
import os
from functools import lru_cache
from typing import Dict
import logging
import copy

logger = logging.getLogger(__name__)

default_zenodo: Dict[str, str] = {
    "ZENODO_ACCESS_TOKEN": "Change me",
    "ZENODO_SANDBOX_ACCESS_TOKEN": "Change me",
}

settings_name = ".zenodo-deposit-settings.toml"

def first_file_that_exists(files):
    for file in files:
        if os.path.exists(file):
            return file
    return None

def read_config_file(file: str = None) -> Dict[str, Dict[str, str]]:
    """
    Read the config file, if given, else look in the standard locations
    will throw an error if the config file is not found, or it is invalid TOML
    """
    logger.debug(f"Attempting to read config file: {file if file else 'default locations'}")
    if file:
        logger.info(f"Reading config file: {file}")
        with open(file, "rb") as f:
            config = toml.load(f)
            logger.debug(f"Loaded config: {config}")
            return config
    else:
        first_config = first_file_that_exists(
            [
                settings_name,
                os.path.expanduser(f"~/{settings_name}"),
            ]
        )
        if first_config:
            logger.info(f"Reading config file: {first_config}")
            with open(first_config, "rb") as f:
                config = toml.load(f)
                logger.debug(f"Loaded config: {config}")
                return config
    logger.debug("No config file found, using default_zenodo")
    return {"zenodo": copy.deepcopy(default_zenodo)}

@lru_cache(maxsize=32)
def config_section(
    config_file=None,
    section: str = "zenodo",
) -> Dict[str, str]:
    """
    Read a specific section from the configuration file, updating it with environment variables
    """
    logger.debug(f"Reading section '{section}' from config file: {config_file}")
    config = read_config_file(config_file)
    config_section = config.get(section)
    if not config_section:
        raise ValueError(f"Section {section} not found in the configuration file")
    config_section = copy.deepcopy(config_section)  # Prevent modifying original
    logger.debug(f"Config section before env update: {config_section}")
    for key in config_section.keys():
        if key in os.environ:
            config_section[key] = os.environ[key]
    logger.debug(f"Config section after env update: {config_section}")
    return config_section

def zenodo_config(config_file=None) -> Dict[str, str]:
    """
    Read the Zenodo configuration from the file (access keys)
    """
    return config_section(config_file, "zenodo")

def validate_zenodo_config(config: Dict[str, str], use_sandbox: bool = False) -> bool:
    """
    Validate the configuration.
    Ensure that the ZENODO_ACCESS_TOKEN or ZENODO_SANDBOX_ACCESS_TOKEN is set
    to a non-empty, non-default value.
    """
    logger.debug(f"Config module path: {__file__}")
    logger.debug(f"Full config before validation: {config}")
    logger.debug(f"Default zenodo config: {default_zenodo}")
    if use_sandbox:
        token = config.get("ZENODO_SANDBOX_ACCESS_TOKEN")
        logger.debug(f"ZENODO_SANDBOX_ACCESS_TOKEN raw: {repr(token)}")
        logger.debug(f"ZENODO_SANDBOX_ACCESS_TOKEN length: {len(token) if token else 0}")
        logger.debug(f"ZENODO_SANDBOX_ACCESS_TOKEN stripped: {token.strip() if token else ''}")
        if not token or token.strip() == "" or token.strip() == default_zenodo["ZENODO_SANDBOX_ACCESS_TOKEN"]:
            raise ValueError(
                f"ZENODO_SANDBOX_ACCESS_TOKEN is not set or invalid, sandbox being used. Config: {config}"
            )
    else:
        token = config.get("ZENODO_ACCESS_TOKEN")
        logger.debug(f"ZENODO_ACCESS_TOKEN raw: {repr(token)}")
        logger.debug(f"ZENODO_ACCESS_TOKEN length: {len(token) if token else 0}")
        logger.debug(f"ZENODO_ACCESS_TOKEN stripped: {token.strip() if token else ''}")
        if not token or token.strip() == "" or token.strip() == default_zenodo["ZENODO_ACCESS_TOKEN"]:
            raise ValueError(
                f"ZENODO_ACCESS_TOKEN is not set or invalid in production environment. Config: {config}"
            )
    logger.debug("Config validation passed")
    return True