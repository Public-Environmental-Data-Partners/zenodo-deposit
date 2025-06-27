import tomllib as toml
import os
import re
from functools import lru_cache
from typing import Dict, Optional
import logging
import copy

logger = logging.getLogger(__name__)

default_zenodo: Dict[str, str] = {
    "ZENODO_ACCESS_TOKEN": "Change me",
    "ZENODO_SANDBOX_ACCESS_TOKEN": "Change me",
}

settings_name = ".zenodo-deposit-settings.toml"

def first_file_that_exists(files):
    """
    Return the first file that exists from a list of files.

    Args:
        files: List of file paths to check.

    Returns:
        str: Path to the first existing file, or None if none exist.
    """
    for file in files:
        if os.path.exists(file):
            return file
    return None

def read_config_file(file: Optional[str] = None) -> Dict[str, Dict[str, str]]:
    """
    Read the config file, if given, else look in standard locations.
    Raises an error if the config file is not found or is invalid TOML.

    Args:
        file: Path to the configuration file (TOML).

    Returns:
        Dict[str, Dict[str, str]]: Configuration dictionary.

    Raises:
        ValueError: If the config file is invalid or cannot be read.
    """
    logger.debug(f"Attempting to read config file: {file if file else 'default locations'}")
    if file:
        logger.info(f"Reading config file: {file}")
        try:
            with open(file, "rb") as f:
                config = toml.load(f)
                logger.debug(f"Loaded config: {config}")
                return config
        except Exception as e:
            logger.error(f"Failed to load config file {file}: {e}")
            raise ValueError(f"Invalid config file: {e}")
    else:
        first_config = first_file_that_exists(
            [
                settings_name,
                os.path.expanduser(f"~/{settings_name}"),
            ]
        )
        if first_config:
            logger.info(f"Reading config file: {first_config}")
            try:
                with open(first_config, "rb") as f:
                    config = toml.load(f)
                    logger.debug(f"Loaded config: {config}")
                    return config
            except Exception as e:
                logger.error(f"Failed to load config file {first_config}: {e}")
                raise ValueError(f"Invalid config file: {e}")
    logger.debug("No config file found, using default_zenodo")
    return {"zenodo": copy.deepcopy(default_zenodo)}

@lru_cache(maxsize=32)
def config_section(
    config_file: Optional[str] = None,
    section: str = "zenodo",
) -> Dict[str, str]:
    """
    Read a specific section from the configuration file, updating it with environment variables.

    Args:
        config_file: Path to the configuration file (TOML).
        section: Section of the config file to read (default: 'zenodo').

    Returns:
        Dict[str, str]: Configuration section dictionary.

    Raises:
        ValueError: If the section is not found in the configuration.
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

def zenodo_config(config_file: Optional[str] = None) -> Dict[str, str]:
    """
    Read the Zenodo configuration from the file (access keys).

    Args:
        config_file: Path to the configuration file (TOML).

    Returns:
        Dict[str, str]: Zenodo configuration dictionary.
    """
    return config_section(config_file, "zenodo")

def validate_zenodo_config(config: Dict[str, str], use_sandbox: bool = False) -> bool:
    """
    Validate the Zenodo configuration.
    Ensure that the ZENODO_ACCESS_TOKEN or ZENODO_SANDBOX_ACCESS_TOKEN is set
    to a non-empty, non-default value and matches expected format.

    Args:
        config: Configuration dictionary.
        use_sandbox: Whether to validate sandbox or production token.

    Returns:
        bool: True if validation passes.

    Raises:
        ValueError: If the required token is missing, invalid, or doesn't match expected format.
    """
    logger.debug(f"Config module path: {__file__}")
    logger.debug(f"Full config before validation: {config}")
    logger.debug(f"Default zenodo config: {default_zenodo}")
    token_key = "ZENODO_SANDBOX_ACCESS_TOKEN" if use_sandbox else "ZENODO_ACCESS_TOKEN"
    token = config.get(token_key)
    logger.debug(f"{token_key} raw: {repr(token)}")
    logger.debug(f"{token_key} length: {len(token) if token else 0}")
    logger.debug(f"{token_key} stripped: {token.strip() if token else ''}")

    if not token or token.strip() == "":
        raise ValueError(
            f"{token_key} is not set or empty. Set it in the config file or environment variable. "
            f"Generate a new token at {'https://sandbox.zenodo.org/account/settings/tokens/' if use_sandbox else 'https://zenodo.org/account/settings/tokens/'}"
        )
    if token.strip() == default_zenodo[token_key]:
        raise ValueError(
            f"{token_key} is set to default value ('{default_zenodo[token_key]}'). "
            f"Replace with a valid token from {'https://sandbox.zenodo.org/account/settings/tokens/' if use_sandbox else 'https://zenodo.org/account/settings/tokens/'}"
        )
    if len(token.strip()) < 32:  # Zenodo tokens are typically long
        raise ValueError(
            f"{token_key} is too short (length: {len(token.strip())}). Expected a valid Zenodo token. "
            f"Generate a new token at {'https://sandbox.zenodo.org/account/settings/tokens/' if use_sandbox else 'https://zenodo.org/account/settings/tokens/'}"
        )
    if not re.match(r"^[a-zA-Z0-9_-]+$", token.strip()):
        raise ValueError(
            f"{token_key} contains invalid characters. Expected alphanumeric, underscore, or hyphen. "
            f"Generate a new token at {'https://sandbox.zenodo.org/account/settings/tokens/' if use_sandbox else 'https://zenodo.org/account/settings/tokens/'}"
        )
    logger.debug(f"Config validation passed for {token_key}")
    return True