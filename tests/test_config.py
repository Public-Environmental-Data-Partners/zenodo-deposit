import pytest
import os
from unittest.mock import patch, mock_open
from zenodo_deposit.config import (
    zenodo_config,
    validate_zenodo_config,
    config_section,
)


@pytest.fixture(autouse=True)
def clear_cache():
    config_section.cache_clear()


def test_default_config():
    result = zenodo_config()
    assert result  # returns a dictionary


def test_config_file():
    with patch(
        "builtins.open",
        mock_open(read_data=b'[zenodo]\n"ZENODO_ACCESS_TOKEN" =  "new_token"'),
    ):
        with patch("os.path.exists", return_value=True):
            result = zenodo_config(config_file="dummy_path")
            assert result["ZENODO_ACCESS_TOKEN"] == "new_token"


def test_local_settings():
    with patch(
        "builtins.open",
        mock_open(read_data=b'[zenodo]\n"ZENODO_ACCESS_TOKEN" = "local_token"'),
    ):
        with patch(
            "os.path.exists",
            side_effect=lambda path: path == ".zenodo-deposit-settings.toml",
        ):
            result = zenodo_config()
            assert result["ZENODO_ACCESS_TOKEN"] == "local_token"


def test_user_settings():
    with patch(
        "builtins.open",
        mock_open(read_data=b'[zenodo]\n"ZENODO_ACCESS_TOKEN" = "user_token"'),
    ):
        with patch(
            "os.path.exists",
            side_effect=lambda path: path
            == os.path.expanduser("~/.zenodo-deposit-settings.toml"),
        ):
            result = zenodo_config()
            assert result["ZENODO_ACCESS_TOKEN"] == "user_token"


def test_environment_variable():
    with patch.dict(os.environ, {"ZENODO_ACCESS_TOKEN": "env_token"}):
        result = zenodo_config()
        assert result["ZENODO_ACCESS_TOKEN"] == "env_token"


def test_validate_config():
    with patch.dict(os.environ, {"ZENODO_ACCESS_TOKEN": "env_token"}):
        result = zenodo_config()
        valid = validate_zenodo_config(result, use_sandbox=False)
        assert valid
        # check that the function raises an error if the access token is not set
        with pytest.raises(ValueError):
            result.pop("ZENODO_ACCESS_TOKEN")
            validate_zenodo_config(result, use_sandbox=False)
