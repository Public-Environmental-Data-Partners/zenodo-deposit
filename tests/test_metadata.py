import pytest
from unittest.mock import patch, mock_open
from zenodo_deposit.metadata import (
    metadata,
    metadata_from_file,
    metadata_from_toml,
    validate_metadata,
    upload_types,
    image_types,
    publication_types,
    cleanup_metadata,
)


def test_metadata():
    template = "Hello, $name!"
    result = metadata(template, {"name": "World"})
    assert result == "Hello, World!"


def test_metadata_from_file():
    with patch("builtins.open", mock_open(read_data="Hello, $name!")):
        result = metadata_from_file("dummy_path", {"name": "World"})
        assert result == "Hello, World!"


def test_metadata_from_toml_dict():
    toml_content = """
    title = "Hello, $name!"
    creators = [{name = "Doe, John"}]
    upload_type = "dataset"
    """
    with patch("builtins.open", mock_open(read_data=toml_content)):
        result = metadata_from_toml("dummy_path", {"name": "World"})
        assert result["title"] == "Hello, World!"
        assert result["creators"] == [{"name": "Doe, John"}]
        assert result["upload_type"] == "dataset"


def test_validate_metadata():
    valid_metadata = {
        "title": "Valid Title",
        "upload_type": "dataset",
        "creators": [{"name": "Doe, John"}],
    }
    assert validate_metadata(valid_metadata)

    with pytest.raises(ValueError, match="Missing required key: title"):
        validate_metadata({})

    with pytest.raises(ValueError, match="title cannot be empty"):
        validate_metadata({"title": "   "})

    with pytest.raises(ValueError, match="title cannot be a template variable"):
        validate_metadata({"title": "$Invalid Title"})


def test_cleanup_metadata():
    metadata = {
        "title": "Valid Title",
        "upload_type": "dataset",
        "creators": [{"name": "Doe, John"}],
        "empty": "",
        "template": "$template",
        "nested": {
            "title": "Valid Title",
            "upload_type": "dataset",
            "creators": [{"name": "Doe, John"}],
            "empty": "",
            "template": "$template",
        },
    }
    cleaned_metadata = {
        "title": "Valid Title",
        "upload_type": "dataset",
        "creators": [{"name": "Doe, John"}],
        "nested": {
            "title": "Valid Title",
            "upload_type": "dataset",
            "creators": [{"name": "Doe, John"}],
        },
    }
    assert cleanup_metadata(metadata) == cleaned_metadata


def test_upload_types():
    assert "dataset" in upload_types
    assert "publication" in upload_types
    assert "other" in upload_types


def test_image_types():
    assert "figure" in image_types
    assert "photo" in image_types
    assert "other" in image_types


def test_publication_types():
    assert "book" in publication_types
    assert "article" in publication_types
    assert "other" in publication_types
