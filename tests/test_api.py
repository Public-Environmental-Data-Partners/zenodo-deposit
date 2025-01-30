import pytest
import os
from unittest.mock import patch
from zenodo_deposit.api import (
    create_deposition,
    add_file,
    add_metadata,
    publish_deposition,
    update_metadata,
    delete_deposition,
    get_deposition,
)


TEST_DATA_LOCATION = "test_data"
current_dir = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_PATH = os.path.join(current_dir, TEST_DATA_LOCATION)


@pytest.fixture
def base_url():
    return "https://sandbox.zenodo.org/api"


@pytest.fixture
def params():
    return {
        "ZENODO_SANDBOX_ACCESS_TOKEN": "test_access_token",
        "ZENODO_ACCESS_TOKEN": "test_access_token",
    }


@pytest.fixture
def deposition_response():
    return {
        "id": 12345,
        "links": {"bucket": "https://sandbox.zenodo.org/api/files/12345"},
    }


@pytest.fixture
def file_response():
    return {
        "key": "Combined.xls",
        "mimetype": "application/vnd.ms-excel",
        "checksum": "md5:2942bfabb3d05332b66eb128e0842cff",
        "size": 13264,
        "created": "2020-02-26T14:20:53.805734+00:00",
        "updated": "2020-02-26T14:20:53.811817+00:00",
        "links": {"self": "https://sandbox.zenodo.org/api/files/12345/Combined.xls"},
    }


def test_create_deposition(base_url, params, deposition_response):
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 201
        mock_post.return_value.json.return_value = deposition_response

        response = create_deposition(base_url, params)
        assert response == deposition_response


def test_deposit_file(base_url, params, file_response):
    bucket_url = "https://sandbox.zenodo.org/api/files/12345"
    file_path = os.path.join(TEST_DATA_PATH, "Combined.xls")

    with patch("requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = file_response

        response = add_file(bucket_url, file_path, params)
        assert response == file_response


def test_add_metadata(base_url, params, deposition_response):
    deposition_id = 12345
    metadata = {
        "title": "My first upload",
        "upload_type": "poster",
        "description": "This is my first upload",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
    }

    with patch("requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = deposition_response

        response = add_metadata(base_url, deposition_id, metadata, params)
        assert response == deposition_response


def test_publish_deposition(base_url, params, deposition_response):
    deposition_id = 12345

    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = deposition_response

        response = publish_deposition(base_url, deposition_id, params)
        assert response == deposition_response


# TODO: Add test for upload function


def test_update_metadata(base_url, params, deposition_response):
    deposition_id = 12345
    metadata = {
        "title": "Updated title",
        "upload_type": "poster",
        "description": "Updated description",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
    }

    with patch("requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = deposition_response

        response = update_metadata(base_url, deposition_id, metadata, params)
        assert response == deposition_response


def test_delete_deposition(base_url, params):
    deposition_id = 12345

    with patch("requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204

        response = delete_deposition(base_url, deposition_id, params)
        assert response


def test_get_deposition(base_url, params, deposition_response):
    deposition_id = 12345

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = deposition_response
        response = get_deposition(deposition_id, params)
        assert response == deposition_response
