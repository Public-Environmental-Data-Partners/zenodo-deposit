import pytest
import requests
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

@pytest.fixture
def base_url():
    return "https://sandbox.zenodo.org/api"

@pytest.fixture
def params():
    return {
        "ZENODO_ACCESS_TOKEN": "test_access_token_production",
        "ZENODO_SANDBOX_ACCESS_TOKEN": "test_access_token_sandbox",
    }

@pytest.fixture
def deposition_response():
    return {
        "id": 12345,
        "links": {"bucket": "https://sandbox.zenodo.org/api/files/12345"},
    }

def test_create_deposition(base_url, params, deposition_response):
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 201
        mock_post.return_value.json.return_value = deposition_response
        response = create_deposition(base_url, params)
        assert response == deposition_response
        mock_post.assert_called_once_with(
            f"{base_url}/deposit/depositions",
            params=params,
            json={},
            headers={"Content-Type": "application/json"},
        )

def test_deposit_file(base_url, params, deposition_response, tmp_path):
    deposition_id = 12345
    bucket_url = deposition_response["links"]["bucket"]
    file_path = tmp_path / "test.txt"
    file_path.write_text("test content")
    
    with patch("requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = {"filename": "test.txt"}
        response = add_file(bucket_url, file_path, params)
        assert response == {"filename": "test.txt"}
        mock_put.assert_called_once()

def test_add_metadata(base_url, params, deposition_response):
    deposition_id = 12345
    metadata = {
        "title": "My first upload",
        "upload_type": "poster",
        "description": "This is my first upload",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
    }
    sandbox = base_url == "https://sandbox.zenodo.org/api"  # Derive sandbox from base_url

    with patch("requests.get") as mock_get, patch("requests.put") as mock_put:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"id": deposition_id, "metadata": {}, "submitted": False}
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = deposition_response
        response = add_metadata(deposition_id, metadata, params, sandbox)
        assert response == deposition_response
        mock_get.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}", params=params
        )
        mock_put.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params=params,
            json={"metadata": metadata},
        )

def test_publish_deposition(base_url, params, deposition_response):
    deposition_id = 12345
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = deposition_response
        response = publish_deposition(base_url, deposition_id, params)
        assert response == deposition_response
        mock_post.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}/actions/publish",
            params=params,
        )

def test_update_metadata(base_url, params, deposition_response):
    deposition_id = 12345
    metadata = {
        "title": "Updated title",
        "upload_type": "poster",
        "description": "Updated description",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
    }
    sandbox = base_url == "https://sandbox.zenodo.org/api"  # Derive sandbox from base_url

    with patch("requests.get") as mock_get, patch("requests.put") as mock_put:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"id": deposition_id, "metadata": {}, "submitted": False}
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = deposition_response
        response = update_metadata(deposition_id, metadata, params, sandbox)
        assert response == deposition_response
        mock_get.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}", params=params
        )
        mock_put.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params=params,
            json={"metadata": metadata},
        )

def test_delete_deposition(base_url, params):
    deposition_id = 12345
    with patch("requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204
        mock_delete.return_value.json.return_value = {}
        response = delete_deposition(base_url, deposition_id, params)
        assert response == {}
        mock_delete.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}", params=params
        )

def test_get_deposition(base_url, params, deposition_response):
    deposition_id = 12345
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = deposition_response
        response = get_deposition(deposition_id, params=params, base_url=base_url)
        assert response == deposition_response
        mock_get.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}", params=params
        )