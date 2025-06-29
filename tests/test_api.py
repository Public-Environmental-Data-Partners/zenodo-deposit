import pytest
import os
from unittest.mock import patch, call
from zenodo_deposit.api import (
    create_deposition,
    add_file,
    add_metadata,
    publish_deposition,
    update_metadata,
    delete_deposition,
    get_deposition,
    upload,
)

TEST_DATA_LOCATION = "test_data"
current_dir = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_PATH = os.path.join(current_dir, TEST_DATA_LOCATION)

@pytest.fixture
def base_url():
    return "https://sandbox.zenodo.org/api"

@pytest.fixture
def config():
    return {
        "ZENODO_SANDBOX_ACCESS_TOKEN": "test_access_token_sandbox",
        "ZENODO_ACCESS_TOKEN": "test_access_token_production",
    }

@pytest.fixture
def params():
    return {"access_token": "test_access_token_sandbox"}

@pytest.fixture
def deposition_response():
    return {
        "id": 12345,
        "links": {"bucket": "https://sandbox.zenodo.org/api/files/12345"},
        "metadata": {
            "title": "Existing Title",
            "upload_type": "dataset",
            "description": "Existing description",
            "creators": [{"name": "Existing, User", "affiliation": "EDGI"}],
            "keywords": ["existing", "keyword"],
            "communities": [{"identifier": "existing"}],
        },
        "submitted": False,
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
        mock_post.assert_called_once_with(
            f"{base_url}/deposit/depositions",
            params=params,
            json={},
            headers={"Content-Type": "application/json"},
        )

def test_deposit_file(base_url, params, file_response):
    bucket_url = "https://sandbox.zenodo.org/api/files/12345"
    file_path = os.path.join(TEST_DATA_PATH, "Combined.xls")
    
    with patch("requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = file_response
        response = add_file(bucket_url, file_path, params)
        assert response == file_response
        mock_put.assert_called_once()

def test_add_metadata(base_url, config, deposition_response):
    deposition_id = 12345
    metadata = {
        "title": "My first upload",
        "description": "This is my first upload",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
        "keywords": ["new", "keyword"],
        "communities": [{"identifier": "edgi"}],
    }
    sandbox = True

    expected_metadata = {
        "title": "My first upload",
        "upload_type": "dataset",
        "description": "This is my first upload",
        "creators": [
            {"name": "Existing, User", "affiliation": "EDGI"},
            {"name": "Doe, John", "affiliation": "Zenodo"},
        ],
        "keywords": ["existing", "keyword", "new"],
        "communities": [{"identifier": "existing"}, {"identifier": "edgi"}],
    }

    with patch("requests.get") as mock_get, patch("requests.put") as mock_put:
        mock_get.return_value = type(
            "Response",
            (),
            {
                "status_code": 200,
                "json": lambda self: {
                    "id": deposition_id,
                    "metadata": deposition_response["metadata"],
                    "submitted": False,
                },
                "raise_for_status": lambda self: None,
            }
        )()

        mock_put.return_value = type(
            "Response",
            (),
            {
                "status_code": 200,
                "json": lambda self: {
                    "id": deposition_id,
                    "metadata": expected_metadata,
                    "links": deposition_response["links"],
                    "submitted": False,
                },
                "raise_for_status": lambda self: None,
            }
        )()

        response = add_metadata(deposition_id, metadata, {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}, sandbox)
        
        response["metadata"]["keywords"] = sorted(response["metadata"]["keywords"])
        expected_metadata["keywords"] = sorted(expected_metadata["keywords"])
        
        assert response["metadata"] == expected_metadata
        mock_get.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params={"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}
        )

        expected_json = {"metadata": {**expected_metadata}}
        actual_call = mock_put.call_args
        actual_json = actual_call[1]["json"]
        actual_json["metadata"]["keywords"] = sorted(actual_json["metadata"]["keywords"])
        assert actual_call == call(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params={"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]},
            json=expected_json,
        )

def test_publish_deposition(base_url, params, deposition_response):
    deposition_id = 12345
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = deposition_response
        response = publish_deposition(base_url, deposition_id, params)
        assert response == deposition_response
        mock_post.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}/actions/publish",
            params=params,
        )

def test_update_metadata(base_url, config, deposition_response):
    deposition_id = 12345
    metadata = {
        "title": "Updated title",
        "upload_type": "poster",
        "description": "Updated description",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
    }
    sandbox = True
    
    with patch("requests.get") as mock_get, patch("requests.put") as mock_put:
        mock_get.return_value = type(
            "Response",
            (),
            {
                "status_code": 200,
                "json": lambda self: {
                    "id": deposition_id,
                    "metadata": deposition_response["metadata"],
                    "submitted": False,
                },
                "raise_for_status": lambda self: None,
            }
        )()

        mock_put.return_value = type(
            "Response",
            (),
            {
                "status_code": 200,
                "json": lambda self: deposition_response,
                "raise_for_status": lambda self: None,
            }
        )()

        response = update_metadata(deposition_id, metadata, {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}, sandbox)
        assert response == deposition_response
        mock_get.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params={"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}
        )
        mock_put.assert_called_once_with(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params={"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]},
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

def test_get_deposition(base_url, config, deposition_response):
    deposition_id = 12345
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [
            type(
                "Response",
                (),
                {
                    "status_code": 200,
                    "json": lambda self: [],
                    "raise_for_status": lambda self: None,
                }
            )(),
            type(
                "Response",
                (),
                {
                    "status_code": 200,
                    "json": lambda self: deposition_response,
                    "raise_for_status": lambda self: None,
                }
            )(),
        ]
        response = get_deposition(deposition_id, config=config, sandbox=True)
        assert response == deposition_response
        mock_get.assert_any_call(
            f"{base_url}/deposit/depositions",
            params={"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}
        )
        mock_get.assert_any_call(
            f"{base_url}/deposit/depositions/{deposition_id}",
            params={"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}
        )
        assert mock_get.call_count == 2

def test_upload(base_url, config, deposition_response, file_response, tmp_path):
    file_path = tmp_path / "Combined.xls"
    file_path.write_text("test content")
    metadata = {
        "title": "Test Upload",
        "upload_type": "dataset",
        "description": "Test dataset upload",
        "creators": [{"name": "Doe, John", "affiliation": "Zenodo"}],
        "keywords": ["test", "dataset"],
        "communities": [{"identifier": "edgi"}],
    }
    sandbox = True
    paths = [str(file_path)]
    
    with patch("zenodo_deposit.api.access_token") as mock_access_token, \
         patch("zenodo_deposit.api.create_deposition") as mock_create, \
         patch("zenodo_deposit.api.add_thing") as mock_add_thing, \
         patch("zenodo_deposit.api.add_metadata") as mock_add_metadata, \
         patch("zenodo_deposit.api.publish_deposition") as mock_publish:
        mock_access_token.return_value = config["ZENODO_SANDBOX_ACCESS_TOKEN"]
        mock_create.return_value = deposition_response
        mock_add_thing.return_value = file_response
        mock_add_metadata.return_value = deposition_response
        mock_publish.return_value = deposition_response
        
        response = upload(paths, metadata, config, sandbox=sandbox, publish=False)
        
        assert response == deposition_response
        mock_access_token.assert_called_once_with(config, sandbox)
        mock_create.assert_called_once_with(base_url, {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]})
        mock_add_thing.assert_called_once_with(
            deposition_response["links"]["bucket"],
            str(file_path),
            {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]},
            None,
            False
        )
        mock_add_metadata.assert_called_once_with(
            deposition_response["id"],
            metadata,
            {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]},
            sandbox
        )
        mock_publish.assert_not_called()
        
        response = upload(paths, metadata, config, sandbox=sandbox, publish=True)
        
        assert response == deposition_response
        mock_publish.assert_called_once_with(
            base_url,
            deposition_response["id"],
            {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]}
        )
        
        file_path2 = tmp_path / "Data2.csv"
        file_path2.write_text("test content 2")
        paths = [str(file_path), str(file_path2)]
        mock_add_thing.reset_mock()
        response = upload(paths, metadata, config, sandbox=sandbox, publish=False)
        
        assert response == deposition_response
        assert mock_add_thing.call_count == 2
        mock_add_thing.assert_any_call(
            deposition_response["links"]["bucket"],
            str(file_path),
            {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]},
            None,
            False
        )
        mock_add_thing.assert_any_call(
            deposition_response["links"]["bucket"],
            str(file_path2),
            {"access_token": config["ZENODO_SANDBOX_ACCESS_TOKEN"]},
            None,
            False
        )
        
        with pytest.raises(ValueError, match="At least one file must be specified for upload"):
            upload([], metadata, config, sandbox=sandbox)
