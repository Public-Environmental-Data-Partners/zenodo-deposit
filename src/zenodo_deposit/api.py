import requests
import json
from typing import Dict


def zenodo_url(sandbox: bool = True) -> str:
    """
    Get the base URL for the Zenodo API.

    Args:
        sandbox (bool): Whether to use the Zenodo sandbox or production URL.

    Returns:
        str: The base URL for the Zenodo API.
    """
    return "https://sandbox.zenodo.org/api" if sandbox else "https://zenodo.org/api"


def access_token(config: Dict, sandbox: bool = True) -> str:
    """
    Get the access token from the configuration.

    Args:
        config (Dict): The configuration containing the access token.
        sandbox (bool): Whether to use the Zenodo sandbox or production access token.

    Returns:
        str: The access token.
    """
    return (
        config.get("ZENODO_SANDBOX_ACCESS_TOKEN")
        if sandbox
        else config.get("ZENODO_ACCESS_TOKEN")
    )


def create_deposition(base_url: str, params: Dict) -> Dict:
    """
    Create a new deposition on Zenodo.

    Args:
        base_url (str): The base URL for the Zenodo API.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{base_url}/deposit/depositions", params=params, json={}, headers=headers
    )
    if response.status_code != 201:
        response.raise_for_status()
    return response.json()


def deposit_file(bucket_url: str, file_path: str, params: Dict) -> Dict:
    """
    Upload a file to the Zenodo deposition bucket.

    Args:
        bucket_url (str): The URL of the deposition bucket.
        file_path (str): The path to the file to upload.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    filename = file_path.split("/")[-1]
    if file_path.startswith("http"):
        with requests.get(file_path, stream=True) as r:
            r.raise_for_status()
            response = requests.put(
                f"{bucket_url}/{filename}", data=r.content, params=params
            )
        response.raise_for_status()
        return response.json()
    with open(file_path, "rb") as fp:
        response = requests.put(f"{bucket_url}/{filename}", data=fp, params=params)
    response.raise_for_status()
    return response.json()


def add_metadata(
    base_url: str, deposition_id: int, metadata: Dict, params: Dict
) -> Dict:
    """
    Add metadata to the Zenodo deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        metadata (Dict): The metadata to add to the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    headers = {"Content-Type": "application/json"}
    data = {"metadata": metadata}
    response = requests.put(
        f"{base_url}/deposit/depositions/{deposition_id}",
        params=params,
        data=json.dumps(data),
        headers=headers,
    )
    response.raise_for_status()
    return response.json()


def publish_deposition(base_url: str, deposition_id: int, params: Dict) -> Dict:
    """
    Publish the Zenodo deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    response = requests.post(
        f"{base_url}/deposit/depositions/{deposition_id}/actions/publish", params=params
    )
    response.raise_for_status()
    return response.json()


def upload(
    file: str, metadata: Dict, config: Dict, sandbox: bool = True, publish: bool = True
) -> Dict:
    """
    Upload a file to Zenodo with the given metadata.

    Args:
        file (str): The path to the file to upload.
        metadata (Dict): The metadata for the upload.
        config (Dict): The configuration containing the access token.
        sandbox (bool): Whether to use the Zenodo sandbox or production URL.
        publish (bool): Whether to publish the deposition after uploading.

    Returns:
        Dict: The response from the Zenodo API.
    """
    token = access_token(config, sandbox)
    if not token:
        raise ValueError("Access token is missing in the configuration")

    base_url = zenodo_url(sandbox)
    params = {"access_token": token}

    # Step 1: Create a new deposition
    deposition = create_deposition(base_url, params)
    deposition_id = deposition["id"]
    bucket_url = deposition["links"]["bucket"]

    # Step 2: Upload the file
    deposit_file(bucket_url, file, params)

    # Step 3: Add metadata
    add_metadata(base_url, deposition_id, metadata, params)

    if publish:
        # Step 4: Publish the deposition
        return publish_deposition(base_url, deposition_id, params)

    return deposition


def update_metadata(
    base_url: str, deposition_id: int, metadata: Dict, params: Dict
) -> Dict:
    """
    Update metadata of the Zenodo deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        metadata (Dict): The metadata to update in the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    headers = {"Content-Type": "application/json"}
    data = {"metadata": metadata}
    response = requests.put(
        f"{base_url}/deposit/depositions/{deposition_id}",
        params=params,
        data=json.dumps(data),
        headers=headers,
    )
    response.raise_for_status()
    return response.json()


def delete_deposition(base_url: str, deposition_id: int, params: Dict) -> Dict:
    """
    Delete the Zenodo deposition. Note: published depositions cannot be deleted.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    response = requests.delete(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    return response.json()


def get_deposition(base_url: str, deposition_id: int, params: Dict) -> Dict:
    """
    Get the Zenodo deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    response = requests.get(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    return response.json()
