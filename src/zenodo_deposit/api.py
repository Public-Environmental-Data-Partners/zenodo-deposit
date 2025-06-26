import requests
import json
from typing import Dict, List, Any
from pathlib import Path
from urllib.parse import urlparse
import logging
import backoff
import zipfile
import tempfile
import re

logger = logging.getLogger(__name__)


def valid_url(url: str) -> bool:
    """
    Check if a URL is valid.

    Args:
        url (str): The URL to check.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        if not parsed.netloc:
            return False
        if not re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", parsed.netloc):
            return False
        return True
    except ValueError:
        return False

def file_list(path: str) -> List[Path]:
    """
    Get a list of files in a directory, recursively.

    Args:
        path (str): The path to the directory.

    Returns:
        List[Path]: A list of files in the directory.
    """
    path = Path(path)
    if path.is_file():
        return [path]
    return [f for f in Path(path).rglob("*") if f.is_file()]


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

    Raises:
        ValueError: If the access token is missing or invalid.
    """
    token_key = "ZENODO_SANDBOX_ACCESS_TOKEN" if sandbox else "ZENODO_ACCESS_TOKEN"
    token = config.get(token_key)
    if not token:
        raise ValueError(f"Access token '{token_key}' is missing in the configuration")
    # Validate token with a lightweight request
    try:
        response = requests.get(f"{zenodo_url(sandbox)}/deposit/depositions", params={"access_token": token})
        if response.status_code == 403:
            raise ValueError(f"Invalid or expired access token '{token_key}'. Regenerate at https://sandbox.zenodo.org/account/settings/applications/")
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise ValueError(f"Failed to validate access token '{token_key}': {str(e)}")
    return token

def create_deposition(base_url: str, params: Dict) -> Dict:
    """
    Create a new deposition on Zenodo.

    Args:
        base_url (str): The base URL for the Zenodo API.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{base_url}/deposit/depositions", params=params, json={}, headers=headers
    )
    logger.debug(f"Create deposition response: {response.status_code} {response.text}")
    if response.status_code != 201:
        response.raise_for_status()
    return response.json()


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and e.response.status_code < 500,
)
def add_url(bucket_url: str, url: str, params: Dict, name: str = None) -> Dict:
    """
    Upload a file from a URL to the Zenodo deposition bucket.

    Args:
        bucket_url: The URL of the deposition bucket.
        url: The URL to the file to upload.
        params: Parameters for the request, including the access token.
        name: The name to save the file as, defaults to the URL's filename.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    logger.info(f"Uploading URL {url} to Zenodo")
    if not valid_url(url):
        raise ValueError(f"Invalid URL: {url}")
    parsed = urlparse(url)
    filename = Path(parsed.path).name if not name else name
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        response = requests.put(
            f"{bucket_url}/{filename}", data=r.content, params=params
        )
    response.raise_for_status()
    return response.json()

@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and e.response.status_code < 500,
)
def add_file(bucket_url: str, file_path: Path, params: Dict, name: str = None) -> Dict:
    """
    Upload a single file to the Zenodo deposition bucket.

    Args:
        bucket_url: The URL of the deposition bucket.
        file_path: The path to the file to upload.
        params: Parameters for the request, including the access token.
        name: The name to save the file as, defaults to the file's name.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    logger.info(f"Uploading file {file_path} to Zenodo")
    file_path = Path(file_path)
    if not file_path.is_file():
        raise ValueError(f"Not a file: {file_path}")
    filename = file_path.name if not name else name
    with open(file_path, "rb") as fp:
        response = requests.put(f"{bucket_url}/{filename}", data=fp, params=params)
    response.raise_for_status()
    return response.json()

def add_directory(bucket_url: str, directory: str, params: Dict, names: List[str] = []) -> List[Dict]:
    """
    Upload all files in a directory to the Zenodo deposition bucket.

    Args:
        bucket_url: The URL of the deposition bucket.
        directory: The path to the directory to upload.
        params: Parameters for the request, including access token.
        names: List of names to save files as; defaults to original filenames if shorter than number of files.

    Returns:
        List[Dict]: List of responses from the Zenodo API.

    Raises:
        ValueError: If the directory is invalid or contains too many files (>100).
        requests.exceptions.HTTPError: If the API request fails.
    """
    logger.info(f"Uploading files in {directory} to Zenodo")
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"Not a directory: {directory}")
    files = file_list(directory)
    if len(names) < len(files):
        names += [None] * (len(files) - len(names))

    if len(files) > 100:
        logger.warning("Uploading more than 100 files. Zipping the directory.")
        return [add_zipped_directory(bucket_url, directory, params)]
    responses = []
    for file, name in zip(files, names):
        responses.append(add_file(bucket_url, file, params, name))
    return responses


def add_zipped_directory(
    bucket_url: str, directory: str, params: Dict, name: str = None
) -> Dict:
    """
    Create a ZIP file of a directory and upload it to the Zenodo deposition bucket.

    Args:
        bucket_url: The URL of the deposition bucket.
        directory: The path to the directory to upload.
        params: Parameters for the request, including access token.
        name: The name to save the ZIP file as, defaults to directory name + '.zip'.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        ValueError: If the directory is invalid.
        requests.exceptions.HTTPError: If the API request fails.
    """
    logger.info(f"Zipping and uploading {directory} to Zenodo")
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"{directory} is not a directory.")
    if not name:
        name = f"{directory.name}.zip"
    else:
        name = f"{name}.zip"
    temp_filename = Path(tempfile.gettempdir()) / name
    with zipfile.ZipFile(temp_filename, "w") as z:
        for file in file_list(directory):
            z.write(file, file.relative_to(directory))
    try:
        result = add_file(bucket_url, temp_filename, params, name)
    finally:
        temp_filename.unlink()
    return result


def add_thing(
    bucket_url: str, thing: str, params: Dict, name: str = None, zip: bool = False
) -> Dict:
    """
    Upload a file, URL, or directory to the Zenodo deposition bucket.

    Args:
        bucket_url: The URL of the deposition bucket.
        thing: Path to a file, a URL, or a directory.
        params: Parameters for the request, including access token.
        name: The name to save the file as, defaults to the original name.
        zip: If True, zip directories before uploading.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        ValueError: If the thing is not a file, URL, or directory.
        requests.exceptions.HTTPError: If the API request fails.
    """
    logger.debug(f"Uploading {thing} to Zenodo")
    if not Path(thing).exists() and not valid_url(thing):
        raise ValueError(f"Path does not exist or is not a valid URL: {thing}")
    if valid_url(thing):
        return add_url(bucket_url, thing, params, name)
    if Path(thing).is_dir():
        if zip:
            return add_zipped_directory(bucket_url, thing, params, name)
        else:
            return add_directory(bucket_url, thing, params)
    if Path(thing).is_file():
        return add_file(bucket_url, thing, params, name)
    raise ValueError(
        f"Do not know how to deposit {thing}. Must be a file, URL, or directory."
    )


def add_metadata(
    base_url: str, deposition_id: int, metadata: Dict, params: Dict
) -> Dict:
    """
    Add metadata to a Zenodo deposition, merging with existing metadata.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        metadata (Dict): The metadata to add to the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    logger.info(f"Adding metadata to deposition {deposition_id}")
    logger.debug(f"New metadata: {metadata}")
    existing_deposition = get_deposition(deposition_id, params=params, base_url=base_url)
    existing_metadata = existing_deposition.get("metadata", {})
    merged_metadata = existing_metadata.copy()
    for key, value in metadata.items():
        if key in ["keywords", "creators", "contributors"]:
            existing_list = merged_metadata.get(key, [])
            new_list = value if isinstance(value, list) else [value]
            merged_list = existing_list + [item for item in new_list if item not in existing_list]
            merged_metadata[key] = merged_list
        else:
            merged_metadata[key] = value
    logger.debug(f"Merged metadata: {merged_metadata}")
    headers = {"Content-Type": "application/json"}
    data = {"metadata": merged_metadata}
    response = requests.put(
        f"{base_url}/deposit/depositions/{deposition_id}",
        params=params,
        data=json.dumps(data),
        headers=headers,
    )
    response.raise_for_status()
    logger.debug(f"Response: {response.status_code} {response.json()}")
    return response.json()


def publish_deposition(base_url: str, deposition_id: int, params: Dict) -> Dict:
    """
    Publish a Zenodo deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    response = requests.post(
        f"{base_url}/deposit/depositions/{deposition_id}/actions/publish", params=params
    )
    response.raise_for_status()
    return response.json()


def upload(
    paths: List[str],
    metadata: Dict,
    config: Dict,
    name: str = None,
    sandbox: bool = True,
    publish: bool = False,
    zip: bool = False,
) -> Dict:
    """
    Upload files to Zenodo with the given metadata.

    Args:
        paths: List of paths to files, URLs, or directories to upload.
        metadata (Dict): The metadata for the upload.
        name (str): The name of the file to save as, defaults to the file name. Only
        works well if it is a single file or URL
        config (Dict): The configuration containing the access token.
        sandbox (bool): If True, use the sandbox environment; otherwise, use production.
        publish (bool): If True, publish the deposition after uploading.
        zip (bool): If True, zip directories before uploading.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        ValueError: If no paths are provided or the access token is missing.
        requests.exceptions.HTTPError: If the API request fails.
    """
    if not paths:
        raise ValueError("At least one file must be specified for upload")
    token = access_token(config, sandbox)
    base_url = zenodo_url(sandbox)
    params = {"access_token": token}

    # Step 1: Create a new deposition
    deposition = create_deposition(base_url, params)
    deposition_id = deposition["id"]
    bucket_url = deposition["links"]["bucket"]

    # Step 2: Add metadata (in case file upload fails)
    add_metadata(base_url, deposition_id, metadata, params)

    # Step 3: Upload the files
    for path in paths:
        add_thing(bucket_url, path, params, name, zip)
    
    # Step 4: Publish the deposition, possibly
    if publish:
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
        Dict: The updated deposition details.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    logger.info(f"Updating metadata for deposition {deposition_id}")
    headers = {"Content-Type": "application/json"}
    response = requests.put(
        f"{base_url}/deposit/depositions/{deposition_id}",
        params=params,
        json={"metadata": metadata},
        headers=headers,
    )
    logger.debug(f"Response: {response.status_code} {response.json()}")
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
        Dict: Empty dict on success (204), or the API response on error.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    response = requests.delete(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    if response.status_code == 204:
        return {}
    return response.json()


def get_deposition(deposition_id: int, config: Dict = None, sandbox: bool = True, base_url: str = None, params: Dict = None) -> Dict:
    """
    Get details of a Zenodo deposition.

    Args:
        deposition_id: The ID of the deposition.
        config: Configuration dictionary containing access tokens (optional).
        sandbox: If True, use the sandbox environment; otherwise, use production.
        base_url: The base URL for the Zenodo API (optional).
        params: Parameters including access token (optional).

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        ValueError: If the access token is missing.
        requests.exceptions.HTTPError: If the API request fails.
    """
    if not base_url:
        base_url = zenodo_url(sandbox)
    if config:
        token = access_token(config, sandbox)
    else:
        token_key = "ZENODO_SANDBOX_ACCESS_TOKEN" if sandbox else "ZENODO_ACCESS_TOKEN"
        token = params.get(token_key) if params else None
        if not token:
            raise ValueError(f"Access token '{token_key}' is missing in the configuration")
    if not params:
        params = {"access_token": token}
    elif params.get("access_token") != token:
        params = params.copy()
        params["access_token"] = token
    response = requests.get(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    return response.json()


def create_new_version(
    base_url: str, deposition_id: int, params: Dict[str, str], config: Dict[str, str], sandbox: bool = True, files_to_add: List[str] = None, zip: bool = False
) -> Dict[str, Any]:
    """
    Create a new version of an existing Zenodo deposition, adding new files.

    Args:
        base_url: The base URL for the Zenodo API.
        deposition_id: The ID of the existing deposition.
        params: Parameters including access token.
        config: Configuration dictionary containing access tokens.
        sandbox: If True, use the sandbox environment; otherwise, use production.
        files_to_add: List of new file paths to upload to the new version.
        zip: If True, zip directories before uploading.

    Returns:
        Dict[str, Any]: The new version deposition details.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
        ValueError: If no files are provided when required.
    """
    logger.info(f"Creating new version for deposition {deposition_id}")
    r = requests.post(
        f"{base_url}/deposit/depositions/{deposition_id}/actions/newversion",
        params=params,
    )
    logger.debug(f"Response: {r.status_code} {r.json()}")
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error(f"Failed to create new version: {r.json()}")
        raise
    new_version_data = r.json()
    new_deposition_id = new_version_data["links"]["latest_draft"].split("/")[-1]
    logger.info(f"New version created with ID: {new_deposition_id}")
    new_deposition = get_deposition(int(new_deposition_id), config, sandbox)
    bucket_url = new_deposition["links"]["bucket"]
    if files_to_add:
        for path in files_to_add:
            add_thing(bucket_url, path, params, zip=zip)
    return new_version_data

def search(
    query: str,
    size: int = 25,
    status: str = None,
    sort: str = None,
    page: int = 1,
    config: Dict = None,
    sandbox: bool = True,
) -> Dict:
    """
    Search for depositions on Zenodo.

    Args:
        query: The search query.
        size: Number of results to return.
        status: Filter by deposition status (e.g., 'draft', 'published', 'all').
        sort: Sort order (e.g., 'bestmatch', 'mostrecent').
        page: Page number for pagination.
        config: Configuration dictionary containing access tokens.
        sandbox: If True, use the sandbox environment; otherwise, use production.

    Returns:
        Dict: The response from the Zenodo API.

    Raises:
        ValueError: If the access token is missing or invalid status/sort values are provided.
        requests.exceptions.HTTPError: If the API request fails.
    """
    base_url = zenodo_url(sandbox)
    token = access_token(config, sandbox)
    params = {"access_token": token, "q": query}
    if size:
        params["size"] = size
    if status:
        acceptable_statuses = ["draft", "published", "all"]
        if status not in acceptable_statuses:
            raise ValueError(
                "Invalid status value. Must be one of: " + ", ".join(acceptable_statuses)
            )
        if status in ["draft", "published"]:
            params["status"] = status
    if sort:
        acceptable_sorts = ["bestmatch", "mostrecent", "-bestmatch", "-mostrecent"]
        if sort not in acceptable_sorts:
            raise ValueError(
                "Invalid sort value. Must be one of: " + ", ".join(acceptable_sorts)
            )
        params["sort"] = sort
    if page:
        params["page"] = page
    response = requests.get(f"{base_url}/records", params=params)
    response.raise_for_status()
    return response.json()