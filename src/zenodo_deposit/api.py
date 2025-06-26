import requests
import json
from typing import Dict, List
from pathlib import Path
from urllib.parse import urlparse
import logging
import backoff
import zipfile
import tempfile

logger = logging.getLogger(__name__)


def valid_url(url: str) -> bool:
    """
    Check if a URL is valid.

    Args:
        url (str): The URL to check.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])


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


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and e.response.status_code < 500,
)
def add_url(bucket_url: str, url: str, params: Dict, name: str = None) -> Dict:
    """
    Upload a file from a URL to the Zenodo deposition bucket. Assumes valid URL.

    Args:
        bucket_url (str): The URL of the deposition bucket.
        url (str): The path to the file to upload.
        params (Dict): The parameters for the request, including the access token.
        name (str): The name of the file to save as, defaults to the URL name.

    Returns:
        Dict: The response from the Zenodo API.
    """
    logger.info(f"Uploading URL {url} to Zenodo")
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
    Upload a single file to the Zenodo deposition bucket. Assumes valid file path.
    Deposit

    Args:
        bucket_url (str): The URL of the deposition bucket.
        file_path (Path): The path to the file to upload.
        params (Dict): The parameters for the request, including the access token.
        name (str): The name of the file to save as, defaults to the file name.

    Returns:
        Dict: The response from the Zenodo API.
    """
    logger.info(f"Uploading file {file_path} to Zenodo")
    file_path = Path(file_path)
    filename = file_path.name if not name else name
    with open(file_path, "rb") as fp:
        response = requests.put(f"{bucket_url}/{filename}", data=fp, params=params)
    response.raise_for_status()
    return response.json()


def add_directory(bucket_url: str, directory: str, params: Dict, names=[]) -> Dict:
    """
    Upload all files in a directory to the Zenodo deposition bucket.

    Args:
        bucket_url (str): The URL of the deposition bucket.
        directory (str): The path to the directory to upload.
        params (Dict): The parameters for the request, including the access token.
        names (List[str]): The names of the files to save as, defaults to the file names.
        if the list is shorter than the number of files, the remaining files will be
        saved with their original names.

    Returns:
        Dict: The response from the Zenodo API.
    """
    logger.info(f"Uploading files in {directory} to Zenodo")
    files = file_list(directory)
    if len(names) < len(files):
        names += [None] * (len(files) - len(names))

    if len(files) > 100:
        logger.warning("Uploading more than 100 files. Zipping the directory.")
        return add_zipped_directory(bucket_url, directory, params)
    responses = []
    for file, name in zip(files, names):
        responses.append(add_file(bucket_url, file, params, name))
    return responses


def add_zipped_directory(
    bucket_url: str, directory: str, params: Dict, name: str = None
) -> Dict:
    """
    Create a ZIP file of a directory and upload it to the Zenodo deposition bucket.
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
    result = add_file(bucket_url, temp_filename, params, name)
    temp_filename.unlink()
    return result


def add_thing(
    bucket_url: str, thing: str, params: Dict, name: str = None, zip: bool = False
) -> Dict:
    """
    Upload a single file or url to the Zenodo deposition bucket.
    """
    logger.debug(f"Uploading {thing} to Zenodo")
    if valid_url(thing):
        return add_url(bucket_url, thing, params, name)
    if Path(thing).is_dir():
        if zip:
            return add_zipped_directory(bucket_url, thing, params, name)
        else:
            return add_directory(
                bucket_url,
                thing,
                params,
            )
    if Path(thing).is_file():
        return add_file(bucket_url, thing, params, name)
    raise ValueError(
        f"Do not know how to deposit {thing}. Must be a file, URL, or directory."
    )


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
        paths (str): The paths of the files to upload.
        metadata (Dict): The metadata for the upload.
        name (str): The name of the file to save as, defaults to the file name. Only
        works well if it is a single file or URL
        config (Dict): The configuration containing the access token.
        sandbox (bool): Whether to use the Zenodo sandbox or production URL.
        publish (bool): Whether to publish the deposition after uploading.
        zip (bool): Whether to zip directories before uploading.

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

    # Step 2: Add metadata (in case file upload fails)
    add_metadata(base_url, deposition_id, metadata, params)

    # Step 3: Upload the files
    for path in paths:
        add_thing(bucket_url, path, params, zip)

    # Step 4: Publish the deposition, possibly
    if publish:
        # Step 4: Publish the deposition
        return publish_deposition(base_url, deposition_id, params)

    return deposition



def update_metadata(base_url: str, deposition_id: int, metadata: Dict, params: Dict) -> Dict:
    """
    Update metadata for a deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        metadata (dict): The metadata to update.
        params (dict): Parameters including access token.

    Returns:
        dict: The updated deposition details.
    """
    logging.info(f"Updating metadata for deposition {deposition_id}")
    r = requests.put(
        f"{base_url}/deposit/depositions/{deposition_id}",
        params=params,
        json={"metadata": metadata},
    )
    logging.debug(f"Response: {r.status_code} {r.json()}")
    r.raise_for_status()
    return r.json()

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
        requests.exceptions.HTTPError: If the API request fails (e.g., 404, 403).
    """
    response = requests.delete(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    if response.status_code == 204:
        return {}
    return response.json()

def get_deposition(deposition_id: int, config: Dict, sandbox: bool = True) -> Dict:
    """
    Get the Zenodo deposition.

    Args:
        base_url (str): The base URL for the Zenodo API.
        deposition_id (int): The ID of the deposition.
        params (Dict): The parameters for the request, including the access token.

    Returns:
        Dict: The response from the Zenodo API.
    """
    base_url = zenodo_url(sandbox)
    token = access_token(config, sandbox)
    if not token:
        raise ValueError("Access token is missing in the configuration")

    base_url = zenodo_url(sandbox)
    params = {"access_token": token}
    response = requests.get(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    return response.json()


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
        query (str): The search query.
        config (Dict): The configuration containing the access token.
        sandbox (bool): Whether to use the Zenodo sandbox or production URL.

    Returns:
        Dict: The response from the Zenodo API.
    """
    base_url = zenodo_url(sandbox)
    token = access_token(config, sandbox)
    if not token:
        raise ValueError("Access token is missing in the configuration")

    params = {"access_token": token, "q": query}
    if size:
        params["size"] = size
    if status:
        acceptable_statuses = ["draft", "published", "all"]
        if status not in acceptable_statuses:
            raise ValueError(
                "Invalid status value. Must be one of: "
                + ", ".join(acceptable_statuses)
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
