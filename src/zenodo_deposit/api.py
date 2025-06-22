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
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])

def file_list(path: str) -> List[Path]:
    path = Path(path)
    if path.is_file():
        return [path]
    return [f for f in Path(path).rglob("*") if f.is_file()]

def zenodo_url(sandbox: bool = True) -> str:
    return "https://sandbox.zenodo.org/api" if sandbox else "https://zenodo.org/api"

def access_token(config: Dict, sandbox: bool = True) -> str:
    return (
        config.get("ZENODO_SANDBOX_ACCESS_TOKEN")
        if sandbox
        else config.get("ZENODO_ACCESS_TOKEN")
    )

def create_deposition(base_url: str, params: Dict) -> Dict:
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
    logger.info(f"Uploading file {file_path} to Zenodo")
    file_path = Path(file_path)
    filename = file_path.name if not name else name
    with open(file_path, "rb") as fp:
        response = requests.put(f"{bucket_url}/{filename}", data=fp, params=params)
    response.raise_for_status()
    return response.json()

def add_directory(bucket_url: str, directory: str, params: Dict, names=[]) -> Dict:
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
    # Filter out invalid paths
    valid_paths = [p for p in paths if Path(p).is_file() or valid_url(p) or Path(p).is_dir()]
    if not valid_paths:
        raise ValueError("No valid files, URLs, or directories provided")
    paths = valid_paths

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
        return publish_deposition(base_url, deposition_id, params)

    return deposition

def update_metadata(
    base_url: str, deposition_id: int, metadata: Dict, params: Dict
) -> Dict:
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
    response = requests.delete(
        f"{base_url}/deposit/depositions/{deposition_id}", params=params
    )
    response.raise_for_status()
    return response.json()

def get_deposition(deposition_id: int, config: Dict, sandbox: bool = True) -> Dict:
    base_url = zenodo_url(sandbox)
    token = access_token(config, sandbox)
    if not token:
        raise ValueError("Access token is missing in the configuration")

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