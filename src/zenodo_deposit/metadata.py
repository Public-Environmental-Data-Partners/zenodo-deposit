from string import Template
import tomllib as toml
import logging

logger = logging.getLogger(__name__)


def metadata(template: str, vars: dict) -> str:
    """
    Replace placeholders in the template with the values in kwargs
    """
    return Template(template).safe_substitute(vars)


def metadata_from_file(file: str, vars: dict) -> str:
    """
    Read a file and replace placeholders in the content with the values in kwargs
    """
    logger.info(f"Reading metadata: {file}")
    with open(file, "r") as f:
        return metadata(f.read(), vars)


def is_template_variable(value: str) -> bool:
    """
    Check if a value is a template variable.
    """
    return value.startswith("$")


def cleanup_metadata(metadata: dict) -> dict:
    """
    Remove empty keys from the metadata.
    Remove any key with values that are a template variable.
    Do this recursively if the value is a dictionary.
    """
    cleaned_metadata = {}
    for key, value in metadata.items():
        if not value:
            continue
        if isinstance(value, dict):
            cleaned_value = cleanup_metadata(value)
            if cleaned_value:
                cleaned_metadata[key] = cleaned_value
        elif isinstance(value, list):
            cleaned_list = []
            for item in value:
                if isinstance(item, dict):
                    cleaned_item = cleanup_metadata(item)
                    if cleaned_item:
                        cleaned_list.append(cleaned_item)
                elif not is_template_variable(item):
                    cleaned_list.append(item)
            if cleaned_list:
                cleaned_metadata[key] = cleaned_list
        elif not is_template_variable(value):
            cleaned_metadata[key] = value
    return cleaned_metadata


def metadata_from_toml(file: str, vars: dict = {}) -> dict:
    """
    Read a toml file and replace placeholders in the content with the values in kwargs
    """
    metadata_string = metadata_from_file(file, vars)
    cleaned = cleanup_metadata(toml.loads(metadata_string))
    logger.debug(f"Cleaned metadata: {cleaned}")
    if not cleaned:
        raise ValueError("Metadata is empty")
    if not validate_metadata(cleaned):
        raise ValueError("Invalid metadata")
    logging.info(f"Metadata: {file} loaded")
    return cleaned


# Validate metadata

upload_types = [
    "publication",  # Publication
    "poster",  # Poster
    "presentation",  # Presentation
    "dataset",  # Dataset
    "image",  # Image
    "video",  # Video/Audio
    "software",  # Software
    "lesson",  # Lesson
    "physicalobject",  # Physical object
    "other",  # Other
]

image_types = [
    "figure",  # Figure
    "plot",  # Plot
    "drawing",  # Drawing
    "diagram",  # Diagram
    "photo",  # Photo
    "other",  # Other
]

publication_types = [
    "annotationcollection",  # Annotation collection
    "book",  # Book
    "section",  # Book section
    "conferencepaper",  # Conference paper
    "datamanagementplan",  # Data management plan
    "article",  # Journal article
    "patent",  # Patent
    "preprint",  # Preprint
    "deliverable",  # Project deliverable
    "milestone",  # Project milestone
    "proposal",  # Proposal
    "report",  # Report
    "softwaredocumentation",  # Software documentation
    "taxonomictreatment",  # Taxonomic treatment
    "technicalnote",  # Technical note
    "thesis",  # Thesis
    "workingpaper",  # Working paper
    "other",  # Other
]


def validate_metadata(metadata: dict) -> bool:
    """
    Check if the metadata has all the required keys.
    Raise ValueError if any required key is missing.
    """
    title = metadata.get("title")
    if not title:
        raise ValueError("Missing required key: title")
    # check it title is just whitespace
    if not title.strip():
        raise ValueError("title cannot be empty")
    # check it's not just a template variable
    if is_template_variable(title):
        raise ValueError("title cannot be a template variable")
    if not metadata.get("creators"):
        raise ValueError("Missing required key: creators")
    # check that each creator has a name
    for creator in metadata.get("creators"):
        if not creator.get("name"):
            raise ValueError("Missing required key for creators: name")
    if not metadata.get("upload_type"):
        raise ValueError("Missing required key: upload_type")
    if metadata.get("upload_type") not in upload_types:
        raise ValueError(f"Invalid upload_type: {metadata.get('upload_type')}")
    if metadata.get("upload_type") == "image" and not metadata.get("image_type"):
        raise ValueError("Missing required key: image_type")
    if metadata.get("access_right") == "embargoed" and not metadata.get("embargo_date"):
        raise ValueError("Missing required key: embargo_date")
    if metadata.get("image_type") and metadata.get("image_type") not in image_types:
        raise ValueError(f"Invalid image_type: {metadata.get('image_type')}")
    if (
        metadata.get("publication_type")
        and metadata.get("publication_type") not in publication_types
    ):
        raise ValueError(
            f"Invalid publication_type: {metadata.get('publication_type')}"
        )
    return metadata
