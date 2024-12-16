# Zenodo Deposit

[![Python tests](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml/badge.svg)](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml)

Not ready for prime time!

See: https://developers.zenodo.org

Initial test with URL:

```
uv run --directory src/zenodo_deposit zenodo-deposit --dev --log-level DEBUG upload --title 'Testing URL with larger dataset' --type 'dataset' --keywords 'rmp, epa' --name 'Fitzgerald, Will' --affiliation 'EDGI' --description 'Location datbase' --metadata ../../metadata_sample.toml https://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv
```

Check the cli.py code for all the options

The file spec can be

- a single file
- a directory
- a URL

If the directory contains more than 100 files, it will be zipped up and uploaded as a single file.

The result comes back in JSON format.

For example:

```json
{
  "created": "2024-12-16T18:51:11.121129+00:00",
  "modified": "2024-12-16T18:51:11.272284+00:00",
  "id": 143439,
  "conceptrecid": "143438",
  "doi": "10.5072/zenodo.143439",
  "conceptdoi": "10.5072/zenodo.143438",
  "doi_url": "https://doi.org/10.5072/zenodo.143439",
  "metadata": {
    "title": "Testing dirctory",
    "doi": "10.5072/zenodo.143439",
    "publication_date": "2024-12-16",
    "description": "State Level RMP datasheets",
    "access_right": "open",
    "creators": [
      {
        "name": "Fitzgerald, Will",
        "affiliation": "EDGI"
      }
    ],
    "keywords": ["rmp, epa"],
    "license": "cc-zero",
    "imprint_publisher": "Zenodo",
    "upload_type": "dataset",
    "prereserve_doi": {
      "doi": "10.5281/zenodo.143439",
      "recid": 143439
    }
  },
  "title": "Testing dirctory",
  "links": {
    "self": "https://sandbox.zenodo.org/api/records/143439",
    "html": "https://sandbox.zenodo.org/records/143439",
    "doi": "https://doi.org/10.5072/zenodo.143439",
    "parent_doi": "https://doi.org/10.5072/zenodo.143438",
    "badge": "https://sandbox.zenodo.org/badge/doi/10.5072%2Fzenodo.143439.svg",
    "conceptbadge": "https://sandbox.zenodo.org/badge/doi/10.5072%2Fzenodo.143438.svg",
    "files": "https://sandbox.zenodo.org/api/records/143439/files",
    "bucket": "https://sandbox.zenodo.org/api/files/71e7bb47-da7b-4072-ba4c-b8d82f10c12f",
    "latest_draft": "https://sandbox.zenodo.org/api/deposit/depositions/143439",
    "latest_draft_html": "https://sandbox.zenodo.org/deposit/143439",
    "publish": "https://sandbox.zenodo.org/api/deposit/depositions/143439/actions/publish",
    "edit": "https://sandbox.zenodo.org/api/deposit/depositions/143439/actions/edit",
    "discard": "https://sandbox.zenodo.org/api/deposit/depositions/143439/actions/discard",
    "newversion": "https://sandbox.zenodo.org/api/deposit/depositions/143439/actions/newversion",
    "registerconceptdoi": "https://sandbox.zenodo.org/api/deposit/depositions/143439/actions/registerconceptdoi",
    "record": "https://sandbox.zenodo.org/api/records/143439",
    "record_html": "https://sandbox.zenodo.org/record/143439",
    "latest": "https://sandbox.zenodo.org/api/records/143439/versions/latest",
    "latest_html": "https://sandbox.zenodo.org/record/143439/versions/latest"
  },
  "record_id": 143439,
  "owner": 28794,
  "files": [
    {
      "id": "9862289f-2086-4d6d-9dec-c2511a199f26",
      "filename": "michigan.zip",
      "filesize": 2674827,
      "checksum": "4440ee3b2a33f0b5af20da2c1f015306",
      "links": {
        "self": "https://sandbox.zenodo.org/api/records/143439/files/9862289f-2086-4d6d-9dec-c2511a199f26",
        "download": "https://sandbox.zenodo.org/api/records/143439/draft/files/michigan.zip/content"
      }
    }
  ],
  "state": "done",
  "submitted": true
}
```

The DOI is in the `.metadata.doi` field.

---
