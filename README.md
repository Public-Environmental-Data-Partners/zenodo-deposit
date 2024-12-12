# Zenodo Deposit

![Python tests](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml/badge.svg)](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml)

Not ready for prime time!

See: https://developers.zenodo.org

Initial test with URL:

```
uv run --directory src/zenodo_deposit zenodo-deposit --dev --log-level DEBUG upload --title 'Testing URL with larger dataset' --type 'dataset' --keywords 'rmp, epa' --name 'Fitzgerald, Will' --affiliation 'EDGI' --description 'Location datbase' --metadata ../../metadata_sample.toml https://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv
```
