# zenodo-deposit: A command line interface to making file deposits to Zenodo

[![Python tests](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml/badge.svg)](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml)
[![Commit activity](https://img.shields.io/github/commit-activity/m/willf/zenodo-deposit)](https://img.shields.io/github/commit-activity/m/willf/zenodo-deposit)
[![License](https://img.shields.io/github/license/willf/zenodo-deposit)](https://img.shields.io/github/license/willf/zenodo-deposit)

A command line interface to making file deposits to Zenoodo

- **Github repository**: <https://github.com/Public-Environmental-Data-Partners/zenodo-deposit/>
- **Documentation** <https://pypi.org/project/zenodo-deposit/>

## Understanding the use case

Sometimes is it more convenient to make a deposit to [Zenodo](https://zenodo.org) using a
command line interface (CLI) rather than through the web interface. This
is especially true when you have a large number of files to deposit, or
when you want to automate the deposit process as part of a larger workflow.
This script provides tools to make file deposits to Zenodo using
the command line.

## Installation

This can installed using `pipx` or `uv`:

```bash
$ pipx install zenodo-deposit
```

or

```bash
$ uv tool install zenodo-deposit
```

## Configuration

Zenodo requires an API token to make the calls to create deposits. You can create one here:

[https://zenodo.org/account/settings/applications/tokens/new/](https://zenodo.org/account/settings/applications/tokens/new/)

The token should be given both **`publish`** and **`write`** scope (although one or the other may be fine
for your specific purposes).

It can be useful to use the Zenodo sandbox to test your processes ahead of time. You will
need a separate user id and API token for the sandbox. You can create an API key here:

[https://sandbox.zenodo.org/account/settings/applications/tokens/new/](https://sandbox.zenodo.org/account/settings/applications/tokens/new/)

The API keys need to go into a `.toml` file. It needs to look like this, replacing the values below
with the tokens created above. 

```toml
[zenodo]
ZENODO_ACCESS_TOKEN = 'your-production-token'
ZENODO_SANDBOX_ACCESS_TOKEN = 'your-sandbox-token'
```

Saving this as a file named `.zenodo-deposit-settings.toml` in your home directory will mean
you will not have to specify the configuration file each time you use it.

## Usage

```bash
$ zd --help
Usage: zd [OPTIONS] COMMAND [ARGS]...

Options:
  --version                       Show the version and exit.
  --sandbox, --dev / --production, --prod
                                  Set Zenodo environment to sandbox or
                                  production  [default: sandbox]
  --config-file PATH              Path to the configuration file
  --log-level [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Set the log level
  --help                          Show this message and exit.

Commands:
  create    Create a new deposition, without uploading a file
  deposit   Deposit a file
  retrieve  Retrieve deposition details
  search    Search for depositions
  upload    Upload one or more files, with metadata, creating a new deposit

```

Example:

```bash
$ zd --dev --log-level DEBUG upload --title 'Testing URL with larger dataset' --type 'dataset' --keywords 'rmp, epa' --name 'Fitzgerald, Will' --affiliation 'EDGI' --description 'Location database' --metadata metadata.toml https://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv
```

## Acknowledgements

This code was developed for EDGI.
