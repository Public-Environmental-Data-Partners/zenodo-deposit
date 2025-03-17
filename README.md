# zenodo-deposit: A command line interface to making file deposits to Zenoodo

[![Python tests](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml/badge.svg)](https://github.com/willf/zenodo-deposit/actions/workflows/test.yml)
[![Commit activity](https://img.shields.io/github/commit-activity/m/willf/zenodo-deposit)](https://img.shields.io/github/commit-activity/m/willf/zenodo-deposit)
[![License](https://img.shields.io/github/license/willf/zenodo-deposit)](https://img.shields.io/github/license/willf/zenodo-deposit)

A command line interface to making file deposits to Zenoodo

- **Github repository**: <https://github.com/willf/zenodo-deposit/>
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
  add-metadata     Add metadata to the deposition
  create           Create a new deposition, without uploading a file
  delete           Delete the deposition
  deposit          Deposit a file
  new-version      Create a new version of a deposition
  publish          Publish the deposition
  retrieve         Retrieve deposition details
  update-metadata  Update the metadata of the deposition
  upload           Upload one or more files, with metadata

```

Example:

```bash
$ zd --dev --log-level DEBUG upload --title 'Testing URL with larger dataset' --type 'dataset' --keywords 'rmp, epa' --name 'Fitzgerald, Will' --affiliation 'EDGI' --description 'Location database' --metadata metadata.toml https://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv
```
