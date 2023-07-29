# Gajana

Project to auto import financial transactions from banks / Mutual Fund aggregators to Google sheets.

## Setup

* Python 3.11.3
* Setup virtual environment: `python3 -m venv venv`
* Install dependencies: `pip install -r requirements.txt`
* Copy and rename `example.env` to `.env` and add data
* Create a [Google Cloud Service account](https://cloud.google.com/iam/docs/service-accounts-create) and save the credentials json as `google_secret.json` in the root folder.
* Give access to the relevant Google sheets to the above service account.

## Dev Setup

* Install dev dependecies: `pip install -r requirements-dev.txt`
* Setup pre-commit with `pre-commit install`
