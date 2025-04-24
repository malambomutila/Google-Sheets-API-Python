# Google Sheets API Python Integration

This repo demonstrates how to integrate Google Sheets with Python, extract data, transform it, and load it into both CSV files and PostgreSQL databases using Apache Airflow.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
  - [1. Setting up Google Cloud Project](#1-setting-up-google-cloud-project)
  - [2. Creating Service Account &amp; Credentials](#2-creating-service-account--credentials)
  - [3. Setting up Google Sheet](#3-setting-up-google-sheet)
  - [4. Installing Dependencies](#4-installing-dependencies)
  - [5. Configure Airflow](#5-configure-airflow)
- [Project Structure](#project-structure)
- [Code Breakdown](#code-breakdown)
  - [Imports &amp; Setup](#imports--setup)
  - [Google Sheets Authentication](#google-sheets-authentication)
  - [Data Fetching](#data-fetching)
  - [Data Transformation](#data-transformation)
  - [Data Export](#data-export)
- [Usage](#usage)
- [DAG Description](#dag-description)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Overview

This project provides a beginner-friendly guide to extract data from Google Sheets using the Google Sheets API v4, perform data transformations, and export the results to both CSV files and PostgreSQL databases. The entire workflow is orchestrated using Apache Airflow.

## Features

- Automated data extraction from Google Sheets
- Data cleaning and transformation
- Export to CSV files
- Loading data into PostgreSQL
- Scheduled execution with Apache Airflow
- Secure authentication with Google Service Accounts
- Comprehensive error handling and logging

## Prerequisites

- Python 3.7+
- Apache Airflow 2.0+
- Google Cloud account
- PostgreSQL database
- `pandas`, `google-auth`, `google-api-python-client` Python packages

## Data Source

Data was pulled from the following Google Sheet:

<img src="images/16_Google_Spreeadsheet.png" alt="Google Spreadsheet" width="800" />

The URL was:

```bash
https://docs.google.com/spreadsheets/d/1Im0jSDB8j6t2afZyq4xvWpERwFXv5Ttw1qrRMUnIBhU/edit?gid=1454368404#gid=1454368404
```

## Setup Instructions

### 1. Setting up Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)

<img src="images/02_Google_Cloud_Home.png" alt="Google Cloud Home" width="700" />

2. Open the Project Picker using Ctrl + O or by clicking the displayed project name next to the Google Cloud logo.

<img src="images/03_New_Project.png" alt="New Project" width="700" />

3. Name your project (e.g., "**GSheets API**"). Note your Project ID (will look like `gsheets-api-457710`).

<img src="images/04_New_Project_Name.png" alt="New Project Name" width="400" />

4. Return to your Google Cloud home. Open the Project Picker and selected the created project.

### 2. Creating Service Account & Credentials

1. In your Google Cloud project home, navigate to "APIs & Services" > "Enable APIs and services". This will open the API Library.

* Project Home

<img src="images/05_Project_Home.png" alt="Project Home" width="500" />

* APIs and Services

<img src="images/06_API_Services.png" alt="API Services" width="500" />

* API Library

<img src="images/07_After_Selecting_Enable_APIs.png" alt="After Selecting Enable APIs" width="500" />

2. Search for "Google Sheets API" and enable it.
3. Go to "APIs & Services" > "Credentials" > "Create Credentials"

<img src="images/10_Credentials.png" alt="Create Credentials" width="500" />

4. This will lead you to the "Credential Type" form. Select "Google Sheets API" and "Application data".

<img src="images/11_Credential_Type.png" alt="Credential Type" width="500" />

5. Complete the "Service Account" form and grant appropriate roles (e.g., "Viewer" for read-only access or "Editor" to read and write.)

<img src="images/12_service_acc_details.png" alt="Service Account Details" width="500" />

<img src="images/13_service_acc_role.png" alt="Service Account Role" width="500" />

Take note of the email address for the service account. In this case:

```bash
python-gsheets-api@gsheets-api-457710.iam.gserviceaccount.com
```

7. Create and download the JSON key file by goinging to "APIs & Services" > "Enabled APIs and services".
   Click the service email under "Service Accounts". Navigate to "Keys" > "ADD KEY" and choose the "JSON" key type.

<img src="images/15_Create_and_Download_Key.png" alt="Create and Download Key" width="500" />

8. Place this JSON file in your project's `data` directory as `gsheetscreds.json`

The downloaded JSON will be in this format:

```json
{
  "type": "service_account",
  "project_id": "gsheets-api-457710",
  "private_key_id": "ccff98b00397c59aefa976de249a20a390823dc1",
  "private_key": "-----BEGIN PRIVATE KEY-----\n.dfdfdfdfd...",
  "client_email": "python-gsheets-api@gsheets-api-457710.iam.gserviceaccount.com",
  "client_id": "105000005713844191512",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-gsheets-api...",
  "universe_domain": "googleapis.com"
}
```

However, I added the Spreadsheet ID and Sheet ID to it. This is optional but if applied then the dictionary/json should be read in the exact way as this project's script. The final JSON was as follows:

```json
{
  "google_acc": {
    "type": "service_account",
    "project_id": "gsheets-api-457710",
    "private_key_id": "ccff98b00397c59aefa976de249a20a390823dc1",
    "private_key": "-----BEGIN PRIVATE KEY-----\n.dfdfdfdfd...",
    "client_email": "python-gsheets-api@gsheets-api-457710.iam.gserviceaccount.com",
    "client_id": "105000005713844191512",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-gsheets-api...",
    "universe_domain": "googleapis.com"
  },
  "spreadsheet": {
    "spreadsheet_id": "1Im0jSDB8j6t2afZyq4xvWpERwFXv5Ttw1qrRMUnIBhU",
    "sheet_id": "145436404"
  }
}
```

### 3. Setting up Google Sheet

1. Create or use an existing Google Sheet
2. Note the Spreadsheet ID (from the URL: `https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit`)
3. Find the Sheet ID (GID) from the URL when viewing a specific sheet (`...#gid=SHEET_ID`)
4. Share your Google Sheet with the service account email (found in the JSON credentials file) and Google Console.

From our URL

```bash
https://docs.google.com/spreadsheets/d/1Im0jSDB8j6t2afZyq4xvWpERwFXv5Ttw1qrRMUnIBhU/edit?gid=1454368404#gid=1454368404
```

The spreadsheet ID was:

```bash
1Im0jSDB8j6t2afZyq4xvWpERwFXv5Ttw1qrRMUnIBhU
```

and the Sheet ID (GID) was:

```bash
1454368404
```

### 4. Installing Dependencies

```bash
pip install pandas google-auth google-api-python-client apache-airflow apache-airflow-providers-postgres sqlalchemy
```

### 5. Configure Airflow

1. Set up a PostgreSQL connection in Airflow. In this project it is named "superset_db".
2. Place the DAG Python file in your Airflow DAGs folder.

## Project Structure

```
├── dags/
│   └── gsheetsapi_dag.py
├── data/
│   ├── gsheetscreds.json
│   └── xfootball_fans.csv
├── images/
├── LICENSE
└── README.md
```

## Code Breakdown

### Imports & Setup

The script begins by importing necessary libraries:

```python
import logging
import os
import json
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

from datetime import datetime, timedelta
from sqlalchemy.types import VARCHAR, DATE, Text, SMALLINT, Float
```

### Google Sheets Authentication

Authentication is handled through the service account credentials:

```python
def get_creds():
    """Get credentials for Google Sheets API."""
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    try:
        gsheetscreds = load_config()
        creds = Credentials.from_service_account_info(gsheetscreds["google_acc"], scopes=SCOPES)
        SPREADSHEET_ID = gsheetscreds["spreadsheet"]["spreadsheet_id"]
        SHEET_ID = int(gsheetscreds["spreadsheet"]["sheet_id"])
        return creds, SPREADSHEET_ID, SHEET_ID
    except KeyError as e:
        raise AirflowFailException(f"Missing expected config key: {e}")
```

### Data Fetching

The script connects to the Google Sheets API and fetches data:

```python
def fetch_data_from_gsheet():
    """Fetch data from Google Sheets."""
    service, spreadsheet = get_google_sheets_client()
    creds, SPREADSHEET_ID, SHEET_ID = get_creds()
    sheet_title = get_sheet_title_by_id(spreadsheet, SHEET_ID)
  
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_title
        ).execute()
        data = result.get("values", [])
        # ...
```

### Data Transformation

Raw data is transformed into a structured format:

```python
def clean_and_transform_data():
    """Clean and transform the data."""
    data = fetch_data_from_gsheet()
    # Convert raw data into a DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])

    # Rename columns to match expected schema
    df = df.rename(columns={
        "Officer": "officer",
        "Football Club": "football_club",
        # ...
```

### Data Export

The data is exported to both CSV and PostgreSQL:

```python
def export_to_csv():
    new_df = clean_and_transform_data()
    try:
        new_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
        logger.info(f"CSV replaced with latest data: {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        raise AirflowFailException("CSV export failed.")
```

```python
def export_to_postgres():
    df = clean_and_transform_data()
    try:
        hook = PostgresHook(postgres_conn_id="superset_db")
        engine = hook.get_sqlalchemy_engine()

        dtype_mapping = {
            "officer": VARCHAR(64),
            # ...
        }

        df.to_sql(
            "xfootall_fans",  
            con=engine,
            index=False,
            if_exists="replace",
            dtype=dtype_mapping
        )
```

## Usage

1. Ensure your credentials file (`gsheetscreds.json`) is properly configured:

```json
{
  "google_acc": {
    "type": "service_account",
    "project_id": "gsheets-api-457710",
    "private_key_id": "ccff98b00397c59aefa976de249a20a390823dc1",
    "private_key": "-----BEGIN PRIVATE KEY-----\n.dfdfdfdfd...",
    "client_email": "python-gsheets-api@gsheets-api-457710.iam.gserviceaccount.com",
    "client_id": "105000005713844191512",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-gsheets-api...",
    "universe_domain": "googleapis.com"
  },
  "spreadsheet": {
    "spreadsheet_id": "1Im0jSDB8j6t2afZyq4xvWpERwFXv5Ttw1qrRMUnIBhU",
    "sheet_id": "145436404"
  }
}
```

2. Place your DAG file in the Airflow DAGs directory
3. Unpause the DAG in Airflow UI
4. The DAG will run hourly, fetching data from your Google Sheet and exporting it to CSV and Postgres. You many remove either functions if you don't wish to have both CSV and Postgres exports.

## DAG Description

The DAG is configured to run on an hourly schedule with the following tasks:

- `Start_task`: Marks the beginning of the workflow
- `Export_To_CSV`: Fetches data from Google Sheet and exports to CSV
- `Export_To_Postgres`: Fetches data from Google Sheet and loads into PostgreSQL
- `End_task`: Marks the successful completion of the workflow

Task dependencies:

```
start_task >> [export_to_csv, export_to_postgres] >> end_task
```

## Troubleshooting

- **"Credentials file not found"**: Ensure `gsheetscreds.json` is in the correct directory
- **"No data returned from the spreadsheet"**: Check sharing permissions and sheet ID
- **"Failed to export to PostgreSQL"**: Verify Airflow PostgreSQL connection is properly configured
- **Permission issues**: Make sure your Google Sheet is shared with the service account email

## Outputs

1. CSV Output

<img src="images/00_csv_output.png" alt="CSV Output" width="800" />

2. Postgres Output

<img src="images/00_PostgresOutput.png" alt="CSV Output" width="800" />

## License

See the LICENSE file for details.
