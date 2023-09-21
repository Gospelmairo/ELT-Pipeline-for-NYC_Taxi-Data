# ELT-Pipeline-for-NYC_Taxi-Data
This repository contains a Python script that creates an ETL pipeline to fetch data from the NYC TLC public dataset, load it into Google Cloud Storage (GCS), and then load it into separate tables in BigQuery. The pipeline also includes loading the taxi_zone data into GCS and BigQuery.

# Prerequisites
Before running the script, ensure you have the following:

<ul>
  <li>
  Python: Make sure Python is installed on your machine. You can download and install Python from the official website: Python Downloads
  </li>

  <li>
  Google Cloud Platform (GCP) Account: Create a GCP account and set up a project with access to GCS and BigQuery.
  </li>
</ul>

## Overview on each file
1. EL_from_web_to_gcs.py : Extracts the data as a zipped csv file that is ziped with .gzip, uncompress the file and upload to the GCS bucket as a csv file.
2. NYC.sql : Sql queries that are used to gain some insights from the data

3. fhv_taxi_etl.py : Extracts the FHV(for-hire-vehicle) file from the GCS bucket, Transform and clean it, Load into BigQuery
4. green_taxi_etl.py : Extracts the green taxi service file from the GCS bucket, Transform and clean it, Load into BigQuery
5. taxi_zone_extract.py : Extracts the taxi zone lookup file from the github repo, Unzip and Load into GCS bucket, Transform and clean it, Load into BigQuery
6. yellow_taxi_etl.py : Extracts the yellow taxi service file from the GCS bucket, Transform and clean it, Load into BigQuery

## Programming language/ SQL Query
<ul>Python</ul>
<ul>SQL</ul>

## Libraries
os

google

tempfile

pandas

requests

gzip

