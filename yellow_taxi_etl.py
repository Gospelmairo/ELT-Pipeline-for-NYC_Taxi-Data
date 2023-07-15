from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
import tempfile


bq_client = bigquery.Client()
service = "yellow"
dataset_name = "Rio3631"
project_id = "myproject-389323"
bucket_name = 'my_altschool-bucket'


def Yellow():
    # ['vendor_id', 'pickup_time', 'dropoff_time', 'passengers',
    #    'distance', 'rate_code_id', 'store_and_fwd_flag',
    #    'pickup_location_id', 'dropoff_location_id', 'payment_type',
    #    'fare', 'extra', 'mta_tax', 'tip', 'tolls_amount',
    #    'improvement_surcharge', 'total_amount', 'congestion_surcharge',
    #    'airport_fee']


    bq_schema = [
        bigquery.SchemaField("vendor_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("passengers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("distance", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("rate_code_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("payment_type", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("fare", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tip", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("congestion_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("airport_fee", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE")
    ]


    table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
    table = bigquery.Table(table_id, schema=bq_schema)
    table = bq_client.create_table(table)  # Make an API request.
    print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)


    for i in ['01','02','03','04','05','06','07','08','09','10','11','12']:
        with tempfile.NamedTemporaryFile("w", suffix=".parquet") as tempdir:
            destination_uri = tempdir.name
            file_name = f'{service}_tripdata_2019-{i}.parquet'

            # Extract the file from Google Cloud Storage
            blob = bucket.blob(file_name)
            blob.download_to_filename(destination_uri)
            print(f"{i} {service} file successfully downloaded from Google Cloud Storage.")
        
            df = pd.read_parquet(destination_uri)
            print(f"{i} parquet file read into dataframe")

            # We will do some data cleaning 
            df = df.rename(index=str, columns={'VendorID' : 'vendor_id',\
                    'tpep_pickup_datetime' : 'pickup_time', \
                    'tpep_dropoff_datetime' : 'dropoff_time', \
                    'passenger_count' : 'passengers', \
                    'RatecodeID' : 'rate_code_id', \
                    'PULocationID' : 'pickup_location_id', \
                    'DOLocationID' : 'dropoff_location_id', \
                    'trip_distance' : 'distance', \
                    'fare_amount' : 'fare', \
                    'tip_amount' : 'tip'})

            # Remove duplicates - first step is to drop generic duplicates 
            df = df.drop_duplicates()

            # Replacing missing rows in the rate_code_id column with the most common rate code ID
            df['rate_code_id'] = df['rate_code_id'].fillna(df['rate_code_id'].mode()[0])

            # Dropping all rows with zero in the distance, fare, total_amount and improvement charge column
            df = df[df['distance'] > 0]
            df = df[df['fare'] > 0]
            print(f"After dropping duplicates, the dataframe is left with {df.shape[0]} rows")
            
            
            # change to pandas date time for further exploration and conversion
            df['pickup_time'] = df['pickup_time'].astype('str')
            df['dropoff_time'] = df['dropoff_time'].astype('str')

            # Categorising the trips into season (Summer & Winter)
            conditions = [(df['pickup_time'] >= '2018-12-01 00:00:00') 
                & (df['pickup_time'] <= '2019-02-29 23:59:59') 
                & (df['dropoff_time'] >= '2018-12-01 00:00:00') 
                & (df['dropoff_time'] <= '2019-02-29 23:59:59'),   (df['pickup_time'] >= '2019-03-01 00:00:00') 
                & (df['pickup_time'] <= '2019-05-31 23:59:59') 
                & (df['dropoff_time'] >= '2019-03-01 00:00:00') 
                & (df['dropoff_time'] <= '2019-05-31 23:59:59'), (df['pickup_time'] >= '2019-06-01 00:00:00') 
                & (df['pickup_time'] <= '2019-08-31 23:59:59') 
                & (df['dropoff_time'] >= '2019-06-01 00:00:00') 
                & (df['dropoff_time'] <= '2019-08-31 23:59:59'), (df['pickup_time'] >= '2019-09-01 00:00:00') 
                & (df['pickup_time'] <= '2019-11-30 23:59:59') 
                & (df['dropoff_time'] >= '2019-09-01 00:00:00') 
                & (df['dropoff_time'] <= '2019-11-30 23:59:59'),(df['pickup_time'] >= '2019-12-01 00:00:00') 
                & (df['pickup_time'] <= '2019-12-31 23:59:59') 
                & (df['dropoff_time'] >= '2019-12-01 00:00:00') 
                & (df['dropoff_time'] <= '2019-12-31 23:59:59'),
            ]

            values = ['winter','spring','summer', 'autumn', 'winter_0']
            df['season'] = np.select(conditions, values)
            df['season'] = df['season'].replace(['winter_0'], 'winter')

            df['pickup_time'] = pd.to_datetime(df['pickup_time'])
            df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])


            # Set up a job configuration
            job_config = bigquery.LoadJobConfig(autodetect=False)

            # Submit the job
            job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  

            # Wait for the job to complete and then show the job results
            job.result()  
            
            # Read back the properties of the table
            table = bq_client.get_table(table_id)  
            print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            print("JOB SUCCESSFUL")


Yellow()


