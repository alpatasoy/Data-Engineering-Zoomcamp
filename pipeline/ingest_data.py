
from sys import prefix
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]






def run():
    year = 2021
    month = 1
    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'
    chunksize = 10
    table_name = 'yellow_taxi_data'
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    
    df_iter = pd.read_csv(
        url,
        nrows=100,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator= True, # activating the iterator
        chunksize = chunksize # records per batch
    )

    first= True
    for df_chunk in tqdm(df_iter):
        if first:
            #first one creates the empty table
            df_chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
            first = False
            print("Table created")
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
        print("ingested: ", len(df_chunk))

if __name__ == '__main__':
    run()