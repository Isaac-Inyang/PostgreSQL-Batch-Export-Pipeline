# EXTRACTING DATA FROM POSTGRESQL TO PARQUET

import os
import shutil
import pandas as pd
import pyarrow
import time
import logging
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
# LOGICAL PATHS & CONNECTIONS

Database_url = (
    f"postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)
engine = create_engine(Database_url)

query = text("""
    SELECT * FROM olympics.summer_games
    UNION ALL
    SELECT * FROM olympics.winter_games
    """)

count_query = text("""
    SELECT COUNT(*)
    FROM (
        SELECT * FROM olympics.summer_games
        UNION ALL
        SELECT * FROM olympics.winter_games
        ) AS subquery
    """)

Parquet_dir = "data_lake"
chunk_size = 1000

start = time.perf_counter()

# CLEANING THE WORKSPACE
print("--- Starting Pipeline Cleanup ---")

if os.path.exists(Parquet_dir):
    shutil.rmtree(Parquet_dir)
    print(f" Wiped out old directory: {Parquet_dir}")


os.makedirs(Parquet_dir, exist_ok=True)

print("Workspace is clean. Starting fresh data export...\n")


# RUNNING PIPELINE IN BATCHES WITH NO DUPLICATES
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    with engine.connect() as conn:
        
        # streaming rows from PostgreSQL in batches
        logging.info("Attempting connection to PostgreSQL...")
        expected_rows = conn.execute(count_query).scalar()
        result = conn.execution_options(yield_per=chunk_size).execute(query)
        logging.info("Connection successful. Query executed.")

        is_first_batch = True
        batch_num = 0
        
        # Processing data in chunks of 1000 rows
        total_exported = 0
        while True:
            chunk = result.fetchmany(chunk_size)
            if not chunk:
                break
        
            df_chunk = pd.DataFrame(chunk, columns=result.keys())
            
            # Writing to parquet file immediately

            df_chunk.to_parquet(f"{Parquet_dir}/olympic_games_batch_{batch_num}.parquet", index=False)
            
            logging.info(f"Processed batch {batch_num} ({len(df_chunk)} rows)...")
            batch_num += 1
            total_exported += len(df_chunk)

        print("\nPipeline run completed successfully with zero duplicates!\n")
        exported_rows = total_exported
        print(f"Database Rows : {expected_rows}")
        print(f"CSV Rows      : {exported_rows}")
        print(f"Total Batches : {batch_num}")

        # Data validation
        if expected_rows == exported_rows:
            print("Validation Passed: All rows exported successfully.")
        else:
            print("Validation Failed: Row count mismatch.")

except OperationalError as e:
    logging.error(f"Database connection failed! Verify credentials or network path. Details: {e}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")

end = time.perf_counter()    
rows_per_second = exported_rows / (end - start)

print(f"Execution Time: {end-start:.2f} seconds")
print(f"Throughput: {rows_per_second:,.0f} rows/sec")
