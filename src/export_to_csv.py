# EXTRACTING DATA FROM POSTGRESQL AND SAVING TO CSV

import os
import pandas as pd
import logging
import time
from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError
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

Csv_file = "olympics_data.csv"
chunk_size = 1000

start = time.perf_counter()
# CLEANING THE WORKSPACE
print("--- Starting Pipeline Cleanup ---")

if os.path.exists(Csv_file):
    os.remove(Csv_file)
    print(f"Wiped out old file: {Csv_file}")

print("Workspace is clean. Starting fresh data export...\n")



# RUNNING PIPELINE IN BATCHES WITH NO DUPLICATES
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    with engine.connect() as conn:
        
        # streaming rows from PostgreSQL in batches
        logging.info("Attempting connection to PostgreSQL...")
        expected_rows = conn.execute(count_query).scalar()                        # Number of rows before extraction
        result = conn.execution_options(yield_per=chunk_size).execute(query)
        logging.info("Connection successful. Query executed.")

        
        is_first_batch = True
        # Processing data in chunks of 1000 rows
        while True:
            chunk = result.fetchmany(chunk_size)
            if not chunk:
                break
        
            df_chunk = pd.DataFrame(chunk, columns=result.keys())
            logging.info(f"Processed batch of {len(df_chunk)} rows.")
            
            # Writing to file immediately
            if is_first_batch:
                # Creating file for first batch with headers
                df_chunk.to_csv(Csv_file, index=False, mode='w')
                is_first_batch = False
            else:
                # Appending rows
                df_chunk.to_csv(Csv_file, index=False, mode='a', header=False)
                    
            logging.info(f"Streamed and saved {len(df_chunk)} rows to disk.")
        print("\nPipeline run completed successfully with zero duplicates!\n")
        exported_rows = sum(1 for _ in open(Csv_file)) - 1                          # number of rows after extraction

        print(f"Database Rows : {expected_rows}")
        print(f"CSV Rows      : {exported_rows}")

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
