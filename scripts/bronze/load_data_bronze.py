""" 
===========================================================================================
Load Bronze Layer (Source -> Bronze)
===========================================================================================
Script Purpose:
- This Script loads data into bronze schema from external source(csv) file.
- It Perform the following actions:
    - Truncate the bronze table beform loading data.
    - Uses the 'Load Data Local Infile' command to load data from csv file to bronze tables.
Parameters:
    - host: MySQL server host
    - user: MySQL username
    - password: MySQL password
    - database: Target database name (bronze)
    - staging_tables: List of tuples containing table names and corresponding file paths
Usage:
    - Configure the DB_CONFIG dictionary with your database connection details.
    - Update the staging_tables list with your table names and file paths.
    - Run the script to load data into the bronze layer.
===========================================================================================
"""

import pymysql
from pymysql.err import MySQLError
import time
from datetime import datetime, timedelta


# -------------- Connect to MySQL-------------

def load_data_infile(host, user, password, database, staging_tables):
    """
    Loads data from a local file into a MySQL table using LOAD DATA LOCAL INFILE.
    """
    try:
        # Connect to the database, enabling local_infile
        cnx = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            local_infile=True # This is key for the LOCAL keyword
        )
        cursor = cnx.cursor()

        for table, file_path in staging_tables:
            sd = datetime.now()
            st = time.perf_counter()

            # Step 1: Truncate table
            query1 = f"TRUNCATE TABLE `{table}`;"

        # Step 2: Load data from local file into the table
        # The LOAD DATA LOCAL INFILE SQL statement
        # Adjust FIELD TERMINATED BY, ENCLOSED BY, and IGNORE lines as per your file format (e.g., CSV, TSV)
            query = f"""
            LOAD DATA LOCAL INFILE '{file_path}'
            INTO TABLE {table}
            FIELDS TERMINATED BY ','
            IGNORE 1 ROWS; -- Ignores the header row in a CSV file
            """

            cursor.execute(query1)
            cursor.execute(query)

            ed = datetime.now()
            et = time.perf_counter()
            ds = et - st
            print(f"Load Duration of {table}: {ds:.4f} seconds")
        # Commit the transaction
        cnx.commit()
        print("All staging tables loaded successfully!")

    except MySQLError as err:
        if err.errno == 1045:
            print("Something is wrong with your user name or password")
        elif err.errno == 1049:
            print("Database does not exist")
        else:
            print(f"An error occurred: {err}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals():
            cnx.close()
            print("MySQL connection closed.")

# ------------------------------
# Configuration
# ------------------------------
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'mysql345',
    'database': 'bronze'
}

# List of staging tables and corresponding CSV files
staging_tables = [
    ("crm_cust_info", "/Users/bharataytoda/Documents/Data_Warehouse/datasets/source_crm/cust_info.csv"),
    ("crm_prd_info",  "/Users/bharataytoda/Documents/Data_Warehouse/datasets/source_crm/prd_info.csv"),
    ("crm_sales_details", "/Users/bharataytoda/Documents/Data_Warehouse/datasets/source_crm/sales_details.csv"),
    ("erp_cust_az12",  "/Users/bharataytoda/Documents/Data_Warehouse/datasets/source_erp/cust_az12.csv"),
    ("erp_loc_a101",  "/Users/bharataytoda/Documents/Data_Warehouse/datasets/source_erp/loc_a101.csv"),
    ("erp_px_cat_g1v2", "/Users/bharataytoda/Documents/Data_Warehouse/datasets/source_erp/px_cat_g1v2.csv")
]

print("Starting data load into Bronze Layer")
# Record the start time (using datetime for a readable format)
start_datetime = datetime.now()
start_time_perf = time.perf_counter()
print(f"Process started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")


load_data_infile(
    DB_CONFIG['host'],
    DB_CONFIG['user'],
    DB_CONFIG['password'],
    DB_CONFIG['database'],
    staging_tables
)

# Record the end time
end_datetime = datetime.now()
end_time_perf = time.perf_counter()

duration_seconds = end_time_perf - start_time_perf 
duration_timedelta = timedelta(seconds=duration_seconds)

print(f"Process ended at:   {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration:           {duration_timedelta}")
print(f"Duration (seconds): {duration_seconds:.4f} seconds")