import pandas as pd
from sqlalchemy import create_engine

# Database connection details
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = 'localhost'
DB_PORT = 5433  # Change if your container exposes a different port
DB_NAME = 'ny_taxi'

# CSV file paths
TRIPDATA_CSV_FILE = '../data/green_tripdata_2019-10.csv'
ZONE_LOOKUP_CSV_FILE = '../data/taxi_zone_lookup.csv'

# PostgreSQL connection string
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine
engine = create_engine(connection_string)

def ingest_csv_to_postgres(csv_file, table_name, engine):
    """
    Function to ingest a CSV file into a PostgreSQL table.
    If the table already exists, it will be replaced.
    """
    print(f"Reading the CSV file for table '{table_name}'...")
    try:
        df = pd.read_csv(csv_file)

        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]

        # Write DataFrame to PostgreSQL table
        print(f"Inserting data into the table '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data for table '{table_name}' inserted successfully!")
    except Exception as e:
        print(f"Error ingesting table '{table_name}': {e}")

# Main script
if __name__ == "__main__":
    try:
        # Ingest green taxi trip data
        ingest_csv_to_postgres(TRIPDATA_CSV_FILE, 'green_taxi_trips', engine)

        # Ingest taxi zone lookup data
        ingest_csv_to_postgres(ZONE_LOOKUP_CSV_FILE, 'taxi_zone_lookup', engine)
    except Exception as main_exception:
        print(f"An error occurred during the import process: {main_exception}")
