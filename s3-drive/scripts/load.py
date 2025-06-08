import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Path to the SQLite database file
db_file = "/opt/airflow/s3-drive/gold_data/sdsync.db"  # Update with your SQLite database file path

# Path to the Parquet file
parquet_file = "/opt/airflow/s3-drive/silver_data/your_parquet_file.parquet"  # Update with your Parquet file path

# Create SQLAlchemy engine for SQLite
engine = create_engine(f"sqlite:///{db_file}")

try:
    # Create the 'molecules' table if it doesn't exist
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS molecules (
                uri TEXT,
                rn TEXT,
                name TEXT,
                inchi TEXT,
                inchiKey TEXT,
                smile TEXT,
                canonicalSmile TEXT,
                molecularFormula TEXT,
                molecularMass REAL,
                hasMolfile TEXT
            )
        """)
        print("Table 'molecules' created or already exists.")

    # Read Parquet file into a pandas DataFrame
    df = pd.read_parquet(parquet_file)

    # Load DataFrame into the 'molecules' table
    df.to_sql(name="molecules", con=engine, if_exists="append", index=False)
    print("Data successfully loaded into the 'molecules' table.")

except SQLAlchemyError as e:
    print(f"Error loading data: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Dispose of the engine
    engine.dispose()