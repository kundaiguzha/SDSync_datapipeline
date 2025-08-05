from pyspark.sql import SparkSession
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd
import json
import os
import glob

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessJSONFilesIntoSingleDF").getOrCreate()

# Define the folder path
folder_path = '/opt/airflow/s3-drive/bronze_data/details/'

# Get list of JSON files in the folder
json_files = glob.glob(os.path.join(folder_path, "*.json"))

# Define schema for DataFrame
schema = StructType([
    StructField("uri", StringType(), True),
    StructField("rn", StringType(), True),
    StructField("name", StringType(), True),
    StructField("inchi", StringType(), True),
    StructField("inchiKey", StringType(), True),
    StructField("smile", StringType(), True),
    StructField("canonicalSmile", StringType(), True),
    StructField("molecularFormula", StringType(), True),
    StructField("molecularMass", FloatType(), True),
    StructField("hasMolfile", StringType(), True)
])

# Initialize an empty list to collect data
all_data = []

# Iterate over each JSON file
for file_path in json_files:
    print(f"Processing file: {file_path}")
    try:
        # Load JSON data
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Prepare data as a dictionary with proper type casting
        row = {
            "uri": str(data.get('uri', '')),
            "rn": str(data.get('rn', '')),
            "name": str(data.get('name', '')),
            "inchi": str(data.get('inchi', '')),
            "inchiKey": str(data.get('inchiKey', '')),
            "smile": str(data.get('smile', '')),
            "canonicalSmile": str(data.get('canonicalSmile', '')),
            "molecularFormula": str(data.get('molecularFormula', '')),
            "molecularMass": float(data.get('molecularMass', 0.0)),  # Cast to float
            "hasMolfile": str(data.get('hasMolfile', ''))
        }

        # Append to the data list
        all_data.append(row)

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

# Create a single DataFrame from all collected data
if all_data:
    df = spark.createDataFrame(all_data, schema)
    
    # Show schema and data for verification
    print("Final DataFrame Schema:")
    df.printSchema()
    print("Final DataFrame Data:")
    
    # Write Paquet files to the silver folder
    output_path = r'/opt/airflow/s3-drive/silver_data/details/output.parquet'
    df_pandas=df.toPandas()
    df_pandas.to_parquet(output_path, index=False)
    # df_pandas.write.mode('overwrite').option('header',True).parquet(output_path)

    df.show(truncate=False)
else:
    print("No valid data was found in the JSON files.")
    df = spark.createDataFrame([], schema)

# Stop Spark session
spark.stop()