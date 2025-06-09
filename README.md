# SDSync ETL Datapipeline 

This project includes an ETL pipeline that intergrates Apache Airflow and Apache Spark for an automated and in-memory fast processing datapipeline, The pipeline has an igestion stage that injests data in the form of JSON files, a transforming stage which transforms the data to the proper data format and a loading stage wich then stores the processed data into a Database.

## Project Structure

The DAG `extract_chemicals` includes the following tasks:

- `start`: A DummyOperator that shows that the pipeline has been triggerd.
- `ingestion_task`: A pythonOperator that ingests raw data from an api and store it inside a bronze layer 
- `run_script`: A SparkSubmitOperator that submits a Python Spark job.
- `end`: A DummyOperator that shows that the jobs have run successfully.

These tasks are executed in a sequence where the `start` task triggers the Spark job, and upon their completion, the `end` task is executed.

## Prerequisites

Before setting up the project, ensure you have the following:

- Docker and Docker Compose installed on your system.
- Apache Airflow Docker image or a custom image with Airflow installed.
- Apache Spark Docker image or a custom image with Spark installed and configured to work with Airflow.
- Docker volumes for Airflow DAGs, logs, and Spark jobs are properly set up.

## Docker Setup

To run this project using Docker, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the directory containing the `docker-compose.yml` file.
3. Build and run the containers using Docker Compose:

```bash
docker-compose up -d --build
```
This command will start the necessary services defined in your docker-compose.yml, such as Airflow webserver, scheduler, Spark master, and worker containers.

## Directory Structure for Jobs
Ensure your Spark job files are placed in the following directories and are accessible to the Airflow container:

* Python job: jobs/python/transform.py
* Python jobs: jobs/python/ingestion.py
* Python jobs: s3_drive/scripts/load.py

These paths should be relative to the mounted Docker volume for Airflow DAGs.

## Usage
After the Docker environment is set up, the `sparking_flow` DAG will be available in the Airflow web UI [localhost:8080](localhost:8080), where it can be triggered manually or run on its daily schedule.

### The DAG will execute the following steps:
* start task which is a dummyOperator to show that the dag has started.
*ingestion task which injests JSON files and store them in a folder as raw data
* Submit the Python Spark job to the Spark cluster which will change the datatypes.

### Note:
You must add the spark cluster url to the spark connection in the configuration on Airflow UI

By Kundai Guzha.
