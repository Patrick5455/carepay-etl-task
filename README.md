## CAREPAY DATA CHALLENGE

### About
This is an ETL task to extract data from a source system, a MySQL database, and move it to a single source of truth system. 
In the single source of truth system the data should be cleaned and should be structured in a way that would allow for easy and low-latency queries.

### How to Run Project - Instructions

> Prerequisites
- create a service account on google cloud with a bigquery access
- create a project and generate a key and download the json file. 
- This json file containing your bigquery credentials should be stored locally in the "carepay_etl/jobs folder"

> Running Docker files
- build/run the docker file found in `mysql_docker_build` to have a running mysql container
- build/run the Dockerfile located at the root of the project

> Running Docker-compose file
- run docker-compose up at the root of the project where the docker-compose.yaml fileis located

> Run from code
- Alternatively run the main.py file at the root of [carepay_etl directory]("./carepay_etl'):
- But make sure  the database container is running [using the Dockerfile found in the `mysql_docker_build` directory](""") 

### Tooling and  Justification
- Python: I used python as the language for development because of it allows for quick Proof of Concept 
- Pandas: I used pandas both in the extraction and transformation layer because of its support to interact with various data engineering modules and tools. 
- BigQuery: I had plans to work with AWS initially but I found BigQuery python setup much faster plus it is alos a columnar data warehouse which fits into my choice of parquet files
- Docker: It provides easily reproducible environment setup for another user of the codebase

### Choice of transformation of data_output

I decided to use parquet as the choice transformation output file for the ffg reasons:

- it is a columnar storage file which fits perfectly with BigQuery the choice Data Warehouse which is alss a columnar type data warehouse
- Consumption of less spade given that we want to be efficient with use of resources
- lastly, given that the end user are mostly analysts, parquet is a better choice compared to avro, 
- accoring to this [source](https://blog.clairvoyantsoft.com/big-data-file-formats-3fb659903271) PARQUET is much better for analytical querying i.e. reads and querying are much more efficient than writing.
