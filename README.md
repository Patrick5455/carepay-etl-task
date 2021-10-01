## CAREPAY DATA CHALLENGE

### About


#### Project Structure
The project has the ffg directories in the main directory [carepay_etl]("./carepay_etl'): 
- 

### How to Run Project - Instructions
- create a service account on google cloud with a bigquery access
- create a project and generate a key and download the json file. 
- This json file containing your bigquery credentials should be stored locally in the "carepay_etl/jobs folder"
- run the docker file found in mysql_docker_build to have a running mysql container
- run the Dockerfile located at the root of the project 
- alternatively run the main.py file at the root of [carepay_etl directory]("./carepay_etl'):

### Tooling and  Justification

### Choice of transformation of data_output

I decided to use parquet as the choice transformation output file for the ffg reasons:

- it is a columnar storage file which fits perfectly with BigQuery the choice Data Warehouse which is alss a columnar type data warehouse
- Consumption of less spade given that we want to be efficient with use of resources
- lastly, given that the end user are mostly analysts, parquet is a better choice compared to avro, 
- accoring to this [source](https://blog.clairvoyantsoft.com/big-data-file-formats-3fb659903271) PARQUET is much better for analytical querying i.e. reads and querying are much more efficient than writing.

### Currently Known Bug
- dataset_id already created on a service account throws some errors
- to avoid this, modify the dataset_id in the constants.py file


