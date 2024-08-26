# assignment

## How to run
Run the below command
`source initial_setup.sh`

You can access the airflow server in http://localhost:8082. User is `admin` and password will be in `airflow/standalone_admin_password.txt` file.


## Issue in starting server
If any issue in not starting the server, run these commands
```
CONTAINER_ID="$(sudo docker compose ps -q)" # Get the id
sudo docker exec -u root -ti $CONTAINER_ID bash
```


## About the assignment
The 2 pipelines are designed with DAGs.

### Pipeline 1
The 1st pipeline is purely a data ingestion pipeline with only 1 specific task.
All the data scraping, data preparation, data cleaning and data storage is handled
in 1 task. The data is stored in sqlite file storage `airflow/ingested_data.db`
file.

### Pipeline 2
The 2nd pipeline is divided into 4 tasks.
1. Mean age of users
2. Top 20 rated movies
3. Top 5 geners for each age group for each occupation
4. Top 10 similar movies
