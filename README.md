# **Airflow DAG: PostgreSQL to Elasticsearch Data Pipeline**

## **Project Overview**

This project demonstrates the use of Apache Airflow to automate a data pipeline that fetches data from a PostgreSQL database, performs data cleaning operations, and then posts the cleaned data to an Elasticsearch index. This pipeline is defined and managed using Apache Airflow, showcasing the integration of multiple technologies such as PostgreSQL, Pandas, and Elasticsearch.

## **Project Background**

In modern data processing workflows, automating data pipelines is essential for efficient and reliable data management. Airflow is a powerful tool for defining, scheduling, and monitoring workflows as Directed Acyclic Graphs (DAGs). This project aims to illustrate how Airflow can be utilized to automate a data pipeline that involves extracting data from a database, transforming (cleaning) it, and loading it into an Elasticsearch index for further analysis or querying.

## **Pipeline Overview**

### **Tasks in the DAG**

1. **Fetch Data from PostgreSQL:**

   - This task connects to a PostgreSQL database using a connection string and fetches data from the `table_m3`. The fetched data is then saved to a CSV file for further processing.
   - **Technology Used:** PostgreSQL, Pandas
   - **Output:** Raw data saved as `P2M3_wawan_data_raw.csv`

2. **Clean Data:**

   - The second task loads the raw data from the CSV file and performs data cleaning operations. It removes duplicate records, normalizes column names, and handles missing values by filling them with default values. The cleaned data is then saved to another CSV file.
   - **Technology Used:** Pandas
   - **Output:** Cleaned data saved as `P2M3_wawan_data_clean.csv`

3. **Post Data to Elasticsearch:**
   - The final task reads the cleaned data from the CSV file and indexes each row as a document in an Elasticsearch index (`table_m3_postgresql`). This enables efficient querying and analysis of the data in Elasticsearch.
   - **Technology Used:** Elasticsearch, Pandas
   - **Elasticsearch Index:** `table_m3_postgresql`

### **DAG Schedule**

- **Schedule Interval:** The DAG is configured to run daily at 06:30 AM, ensuring that the pipeline processes and loads new data into Elasticsearch on a regular basis.

## **Airflow Configuration**

### **Libraries and Tools Used**

- **Airflow**: To define, schedule, and manage the DAG and its tasks.
- **Pandas**: For data manipulation and cleaning.
- **psycopg2**: For connecting to the PostgreSQL database.
- **Elasticsearch-py**: For interacting with the Elasticsearch cluster.
- **Logging**: To log information during task execution for monitoring and debugging.

### **Default Arguments**

- **Owner**: `wawan`
- **Start Date**: `2024-07-21 10:40:00`
- **Retries**: 1
- **Retry Delay**: 1 minute

### **DAG Definition**

- **DAG Name**: `P2M3_wawan_DAG`
- **Description**: A data pipeline that fetches data from PostgreSQL, cleans it, and posts it to Elasticsearch.

### **Task Dependencies**

The tasks are executed in the following order:

1. Fetch Data from PostgreSQL
2. Clean Data
3. Post Data to Elasticsearch

## **How to Run the Project**

1. **Set Up Airflow Environment:** Ensure that Apache Airflow is installed and properly configured on your system.
2. **Install Required Libraries:** Use `pip install -r requirements.txt` to install all necessary Python libraries.
3. **Place the DAG File:** Place the `P2M3_wawan_DAG.py` file in the Airflow `dags` directory.
4. **Start Airflow:** Start the Airflow scheduler and webserver to begin executing the DAG.
5. **Monitor DAG Execution:** Use the Airflow web interface to monitor the execution of the DAG and troubleshoot any issues.

This project was developed as part of the Data Science program at Hacktiv8. Special thanks to mentors and colleagues for their guidance and support.
