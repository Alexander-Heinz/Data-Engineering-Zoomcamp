## Module 3 Homework (DRAFT)

Solution: https://www.youtube.com/watch?v=8g_lRKaC9ro

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>


Steps taken:
# Steps for Creating an External Table in BigQuery

## 1. **Setup Google Cloud on Mac with CLI SDK**
   - Install Google Cloud SDK:
     ```bash
     brew install --cask google-cloud-sdk
     ```
   - Authenticate with Google Cloud:
     ```bash
     gcloud auth login
     ```
   - Set the project:
     ```bash
     gcloud config set project YOUR_PROJECT_ID
     ```

## 2. **Upload Data to Google Cloud Storage**
    - run `python load_yellow_taxi_data.py`

## 3. **Create the External Table in BigQuery**
   - Go to the [BigQuery Console](https://console.cloud.google.com/bigquery).
   - Create a new dataset (`green_taxi_data`). (Dataset erstellen) DO not use "link zu einem externen Dataset" as it is not what we want.
   - Create a new table: (Tabelle erstellen)
     - **Source**: Select **Google Cloud Storage**.
     - **File Format**: Choose **Parquet**.
     - **File Path**: Set the path to the data:
       ```bash
data-engineering-zoomcamp_hw03/green_tripdata_2022-*.parquet
       ```
     - **Table Type**: Choose **External Table**. (or NATIVE for native)
     - **Schema**: Set to **Auto-detect**.
     - Click **Create Table**.

## 4. **Verify the External Table**
   - Once created, you can query the data from the external table without loading it into BigQuery storage.



## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
**- 840,402**
- 1,936,423
- 253,647

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**- 0 MB for the External Table and 6.41MB for the Materialized Table**
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table


Query:
```
SELECT COUNT(DISTINCT PULocationID)  FROM `neon-nexus-450515-j8.green_taxi_data.green_taxi_data`
```

I am getting 6.41MB for both...


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
**- 1,622**

Query:
```
SELECT COUNT(*) FROM `neon-nexus-450515-j8.green_taxi_data.green_taxi_data_ext` WHERE fare_amount = 0
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
**- Partition by lpep_pickup_datetime  Cluster on PUlocationID**
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID


Query:

```
CREATE OR REPLACE TABLE `neon-nexus-450515-j8.green_taxi_data.green_taxi_data_part`
PARTITION BY DATE(lpep_pickup_datetime)  -- Partition by `lpep_pickup_datetime` column
CLUSTER BY PUlocationID  -- Cluster by `PUlocationID` column
AS
SELECT *
FROM `neon-nexus-450515-j8.green_taxi_data.green_taxi_data`;
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
**- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table**
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table


results:

Partitioned: 
```
Verarbeitete Byte
6,49 MB
In Rechnung gestellte Byte
10 MB
```

Non-Partitioned:
```
Verarbeitete Byte
12,82 MB
In Rechnung gestellte Byte
13 MB
```


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
**- GCP Bucket**
- Big Table
- Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
**- False**


(It’s not a "one-size-fits-all" approach, but it’s very useful for large, frequently queried datasets with filters or aggregations on specific columns.)

## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

 
## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3


