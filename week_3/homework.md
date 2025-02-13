## Module 3 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. 
This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or 
shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the Yellow Taxi Trip Records for **January 2024 - June 2024 NOT the entire year of data** 
Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Kestra, Mage, Airflow or Prefect etc. do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>

**Load Script:** You can manually download the parquet files and upload them to your GCS Bucket or you can use the linked script [here](./load_yellow_taxi_data.py):<br>
You will simply need to generate a Service Account with GCS Admin Priveleges or be authenticated with the Google SDK and update the bucket name in the script to the name of your bucket<br>
Nothing is fool proof so make sure that all 6 files show in your GCS Bucket before begining.</br><br>

<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>BIG QUERY SETUP:</b></br>
Create an external table using the Yellow Taxi Trip Records. </br>
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). </br>
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
data-engineering-zoomcamp_hw03/yellow_tripdata_2024-*.parquet
       ```
     - **Table Type**: Choose **External Table**. (or NATIVE for native)
     - **Schema**: Set to **Auto-detect**.
     - Click **Create Table**.


## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
**- 20,332,093**
- 85,431,289


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
**- 0 MB for the External Table and 155.12 MB for the Materialized Table**
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```
Verarbeitete Byte
155,12 MB
In Rechnung gestellte Byte
155 MB
```

external:
```
Verarbeitete Byte
286,11 MB
In Rechnung gestellte Byte
287 MB
```

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
**- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires**
**reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed


Key takeaways:
* BigQuery is a columnar database. This means that instead of storing data row-by-row like in traditional relational databases, it stores data column-by-column. This design is optimized for analytical queries, especially when you're querying large datasets and only need specific columns.
* When you query BigQuery, it only needs to read the columns you ask for, rather than reading entire rows. This is one of the reasons why BigQuery is efficient for large-scale queries — it can avoid reading unnecessary data.


## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
**- 8,333**

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
**- Partition by tpep_dropoff_datetime and Cluster on VendorID**
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

Query:

```
CREATE OR REPLACE TABLE `neon-nexus-450515-j8.yellow_taxi_data.green_taxi_data_part`
PARTITION BY DATE(tpep_pickup_datetime)  -- Partition by `tpep_pickup_datetime` column
CLUSTER BY PUlocationID  -- Cluster by `PUlocationID` column
AS
SELECT *
FROM `neon-nexus-450515-j8.yellow_taxi_data.yellow_taxi_data`;
```


## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
**- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table


```
SELECT DISTINCT VendorID FROM `neon-nexus-450515-j8.yellow_taxi_data.yellow_taxi_data` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```


## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
**- GCP Bucket**
- Big Table

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
**- False**


## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?


## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw3
