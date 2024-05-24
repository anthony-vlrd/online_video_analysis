# Scalable ETL Pipeline on AWS for Online Video Data Analysis

## Project Overview
This project involves building a scalable ETL (Extract, Transform, Load) pipeline on AWS to analyze online video data and draw insights on trends for various regions. The pipeline utilizes various AWS services to ensure security, scalability, and efficiency.

## Architecture
- **IAM Roles**: Defined IAM roles to securely run services.
- **Data Lake**: Created a data lake consisting of three S3 buckets:
  - `raw-data landing bucket`
  - `processed-data bucket`
  - `reporting and analysis bucket`
- **AWS Glue**: Used AWS Glue to process the data and Glue Crawlers to populate the database.
- **AWS Lambda**: Wrote a Lambda function to read JSON data, extract relevant data, and store it as Parquet. The Lambda function triggers when a new JSON object is dropped in the raw bucket.
- **Amazon Athena**: Queried the data using Athena.
- **Amazon QuickSight**: Drew insights and created dashboards for them in QuickSight.

## Detailed Steps

### 1. Setting Up IAM Roles
Defined IAM roles with the necessary permissions to securely run the various AWS services involved in the ETL pipeline.

### 2. Creating the Data Lake
- **S3 Buckets**:
  - `raw-data landing bucket`: Stores the raw JSON data.
  - `processed-data bucket`: Stores the processed data in Parquet format.
  - `reporting and analysis bucket`: Stores data for reporting and analysis.

### 3. Data Processing with AWS Glue
- **Glue Jobs**: Created Glue jobs to process the data.
- **Glue Crawlers**: Set up Glue Crawlers to automatically populate the Glue Data Catalog with metadata about the data.

### 4. Lambda Function for Data Transformation
- **Functionality**: The Lambda function reads JSON data from the raw-data landing bucket, extracts relevant information, and stores it as Parquet in the processed-data bucket.
- **Trigger**: The Lambda function is triggered by an S3 event whenever a new JSON object is uploaded to the raw-data landing bucket.

### 5. Querying Data with Athena
Used Amazon Athena to run SQL queries on the processed data stored in the S3 bucket. This allowed for flexible and scalable querying of large datasets.

### 6. Creating Dashboards with QuickSight
- **Data Visualization**: Used Amazon QuickSight to create dashboards that provide insights into the online video data trends across various regions.
- **Integration**: Integrated QuickSight with Athena to directly query the processed data and generate visual reports.

