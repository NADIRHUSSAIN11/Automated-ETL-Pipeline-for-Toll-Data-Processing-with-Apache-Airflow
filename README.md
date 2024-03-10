# Automated ETL Pipeline for Toll Data Processing with Apache Airflow

This project automates the ETL (Extract, Transform, Load) process for toll data using Apache Airflow. The DAG (`ETL_toll_data`) consists of several tasks that perform the following operations:

1. **Unzip Data**: Unzips the `tolldata.tgz` file.
2. **Extract Data from CSV**: Reads data from `vehicle-data.csv`, selects specific columns, and saves the result to a new CSV file.
3. **Extract Data from TSV**: Reads data from `tollplaza-data.tsv`, selects specific columns, and saves the result to a new CSV file.
4. **Extract Data from Fixed Width Text**: Reads data from `payment-data.txt`, selects specific columns, and saves the result to a new CSV file.
5. **Consolidate Data**: Merges the data from CSV, TSV, and Fixed Width Text files and saves the result to a new CSV file.
6. **Transform Data**: Transforms the `Vehicletype` column to uppercase and saves the transformed data to a new CSV file.
