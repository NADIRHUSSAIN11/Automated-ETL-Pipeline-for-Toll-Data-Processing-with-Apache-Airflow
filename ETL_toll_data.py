from airflow import DAG 
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

#defining arguments 

default_arguments={
    'owner':'Nadir Hussain',
    'start_date':datetime(2024,3,6),
    'email':'any@gmail.com',
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

#defining dag

dag=DAG(
    dag_id='ETL_toll_data',
    default_args=default_arguments,
    schedule_interval=timedelta(days=1),
    description="Apache Airflow Final Assignment"
)

#defining task
unzip_data=BashOperator(
    task_id='unzip_data',
    bash_command='unzip tolldata.tgz',
    dag=dag
)

def extract():
    df=pd.read_csv('vehicle-data.csv',
    names=['Rowid','Timestamp','AnonymizedVehiclenumber','Vehicletype','NumberOfAxles','VehicleCode'])
    df=df[['Rowid','Timestamp','AnonymizedVehiclenumber','Vehicletype']]
    df.to_csv('csv_data.csv')


extract_data_from_csv=PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract,
    dag=dag
)

def extracttsv():
    df=pd.read_csv('tollplaza-data.tsv', sep='\t',names=['Rowid','Timestamp','AnonymizedVehiclenumber','Vehicletype','NumberOfAxles','TollplazaId','TollplazaCode'])
    df=df[['NumberOfAxles','TollplazaId','TollplazaCode']]
    df.to_csv("tsv_data.csv")


extract_data_from_tsv=PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extracttsv,
    dag=dag
)

def extracttxt():
    separator_pattern = r'\s+'
    df = pd.read_csv("payment-data.txt", sep=separator_pattern, names=['Rowid', 'Day', 'Month', 'Date', 'Time', 'Year', 'AnonymizedVehiclenumber', 'TollplazaId', 'TollplazaCode', 'TypeOfPaymentCode', 'VehicleCode'])
    df.drop(['Day', 'Month', 'Date', 'Year'], axis=1, inplace=True)
    df=df[['TypeOfPaymentCode', 'VehicleCode']]
    df.to_csv("fixed_width_data.csv")

extract_data_from_fixed_width=PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extracttxt,
    dag=dag
)

def consolidate():
    df1=pd.read_csv("csv_data.csv")
    df2=pd.read_csv("tsv_data.csv")
    df3=pd.read_csv("fixed_width_data.csv")
    df=pd.merge(pd.merge(df1, df2, on='Unnamed: 0', how='inner'), df3, on='Unnamed: 0', how='inner')
    df.drop(['Unnamed: 0'], axis=1, inplace=True)
    df.to_csv("extracted_data.csv")

consolidate_data=PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate,
    dag=dag
)

def transform():
    df=pd.read_csv('extracted_data.csv')
    df['Vehicletype'] = df['Vehicletype'].str.upper()
    df.to_csv("transformed_data.csv")

transform_data=PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data



