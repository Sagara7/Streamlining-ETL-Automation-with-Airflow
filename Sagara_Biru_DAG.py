"""
Nama: Sagara Biru Wilantara
Batch: HCK-007
Phase: 2

Milestone 3

"""


from datetime import datetime
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd

def load_data():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_M3", conn)
    df.to_csv('P2M3_Sagara_data_raw.csv',index=False)

import pandas as pd

def cleaning_data():
    df = pd.read_csv('/opt/airflow/dags/P2M3_Sagara_data_raw.csv')

    def fill_missing_values(column):
        if df[column].dtype == 'float64':
            median = df[column].median()
            df[column].fillna(median, inplace=True)
            if column in ['loanamount', 'loan_amount_term', 'credit_history']:
                df[column] = df[column].astype(int)
        elif df[column].dtype == 'object':
            mode_value = df[column].mode()[0]
            df[column].fillna(mode_value, inplace=True)

    columns_to_clean = [
        'loan_id', 'gender', 'married', 'dependents', 'education',
        'self_employed', 'applicantincome', 'coapplicantincome', 'loanamount',
        'loan_amount_term', 'credit_history', 'property_area', 'loan_status'
    ]

    for col in columns_to_clean:
        fill_missing_values(col)

    df.to_csv('/opt/airflow/dags/P2M3_Sagara_data_clean.csv', index=False)



def push_es ():
    es = Elasticsearch("http://localhost:9200") # define elasticsearch ke variable
    df_cleaned=pd.read_csv('/opt/airflow/dags/P2M3_Sagara_data_clean.csv') # import csv clean
    for i,r in df_cleaned.iterrows(): # looping untuk masuk ke elastic search
        doc=r.to_json()
        res=es.index(index="data_clean", body=doc)
        print(res)  

default_args= {
    'owner': 'Sagara',
    'start_date': datetime(2023, 10, 1) }

with DAG(
    "Milestone3",
    description='Milestone3',
    schedule_interval='@yearly',
    default_args=default_args, 
    catchup=False) as dag:

    # Task 1
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data

    )

    # Task 2
    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data

    )

    #Task 3
    push_es = PythonOperator(
        task_id='push_es',
        python_callable=push_es

    )

    load_data >> cleaning_data >> push_es