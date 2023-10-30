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

def cleaning_data(kolom):
    df = pd.read_csv('/opt/airflow/dags/P2M3_Sagara_data_raw.csv')
        # Mengisi missing value pada kolom yang bersifat float
    if df[kolom].dtypes == 'float' :
        median = df[kolom].median()
        df[kolom].fillna(median,inplace=True)
        
        # Transformasi kolom loan amount, loan amount term, dan credit history dari float menjadi int
        if kolom == 'loanamount'or'loan_amount_term'or 'credit_history':
            df[kolom] = df[kolom].astype(int)

    # Mengisi missing value pada kolom yang bersifat object
    elif df[kolom].dtypes == 'object' :
        mode_value = df[kolom].mode()[0]
        df[kolom].fillna(mode_value, inplace=True)

    df.to_csv('/opt/airflow/dags/P2M3_Sagara_data_clean.csv', index=False)

def push_es ():
    es = Elasticsearch("http://localhost:9200") # define elasticsearch ke variable
    df_cleaned=pd.read_csv('/opt/airflow/dags/P2M3_Sagara_data_clean.csv') # import csv clean
    for i,r in df_cleaned.iterrows(): # looping untuk masuk ke elastic search
        doc=r.to_json()
        res=es.index(index="data_clean", body=doc)
        print(res)
