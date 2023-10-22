from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

import requests
import os
import pandas as pd

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="dag_download_file", 
    start_date=datetime(2023, 10, 22), 
    # schedule="0 0 * * *",
    schedule=timedelta(minutes=5),
    ) as dag:

    # Tasks are represented as operators
    op_dag_download_file = BashOperator(task_id="dag_download_file_1", bash_command="echo dag_download_file_2")

    @task()
    def f_dag_download_file():
        print("dag_download_file_1 start")

        file_url = 'https://opendata.karanganyarkab.go.id/dataset/e07335c4-b0c0-4b08-8f49-87089156de06/resource/6d3b88ca-f66c-47a9-b0b8-dab198c6d547/download/rekap-aduan-juli-2021.xlsx'
        response = requests.get(file_url)
        print(response.status_code)

        if response.status_code == 200:
            print('ini response')
            if not os.path.exists('data'):
                os.makedirs('data')
            file_path = os.path.join('data', 'example.xlsx')
            with open(file_path, 'wb') as file:
                print('save file')
                file.write(response.content)
            print(f"File downloaded and saved at {file_path}")
        else:
            print("Failed to download the file.")

        print("dag_download_file_1 finish")

    op_dag_read_file = BashOperator(task_id="dag_read_file_1", bash_command="echo dag_read_file_2")

    @task()
    def f_dag_read_file():
        print("dag_read_file_1 start")
        # nama_file_excel = "REKAP ADUAN JULI 2021 Pemerintah Kabupaten Karanganyar.xlsx"
        nama_file_excel = "dags/REKAP ADUAN JULI 2021 Pemerintah Kabupaten Karanganyar.xlsx"

        try:
            data = pd.read_excel(nama_file_excel, skiprows=2)
            print(data)
        except Exception as e:
            print(f"An error occurred: {e}")



        # nama_kolom = 'ADUAN'

        # if nama_kolom in data:
        #     for x in range(len(data[nama_kolom])):
        #     # print(type(data[nama_kolom][x]))
        #         cek_kolom = isinstance(data[nama_kolom][x], str)
        #         if cek_kolom:
        #             print(data[nama_kolom][x], 'aduan')

        print("dag_read_file_1 finish")

    # Set dependencies between tasks
    op_dag_download_file >> f_dag_download_file()
    op_dag_read_file >> f_dag_read_file()