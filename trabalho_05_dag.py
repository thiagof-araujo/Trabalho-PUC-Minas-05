from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd
import boto3

def sexo_classe_quantidade():
    url = 'https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv'
    titanic = pd.read_csv(url, sep =';')
    df_sexo_classe = titanic
    df_sexo_classe = df_sexo_classe.groupby(['Pclass', 'Sex']).size().reset_index(name='Quantidade')
    df_sexo_classe = df_sexo_classe.sort_values(by='Pclass', ascending=True)
    df_sexo_classe.to_csv('s3://puc-minas-trabalho05-653133531046/processing-zone/sexo_classe.csv', sep=';', encoding='utf-8', index=False)

def sexo_classe_media():
    url = 'https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv'
    titanic = pd.read_csv(url, sep =';')
    df_sexo_classe_media = titanic
    df_sexo_classe_media = df_sexo_classe_media.groupby(['Pclass', 'Sex'])['Fare'].mean().reset_index(name='Média')
    df_sexo_classe_media = df_sexo_classe_media.sort_values(by='Pclass', ascending=True)
    df_sexo_classe_media.to_csv('s3://puc-minas-trabalho05-653133531046/processing-zone/sexo_classe_media.csv', sep=';', encoding='utf-8', index=False)


default_args = {
    'owner': 'Thiago',
    'start_date': datetime(2022, 5, 20)
}

with DAG(dag_id='trabalho05', 
        default_args=default_args, 
        schedule_interval="*/2 * * * *", 
        description="Trabalho Aula 05", 
        catchup=False, 
        tags=['trabalho_05']
        ) as dag:

    t1 = DummyOperator(
        task_id = "Inicio",
    )


    t2 = PythonOperator(
        task_id = 'sexo_classe_quantidade',
        python_callable = sexo_classe_quantidade,
        dag = dag,
    )

    t3 = PythonOperator(
        task_id = 'sexo_classe_media',
        python_callable = sexo_classe_media,
        dag = dag,
    )


    t4 = DummyOperator(
        task_id = "Fim",
    )

    t1 >> t2 >> t3 >> t4