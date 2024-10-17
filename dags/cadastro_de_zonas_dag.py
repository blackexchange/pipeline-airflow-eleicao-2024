from airflow import DAG
from pyspark.sql import SparkSession

import zipfile
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable 
from datetime import datetime, timedelta
import os
import logging
import logging


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

def create_spark_session():
    """Cria uma sessão Spark"""
    return SparkSession.builder \
        .appName("Processar Arquivos") \
        .master("local[*]") \
        .config("spark.executor.memory", "1g")  \
        .config("spark.driver.memory", "1g")   \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def run_spark_task(input_path: str, output_path: str):
    """Executa a função de processamento do Spark para gerar o parquet"""
    spark = create_spark_session()
    print(f"Gerando DataFrame no Spark...")

    df = spark.read.csv(input_path, sep=';', header=True, inferSchema=True,  encoding='latin1')

    # Filtrar os eventos que correspondem a "Eleitor foi habilitado" e "O voto do eleitor foi computado"
    df_filtered = df.select("AA_ELEICAO", "CD_PLEITO", "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO", "NR_ZONA", "NR_URNA_ESPERADA")

    df_final = df_filtered.dropDuplicates(["AA_ELEICAO", "CD_PLEITO", "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO", "NR_ZONA", "NR_URNA_ESPERADA"])

    #df_final.repartition(1).write.mode("overwrite").partitionBy("SG_UF").option("header", "true").csv(output_path + "/csv")
    df_final.coalesce(1).write.mode("overwrite").partitionBy("SG_UF").option("header", "true").parquet(output_path + "/parquet")
    print(f"Número de linhas processadas: {df_final.count()}")
    spark.stop()  # Para a sessão Spark após o processame

 

def unzip_file(zip_path, extract_to, filter='jez'):
    print(f"Extraindo arquivos...")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Verifica se a extensão do arquivo corresponde ao filtro
            if file_info.filename.endswith(filter):
                # Extrai apenas os arquivos que correspondem ao filtro de extensão
                zip_ref.extract(file_info, extract_to)

    
    if(os.path.exists(extract_to)):
        return True
    else:
        return False
    


def download_file(linkFile,path,file_name):
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, path,file_name)
    

    if os.path.exists(file_path):
        return True
        
    print(f"Baixando { file_name}...")

    response = requests.get(linkFile, stream=True)

   
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):  # Definido para baixar em pedaços de 8KB
            f.write(chunk)
                
    if(os.path.exists(file_path)):
        return True
    else:
        return False



def prepare_files(params, input_path):
    param = params.split('_')

    date = param[1]
    turn = param[0]
    # Parâmetros ajustados para a função
    uf_list = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'ZZ']
    uf_list = ['AC', 'AL']
    for uf in uf_list:
        uf_filename = f"CESP_{turn}t_{uf}_{date}.zip"
        url_download = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/correspesp/"
        zip_filepath = os.path.join(input_path, uf_filename)
        
        # Download do arquivo
        try:
            logging.info(f"Baixando o arquivo {uf_filename}...")
            download_file(url_download + uf_filename, input_path, uf_filename)
        except Exception as e:
            logging.error(f"Erro ao baixar o arquivo {uf_filename}: {e}")
            continue  # Segue para o próximo UF se houver erro
            
        # Descompactar o arquivo
        try:
            logging.info(f"Descompactando o arquivo {uf_filename}...")
            unzip_file(zip_filepath, input_path + "/unzipped", 'csv')
        except Exception as e:
            logging.error(f"Erro ao descompactar o arquivo {uf_filename}: {e}")
            continue  # Segue para o próximo UF se houver erro



# Definindo a DAG
with DAG('pipeline_cadastro_de_zonas', 
         default_args=default_args, 
         schedule_interval='@daily', 
         catchup=False) as dag:

    # Task 1: Preparar os arquivos (baixar e descompactar)
    prepare_files_task = PythonOperator(
        task_id='prepare_files',
        python_callable=prepare_files,
        op_kwargs={
            'params': Variable.get('TURN_DATE'),  # Parâmetros dinâmicos para nomeação
            'input_path': Variable.get('INPUT_FOLDER'),  # Caminho para armazenar os arquivos
        }
    )
    
    # Task 2: Processar arquivos e gerar Parquet
    generate_parquet_task = PythonOperator(
        task_id='generate_parquet',
        python_callable=run_spark_task,
        op_kwargs={
            'input_path': 'in/unzipped',  # Caminho dos arquivos descompactados
            'output_path': 'out',  # Diretório onde o Parquet será salvo
        }
    )
    
# Definindo a ordem de execução das tarefas
prepare_files_task >> generate_parquet_task