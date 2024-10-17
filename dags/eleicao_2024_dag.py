import time
from airflow import DAG
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date, lit, to_timestamp, lag, lead, when
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import shutil

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


def clean_directories(**kwargs):
    directories = kwargs.get('directories', [])
    
    for dir_path in directories:
        if os.path.exists(dir_path):
            logging.info(f"Limpando o diretório: {dir_path}")
            try:
                if os.path.isdir(dir_path):
                    shutil.rmtree(dir_path)  # Remove a pasta e seus conteúdos
                    logging.info(f"Pasta removida: {dir_path}")

                elif os.path.isfile(dir_path):
                        os.remove(dir_path)
                        logging.info(f"Arquivo removido: {dir_path}")

            except Exception as e:
                logging.error(f"Erro ao remover {dir_path}: {str(e)}")

            
        else:
            logging.warning(f"Diretório não encontrado: {dir_path}")



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

    retries = 5
    while retries > 0 and not os.listdir(input_path):
        logging.info(f"Aguardando arquivos em {input_path} serem lidos...")
        time.sleep(5)  # Aguardar 10 segundos antes de tentar novamente
        retries -= 1

    print(f"Gerando DataFrame no Spark...")

    schema = StructType([
        StructField("DataHora", StringType(), True),
        StructField("LogLevel", StringType(), True),
        StructField("CodigoUrna", StringType(), True),
        StructField("Acao", StringType(), True),
        StructField("Mensagem", StringType(), True),
        StructField("Código Verificação", StringType(), True)
    ])

    df = spark.read.csv(input_path + "/**/*.dat", sep='\t', header=False, schema=schema,  encoding='latin1')

    df = df.withColumn("DataHora", to_timestamp(df["DataHora"], "dd/MM/yyyy HH:mm:ss"))

    # Filtrar os eventos que correspondem a "Eleitor foi habilitado" e "O voto do eleitor foi computado"
    df_filtered = df.filter(
        (F.col("Acao") =="VOTA") & 
        (F.col("DataHora") >= "2024-10-06") &
        ((F.col("Mensagem").like("%Eleitor foi habilitado%")) | 
        (F.col("Mensagem").like("%O voto do eleitor foi computado%")))
    )

    # Usar windowing para identificar o próximo evento "O voto do eleitor foi computado"
    window = Window.partitionBy("CodigoUrna").orderBy("DataHora")

    # Encontrar o próximo evento correspondente
    df_result = df_filtered.withColumn("Next_Event", F.lead("Mensagem").over(window)) \
        .withColumn("Next_DataHora", F.lead("DataHora").over(window)) \
        .filter((F.col("Mensagem").like("%Eleitor foi habilitado%")) & 
                (F.col("Next_Event").like("%O voto do eleitor foi computado%")))

    # Selecionar as colunas necessárias
    df_final = df_result.select(
        "CodigoUrna", 
        "DataHora", 
        F.col("Next_DataHora").alias("DataHora_Fim"),
        (F.unix_timestamp("Next_DataHora") - F.unix_timestamp("DataHora")).alias("TempoSec")  # Diferença em segundos

    )
    #df_final.write.mode("overwrite").parquet(output_path)
    #df_final.coalesce(1).write.mode("append").partitionBy("SG_UF").option("header", "true").parquet(output_path + "/parquet")
    df_final.coalesce(1).write.mode("append").option("header", "true").parquet(output_path + "/parquet")

    #df_final.write.mode("overwrite").csv(output_path +"/csv/log_urna.csv")
    print(f"Gerado.")

   # df_final.show(5, truncate=False)
    spark.stop()  # Para a sessão Spark após o processame

    op_kwargs={'directories': [input_path]}
    clean_directories(**op_kwargs)
                


def extract_log_text(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        dir_name = os.path.basename(zip_path)
        dir_name = str(dir_name).split('.')[0]
        zip_ref.extractall(f"{extract_to}/{dir_name}")

    if(os.path.exists(extract_to)):
        return True
    else:
        return False

def process_extracted_logs(input_path):
    logs_dir = input_path +  "/logs"
    unzipped_dir = input_path + "/unzipped"


    retries = 5
    while retries > 0 and not os.listdir(unzipped_dir):
        logging.info(f"Aguardando arquivos em {unzipped_dir} serem descompactados...")
        time.sleep(5)  # Aguardar 10 segundos antes de tentar novamente
        retries -= 1
    
    #logging.error(f"Extraindo logs {unzipped_dir} para {logs_dir}")
    
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Iterar sobre os arquivos descompactados
    for filename in os.listdir(unzipped_dir):
        file_path = unzipped_dir +"/"+ filename
        if os.path.isfile(file_path):
            try:
                extract_log_text(file_path, logs_dir)
            except Exception as e:
                logging.error(f"Erro ao extrair o arquivo {file_path}: {e}")
                
    op_kwargs={'directories': [unzipped_dir]}
    clean_directories(**op_kwargs)
                


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

    year = param[1][4:8]
    turn = param[0]
    
    # Parâmetros ajustados para a função
    uf_list = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'ZZ']
    uf_list = ['TO']

    for uf in uf_list:
        uf_filename = f"bu_imgbu_logjez_rdv_vscmr_{year}_{turn}t_{uf}.zip"
        url_download = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/arqurnatot/"

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
            unzip_file(zip_filepath, input_path + "/unzipped")
            logging.info(f"Removendo arquivo {uf_filename}...")
            os.remove( input_path + "/" + uf_filename)

        except Exception as e:
            logging.error(f"Erro ao descompactar o arquivo {uf_filename}: {e}")
            continue  # Segue para o próximo UF se houver erro



# Definindo a DAG
with DAG('pipeline_eleicao_2024', 
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


    process_extracted_logs_task = PythonOperator(
        task_id='process_extracted_logs',
        python_callable=process_extracted_logs,
        op_kwargs={
            'input_path': Variable.get('INPUT_FOLDER'),  # Caminho para armazenar os arquivos
        }
    )
    
    # Task 2: Processar arquivos e gerar Parquet
    generate_parquet_task = PythonOperator(
        task_id='generate_parquet',
        python_callable=run_spark_task,
        op_kwargs={
            'input_path': 'in/logs',  # Caminho dos arquivos descompactados
            'output_path': 'out',  # Diretório onde o Parquet será salvo
        }
    )
    
    clean_directories_task = PythonOperator(
        task_id='clean_directories',
        python_callable=clean_directories,
        op_kwargs={
            'directories': ['/opt/airflow/in']
        }
    )
    
# Definindo a ordem de execução das tarefas
clean_directories_task >> prepare_files_task >> process_extracted_logs_task >> generate_parquet_task