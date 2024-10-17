from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
import re
import pandas as pd
import logging
from typing import List
from bs4 import BeautifulSoup
import requests

def list_totalization_files(url="https://dadosabertos.tse.jus.br/dataset/resultados-2024-arquivos-transmitidos-para-totalizacao"):

    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")

    ul = soup.find('ul', class_='resource-list')
    lis = ul.find_all("li")

    ds_files = []

    for li in lis:

        link = li.find('a', class_='heading')
        if link:
            title = link['title']

            linkCdn = li.find('a', class_='resource-url-analytics')

            if linkCdn:
                linkFile = linkCdn['href']


            if linkFile.endswith('zip'):
                file_name = linkFile.split("/")[-1]
                ds_files.append(file_name)
    return ds_files


def download_file(linkFile,path,file_name):
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, path,file_name)
    

    if os.path.exists(file_path):
        return True
        
    print("Baixando " + file_name)

    response = requests.get(linkFile, stream=True)

    print("Gravando " + file_name)
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):  # Definido para baixar em pedaços de 8KB
            f.write(chunk)
                
    if(os.path.exists(file_path)):
        return True
    else:
        return False



def unzip_file(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Verifica se a extensão do arquivo corresponde ao filtro
            if file_info.filename.endswith("jez"):
                # Extrai apenas os arquivos que correspondem ao filtro de extensão
                zip_ref.extract(file_info, extract_to)
                print(f"Extraído: {file_info.filename}")


    if(os.path.exists(extract_to)):
        return True
    else:
        return False
    


def extract_log_text(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Arquivos descompactados em: {extract_to}")


    if(os.path.exists(extract_to)):
        return True
    else:
        return False


def process_log_file(file_path):
    # Lê o arquivo CSV delimitado por tabulações
    df = pd.read_csv(file_path, 
                     sep='\t', 
                     header=None, 
                     names=['Data e Hora', 'Log Level', 'Código Urna', 'Ação', 'Mensagem', 'Código Verificação'],
                     encoding='latin-1',  # Mudando a codificação para 'latin-1'
                     on_bad_lines='skip')

    # Adiciona o nome do arquivo como uma nova coluna
    df['Arquivo'] = file_path

    # Divide a coluna 'Data e Hora' em 'Data' e 'Hora'
    df[['Data', 'Hora']] = df['Data e Hora'].str.split(' ', n=1, expand=True)

    # Converte a coluna 'Data' para formato datetime
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y')

    # Filtra as datas superiores ou iguais a 06/10/2024
    df = df[df['Data'] >= '2024-10-06']

    # Inicializar variáveis para armazenar as informações da urna
    urna_info = {
        'Turno da UE': None,
        'Modelo de Urna': None,
        'Fase da UE': None,
        'Modo de Carga da UE': None,
        'Município': None,
        'Zona Eleitoral': None,
        'Seção Eleitoral': None
    }

    # Varre o DataFrame linha por linha para preencher as informações da urna
    for index, row in df.iterrows():
        if pd.isna(urna_info['Turno da UE']) and 'Turno da UE' in row['Mensagem']:
            urna_info['Turno da UE'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Modelo de Urna']) and 'Modelo de Urna' in row['Mensagem']:
            urna_info['Modelo de Urna'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Fase da UE']) and 'Fase da UE' in row['Mensagem']:
            urna_info['Fase da UE'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Modo de Carga da UE']) and 'Modo de carga da UE' in row['Mensagem']:
            urna_info['Modo de Carga da UE'] = row['Mensagem'].split('-')[-1].strip()

        if pd.isna(urna_info['Município']) and 'Município' in row['Mensagem']:
            urna_info['Município'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Zona Eleitoral']) and 'Zona Eleitoral' in row['Mensagem']:
            urna_info['Zona Eleitoral'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Seção Eleitoral']) and 'Seção Eleitoral' in row['Mensagem']:
            urna_info['Seção Eleitoral'] = row['Mensagem'].split(':')[-1].strip()

        # Agora que temos todas as informações da urna, podemos preenchê-las nas linhas seguintes
        if all(v is not None for v in urna_info.values()):
            break

    # Agora vamos aplicar essas informações para todas as linhas
    for col, value in urna_info.items():
        df[col] = value

    # Filtra apenas as linhas com log level 'INFO' e ação 'VOTA'
    df_votos = df[(df['Log Level'] == 'INFO') & (df['Ação'] == 'VOTA')]

    # Identificação de início e fim do voto
    start_vote_mask = df_votos['Mensagem'].str.contains('Eleitor foi habilitado')
    end_vote_mask = df_votos['Mensagem'].str.contains('O voto do eleitor foi computado')

    # Cria DataFrames separados para início e fim do voto
    df_start = df_votos[start_vote_mask].copy()
    df_end = df_votos[end_vote_mask].copy()

    # Adiciona uma coluna de índice para rastrear a ordem de entrada no log
    df_start['Voto ID'] = range(1, len(df_start) + 1)
    df_end['Voto ID'] = range(1, len(df_end) + 1)

    # Renomeia as colunas para distinguir início e fim do voto
    df_start.rename(columns={'Hora': 'Hora Início', 'Mensagem': 'Mensagem Início'}, inplace=True)
    df_end.rename(columns={'Hora': 'Hora Fim', 'Mensagem': 'Mensagem Fim'}, inplace=True)

    # Merge (juntar) os dados de início e fim baseados no índice Voto ID
    df_combined = pd.merge(df_start[['Voto ID', 'Data', 'Hora Início', 'Mensagem Início']],
                           df_end[['Voto ID', 'Data', 'Hora Fim', 'Mensagem Fim']],
                           on=['Voto ID', 'Data'], how='inner')

    # Adiciona as informações da urna ao DataFrame combinado
    for col, value in urna_info.items():
        df_combined[col] = value

    # Exibe o DataFrame combinado (opcional)
    return df_combined

# Definir a DAG e seus parâmetros
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('pipeline_urna_eleitoral', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
 # Task 1: Baixar o arquivo zip
    download_list_task = PythonOperator(
        task_id='download_list',
        python_callable=list_totalization_files,
        op_kwargs={
            'url': 'https://dadosabertos.tse.jus.br/dataset/resultados-2024-arquivos-transmitidos-para-totalizacao',  # Substitua pelo URL real
        }
    )

    # Task 1: Baixar o arquivo zip
    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        op_kwargs={
            'url': 'https://site.com/arquivo_urna.zip',  # Substitua pelo URL real
            'output_path': '/path_para_salvar_arquivo/arquivo_urna.zip'
        }
    )

    # Task 2: Descompactar o arquivo baixado
    unzip_task = PythonOperator(
        task_id='unzip_file',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': '/path_para_salvar_arquivo/arquivo_urna.zip',
            'extract_to': '/path_para_extrair_arquivos'
        }
    )

    # Task 3: Processar arquivos com Regex
    process_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        op_kwargs={
            'extracted_folder': '/path_para_extrair_arquivos',
            'regex_pattern': r'exemplo_de_regex',  # Exemplo: r'(\d+);(\w+);(\d+)' para CSV-like patterns
        }
    )

    # Task 4: Criar DataFrame
    create_df_task = PythonOperator(
        task_id='create_dataframe',
        python_callable=create_dataframe,
        op_kwargs={
            'data': "{{ task_instance.xcom_pull(task_ids='process_files') }}",
            'columns': ['coluna1', 'coluna2', 'coluna3'],  # Substitua com os nomes das colunas apropriados
        }
    )

    # Definir a sequência das tasks
    download_task >> unzip_task >> process_task >> create_df_task
