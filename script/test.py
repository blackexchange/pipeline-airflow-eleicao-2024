from datetime import datetime, timedelta
import zipfile
import os
import re
import pandas as pd


def un7(folder, file):

  if file.endswith('jez'):
    with py7zr.SevenZipFile(file, mode='r') as z:
        z.extractall(folder)
        return True
  return False



def unzip(arquivo):
  print("Descompactando...")

  match = re.search(r'(\d{4}_\d{1}t_[A-Z]{2})', arquivo)
  if match:
      destiny = match.group(1)

      if not os.path.exists(destiny):
        os.makedirs(destiny)

      with zipfile.ZipFile(arquivo, 'r') as zip_ref:
        for file in zip_ref.namelist():
          if file.endswith('jez'):
              zip_ref.extract(file, destiny)
      print("Descompactação realizada!")
      #os.remove(arquivo)

      return destiny


def processar(folder):

    files = os.listdir(folder)
    total = len(files)
    #files = [f for f in fileA if "jez" in f]
    #[un7(folder+"/"+arquivo) for arquivo in files]

    i = 0
    j = 0
    df = pd.DataFrame()
    print (f"Processando {total} arquivos...")
    for arquivo in files:

        i += 1

        if un7(folder, folder + "/" + arquivo):
            dfa = lerLog(folder + "/logd.dat")

            if dfa.empty:
              print("Arquivo descartado " +arquivo )
            else:
              df = pd.concat([df, dfa], ignore_index=True)
              if i > 100:
                  total -= 100
                  j += 1
                  print (f'--Fechando pacote {folder}_A{j:04}. Restante {total} arquivos')
                  df.to_csv(f"/content/drive/MyDrive/LOG_URNAS/{folder}_A{j:04}.csv")
                  i = 0
                  df = pd.DataFrame()  # redefina o DataFrame

            os.remove(folder + "/" + arquivo)
            os.remove(folder + "/logd.dat")

    if not df.empty:
      j += 1
      df.to_csv(f"/content/drive/MyDrive/LOG_URNAS/{folder}_A{j:04}.csv")
      df = pd.DataFrame()
      print (f'--Fechando pacote {folder}_A{j:04}')

    #segunda leva
    df2 = pd.DataFrame()
    filesJez = os.listdir(folder)
    total = len(filesJez)
    print (f"Processando {total} arquivos...")

    i = 0
    for arquivo in filesJez:

        i += 1

        if un7(folder, folder + "/" + arquivo):
            dfa = lerLog(folder + "/logd.dat")
            if dfa.empty:
              print("Arquivo descartado " +arquivo )
            else:
              df2 = pd.concat([df2, dfa], ignore_index=True)
              if i > 100:
                  total -= 100
                  j += 1
                  print (f'--Fechando pacote {folder}_A{j:04}. Restando {total} arquivos')
                  df2.to_csv(f"/content/drive/MyDrive/LOG_URNAS/{folder}_B{j:04}.csv")
                  i = 0
                  df2 = pd.DataFrame()  # redefina o DataFrame
            os.remove(folder + "/" + arquivo)
            os.remove(folder + "/logd.dat")

    if not df2.empty:
      j += 1
      df2.to_csv(f"/content/drive/MyDrive/LOG_URNAS/{folder}_B{j:04}.csv")
      df2 = pd.DataFrame()
      print (f'--Fechando pacote {folder}_A{j:04}')


    print("Concluído")


def lerLog(arquivo_path):
  date_parser = lambda x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S')

  # Ler o arquivo CSV

  df =  pd.read_csv(arquivo_path, encoding='ISO-8859-1',  delimiter='\t', names=['DataHora', 'LogType', 'Codigo', 'Origem', 'Mensagem', 'Hash'],parse_dates=['DataHora'], date_parser=date_parser)

  # Definir uma variável para armazenar os detalhes do cabeçalho
  cabecalho_atual = {}

  votos = []

  # Percorrer o DataFrame
  for idx, row in df.iterrows():
      if row['Mensagem'].startswith("Turno da UE:"):
          cabecalho_atual["TURNO"] = row['Mensagem'].split(":")[1].strip()

      elif row['Mensagem'].startswith("Identificação do Modelo de Urna:"):
          cabecalho_atual["MOD_URNA"] = row['Mensagem'].split(":")[1].strip()

      elif row['Mensagem'].startswith("Município:"):
          cabecalho_atual["MUNICIPIO"] = row['Mensagem'].split(":")[1].strip()

      elif row['Mensagem'].startswith("Zona Eleitoral:"):
          cabecalho_atual["ZONA"] = row['Mensagem'].split(":")[1].strip()

      elif row['Mensagem'].startswith("Local de Votação:"):
          cabecalho_atual["LOCAL"] = row['Mensagem'].split(":")[1].strip()

      elif row['Mensagem'].startswith("Seção Eleitoral:"):
          cabecalho_atual["SECAO"] = row['Mensagem'].split(":")[1].strip()

      elif row['Mensagem'] == "Aguardando digitação do título":
          inicio = row['DataHora']

      elif row['Mensagem'] == "O voto do eleitor foi computado":
          fim = row['DataHora']
          votos.append({
              **cabecalho_atual,
              "INICIO_VOTO": inicio,
              "FIM_VOTO": fim,
              "TEMPO": pd.to_datetime(fim) - pd.to_datetime(inicio)
          })

  # Converta a lista de votos em um DataFrame

  df_votos = pd.DataFrame(votos)
  if not df_votos.empty:
    df_votos['TEMPO_SEG']=df_votos.TEMPO.dt.seconds

  return df_votos

