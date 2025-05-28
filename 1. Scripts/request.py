import requests
import zipfile
import io
import pandas as pd

def baixar_e_processar_arquivo(url, file_type, dataframes):
    try:
        response = requests.get(url, stream=(file_type=='.csv'), timeout=60)
        
        if response.status_code == 200:
            if file_type == '.zip':
                with zipfile.ZipFile(io.BytesIO(response.content)) as file:
                    # pegando o nome dos arquivos dentro do ZIP
                    arquivos = file.namelist()
                    for arquivo in arquivos:
                        print(f'Processando o arquivo ZIP: {arquivo}')
            

            elif file_type == '.csv':
                print(f'Processando o arquivo CSV: {url}')
                df = pd.read_csv(io.StringIO(response.text), sep=';', encoding='utf-8-sig') 
                print(df.head()) 
                
                # Adicionando o DataFrame à lista
                dataframes.append(df)
        else:
            print(f'Erro ao baixar o arquivo: {response.status_code}')

    except requests.exceptions.RequestException as e:
        print(f'Erro ao tentar baixar o arquivo: {url}\n{e}')

anos = range(2020, 2021)  
semestres = ['01', '02']


df_list = []


for ano in anos:
    for semestre in semestres:
        # Tentando primeiro o arquivo ZIP
        url_zip = f'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{ano}-{semestre}.zip'
        baixar_e_processar_arquivo(url_zip, '.zip', df_list)  # Passa o tipo de arquivo como '.zip'

        # Depois tenta o arquivo CSV
        url_csv = f'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{ano}-{semestre}.csv'
        print(f'Baixando dados de: {url_csv}')
        baixar_e_processar_arquivo(url_csv, '.csv', df_list)  # Passa o tipo de arquivo como '.csv'

# Concatenando todos os DataFrames da lista em um único DataFrame
df_final = pd.concat(df_list, ignore_index=True)

# Salvando o DataFrame final em um arquivo CSV
df_final.to_csv('2. bases/dados_combinados.csv', index=False)
print('Arquivo final salvo como dados_combinados.csv')
