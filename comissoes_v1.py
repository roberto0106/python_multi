# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

# Defina a localização para o formato que utiliza ponto para separar os milhares e vírgula para separar decimais
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

# Autenticação do Google Cloud
# gcloud auth login

"""# Compensação"""
# In[]
compensacao = pd.read_csv(r'C:\Users\roberto.flor\Documents\comissões\CustomerLineItems.csv', sep=';')

# Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos = unidecode(nome_coluna)  # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace('.', '')  # Remove pontos
    nome_formatado = nome_sem_pontos.replace(' ', '_')  # Substitui espaços por underscores
    nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
    return nome_formatado_minusculo

# Aplica a função a todos os nomes de coluna
compensacao.columns = [formatar_nome_coluna(nome) for nome in compensacao.columns]

compensacao.to_csv('compensacao_bigquery.csv',encoding='utf-8',sep=';')
# In[]
# Especifique o projeto e o dataset do BigQuery
# projeto = 'dotted-cedar-149302'
# dataset = 'laboratorio'

# # Nome da tabela no BigQuery
# nome_tabela = 'compensacao'

# # Gravar o DataFrame no BigQuery
# tabela_destino = f'{projeto}.{dataset}.{nome_tabela}'
# pandas_gbq.to_gbq(compensacao, tabela_destino, project_id=projeto, if_exists='replace')

"""# Demonstrativo"""

# In[]

# Carregar o arquivo CSV em um DataFrame
caminho_arquivo_1000 = r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\FECHAMENTO INTERNO INDUSTRIAL (AGOSTO).csv'
caminho_arquivo_1000_fat = r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\FECHAMENTO INTERNO INDUSTRIAL (AGOSTO-FAT).csv'
caminho_arquivo_3000 = r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\FECHAMENTO INTERNO GIGA (AGOSTO).csv'
caminho_arquivo_4000 = r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\FECHAMENTO INTERNO PROINOX (AGOSTO).csv'

def replace_comma_with_dot(text):
    if isinstance(text, str):
        return text.replace("'", '').replace("'", "")
    return text

demonstrativo_1000 = pd.read_csv(caminho_arquivo_1000, sep=';', encoding='utf-8-sig')
demonstrativo_1000_fat = pd.read_csv(caminho_arquivo_1000_fat, sep=';', encoding='utf-8-sig')
demonstrativo_3000 = pd.read_csv(caminho_arquivo_3000, sep=';', encoding='utf-8-sig')
demonstrativo_4000 = pd.read_csv(caminho_arquivo_4000, sep=';', encoding='utf-8-sig')

demonstrativo = pd.concat([demonstrativo_1000, demonstrativo_3000, demonstrativo_4000,demonstrativo_1000_fat], ignore_index=True)

# Aplicar a função replace_comma_with_dot à coluna valor_do_titulo
demonstrativo['Valor do título'] = demonstrativo['Valor do título'].apply(replace_comma_with_dot)
demonstrativo['Valor base da comissão'] = demonstrativo['Valor base da comissão'].apply(replace_comma_with_dot)
demonstrativo['Valor da comissão'] = demonstrativo['Valor da comissão'].apply(replace_comma_with_dot)
demonstrativo['Percentual de comissão'] = demonstrativo['Percentual de comissão'].apply(replace_comma_with_dot)

# Converter colunas de datas para o formato datetime
date_columns = ['Data do documento', 'Data do vencimento', 'Data de compensação', 'Dt.criação']
for col in date_columns:
    demonstrativo[col] = pd.to_datetime(demonstrativo[col], errors='coerce')


# In[]

columns_selected = [   
    'Empresa',
    'Exercício',
    'Nº documento',
    'Número do Documento Original',
    'Nome do representante',
    'Item',
    'Tipo de movimento',
    'Código representante',
    'Cliente',
    'Data do documento',
    'Data de compensação',
    'Doc.compensação',
    'Percentual de comissão',
    'Dt.criação',
    'Criado por',
    'Apuração',
    'Referência cliente',
    'Denominação Tabelas'
]

# Transforme o restante das colunas de acordo com o dicionário column_types
demonstrativo_consolidado = demonstrativo[columns_selected].dropna(subset=['Doc.compensação','Código representante'])


# Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos =unidecode(nome_coluna)   # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace('.', '')  # Remove pontos
    nome_sem_barras = nome_sem_pontos.replace('/', '_')  # Remove pontos
    nome_sem_abertura_parenteses = nome_sem_barras.replace('(', '_')  # Remove pontos
    nome_sem_fechamento_parenteses = nome_sem_abertura_parenteses.replace(')', '_')  # Remove pontos
    nome_formatado = nome_sem_fechamento_parenteses.replace(' ', '_')  # Substitui espaços por underscores
    nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
    return nome_formatado_minusculo

demonstrativo_consolidado.columns = [formatar_nome_coluna(nome) for nome in demonstrativo_consolidado.columns]


# Função para converter string em float
def string_to_float(valor_string):
    return float(valor_string.replace(',', '.'))


demonstrativo_consolidado['data_de_compensacao'] = demonstrativo_consolidado['data_de_compensacao'].fillna("Null")

# demonstrativo_consolidado['percentual_de_comissao'] = demonstrativo_consolidado['percentual_de_comissao'].apply(string_to_float)
demonstrativo_consolidado.to_csv('demonstrativo.csv',encoding='utf-8',sep=';')

# projeto = 'dotted-cedar-149302'
# dataset = 'laboratorio'

# # Nome da tabela no BigQuery
# nome_tabela = 'laboratorio.teste'

# # pandas_gbq.to_gbq(demonstrativo_consolidado, nome_tabela, project_id=projeto, if_exists='replace', table_schema=schema)
# pandas_gbq.to_gbq(demonstrativo_consolidado, nome_tabela, project_id=projeto, if_exists='replace')
# # %%

# %%
