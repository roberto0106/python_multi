# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

referencias = '20230801 a 20230831'
parametros = ['TABELA COMISSÕES EXTERNO - FAT','TABELA COMISSÕES EXTERNO - LIQ']

# In[]

# Carregar o arquivo CSV em um DataFrame
caminho_giga = r'files/DEMONSTRATIVO EXTERNO GIGA (AGOSTO 2023).xlsx'
caminho_industrial = r'files/DEMONSTRATIVO EXTERNO INDUSTRIAL (AGOSTO 2023).xlsx'
caminho_compensacao = r'files/COMPENSACAO EXTERNO (AGOSTO 2023).xlsx'

demonstrativo_1000 = pd.read_excel(caminho_giga)
demonstrativo_3000 = pd.read_excel(caminho_industrial)
compensacao = pd.read_excel(caminho_compensacao)

demonstrativo_consolidado = pd.concat([demonstrativo_1000, demonstrativo_3000, ], ignore_index=True)

demonstrativo = demonstrativo_consolidado.loc[demonstrativo_consolidado['Denominação Tabelas'].isin(parametros)]

# In[]

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
compensacao.columns = [formatar_nome_coluna(nome) for nome in compensacao.columns]


demonstrativo_consolidado['referencia_extracao'] = referencias
compensacao['referencia_extracao'] = referencias


# In[]

projeto = 'rh-analytics-397518'
dataset = 'comissoes_externos'

# Nome da tabela no BigQuery
nome_tabela_demonstrativo = 'comissoes_externos.demonstrativo'
nome_tabela_compensacao = 'comissoes_externos.compensacao'

pandas_gbq.to_gbq(demonstrativo_consolidado, nome_tabela_demonstrativo, project_id=projeto, if_exists='replace')
pandas_gbq.to_gbq(compensacao, nome_tabela_compensacao, project_id=projeto, if_exists='replace')
# %%