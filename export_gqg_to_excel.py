# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

projeto = 'rh-analytics-397518'
dataset = 'comissoes_externos'
# Execute uma consulta SQL
extrato=  'comissoes_externos.comissao_vendedores_externos_exrato_v2'
comissoes=  'comissoes_externos.comissao_vendedores_externos_v2'

df_comissoes = pd.read_gbq(comissoes, project_id=projeto)
df_extrato = pd.read_gbq(extrato, project_id=projeto)

# Verifique as colunas de data e hora com informações de fuso horário
colunas_com_timezone = ['criacao']  # Substitua pelos nomes reais das colunas

# Converta as colunas com informações de fuso horário para 'timezone unaware'
for coluna in colunas_com_timezone:
    df_comissoes[coluna] = df_comissoes[coluna].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)

df_comissoes.to_excel("files/comissoes_externos_23.xlsx", sheet_name='pagamento')
df_extrato.to_excel("files/comissoes_externos_extrato_ago_23.xlsx", sheet_name='extrato')


