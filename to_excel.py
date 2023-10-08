# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

projeto = 'rh-analytics-397518'
dataset = 'comissoes_internos'
# Execute uma consulta SQL
extrato=  'estudo_comissoes.extrato_externos'


df_extrato = pd.read_gbq(extrato, project_id=projeto)

# Excel does not support datetimes with timezones. Please ensure that datetimes are timezone unaware before writing to Excel.

# In[]

# # Verifique as colunas de data e hora com informações de fuso horário
colunas_com_timezone_extrato = ['data_do_documento',
                                'data_de_compensacao',
                                'data_do_vencimento']  # Substitua pelos nomes reais das colunas

# colunas_com_timezone_comissoes = ['criacao']  # Substitua pelos nomes reais das colunas

# # Converta as colunas com informações de fuso horário para 'timezone unaware'
for coluna in colunas_com_timezone_extrato:
    df_extrato[coluna] = df_extrato[coluna].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)
# In[]

df_extrato.to_excel("files/comissoes_externos_06_10.xlsx", sheet_name='extrato')



# %%
