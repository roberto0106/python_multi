# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

projeto = 'rh-analytics-397518'
dataset = 'estudo_comissoes'
# Execute uma consulta SQL
compensacao=  'estudo_comissoes.compensacao_setembro_2023'

df_compensacao = pd.read_gbq(compensacao, project_id=projeto)


# %%

indices = ['chave_compensacao']
valores = ['montante__me_']
colunas = ['tipo_lctocontabil']

pivot = df_compensacao.pivot_table(values=valores, index=indices, columns=colunas ,aggfunc='sum')
filled_pivot = pivot.fillna(0)
print(filled_pivot)

# %%
filled_pivot.to_csv(r'files/dinamica_verbas_compensacao.csv', sep=';')
# %%
