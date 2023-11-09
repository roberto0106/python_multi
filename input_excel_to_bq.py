# In[]

import pandas as pd
from datetime import datetime
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale
from ferramentas import Ferramentas

f = Ferramentas()

arquivo = pd.read_excel(r"files/BD_RH_OUTUBRO.xlsx")

arquivo_tratado = f.formatar_colunas_dataframe(arquivo)

# In[]

df_bq = f.convert_to_bigquery_dtypes(arquivo_tratado)


# In[]

projeto = "rh-analytics-397518"
dataset = "powerbi"
tabela = "powerbi.bd_rh"

pandas_gbq.to_gbq(df_bq, tabela, project_id=projeto, if_exists="replace")


# %%
