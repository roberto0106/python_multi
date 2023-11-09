# In[]

import os
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

# In[]

caminhos = f.listar_arquivos_em_pasta(pasta="files\Demonstrativos Históricos")
consolidado = f.consolidar_arquivos(caminhos, "Sheet1")
df_sem_colunas_nulas = consolidado.dropna(axis=1, how="all")


# In[]

colunas_data = [
    "data_do_documento",
    "data_do_vencimento",
    "data_de_compensacao",
    "dtcriacao",
    "data_de_debito_do_estorno_da_comissao",
]

df_tratado = f.formatar_colunas_dataframe(df_sem_colunas_nulas)

df_final = f.tratar_coluna_data(df_tratado, colunas_data)

# In[]

# Lista de nomes de colunas a serem excluídas
colunas_a_excluir = ["hora", "criado_as"]

# Use o método drop para excluir as colunas
df = df_final.drop(columns=colunas_a_excluir)


# In[]

projeto = "rh-analytics-397518"
dataset = "comissoes"
tabela = "comissoes.comissoes_demonstrativos"

pandas_gbq.to_gbq(df, tabela, project_id=projeto, if_exists="replace")

# %%
