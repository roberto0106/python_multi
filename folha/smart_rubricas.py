# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

projeto = "rh-analytics-397518"
dataset = "people_analytics"
# Execute uma consulta SQL
extrato = "people_analytics.rubricas"

df_extrato = pd.read_gbq(extrato, project_id=projeto)

# %%
columns_level = [
    "nome_da_origem",
    "no_pessoal",
    "cpf",
    "nome_do_empregado_ou_candidato",
    "txtarea_processfp",
    "data_de_pagamento",
    "rubrica_salarial",
    "txtdescrrubricasalarial",
    "numero",
    "montante",
    "texto_da_area_recursos_humanos",
    "area_recursoshumanos",
    "update",
]


df_extrato["referencia"] = (
    df_extrato["data_de_pagamento"].dt.to_period("M").dt.to_timestamp()
)
df_extrato["referencia"] = df_extrato["referencia"].astype("datetime64[us]")


# pivot df_extrato
df_pivot = df_extrato.pivot_table(
    index=[
        "no_pessoal",
        "cpf",
        "nome_do_empregado_ou_candidato",
        "txtarea_processfp",
        "referencia",
    ],
    columns="txtdescrrubricasalarial",
    values="montante",
    aggfunc="sum",  # ou outra função de agregação, como 'mean', conforme sua necessidade
).reset_index()


# %%


def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos = unidecode(nome_coluna)  # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace(".", "")  # Remove pontos
    nome_sem_pontos = nome_sem_acentos.replace("%", "")  # Remove pontos
    nome_sem_barras = nome_sem_pontos.replace("/", "_")  # Remove pontos
    nome_sem_abertura_parenteses = nome_sem_barras.replace("(", "_")  # Remove pontos
    nome_sem_fechamento_parenteses = nome_sem_abertura_parenteses.replace(
        ")", "_"
    )  # Remove pontos
    nome_formatado = nome_sem_fechamento_parenteses.replace(
        " ", "_"
    )  # Substitui espaços por underscores
    nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
    return nome_formatado_minusculo


df_pivot.columns = [formatar_nome_coluna(nome) for nome in df_pivot.columns]


projeto = "rh-analytics-397518"
dataset = "powerbi"
tabela = "powerbi.smart_rubricas"


pandas_gbq.to_gbq(df_pivot, tabela, project_id=projeto, if_exists="replace")

# %%
