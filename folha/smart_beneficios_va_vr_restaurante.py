# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

projeto = "rh-analytics-397518"
dataset = "powerbi"
# Execute uma consulta SQL
extrato = "powerbi.beneficios_va_vr_restaurante"

df_extrato = pd.read_gbq(extrato, project_id=projeto)

# %%

df_extrato["referencia"] = df_extrato["referencia"].astype("datetime64[us]")


# pivot df_extrato
df_pivot = df_extrato.pivot_table(
    index=[
        "id_da_pessoa",
        "no_pessoal",
        "referencia",
    ],
    columns="tipo_de_plano_de_beneficios_co",
    values="credito_er",
    aggfunc="sum",  # ou outra função de agregação, como 'mean', conforme sua necessidade
).reset_index()

# In[]


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
df_pivot = df_pivot.rename(columns={"vale_aliment.": "vale_aliment"})


# %%

projeto = "rh-analytics-397518"
dataset = "powerbi"
tabela = "powerbi.smart_va_vr"


pandas_gbq.to_gbq(df_pivot, tabela, project_id=projeto, if_exists="replace")

# %%
