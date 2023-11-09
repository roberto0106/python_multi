# In[]

import os
import pandas as pd
from datetime import date, datetime
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale
from ferramentas import Ferramentas

f = Ferramentas()

df_extrato = pd.read_excel(
    r"files/comissoes_extrato_setembro_externos_powerbi.xlsx", sheet_name="extrato"
)
df_consolidado = pd.read_excel(
    r"files/comissoes_extrato_setembro_externos_powerbi.xlsx", sheet_name="consolidado"
)

# In[]
df_extrato_tratado = f.formatar_colunas_dataframe(df_extrato)
df_consolidado_tratado = f.formatar_colunas_dataframe(df_consolidado)


# %%

df_extrato_final = f.convert_to_bigquery_dtypes(df_extrato_tratado)
df_consolidado_final = f.convert_to_bigquery_dtypes(df_consolidado_tratado)
df_consolidado_final["competencia"] = "2023-09-01"

# In[]

# Defina uma lista de tipos de dados correspondentes às colunas
tipos_de_dados_extrato = {
    "email_reps": str,
    "diretor": str,
    "gerente": str,
    "empresa": str,
    "exercicio": str,
    "item": str,
    "documento_de_vendas": str,
    "cod_protheus": str,
    "codigo_representante": str,
    "nome_do_representante": str,
    "referencia": str,
    "cliente": str,
    "nome_do_cliente": str,
    "data_do_documento": "datetime64[ns]",
    "data_do_vencimento": "datetime64[ns]",
    "data_de_compensacao": "datetime64[ns]",
    "doccompensacao": str,
    "regiao": str,
    "descricao": str,
    "valor_do_titulo": np.float64,
    "valor_base_comissao_demonstrativo": str,
    "valor_comissao": np.float64,
    "percentual_de_comissao": str,
    "referencia_cliente": str,
    "pedido_para_nf_pagto": str,
    "competencia": "datetime64[ns]",
}

tipos_de_dados_consolidado = {
    "empresa": str,
    "codigo_representante": str,
    "representante": str,
    "email_reps": str,
    "abatimento": np.float64,
    "premio": np.float64,
    "comissao": np.float64,
    "total": np.float64,
    "centro_de_custo": str,
    "pedido": str,
    "competencia": str,
}


# Use um loop para aplicar os tipos de dados apropriados às colunas

for col, dtype in tipos_de_dados_extrato.items():
    try:
        df_extrato_final[col] = df_extrato_final[col].astype(dtype)
    except Exception as e:
        print(f"Erro ao converter a coluna '{col}' para o tipo {dtype}. Erro: {e}")

for col, dtype in tipos_de_dados_consolidado.items():
    try:
        df_consolidado_final[col] = df_consolidado_final[col].astype(dtype)
    except Exception as e:
        print(f"Erro ao converter a coluna '{col}' para o tipo {dtype}. Erro: {e}")


# %%
projeto = "rh-analytics-397518"
dataset = "powerbi_extratos_comissao"
tabela_extrato = "powerbi_extratos_comissao.extrato_externos"
tabela_consolidado = "powerbi_extratos_comissao.consolidado_externos"

pandas_gbq.to_gbq(
    df_extrato_final, tabela_extrato, project_id=projeto, if_exists="replace"
)
pandas_gbq.to_gbq(
    df_consolidado_final, tabela_consolidado, project_id=projeto, if_exists="replace"
)

# %%
