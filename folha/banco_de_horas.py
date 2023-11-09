# In[]

import pandas as pd
from datetime import datetime
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

caminho_unificado = r"files/Banco de Horas.XLSX"

arquivo = pd.read_excel(caminho_unificado)


# Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos = unidecode(nome_coluna)  # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace(".", "")  # Remove pontos
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


arquivo.columns = [formatar_nome_coluna(nome) for nome in arquivo.columns]

arquivo["matricula"] = arquivo["nome"].str.extract(r"\((\d+)\)")
arquivo["matricula"] = arquivo["matricula"].astype(np.int64)
# arquivo["saida"] = arquivo["saida"].astype(str)
# arquivo["saida"] = arquivo["saida"].str.replace("9999-12-31 00:00:00", "")

print(list(arquivo.columns))

# In[]

# Defina uma lista de tipos de dados correspondentes às colunas
tipos_de_dados = {
    "referencia": "datetime64[us]",
    "nome": str,
    "banco_de_competencia": str,
    "saldo_inicial": np.float64,
    "acumulado": np.float64,
    "usado": np.float64,
    "apagados": np.float64,
    "demitido": np.float64,
    "transferido": np.float64,
    "saldo_final": np.float64,
    "matricula": np.int64,
}

tratando_floats = {
    "saldo_inicial",
    "acumulado",
    "usado",
    "apagados",
    "demitido",
    "transferido",
    "saldo_final",
}

# Use um loop para aplicar os tipos de dados apropriados às colunas

for col in tratando_floats:
    arquivo[col] = arquivo[col].astype(str)
    arquivo[col] = arquivo[col].str.replace(",", "")


# In[]


# Use um loop para aplicar os tipos de dados apropriados às colunas

for col, dtype in tipos_de_dados.items():
    arquivo[col] = arquivo[col].astype(dtype)
esquema = []


for col_name, col_type in arquivo.dtypes.items():
    if col_type == "int64":
        field_type = "INTEGER"
    elif col_type == "float64":
        field_type = "FLOAT"
    elif col_type == "object":
        field_type = "STRING"
    else:
        # Caso padrão, você pode definir um tipo padrão adequado aqui
        field_type = "STRING"
        esquema.append({"name": col_name, "type": field_type})

projeto = "rh-analytics-397518"
dataset = "powerbi"
tabela = "powerbi.banco_de_horas"

arquivo["update"] = datetime.now()

pandas_gbq.to_gbq(arquivo, tabela, project_id=projeto, if_exists="replace")


# %%
