# In[]

import pandas as pd
from datetime import date, datetime
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

# In[]
caminho_unificado = r"files/funcionarios.xlsx"

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

# amostra_nan = arquivo[arquivo['cpf'].isna()].sample(n=5)


arquivo["cpf"] = arquivo["cpf"].astype(str)

arquivo["cpf"] = arquivo["cpf"].str.replace(".", "")
arquivo["cpf"] = arquivo["cpf"].str.replace("-", "")

# In[]
arquivo["saida"] = arquivo["saida"].astype(str)
arquivo["saida"] = arquivo["saida"].replace("9999-12-31 00:00:00", "")

arquivo["update"] = datetime.now()
print(list(arquivo.columns))

# In[]

# Defina uma lista de tipos de dados correspondentes às colunas
tipos_de_dados = {
    "referencia": "datetime64[us]",
    "area_de_recursos_humanos": str,
    "grupo_de_empregados": str,
    "cpf": np.int64,
    "no_pess": np.int64,
    "no_pessoal": str,
    "status_da_ocupacao": str,
    "entrada": "datetime64[us]",
    "saida": "datetime64[us]",
    "centro_cst": str,
    "centro_de_custo": str,
    "posicao": str,
    "posicao1": str,
    "montante": np.float64,
    "moeda": str,
    "empresa": str,
    "sexo": str,
    "dtnasc": "datetime64[us]",
    "chave_para_estado_civil": str,
    "update": "datetime64[us]",
}

# Use um loop para aplicar os tipos de dados apropriados às colunas

for col, dtype in tipos_de_dados.items():
    try:
        arquivo[col] = arquivo[col].astype(dtype)
    except Exception as e:
        print(f"Erro ao converter a coluna '{col}' para o tipo {dtype}. Erro: {e}")


# In[]

projeto = "rh-analytics-397518"
dataset = "powerbi"
tabela = "powerbi.funcionarios"


pandas_gbq.to_gbq(arquivo, tabela, project_id=projeto, if_exists="replace")


# %%
