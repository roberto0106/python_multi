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

caminho_unificado = r"files/folha_outubro_fechamento.xlsx"

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

arquivo["numero_no_cadastro_de_pessoa_f"] = arquivo[
    "numero_no_cadastro_de_pessoa_f"
].str.replace(".", "")
arquivo["numero_no_cadastro_de_pessoa_f"] = arquivo[
    "numero_no_cadastro_de_pessoa_f"
].str.replace("-", "")

arquivo["saida"] = arquivo["saida"].astype(str)

# Substitua "9999-12-31 00:00:00" por um valor vazio (NaN)
arquivo["saida"] = arquivo["saida"].replace("9999-12-31 00:00:00", "")
arquivo["inicio"] = arquivo["inicio"].replace("9999-12-31 00:00:00", "")

print(arquivo.info())

# In[]

# Defina uma lista de tipos de dados correspondentes às colunas
tipos_de_dados = {
    "area_de_recursos_humanos": str,
    "grupo_de_empregados": str,
    "numero_no_cadastro_de_pessoa_f": np.int64,
    "no_pess": np.int64,
    "no_pessoal": str,
    "status_da_ocupacao": str,
    "entrada": "datetime64[ns]",
    "saida": "datetime64[ns]",
    "centro_cst": str,
    "inicio": "datetime64[ns]",
    "centro_de_custo": str,
    "posicao": np.int64,
    "posicao1": str,
    "montante": str,
    "moeda": str,
    "empresa": str,
    "sexo": str,
    "dtnasc": "datetime64[ns]",
    "chave_para_estado_civil": str,
}

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
dataset = "oficina"
tabela = "oficina.folha_outubro"

arquivo["update"] = datetime.now()

# arquivo.to_excel(r'files/folha_setembro.xlsx')


pandas_gbq.to_gbq(arquivo, tabela, project_id=projeto, if_exists="replace")


# %%
