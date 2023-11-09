# In[]

from datetime import date, datetime
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale
from ferramentas import Ferramentas

f = Ferramentas()


# In[]

caminho_unificado = r"files/COMISSAO OUTUBRO/compensacao.xlsx"

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


# In[]

arquivo_tratado = f.convert_to_bigquery_dtypes(arquivo)

arquivo_tratado["chave_compensacao"] = (
    arquivo_tratado["empresa"].astype(str)
    + arquivo_tratado["exercicio"].astype(str)
    + arquivo_tratado["lancamento_contabil"].astype(str)
    + arquivo_tratado["item_visao_lcto"].astype(str)
)

arquivo_tratado["corte"] = "01/10 a 31/10"
arquivo_tratado["update"] = datetime.now()

projeto = "rh-analytics-397518"
dataset = "comissoes"
tabela = "comissoes.compensacoes_externos"

pandas_gbq.to_gbq(arquivo_tratado, tabela, project_id=projeto, if_exists="replace")


# %%
