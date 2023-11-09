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

caminho_unificado = r"files/COMISSAO OUTUBRO/demonstrativo.xlsx"

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

# replace value "Nan" to other column of dataframe

arquivo["numero_do_documento_original"].fillna(arquivo["no_documento"], inplace=True)
arquivo["no_documento"].fillna(0, inplace=True)


arquivo["numero_do_documento_original"] = arquivo[
    "numero_do_documento_original"
].astype(int)

# In[]
# Defina uma lista de tipos de dados correspondentes às colunas
tipos_de_dados = {
    "marcado": str,
    "empresa": np.int64,
    "exercicio": np.int64,
    "no_documento": np.int64,
    "documento_de_vendas": str,
    "item": np.int64,
    "tipo_de_movimento": str,
    "codigo_representante": str,
    "nome_do_representante": str,
    "referencia": str,
    "cliente": str,
    "nome_1": str,
    "data_do_documento": "datetime64[ns]",
    "data_do_vencimento": "datetime64[ns]",
    "data_de_compensacao": "datetime64[ns]",
    "doccompensacao": str,
    "regiao": str,
    "lancamento_manua": str,
    "contabilizou": str,
    "moeda_do_documento": str,
    "descricao": str,
    "valor_do_titulo": float,
    "valor_base_da_comissao": float,
    "retencao_da_comissao_irf": float,
    "valor_da_comissao": float,
    "percentual_de_comissao": float,
    "valor_liquido_de_comissao": float,
    "valor_da_indenizacao": float,
    "irf_da_indenizacao": float,
    "percentual_indenizacao": float,
    "valor_liquido_da_indenizacao": float,
    "prazo_medio": str,
    "comissao_faturamento_ou_prazo_medio": str,
    "estorno": str,
    "estorno_com": str,
    "debito_de_comissao_por_titulo_em_atraso": str,
    "numero_do_documento_original": np.int64,
    "numero_do_documento_de_debito_por_titulo": str,
    "num_doc_orig_debito_por_titulo_atraso": str,
    "data_de_debito_do_estorno_da_comissao": "datetime64[ns]",
    "re-credito_de_comissao": str,
    "data_do_re-credito": "datetime64[ns]",
    "numero_do_documento_do_re-credito": str,
    "numero_do_documento_original_do_re-credi": str,
    "estorno_de_pagamento_liquidado": str,
    "centro_custo": str,
    "dtcriacao": "datetime64[ns]",
    "criado_as": str,
    "criado_por": str,
    "marca": str,
    "duplicatas_pelo_prazo_medio_liber_pagto": str,
    "partida_foi_desmembrada": str,
    "apuracao": str,
    "referencia_cliente": str,
    "local": str,
    "nome_da_tabela": str,
    "documento_de_compras": str,
    "no_do_documento_de_adiantamento": str,
    "chave_referencia_3": str,
    "observacao_comissao": str,
    "data": str,
    "hora": str,
    "denominacao_tabelas": str,
}
# Use um loop para aplicar os tipos de dados apropriados às colunas

# for col, dtype in tipos_de_dados.items():
#     arquivo[col] = arquivo[col].astype(dtype)

arquivo_tratado = f.convert_to_bigquery_dtypes(arquivo)

arquivo_tratado["chave_demonstrativo"] = (
    arquivo_tratado["empresa"].astype(str)
    + arquivo_tratado["exercicio"].astype(str)
    + arquivo_tratado["numero_do_documento_original"].astype(str)
    + arquivo_tratado["item"].astype(str)
)


arquivo_tratado.drop(columns=["hora", "criado_as"], inplace=True)

arquivo_tratado["corte"] = "01/10 a 31/10"
arquivo_tratado["update"] = datetime.now()

# In[]
projeto = "rh-analytics-397518"
dataset = "comissoes"
tabela = "comissoes.demonstrativos_externos"

pandas_gbq.to_gbq(arquivo_tratado, tabela, project_id=projeto, if_exists="replace")
# pandas_gbq.to_gbq(arquivo, tabela, project_id=projeto, if_exists='append')


# %%
