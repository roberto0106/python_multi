# In[]
import pandas as pd
import numpy as np
import csv
import unicodedata  # Biblioteca para retirar acentos
import pandas_gbq
from google.cloud import bigquery

# # Autenticação do Google Cloud
# # gcloud auth login

# """# Compensação"""
# # In[]
# compensacao = pd.read_csv(r'C:\Users\roberto.flor\Documents\comissões\compensacao.csv', sep=';')

# # Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
# def formatar_nome_coluna(nome_coluna):
#     nome_sem_acentos = unicodedata.normalize('NFD', nome_coluna)  # Remove acentos
#     nome_sem_pontos = nome_sem_acentos.replace('.', '')  # Remove pontos
#     nome_formatado = nome_sem_pontos.replace(' ', '_')  # Substitui espaços por underscores
#     nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
#     return nome_formatado_minusculo

# # Aplica a função a todos os nomes de coluna
# compensacao.columns = [formatar_nome_coluna(nome) for nome in compensacao.columns]
# # In[]
# # Especifique o projeto e o dataset do BigQuery
# projeto = 'dotted-cedar-149302'
# dataset = 'laboratorio'

# # Nome da tabela no BigQuery
# nome_tabela = 'compensacao'

# # Gravar o DataFrame no BigQuery
# tabela_destino = f'{projeto}.{dataset}.{nome_tabela}'
# pandas_gbq.to_gbq(compensacao, tabela_destino, project_id=projeto, if_exists='replace')

"""# Demonstrativo"""

demonstrativo_1000 = pd.read_csv(r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\demonstrativo_empresa_1000.csv', sep=';',dtype={'a_vencer_liq_simb_': str, 'Montante (ME)': str})
demonstrativo_3000 = pd.read_csv(r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\demonstrativo_empresa_3000.csv', sep=';',dtype={'a_vencer_liq_simb_': str, 'Montante (ME)': str})
demonstrativo_4000 = pd.read_csv(r'C:\Users\roberto.flor\Documents\comissões\demonstrativos\demonstrativo_empresa_4000.csv', sep=';',dtype={'a_vencer_liq_simb_': str, 'Montante (ME)': str})




data_selected = demonstrativo[selected_columns]# Transforme o restante das colunas de acordo com o dicionário column_types
data_transformed = data_selected.astype(column_types)

# Definir os tipos de coluna desejados
column_types = {
    "Empresa": "Int64",
    "Status comp.": "Int64",
    "Nº ID fiscal 1": "str",
    "Cliente": "str",
    "Nome do cliente": "str",
    "Tipo lçto.contábil": "str",
    "Dt.lnçmto.cont.": "datetime64[ns]",
    "Data-base": "datetime64[ns]",
    "Atribuição": "str",
    "Referência": "str",
    "Item visão lçto.": "Int64",
    "Lançamento contábil": "Int64",
    "Data vencimento líq.": "datetime64[ns]",
    "Débito/crédito": "str",
    "Montante (ME)": "str",
    "Banco da empresa": "str",
    "Chave ref.3": "str",
    "Texto de item": "str",
    "Lançto.compensação": "str",
    "Data de compensação": "datetime64[ns]",
    "Razão especial": "str",
    "A vencer líq.(símb.)": "str",
    "Condições pagamento": "str",
    "Dias de desconto 1": "Int64",
    "Forma de pagamento": "str",
    "Referência à fatura": "str",
    "Nome da região": "str",
    "PI criada por": "str",
    "Chave de lançamento": "str"
}


# Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos =unicodedata.normalize('NFD', nome_coluna)   # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace('.', '')  # Remove pontos
    nome_sem_barras = nome_sem_pontos.replace('/', '_')  # Remove pontos
    nome_sem_abertura_parenteses = nome_sem_barras.replace('(', '_')  # Remove pontos
    nome_sem_fechamento_parenteses = nome_sem_abertura_parenteses.replace(')', '_')  # Remove pontos
    nome_formatado = nome_sem_fechamento_parenteses.replace(' ', '_')  # Substitui espaços por underscores
    nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
    return nome_formatado_minusculo

data_transformed.columns = [formatar_nome_coluna(nome) for nome in data_transformed.columns]

projeto = 'dotted-cedar-149302'
dataset = 'laboratorio'

# Nome da tabela no BigQuery
nome_tabela = 'laboratorio.teste'

# Gravar o DataFrame no BigQuery
# Defina o esquema das colunas com tipos de dados corretos
schema = [
    {'name': 'empresa', 'type': 'INTEGER'},
    {'name': 'status_comp', 'type': 'INTEGER'},
    {'name': 'no_id_fiscal_1', 'type': 'STRING'},
    {'name': 'cliente', 'type': 'STRING'},
    {'name': 'nome_do_cliente', 'type': 'STRING'},
    {'name': 'tipo_lctocontabil', 'type': 'STRING'},
    {'name': 'dtlncmtocont', 'type': 'DATE'},
    {'name': 'data-base', 'type': 'DATE'},
    {'name': 'atribuicao', 'type': 'STRING'},
    {'name': 'referencia', 'type': 'STRING'},
    {'name': 'item_visao_lcto', 'type': 'INTEGER'},
    {'name': 'lancamento_contabil', 'type': 'INTEGER'},
    {'name': 'data_vencimento_liq', 'type': 'DATE'},
    {'name': 'debito_credito', 'type': 'STRING'},
    {'name': 'montante__me_', 'type': 'STRING'},
    {'name': 'banco_da_empresa', 'type': 'STRING'},
    {'name': 'chave_ref3', 'type': 'STRING'},
    {'name': 'texto_de_item', 'type': 'STRING'},
    {'name': 'lanctocompensacao', 'type': 'STRING'},
    {'name': 'data_de_compensacao', 'type': 'DATE'},
    {'name': 'razao_especial', 'type': 'STRING'},
    {'name': 'a_vencer_liq_simb_', 'type': 'STRING'},
    {'name': 'condicoes_pagamento', 'type': 'STRING'},
    {'name': 'dias_de_desconto_1', 'type': 'INTEGER'},
    {'name': 'forma_de_pagamento', 'type': 'STRING'},
    {'name': 'referencia_a_fatura', 'type': 'STRING'},
    {'name': 'nome_da_regiao', 'type': 'STRING'},
    {'name': 'pi_criada_por', 'type': 'STRING'},
    {'name': 'chave_de_lancamento', 'type': 'STRING'}
]

pandas_gbq.to_gbq(data_transformed, nome_tabela, project_id=projeto, if_exists='replace', table_schema=schema)