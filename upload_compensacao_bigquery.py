# In[]

from datetime import date
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

caminho_unificado = r'files/DEMONSTRATIVOS SETEMBRO/compensacao_setembro_2023.xlsx'

arquivo = pd.read_excel(caminho_unificado)

# Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos =unidecode(nome_coluna)   # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace('.', '')  # Remove pontos
    nome_sem_barras = nome_sem_pontos.replace('/', '_')  # Remove pontos
    nome_sem_abertura_parenteses = nome_sem_barras.replace('(', '_')  # Remove pontos
    nome_sem_fechamento_parenteses = nome_sem_abertura_parenteses.replace(')', '_')  # Remove pontos
    nome_formatado = nome_sem_fechamento_parenteses.replace(' ', '_')  # Substitui espaços por underscores
    nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
    return nome_formatado_minusculo

arquivo.columns = [formatar_nome_coluna(nome) for nome in arquivo.columns]


# In[]


# Defina uma lista de tipos de dados correspondentes às colunas
tipos_de_dados = {
    'nome_do_cliente': str,
    'cliente': str,
    'empresa': str,
    'status_comp': str,
    # 'lancamento_contabil': date,
    'tipo_lctocontabil': str,
    'lanctocompensacao': np.int64,
    'data_de_compensacao': 'datetime64[ns]',
    'montante__me_': float,
    'item_visao_lcto': np.int64,
    'exercicio': np.int64,
    'chave_de_lancamento': str,
    'debito_credito': str,
}
# Use um loop para aplicar os tipos de dados apropriados às colunas

for col, dtype in tipos_de_dados.items():
    arquivo[col] = arquivo[col].astype(dtype)
esquema = []

for col_name, col_type in arquivo.dtypes.items():
    if col_type == 'int64':
        field_type = 'INTEGER'
    elif col_type == 'float64':
        field_type = 'FLOAT'
    elif col_type == 'object':
        field_type = 'STRING'
    else:
        # Caso padrão, você pode definir um tipo padrão adequado aqui
        field_type = 'STRING'

arquivo['chave_compensacao'] = arquivo['empresa'].astype(str) + arquivo['exercicio'].astype(str) + arquivo['lancamento_contabil'].astype(str) + arquivo['item_visao_lcto'].astype(str)



projeto = 'rh-analytics-397518'
dataset = 'estudo_comissoes'
tabela = 'estudo_comissoes.compensacao_setembro_2023'

pandas_gbq.to_gbq(arquivo, tabela, project_id=projeto, if_exists='replace')




# %%

