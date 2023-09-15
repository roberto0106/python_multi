import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

de_para_clientes = pd.read_excel(r'C:\Users\roberto.flor\Documents\comissões\de_para_clientes_extrato.xlsx')
de_para_vendedores = pd.read_excel(r'C:\Users\roberto.flor\Documents\folha_08.xlsx')
de_para_cod_sap = pd.read_excel(r'C:\Users\roberto.flor\Documents\vendedores.xlsx')

de_para_clientes.columns = de_para_clientes.columns.str.lower()
de_para_clientes.columns = de_para_clientes.columns.str.replace('.', '_')
# de_para_clientes = de_para_clientes.astype(str)
de_para_clientes.drop(columns='tb_clientes_a1_nreduz', inplace=True)
de_para_clientes = de_para_clientes.drop_duplicates()
de_para_clientes.replace({np.nan: None}, inplace=True)
df_para_clientes = de_para_clientes.astype(str)

projeto = 'rh-analytics-397518'
dataset = 'comissoes_internos'
dataset_vendedores = 'dados_vendedores'
# Nome da tabela no BigQuery
nome_tabela_demonstrativo = 'comissoes_internos.de_para_clientes'
nome_tabela_vendedores = 'dados_vendedores.matriculas'
nome_tabela_cod_sap = 'dados_vendedores.cod_sap'

de_para_cod_sap['documento'] = de_para_cod_sap['documento'].astype('Int64').abs()
de_para_cod_sap['cod_sap'] = de_para_cod_sap['cod_sap'].astype(str)

schema_table = [
    {'name': 'documento', 'type': 'INTEGER'},
    {'name': 'cod_sap', 'type': 'STRING'}
]

pandas_gbq.to_gbq(de_para_cod_sap, nome_tabela_cod_sap, project_id=projeto, if_exists='replace',table_schema=schema_table)
pandas_gbq.to_gbq(de_para_vendedores, nome_tabela_vendedores, project_id=projeto, if_exists='replace')
pandas_gbq.to_gbq(df_para_clientes, nome_tabela_demonstrativo, project_id=projeto, if_exists='replace')
