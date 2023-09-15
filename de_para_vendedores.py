import pandas as pd
import pandas_gbq
import json
from google.cloud import bigquery

df_sap_gui = pd.read_excel(r'files/dados_vendedores_sap_gui.XLSX')
df_sap_s4 = pd.read_excel(r'files/vendedores_s4.xlsx')

df_sap_gui.columns = df_sap_gui.columns.str.lower()
df_sap_s4.columns = df_sap_s4.columns.str.lower()

df_sap_s4.rename(columns = {
    "a3_cgc":"cpf",
    "a3_cod":"cod_mercury",
    "árrh":"area_rh",
    "área de recursos humanos":"setor",
    "sapcodven":"cod_sap"
    },inplace = True)

df_sap_gui.rename(columns = {
    "número no cadastro de pessoa f":"cpf",
    "nº pessoal":"nome",
    "nº pess.":"matricula",
    "status da ocupação":"status",
    "e-mail":"email",
    },inplace = True)

df_sap_s4.columns = df_sap_s4.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

df_sap_gui['cpf'] = df_sap_gui['cpf'].str.replace('[.-]', '', regex=True)
df_sap_gui['cpf'] = df_sap_gui['cpf'].astype('Int64')
df_sap_s4['cpf'] = df_sap_s4['cpf'].astype('Int64')



#limpando duplicidades
df_sap_gui = df_sap_gui[(df_sap_gui['cpf'].notnull()) & (df_sap_gui['email'].notnull())]

df_sap_gui = df_sap_gui.drop_duplicates(subset=['cpf','nome','matricula'])
# df_sap_s4 = df_sap_s4.drop_duplicates(subset=['cpf','nome','matricula'])

df = pd.merge(df_sap_s4,df_sap_gui, on="cpf", how="left")
#df is your dataframe
df[df.status == "ativo"]
df.dropna(subset='cpf',inplace=True)


df['aux_tipo_doc'] =  df['cpf'].astype(str)
df['qtd_caracteres_doc'] = df['aux_tipo_doc'].str.len()

    # Cria uma coluna com o tipo de documento
df['tipo_documento'] = df['cpf'].apply(lambda x: 'qtd_caracteres_doc' if x > 11 else 'tipo_cnpj')

df['qtd_caracteres_doc'].value_counts()
df['tipo_documento'].value_counts()


esquema = []
tipos = []

for col_name, col_type in df.dtypes.items():
    if col_type == 'int64':
        field_type = 'INTEGER'
    elif col_type == 'float64':
        field_type = 'FLOAT'
    elif col_type == 'object':
        field_type = 'STRING'
    else:
    # Caso padrão, você pode definir um tipo padrão adequado aqui
        field_type = 'STRING'
        
    esquema.append({'name': col_name, 'type': field_type})
    tipos.append({col_name,  field_type})


# Dicionário de mapeamento de substituição de chaves
mapeamento_substituicoes = {
    "FLOAT": "float",
    "STRING": "string",
    "INTEGER": "int",
}

# Crie um novo JSON com as substituições de chaves
novo_json = {}
for chave, valor in tipos:
    nova_chave = mapeamento_substituicoes.get(chave, chave)  # Use a nova chave se estiver no mapeamento, caso contrário, mantenha a chave original
    novo_json[nova_chave] = valor

# Use um loop para aplicar os tipos de dados apropriados às colunas
# for col, dtype in esquema:
#     df[col] = df[col].astype(dtype)

projeto = 'rh-analytics-397518'
dataset = 'conjunto_de_dados'
tabela = 'conjunto_de_dados.nome_da_tabela'
pandas_gbq.to_gbq(df, tabela, project_id=projeto, if_exists='replace')


df_final = df[['cpf','cod_mercury']]


projeto = 'rh-analytics-397518'
dataset = 'dados_vendedores'
tabela = 'dados_vendedores.internos'
pandas_gbq.to_gbq(df_final, tabela, project_id=projeto, if_exists='replace',table_schema=esquema)
