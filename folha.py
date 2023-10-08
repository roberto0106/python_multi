# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

# %%
caminho_folha = r'files/folha.xlsx'
folha = pd.read_excel(caminho_folha)

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

folha.columns = [formatar_nome_coluna(nome) for nome in folha.columns]


# In[]

folha["inicio"] = pd.to_datetime(folha["inicio"], format="%m/%Y")

folha['mes_referencia'] = folha['inicio'].dt.strftime('%Y-%m')


selecao = [
    'numero_no_cadastro_de_pessoa_f',
    'montante',
    'mes_referencia'
]

# selecionando as colunas selecao no dataframe folha
df1= folha[selecao].copy().reset_index()

# sort dataframe by 'numero_no_cadastro_de_pessoa_f','inicio','montante'
df2 = df1.sort_values(['numero_no_cadastro_de_pessoa_f','mes_referencia','montante']).drop_duplicates(
    subset=['numero_no_cadastro_de_pessoa_f', 'mes_referencia'], 
    keep='last'
    )

# pivot df2 to row='numero_no_cadastro_de_pessoa_f', column='inicio', value=sum(montante)
df3 = pd.pivot_table(data=df2,
                    index=['numero_no_cadastro_de_pessoa_f'],
                    columns=["mes_referencia"],
                    values="montante")
        

# Definir uma função personalizada para aplicar ao groupby
def custom_agg(group):
    max_idx = group['Montante'].idxmax()
    min_idx = group['Montante'].idxmin()
    return pd.Series({
        'Maior_Salario': group.loc[max_idx, 'Montante'],
        'Posicao_Maior_Salario': group.loc[max_idx, 'Posição'],
        'Inicio_Maior_Salario': group.loc[max_idx, 'Início'],
        'Menor_Salario': group.loc[min_idx, 'Montante'],
        'Posicao_Menor_Salario': group.loc[min_idx, 'Posição'],
        'Inicio_Menor_Salario': group.loc[min_idx, 'Início'],
        'Intervalo_Menor_Maior': (group.loc[max_idx, 'Início'] - group.loc[min_idx, 'Início']).days,
        'Quantidade_Distinta_Montante': group['Montante'].nunique()
    })

# Agrupar e aplicar a função personalizada
grouped = folha.groupby('Nº pessoal').apply(custom_agg).reset_index()

# Renomear a coluna
grouped.rename(columns={'Nº pessoal': 'Nº_pessoal'}, inplace=True)

# Salvar o resultado em um novo arquivo CSV
grouped.to_csv('resultado.csv', index=False)
# %%
