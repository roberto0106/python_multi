# In[]
import pandas as pd
import numpy as np
import csv
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import locale

# %%
caminho_folha = r'C:\Users\roberto.flor\Documents\folha.xlsx'
folha = pd.read_excel(caminho_folha,sheet_name='tratada')


# In[]

folha["Início"] = pd.to_datetime(folha["Início"], format="%d/%m/%Y")

folha.sort_values(by="Montante", inplace=True)
folha["Intervalo de Dias"] = folha["Início"].diff().dt.days
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