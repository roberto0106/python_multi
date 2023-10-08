import pandas as pd
import numpy as np
from unidecode import unidecode
import pandas_gbq
from google.cloud import bigquery
import tkinter as tk
from tkinter import filedialog

# Função para retirar acentos, substituir espaços por underscores, remover pontos e converter para minúsculas
def formatar_nome_coluna(nome_coluna):
    nome_sem_acentos = unidecode(nome_coluna)   # Remove acentos
    nome_sem_pontos = nome_sem_acentos.replace('.', '')  # Remove pontos
    nome_sem_barras = nome_sem_pontos.replace('/', '_')  # Remove pontos
    nome_sem_abertura_parenteses = nome_sem_barras.replace('(', '_')  # Remove pontos
    nome_sem_fechamento_parenteses = nome_sem_abertura_parenteses.replace(')', '_')  # Remove pontos
    nome_formatado = nome_sem_fechamento_parenteses.replace(' ', '_')  # Substitui espaços por underscores
    nome_formatado_minusculo = nome_formatado.lower()  # Converte para minúsculas
    return nome_formatado_minusculo

def carregar_arquivo():
    caminho_arquivo = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
    if caminho_arquivo:
        try:
            arquivo = pd.read_excel(caminho_arquivo)
            arquivo.columns = [formatar_nome_coluna(nome) for nome in arquivo.columns]
            
            # Obtém o dataset e nome da tabela inseridos pelo usuário
            dataset = dataset_entry.get()
            nome_tabela = nome_tabela_entry.get()
            
            projeto = 'rh-analytics-397518'
            nome_tabela_completo = f'{dataset}.{nome_tabela}'
            pandas_gbq.to_gbq(arquivo, nome_tabela_completo, project_id=projeto, if_exists='replace')
            
            status_label.config(text="Arquivo carregado com sucesso.")
        except Exception as e:
            status_label.config(text=f"Erro ao carregar o arquivo: {str(e)}")

# Cria a janela principal
root = tk.Tk()
root.title("Carregar Dados para o BigQuery")

# Label e campo de entrada para dataset
dataset_label = tk.Label(root, text="Dataset:")
dataset_label.pack()
dataset_entry = tk.Entry(root)
dataset_entry.pack()

# Label e campo de entrada para nome da tabela
nome_tabela_label = tk.Label(root, text="Nome da Tabela:")
nome_tabela_label.pack()
nome_tabela_entry = tk.Entry(root)
nome_tabela_entry.pack()

# Botão para carregar arquivo
load_button = tk.Button(root, text="Carregar Arquivo Excel", command=carregar_arquivo)
load_button.pack(pady=20)

# Label para exibir status
status_label = tk.Label(root, text="", fg="green")
status_label.pack()

root.mainloop()
