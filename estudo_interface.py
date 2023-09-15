import tkinter as tk
from tkinter import filedialog
from tkinter import simpledialog
import pandas as pd
import xlsxwriter
from datetime import datetime

# Dicionário de correspondência entre meses em português e em inglês
meses = {
    'jan': 'Jan',
    'fev': 'Feb',
    'mar': 'Mar',
    'abr': 'Apr',
    'mai': 'May',
    'jun': 'Jun',
    'jul': 'Jul',
    'ago': 'Aug',
    'set': 'Sep',
    'out': 'Oct',
    'nov': 'Nov',
    'dez': 'Dec'
}

# Função para formatar a data
def formatar_data(data_str):
    for pt, en in meses.items():
        data_str = data_str.replace(pt, en)
    return datetime.strptime(data_str, '%b-%Y')

# Função para processar o arquivo Excel
def processar_arquivo():
    file_path = filedialog.askopenfilename(filetypes=[("Arquivos Excel", "*.xlsx")])
    
    if file_path:
        try:
            df = pd.read_excel(file_path)
            
            # Solicita ao usuário a data de referência
            data_referencia_str = simpledialog.askstring("Data de Referência", "Digite a data de referência (formato: YYYY-MM-DD):")
            data_referencia = datetime.strptime(data_referencia_str, '%Y-%m-%d')
            
            df = df.melt(id_vars=['MÊS'], value_name='FATOR', var_name='ANO')
            df['REFERENCIA'] = df['MÊS'].str.lower()+'-'+df['ANO']
            df['REFERENCIA'] = df['REFERENCIA'].str.replace(' ', '')
            
            df['DATA_FORMATADA'] = df['REFERENCIA'].apply(formatar_data)
            
            df = df.sort_values(by='DATA_FORMATADA', ascending=True)
            df = df.reset_index(drop=True)
            
            df_filtrado = df[df['DATA_FORMATADA'] > data_referencia]
            
            save_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Arquivos Excel", "*.xlsx")])
            
            if save_path:
                with pd.ExcelWriter(save_path, engine='xlsxwriter') as excel_writer:
                    df.to_excel(excel_writer, index=False, sheet_name='Sheet1')
                    
                    workbook = excel_writer.book
                    worksheet = excel_writer.sheets['Sheet1']
                    
                    six_decimal_format = workbook.add_format({'num_format': '0.000000'})
                    worksheet.set_column('C:C', None, six_decimal_format)
                    
                tk.messagebox.showinfo("Sucesso", "Arquivo processado e salvo com sucesso!")
        except Exception as e:
            tk.messagebox.showerror("Erro", f"Ocorreu um erro ao processar o arquivo: {str(e)}")

# Criação da janela principal com o título "Distrato"
root = tk.Tk()
root.title("Distrato")
root.title("Distrato")

# Criação do menu
menu = tk.Menu(root)
root.config(menu=menu)

# Criação do submenu "Arquivo"
file_menu = tk.Menu(menu)
menu.add_cascade(label="Arquivo", menu=file_menu)
file_menu.add_command(label="Processar Arquivo", command=processar_arquivo)
file_menu.add_separator()
file_menu.add_command(label="Sair", command=root.quit)

# Criação do submenu "Ajuda"
help_menu = tk.Menu(menu)
menu.add_cascade(label="Ajuda", menu=help_menu)
help_menu.add_command(label="Sobre", command=lambda: tk.messagebox.showinfo("Sobre", "Distrato\n\nDistrato é um programa para processar arquivos Excel de fatores de distrato.\n\nDesenvolvido por <NAME>"))

# Criação do botão "Processar Arquivo"
processar_arquivo_button = tk.Button(root, text="Processar Arquivo", command=processar_arquivo)
processar_arquivo_button.grid(row=0, column=0, padx=10, pady=10)

# Criação do botão "Sair"
sair_button = tk.Button(root, text="Sair", command=root.quit)
sair_button.grid(row=0, column=1, padx=10, pady=10)

