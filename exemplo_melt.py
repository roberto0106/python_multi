import pandas as pd
import xlsxwriter
from datetime import datetime
from pandas import Timestamp


# Read the Excel file
df = pd.read_excel('files/TabelaDebitosJudiciais.xlsx')
compensados = pd.read_excel('files/SupplierLineItems.xlsx')
compensados = compensados.loc[compensados['Tipo lçto.contábil']=='ZP']

data_referencia = datetime(2020, 1, 1)

# Melt the data frame
df = df.melt(id_vars=['MÊS'], value_name='FATOR', var_name='ANO')
df['REFERENCIA'] = df['MÊS'].str.lower()+'-'+df['ANO']
df['REFERENCIA'] = df['REFERENCIA'].str.replace(' ', '')
df.dropna(subset='FATOR', inplace=True)

df_auxiliar = df.sort_values(by='REFERENCIA', ascending=False)
valor_da_maior_data = df_auxiliar.iloc[0]['FATOR']


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
def formatar_data_a(data_str):
    for pt, en in meses.items():
        data_str = data_str.replace(pt, en)
    return datetime.strptime(data_str, '%b-%Y')

# Aplique a formatação da data à coluna 'Mês_Ano' e crie uma nova coluna 'Data_Formatada'
df['DATA_FORMATADA'] = df['REFERENCIA'].apply(formatar_data_a)

# Converta a coluna 'Dt.lnçmto.cont.' para o tipo datetime, se ainda não estiver no formato correto
compensados['Dt.lnçmto.cont.'] = pd.to_datetime(compensados['Dt.lnçmto.cont.'])

# Crie uma nova coluna 'DATA_FORMATADA' com o primeiro dia do mês
compensados['DATA_FORMATADA'] = compensados['Dt.lnçmto.cont.'].dt.to_period('M').dt.to_timestamp()

# Ordene o DataFrame com base na coluna 'MinhaColuna' em ordem crescente
df = df.sort_values(by='DATA_FORMATADA', ascending=True)

# Reindexe o DataFrame para reorganizar os índices após a classificação
df = df.reset_index(drop=True)

# Aplique a condição de filtro para obter valores maiores do que '2023-01-01'
df_filtrado = df[df['DATA_FORMATADA'] > data_referencia]
df_final = df_filtrado.merge(compensados, how='left',left_on='DATA_FORMATADA',right_on='DATA_FORMATADA')

df_final['PRE_CALCULO'] = (df_final['Montante (ME)']/df_final['FATOR'])*valor_da_maior_data
df_final['VALOR_FINAL'] = df_final['PRE_CALCULO'] /12

total = df_final['VALOR_FINAL'].sum()

df_auxiliar_aviso_previo = df_final.sort_values(by='DATA_FORMATADA', ascending=False)
ultimos_tres_meses = df_auxiliar_aviso_previo.head(3)

aviso_previo = ultimos_tres_meses['PRE_CALCULO'].sum()
total_indenizacoes = aviso_previo + total
ir = total_indenizacoes * 0.15
total_a_receber = total_indenizacoes-ir

resultado = df_final[['DATA_FORMATADA','Montante (ME)','FATOR','PRE_CALCULO','VALOR_FINAL']]



# In[]

# Crie um objeto ExcelWriter
with pd.ExcelWriter('files/TabelaFinal.xlsx', engine='xlsxwriter') as excel_writer:
    # Crie a planilha 'Sheet1'
    worksheet = excel_writer.book.add_worksheet('Sheet1')

    # formatando
    six_decimal_format = excel_writer.book.add_format({'num_format': '0.000000'})

    cell_format = excel_writer.book.add_format({
        'bg_color': 'gray',
        'font_color': 'black',
        })

    ir_format = excel_writer.book.add_format({
        'bg_color': 'gray',
        'font_color': 'red',
        })
    
     # Lendo o tamanho do dataframe
    nrow = resultado.shape[0]+6
    
    # Apply the format to the 'FATOR' column
    worksheet.set_column('C:C', None, six_decimal_format)

    # Crie DataFrames separados para cada coluna de cabeçalho

    worksheet.merge_range('A1:D1','Cálculo de Indenização')
    worksheet.merge_range('A2:D2','Data de Admissão')
    worksheet.merge_range('A3:D3','Data Base')
    
    worksheet.write(nrow, 0, 'Total')
    worksheet.write(nrow, 4, total)
    
    worksheet.write(nrow+2, 0, 'Aviso_previo')
    worksheet.write(nrow+2, 4, aviso_previo)
    
    worksheet.write(nrow+4, 0, 'Total de Indenizacoes')
    worksheet.write(nrow+4, 4, total_indenizacoes)
    
    worksheet.write(nrow+6, 0, 'I.R')
    worksheet.write(nrow+6, 4, ir)
    
    worksheet.write(nrow+8, 0, 'Total a Receber')
    worksheet.write(nrow+8, 4, total_a_receber)

    # Adicione o DataFrame existente abaixo dos cabeçalhos
    resultado.to_excel(excel_writer, index=False, sheet_name='Sheet1', startrow=4, startcol=0)
# %%
