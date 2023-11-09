# ferramentas.py

import os
import pandas as pd
from typing import List, Optional
from unidecode import unidecode
from tqdm import tqdm


class Ferramentas:
    dtype_map = {
        "int64": "INT64",
        "float64": "FLOAT64",
        "object": "STRING",
        "bool": "BOOL",
        "datetime64[ns]": "TIMESTAMP",
        "datetime64": "TIMESTAMP",  # Para cobrir datetime sem frações de segundo
        "timedelta64": "STRING",  # Tipo timedelta é tratado como string no BigQuery
        "category": "STRING",  # Tipos de categoria são tratados como strings no BigQuery
        "complex128": "STRING",  # Tipos complexos são tratados como strings
        "complex64": "STRING",
        "uint8": "INT64",  # Você pode ajustar isso com base nas necessidades
        "uint16": "INT64",
        "uint32": "INT64",
        "uint64": "INT64",
        "int8": "INT64",
        "int16": "INT64",
        "int32": "INT64",
        "int64": "INT64",
        "float16": "FLOAT64",  # Você pode ajustar isso com base nas necessidades
        "float32": "FLOAT64",
        "float64": "FLOAT64",
    }

    def __init__(self, project_id=None):
        self.project_id = project_id

    def convert_to_bigquery_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            try:
                if df[col].dtype.name in self.dtype_map:
                    bq_type = self.dtype_map[df[col].dtype.name]
                    if bq_type == "TIMESTAMP":
                        # Substituir valores NaN por uma data padrão
                        # df[col].fillna("2200-12-31 00:00:00", inplace=True)
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    elif bq_type in ["INT64", "FLOAT64"]:
                        df[col] = df[col].astype(bq_type.lower())
                    elif bq_type == "TIME":
                        # Substituir valores NaN em colunas TIME por '00:00:00'
                        df[col].fillna("00:00:00", inplace=True)
                        # Converter datetime.time em strings no formato 'HH:MM:SS'
                        df[col] = df[col].apply(
                            lambda x: x.strftime("%H:%M:%S")
                            if not pd.isna(x)
                            else "00:00:00"
                        )
            except Exception as e:
                print(
                    f"Erro ao converter a coluna '{col}' de tipo '{df[col].dtype.name}' para '{bq_type}'. Erro: {e}"
                )
                continue
        return df

    def aux_formatar_nome_coluna(self, nome_coluna: str) -> str:
        return (
            unidecode(nome_coluna)  # Remove acentos
            .replace(".", "")  # Remove pontos
            .replace("/", "_")  # Substitui barras por underscores
            .replace("(", "_")  # Substitui parênteses de abertura por underscores
            .replace(")", "_")  # Substitui parênteses de fechamento por underscores
            .replace(" ", "_")  # Substitui espaços por underscores
            .lower()  # Converte para minúsculas
        )

    def formatar_colunas_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Esta é uma função que retira carateres especiais dos nomes das colunas.

        Argumentos:
        - df: dataframe que tera seus rótulos de colunas formatados

        Retorna:
        Um dataframe com colunas formatadas.
        """
        df.columns = [
            self.aux_formatar_nome_coluna(nome)
            for nome in tqdm(df.columns, desc="Formatando colunas")
        ]
        return df

    def consolidar_arquivos(self, caminhos: List[str], sheet_name: str):
        """
        Esta é uma função que lê e consolida arquivos de excel.

        Argumentos:
        - caminhos: list com os caminhos onde estão os arquivos.
        - sheet_name: nome da aba onde estão os dados que deseja importar.

        Retorna:
        Um dataframe com as informações consolidadas.
        """
        dataframes = []

        for caminho in tqdm(caminhos, desc="Lendo e consolidando arquivos"):
            try:
                df = pd.read_excel(caminho, sheet_name=sheet_name).assign(
                    nome_arquivo=os.path.basename(caminho)
                )
                dataframes.append(df)
            except FileNotFoundError:
                print(f"Arquivo não encontrado: {caminho}")
            except Exception as e:
                print(f"Erro ao ler o arquivo {caminho}: {e}")

        # Concatenar todos os dataframes em um único dataframe
        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        else:
            print("Nenhum dataframe válido para concatenar.")
            return None

    def listar_arquivos_em_pasta(self, pasta: str):
        """
        Este método lista os caminhos de todos os arquivos em uma pasta.

        Argumentos:
        - pasta: O caminho da pasta que você deseja listar.

        Retorna:
        Uma lista com os caminhos de todos os arquivos na pasta.
        """
        caminhos = []

        try:
            for nome_arquivo in tqdm(
                os.listdir(pasta), desc="Lendo caminho dos arquivos"
            ):
                caminho_arquivo = os.path.join(pasta, nome_arquivo)
                if os.path.isfile(caminho_arquivo):
                    caminhos.append(caminho_arquivo)
        except FileNotFoundError:
            print(f"Pasta não encontrada: {pasta}")

        return caminhos

    def tratar_coluna_data(self, df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
        """
        Este método substitui os valores NaN ou vazios por '2262-04-10 00:00:00' em colunas específicas de um DataFrame e
        converte os valores para o tipo datetime.

        Argumentos:
        - df: O DataFrame no qual você deseja realizar a substituição.
        - colunas: A lista de nomes das colunas nas quais deseja realizar a substituição.

        Retorna:
        O DataFrame com os valores NaN ou vazios nas colunas especificadas substituídos por '2262-04-10 00:00:00'
        e convertidos para datetime.
        """
        for coluna in colunas:
            df[coluna] = [
                "2262-04-10 00:00:00" if pd.isna(valor) or valor == "" else valor
                for valor in df[coluna]
            ]
            # Convertendo os valores da coluna para datetime após a substituição
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce")
        return df
