'''
    Class to process the data regarding the Covid-19 tests based on the IntegraSUS database.
'''
import pandas as pd
import numpy as np
import lib.utils as utils
import lib.db_utils as dutils
import datetime as dt
from datetime import timedelta
from collections import defaultdict

class TestesIntegraTransform:
    def __init__(self, testes_fname):
        self.testes_fname = testes_fname
        self.testes_df = None

    def remove_duplicates():
        '''
        
        '''
        pass

    def load_and_transform(self, return_=True):
        '''
        
        '''
        colnames = ["nome_paciente", "data_nascimento", "idade_anos", "bairro_ajustado", "sexo_paciente",
                    "municipio_paciente", "fx_etaria", "data_solicitacao_exame", "data_coleta_exame",
                    "data_resultado_exame", "resultado_final_exame"]
        self.testes_df = dutils.open_testes_integra(self.testes_fname, colnames=colnames, delimiter=",")

        # Apply filters
        # Process the "Sexo" column
        self.testes_df["sexo_paciente"] = self.testes_df["sexo_paciente"].apply(lambda x: f_sexo(x))
        # Ascending order of "Data da Solicitação"
        self.testes_df = self.testes_df.sort_values(by="data_solicitacao_exame", ascending=True)
        # Modify results column
        self.testes_df["RESULTADO FINAL"] = self.testes_df["resultado_final_exame"].apply(lambda x: f_resultado(x))
        # Consider only the positive cases.
        self.testes_df = self.testes_df[self.testes_df["RESULTADO FINAL"]=="POSITIVO"]
        # Process names in "Paciente" column
        #fields_for_nan = ["Paciente", "Sexo", "Data de Nascimento"]
        #self.testes_df = self.testes_df.dropna(subset=fields_for_nan, axis=0)
        self.testes_df["PACIENTE NOME TRATADO"] = self.testes_df["nome_paciente"].apply(lambda x: utils.replace_string(x))
        self.testes_df = self.testes_df.dropna(subset=["PACIENTE NOME TRATADO", "data_nascimento", "sexo_paciente"], how="any")

        # remove duplicates by name?

        if return_:
            return self.testes_df

def f_sexo(x):
    if pd.isna(x): return np.nan
    if x=="MASC":
        return "M"
    elif x=="FEM":
        return "F"
    else:
        return np.nan

def f_resultado(x):
    if pd.isna(x): return "OUTROS"
    if x=="POSITIVO":
        return "POSITIVO"
    else:
        return "OUTROS"
