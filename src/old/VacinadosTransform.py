'''
    Class to process the data regarding the Covid-19 vaccines applied to the 
    Fortaleza's population.
'''
import pandas as pd
import numpy as np
import lib.utils as utils
import lib.db_utils as dutils
import datetime as dt
from datetime import timedelta
from collections import defaultdict

class VacinadosTransform:
    def __init__(self, result_dict):
        '''
            Args:
                result_dict:
                    format -> {
                        "VACINAS APLICADAS": path_and_to_vacinas_aplicadas,
                        "VACINADOS": path_and_to_vacinados
                    }
        '''
        self.applied_vaccines_fname = result_dict["VACINAS APLICADAS"]
        self.vacinados_fname = result_dict["VACINADOS"]
        self.doses_df = None
        self.vacinados_df = None

    def generate_vacinados(self, return_=True, delimiter=";"):
        '''
            Using the databases of applied vaccine doses, generates a final dataset where each
            row is a person with its associated vaccine informations.

            Args:
                return_:
                    Bool to signal whether to return the generated table.
                delimiter:
                    Delimiter character to open the CSV for the applied vaccine doses.
                self.applied_vaccines_fname:
                    Name of the database containing the applied vaccine doses.
            Return:
                self.vacinados_df:
                    Final table generated.
        '''
        colnames = ["usuario", "cpf_usuario", "data_nascimento", "data_aplicacao_ajustada", "vacina", "dose",
                    "fornecedor", "idade_anos", "fx_etaria2", "sexo", "grupo_atendimento", 
                    "grupodeatendimento_old", "id_bairro", "bairro_ajustado", "municipio_residencia", "tipo_atendimento", 
                    "tipo_usuario", "grupoprioritario_novo"]
        self.doses_df = dutils.open_vacinas(self.applied_vaccines_fname, colnames, delimiter=delimiter, encoding="utf-8")

        print("Indexando CPFs ...")
        cpf_ilocs = defaultdict(lambda: [])
        [ cpf_ilocs[cpf].append(index) for index, cpf in enumerate(self.doses_df["cpf_usuario"].tolist())]
        print("Indexando CPFs ... Concluído.")

        # -- Generate vaccinated list --
        print("Gerando tabela de indivíduos vacinados ...")
        self.vacinados_df = utils.generate_vacinados_data(self.doses_df, cpf_ilocs)
        print("Gerando tabela de indivíduos vacinados ... Concluído.")
    
        if return_:
            return self.vacinados_df

    def load_and_transform(self, return_=True, nrows=None):
        '''
        
        '''
        colnames = ["nome", "cpf", "sexo", "bairro id", "vacina", "fornecedor", "data nascimento",
                    "data D1", "data D2", "idade anos", "faixa etaria", "bairro", "tipo atendimento",
                    "tipo usuario", "grupo prioritario"]
        self.vacinados_df = dutils.open_vacinados(self.vacinados_fname, colnames=colnames, delimiter=",", nrows=nrows)

        # Apply filters
        # Remove records with the fields "cpf_usuario" and "data_nascimento" missing.
        self.vacinados_df = self.vacinados_df.dropna(subset=["cpf", "data nascimento","sexo", "vacina"], how="any", axis=0)
        # Format the field of "grupo prioritario" to reduce the rows with compound information.
        self.vacinados_df["grupo prioritario"] = self.vacinados_df["grupo prioritario"].apply(lambda x: x.split("-")[0] if not pd.isna(x) else x)
        # Process the CPF field
        self.vacinados_df["cpf"] = self.vacinados_df["cpf"].astype(str, errors="ignore")
        self.vacinados_df["cpf"] = self.vacinados_df["cpf"].apply(lambda x: x.split(".")[0] if not pd.isna(x) else np.nan)
        # Process the names of each person
        self.vacinados_df["NOME TRATADO"] = self.vacinados_df["nome"].apply(lambda x: utils.replace_string(x))
        # Find all persons having an inconsistent set of vaccination dates.
        subset = ["data D1", "data D2"]
        self.vacinados_df["data aplicacao consistente"] = self.vacinados_df[subset].apply(lambda x: f_d1d2(x["data D1"], x["data D2"]), axis=1)
        if return_:
            return self.vacinados_df

def f_cpf(x):
    '''
    
    '''
    if pd.isna(x):
        return np.nan
    elif type(x)==str:
        if not x.isnumeric():
            return np.nan
    else:
        return str(int(float(x)))

def f_d1d2(x1, x2):
    '''
        Description.

        Args:
            x:
            subset:
    '''
    d1 = x1
    d2 = x2
    if pd.isna(d1) and not pd.isna(d2):
        return "N"
    elif not pd.isna(d1) and not pd.isna(d2):
        d1 = d1.date()
        d2 = d2.date()
        if d1>=d2:
            return "N"
        else:
            return "S"
    else:
        return "S"