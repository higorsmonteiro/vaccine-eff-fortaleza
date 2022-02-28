'''
    Class to process the data regarding the Covid-19 vaccination registration of 
    Fortaleza's population.
'''
import pandas as pd
import numpy as np
import lib.utils as utils
import lib.db_utils as dutils
import datetime as dt
from datetime import timedelta
from collections import defaultdict

class VacineJaTransform:
    def __init__(self, vacineja_fname):
        self.vacineja_fname = vacineja_fname
        self.vacineja_df = None
        self.vacineja_df_joined = None
    
    def load_and_transform(self, return_=True, nrows=None):
        '''
        
        '''
        colnames = ["nome", "cpf", "data_nascimento", "bairro", "cidade", "sexo", "created_at", "nome_mae"]
        self.vacineja_df = dutils.open_vacineja(self.vacineja_fname, colnames=colnames, delimiter=";", nrows=nrows)

        # Apply filters
        # Remove NaN values for "bairro" and "cidade".
        self.vacineja_df = self.vacineja_df.dropna(subset=["nome", "bairro", "cidade", "sexo", "nome_mae"], axis=0)
        # Process the names in case we need to join with a table without the "cpf" field.
        self.vacineja_df["NOME TRATADO"] = self.vacineja_df["nome"].apply(lambda x: utils.replace_string(x))
        self.vacineja_df["NOME MAE TRATADO"] = self.vacineja_df["nome_mae"].apply(lambda x: utils.replace_string(x))
        # Replace "NaN" in "sexo"
        #self.vacineja_df["sexo"] = self.vacineja_df["sexo"].fillna("FALTANDO")
        self.vacineja_df["cpf"] = self.vacineja_df["cpf"].astype(str, errors="ignore")
        self.vacineja_df["cpf"] = self.vacineja_df["cpf"].apply(lambda x: x.split(".")[0] if not pd.isna(x) else np.nan)
        #self.vacineja_df["cpf"] = self.vacineja_df["cpf"].apply(lambda x: f_cpf(x))
        # Remove duplicates on "cpf"
        self.vacineja_df = self.vacineja_df.drop_duplicates(subset="cpf", keep="first")
        # Filter "cidade" field only for "FORTALEZA".
        self.vacineja_df = self.vacineja_df[self.vacineja_df["cidade"]=="FORTALEZA"]
        #print(self.vacineja_df.head())
        self.vacineja_df = self.vacineja_df.drop(["created_at", "cidade"], axis=1)
        
        if return_:
            return self.vacineja_df

    def join_ja_vacinados(self, vacinados_df, cpf_field="cpf", return_=True):
        '''
        
        '''
        if self.vacineja_df is None:
            return -1
        self.vacinejaplus_df = self.vacineja_df.merge(vacinados_df, left_on="cpf", right_on=cpf_field, how="left",
                                                      suffixes=("_vacineja", "_vacinados"))
        self.vacinejaplus_df = self.vacinejaplus_df.drop_duplicates(subset=["cpf"], keep="first")
        #self.vacinejaplus_df = self.vacinejaplus_df.drop(["data nascimento"], axis=1)

        if return_:
            return self.vacinejaplus_df

    
    def cohort_table(self, vacinados_df, cpf_field="cpf_usuario", init_date=dt.date(2021, 1, 21), final_date=dt.date(2021, 6, 30),
                    partial_immunization_days=14, fully_immunization_days=14, return_=True):
        '''
            Using the original table from the class (containing all registrations for vaccination) and the
            database with all the vaccinated people, we create new fields regarding the vaccination status
            of each person before, during and after the cohort. 

            The vaccination status will be:
                - Vaccinated or not.
                - If vaccinated: none, partial or fully.

            Args:
                vacinados_df:
                init_date:
                final_date:
                partial_immunization_days:
                fully_immunization_days:

            Return:
                info_table:
        '''
        if self.vacineja_df is None:
            return -1
        #self.vacineja_df = self.vacineja_df.iloc[:100000,:] ### DELETEEEEEEEEEEEEEEE AFTER READY

        init_cohort = init_date
        final_cohort = final_date

        # Merge databases using the "cpf" information.
        fully = fully_immunization_days
        partial = partial_immunization_days
        subset = ["data_aplicacao_D1", "data_aplicacao_D2"]
        self.vacineja_df_joined = self.vacineja_df.merge(vacinados_df, left_on="cpf", right_on=cpf_field, how="outer", suffixes=("_vacineja", "_vacinados"))
        self.vacineja_df_joined = self.vacineja_df_joined.drop_duplicates(subset=["cpf"], keep="first")
        self.vacineja_df_joined["VACINA STATUS - COORTE"] = self.vacineja_df_joined[subset].apply(lambda x: f_when_vaccine(x, init_cohort, final_cohort), axis=1)
        self.vacineja_df_joined["IMUNIZACAO MAXIMA ATE FIM DA COORTE"] = self.vacineja_df_joined[subset].apply(lambda x: f_immunization(x, init_cohort, final_cohort, partial, fully), axis=1)
        self.vacineja_df_joined = self.vacineja_df_joined.drop(["usuario", "created_at", "cidade"], axis=1)
        
        if return_:
            return self.vacineja_df_joined

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

def f_when_vaccine(x, init_cohort, final_cohort):
    '''
        
    '''
    d1 = x["data_aplicacao_D1"]
    d2 = x["data_aplicacao_D2"]

    if pd.isna(d1): d1 = -1
    else: d1 = d1.date()
    if pd.isna(d2): d2 = -1
    else: d2 = d2.date()

    if d1!=-1 and d1<init_cohort:
        return "D1 ANTES DA COORTE"
    elif d1!=-1 and d1>=init_cohort and d1<=final_cohort:
        return "D1 DURANTE COORTE"
    elif d1!=-1 and d1>final_cohort:
        return "D1 APOS COORTE"
    elif d2!=-1:
        return "INVALIDO - D2 ANTES DA D1"
    else:
        return "NAO VACINADO"

def f_immunization(x, init_cohort, final_cohort, partial=14, fully=14):
    '''
        Classify the maximum immunization status during the cohort. 
    '''
    d1 = x["data_aplicacao_D1"]
    d2 = x["data_aplicacao_D2"]

    if pd.isna(d1): d1 = -1
    else: d1 = d1.date()
    if pd.isna(d2): d2 = -1
    else: d2 = d2.date()

    if d1!=-1 and (d1<init_cohort or d1>final_cohort):
        return "D1 FORA DA COORTE"
    if d1==-1 and d2!=-1:
        return "INVALIDO - D2 ANTES DA D1"

    if d2!=-1 and d2+timedelta(days=fully)<=final_cohort:
        return "TOTALMENTE IMUNIZADO"
    if d2!=-1 and d2+timedelta(days=partial)>final_cohort:
        return "PARCIALMENTE IMUNIZADO"
    if d1!=-1 and d1>=init_cohort and d1+timedelta(days=partial)<=final_cohort:
        return "PARCIALMENTE IMUNIZADO"
    if d1!=-1 and d1<=final_cohort and d1+timedelta(days=partial)>final_cohort:
        return "VACINADO SEM IMUNIZACAO"
    else:
        return "NAO VACINADO"
    