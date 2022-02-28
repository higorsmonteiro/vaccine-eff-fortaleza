'''
    ...
'''
import os
import numpy as np
import pandas as pd
import lib.utils as utils
import lib.db_utils as dutils
import datetime as dt
from collections import defaultdict

class JoinOutcomes:
    def __init__(self, vacineja2plus_df):
        self.vacineja2plus_df = vacineja2plus_df.copy()
        self.obitos_df = None
    
    def join_obitos(self, obitos_path, return_=True, verbose=False):
        '''
        
        '''
        # Create two pseudounique primary keys by concatenating more than one column as strings
        col_obitos = ["nome_tratado", "nome_mae_tratado", "data_nascimento"]
        cols_to_include = ["data_obito", "data_pri_sintomas_nova"]
        self.obitos_df = dutils.open_obitos(obitos_path)
        self.obitos_df = self.obitos_df.dropna(subset=col_obitos, how="any", axis=0)
        if verbose:
            print(f"Dimensão Óbitos: {self.obitos_df.shape}")
        self.obitos_df["COL COMPARACAO1"] = self.obitos_df[col_obitos].apply(lambda x: x[col_obitos[0]]+str(x[col_obitos[2]].date()), axis=1)
        self.obitos_df["COL COMPARACAO2"] = self.obitos_df[col_obitos].apply(lambda x: x[col_obitos[0]]+x[col_obitos[1]], axis=1)

        col_cases = ["NOME TRATADO_vacineja", "NOME MAE TRATADO", "data_nascimento"]
        self.vacineja2plus_df["COL COMPARACAO1"] = self.vacineja2plus_df[col_cases].apply(lambda x: x[col_cases[0]]+str(x[col_cases[2]].date()) if not pd.isna(x[col_cases[0]]) else -1, axis=1)
        self.vacineja2plus_df["COL COMPARACAO2"] = self.vacineja2plus_df[col_cases].apply(lambda x: x[col_cases[0]]+x[col_cases[1]] if not pd.isna(x[col_cases[0]]) else -1, axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.merge(self.obitos_df[["COL COMPARACAO1"]+cols_to_include], on="COL COMPARACAO1", how="left")
        self.vacineja2plus_df = self.vacineja2plus_df.merge(self.obitos_df[["COL COMPARACAO2"]+cols_to_include], on="COL COMPARACAO2", how="left", suffixes=("", "_"))

        sbst = ["data_obito", "data_obito_"]
        self.vacineja2plus_df["data_obito"] = self.vacineja2plus_df[sbst].apply(lambda x: x[sbst[0]] if not pd.isna(x[sbst[0]]) else x[sbst[1]], axis=1)
        sbst = ["data_pri_sintomas_nova", "data_pri_sintomas_nova_"]
        self.vacineja2plus_df["data_pri_sintomas_nova"] = self.vacineja2plus_df[sbst].apply(lambda x: x[sbst[0]] if not pd.isna(x[sbst[0]]) else x[sbst[1]], axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.drop(["data_obito_", "data_pri_sintomas_nova_", "COL COMPARACAO1", "COL COMPARACAO2"], axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.rename({"data_obito": "DATA OBITO", "data_pri_sintomas_nova": "DATA SINTOMAS"}, axis=1)
        if verbose:
            print(f"Óbitos cruzados com sucesso: {self.vacineja2plus_df['DATA OBITO'].notnull().sum()}")

        sbst = ["data D1", "data D2", "DATA OBITO"]
        self.vacineja2plus_df["OBITO INCONSISTENCIA"] = self.vacineja2plus_df[sbst].apply(lambda x: f_vaccination_death(x[sbst[2]], x[sbst[0]], x[sbst[1]]), axis=1)

        if return_:
            return self.vacineja2plus_df

    def join_obitos_old(self, obitos_path, return_=True):
        '''
        
        '''
        # Create pseudounique primary key by merging different columns as strings - Deaths
        col_obitos = ["nome_tratado", "data_nascimento"]
        cols_to_include = ["COL COMPARACAO", "data_obito", "data_pri_sintomas_nova"]
        self.obitos_df = dutils.open_obitos(obitos_path)
        
        self.obitos_df = self.obitos_df.dropna(subset=col_obitos, how="any", axis=0)
        self.obitos_df["COL COMPARACAO"] = self.obitos_df[col_obitos].apply(lambda x: x[col_obitos[0]]+str(x[col_obitos[1]].date()), axis=1)
        
        col_cases = ["NOME TRATADO_vacineja", "data_nascimento"]
        self.vacineja2plus_df["COL COMPARACAO"] = self.vacineja2plus_df[col_cases].apply(lambda x: x[col_cases[0]]+str(x[col_cases[1]].date()) if not pd.isna(x[col_cases[0]]) else -1, axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.merge(self.obitos_df[cols_to_include], on="COL COMPARACAO", how="left")
        self.vacineja2plus_df = self.vacineja2plus_df.drop("COL COMPARACAO", axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.rename({"data_obito": "DATA OBITO", "data_pri_sintomas_nova": "DATA SINTOMAS"}, axis=1)

        # Verify if there are inconsistencies with the date of death and dates of vaccination (D1 and D2)
        sbst = ["data D1", "data D2", "DATA OBITO"]
        self.vacineja2plus_df["OBITO INCONSISTENCIA"] = self.vacineja2plus_df[sbst].apply(lambda x: f_vaccination_death(x[sbst[2]], x[sbst[0]], x[sbst[1]]), axis=1)

        if return_:
            return self.vacineja2plus_df

    def join_hospitalization(self, hospital_path, return_=True, verbose=True):
        '''
        
        '''
        col_hosp = ["nome_tratado_hosp", "nome_mae_tratado", "DT_NASC"]
        cols_to_load = ["DT_INTERNA", "DT_SIN_PRI", "NM_MAE_PAC", "DT_NASC", "NM_PACIENT", "ID_UNIDADE", "CS_SEXO", "NM_BAIRRO", "FEBRE", "TOSSE", 
                        "GARGANTA", "DISPNEIA", "DIARREIA", "VOMITO", "SATURACAO", "OUTRO_SIN", "FATOR_RISC", "EVOLUCAO", "DT_ENCERRA", "UTI"]
        self.hosp_df = dutils.open_hospitalizacao(hospital_path, colnames=cols_to_load)

        self.hosp_df = self.hosp_df.dropna(subset=col_hosp, how="any", axis=0)
        if verbose:
            print(f"Dimensão Hospitalizados: {self.hosp_df.shape}")

        col_cases = ["NOME TRATADO_vacineja", "NOME MAE TRATADO", "data_nascimento"]
        self.vacineja2plus_df["COL COMPARACAO1"] = self.vacineja2plus_df[col_cases].apply(lambda x: x[col_cases[0]]+str(x[col_cases[2]].date()) if not pd.isna(x[col_cases[0]]) else -1, axis=1)
        self.vacineja2plus_df["COL COMPARACAO2"] = self.vacineja2plus_df[col_cases].apply(lambda x: x[col_cases[0]]+x[col_cases[1]] if not pd.isna(x[col_cases[0]]) else -1, axis=1)
        self.hosp_df["COL COMPARACAO1"] = self.hosp_df[col_hosp].apply(lambda x: x[col_hosp[0]]+str(x[col_hosp[2]].date()), axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.merge(self.hosp_df, on="COL COMPARACAO1", how="left")
        self.hosp_df["COL COMPARACAO2"] = self.hosp_df[col_hosp].apply(lambda x: x[col_hosp[0]]+x[col_hosp[1]], axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.merge(self.hosp_df[["COL COMPARACAO2", "DT_INTERNA"]], on="COL COMPARACAO2", how="left", suffixes=("", "_"))

        sbst = ["DT_INTERNA", "DT_INTERNA_"]
        self.vacineja2plus_df["DT_INTERNA"] = self.vacineja2plus_df[sbst].apply(lambda x: x[sbst[0]] if not pd.isna(x[sbst[0]]) else x[sbst[1]], axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.drop(["DT_INTERNA_", "NM_MAE_PAC", "COL COMPARACAO1", "COL COMPARACAO2", "DT_NASC", "nome_tratado_hosp", "NM_PACIENT"], axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.rename({"DT_INTERNA": "DATA HOSPITALIZACAO", "DT_SIN_PRI":"DATA SINTOMAS (HOSPITALIZACAO)"}, axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.drop_duplicates(subset=["cpf"], keep="first")
        if verbose:
            print(f"Hospitalizados cruzados com sucesso: {self.vacineja2plus_df['DATA HOSPITALIZACAO'].notnull().sum()}")

        if return_:
            return self.vacineja2plus_df

    def join_hospitalization_old(self, hospital_path, return_=True):
        '''
        
        '''
        # Create pseudounique primary key by merging different columns as strings - Deaths
        col_hosp = ["nome_tratado_hosp", "DT_NASC"]
        cols_to_load = ["DT_INTERNA", "DT_SIN_PRI", "NM_MAE_PAC", "DT_NASC", "NM_PACIENT", "ID_UNIDADE", "CS_SEXO", "NM_BAIRRO", "FEBRE", "TOSSE", 
                        "GARGANTA", "DISPNEIA", "DIARREIA", "VOMITO", "SATURACAO", "OUTRO_SIN", "FATOR_RISC", "EVOLUCAO", "DT_ENCERRA", "UTI"]
        self.hosp_df = dutils.open_hospitalizacao(hospital_path, colnames=cols_to_load)

        self.hosp_df = self.hosp_df.dropna(subset=col_hosp, how="any", axis=0)
        self.hosp_df["COL COMPARACAO"] = self.hosp_df[col_hosp].apply(lambda x: x[col_hosp[0]]+str(x[col_hosp[1]].date()), axis=1)

        col_cases = ["NOME TRATADO_vacineja", "data_nascimento"]
        self.vacineja2plus_df["COL COMPARACAO"] = self.vacineja2plus_df[col_cases].apply(lambda x: x[col_cases[0]]+str(x[col_cases[1]].date()) if not pd.isna(x[col_cases[0]]) else -1, axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.merge(self.hosp_df, on="COL COMPARACAO", how="left")
        self.vacineja2plus_df = self.vacineja2plus_df.drop("COL COMPARACAO", axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.rename({"DT_INTERNA": "DATA HOSPITALIZACAO", "DT_SIN_PRI":"DATA SINTOMAS (HOSPITALIZACAO)"}, axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.drop(["DT_NASC", "nome_tratado_hosp", "NM_PACIENT"], axis=1)
        self.vacineja2plus_df = self.vacineja2plus_df.drop_duplicates(subset=["cpf"], keep="first")

        if return_:
            return self.vacineja2plus_df

    def rename_columns(self, return_=True):
        '''
        
        '''
        self.vacineja2plus_df = self.vacineja2plus_df.rename({"nome_vacineja": "NOME(VACINEJA)", "cpf": "CPF", 
                                                "data_nascimento": "DATA NASCIMENTO(VACINEJA)", "bairro_vacineja": "BAIRRO(VACINEJA)",
                                                "NOME TRATADO_vacineja": "NOME TRATADO(VACINEJA)", "nome_vacinados": "NOME(VACINADOS)",
                                                "sexo_vacineja":"SEXO(VACINEJA)", "sexo_vacinados" : "SEXO(VACINADOS)",
                                                "data D1": "DATA D1(VACINADOS)", "data D2": "DATA D2(VACINADOS)", "vacina": "VACINA(VACINADOS)",
                                                "fornecedor":"FORNECEDOR(VACINADOS)", "idade anos": "IDADE", "faixa etaria":"FAIXA ETARIA",
                                                "bairro_vacinados": "BAIRRO(VACINADOS)", "tipo atendimento": "TIPO ATENDIMENTO",
                                                "tipo usuario": "TIPO USUARIO", "grupo prioritario": "GRUPO PRIORITARIO",
                                                "NOME TRATADO_vacinados": "NOME TRATADO(VACINADOS)", "bairro id": "BAIRRO ID", 
                                                "data aplicacao consistente":"DATA VACINA CONSISTENCIA", "nome_mae": "NOME MAE(VACINEJA)",
                                                "NM_BAIRRO": "BAIRRO(HOSPITALIZACAO)", "OUTRO_SIN": "OUTRO SINTOMA",
                                                "FATOR_RISC": "FATOR RISCO", "DT_ENCERRA": "DATA ENCERRAMENTO",
                                                "data nascimento": "DATA NASCIMENTO(VACINADOS)"}, axis=1)
        if return_:
            return self.vacineja2plus_df

def find_pair(cur_date, cpf_case, age_case, sex_case, control_reservoir, control_used, control_d1_date, age_interval=1):
    '''
    
    '''
    eligible_controls = []
    for j in np.arange(age_case-age_interval, age_case+age_interval+1, 1):
        eligible_controls += control_reservoir[(j, sex_case)]
    
    for cpf_control in eligible_controls:
        if not control_used[cpf_control]:
            if control_d1_date[cpf_control]==-1 or control_d1_date[cpf_control]>cur_date:
                control_used[cpf_control] = True
                return cpf_control
    return -1

def f_vaccination_death(death_date, d1_date, d2_date):
    '''
    
    '''
    if pd.isna(death_date):
        return "N"
    else:
        death_date = death_date.date()
        if not pd.isna(d2_date):
            if d2_date.date()>death_date:
                return "S"
            elif not pd.isna(d1_date) and d1_date.date()>death_date:
                return "S"
            return "N"
        else:
            return "N"        


        
        


        