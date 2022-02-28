'''
    Class to process the data regarding the Covid-19 tests based on the GAL database.
'''
import os
from typing import final
import pandas as pd
import numpy as np
import lib.utils as utils
import lib.db_utils as dutils
import datetime as dt
from datetime import timedelta
from collections import defaultdict

class TestesGalTransform:
    def __init__(self, testes_fname, testes_path=None):
        self.testes_fname = testes_fname
        self.testes_path = testes_path
        self.testes_df = None

    def join_separate_files(self, output_name="testes_gal_JAN_AGO2021.csv"):
        '''
        
        '''
        filelst = ["janeiro_2021.csv", "fevereiro_2021.csv", "marco_2021.csv", "abril_2021.csv", 
                    "maio_2021.csv", "junho_2021.csv", "julho_2021.csv", "agosto_2021.csv"]
        if self.testes_path is not None:
            tb_list = []
            for fname in filelst:
                df = pd.read_csv(os.path.join(self.testes_path, fname), index_col=0, low_memory=False)
                tb_list.append(df)
            final_tb = pd.concat(tb_list)
            final_tb = final_tb.reset_index()
            final_tb = final_tb.rename({"index": "Index tabela original"}, axis=1)
            final_tb.to_csv(os.path.join(self.testes_path, output_name))

    def remove_duplicates(self, return_=True, fields=None):
        '''
            Knowing that one person can have several records for Covid-19 testing, we can
            consider only the unique names. However, we should always verify if one person had
            at least one POSITIVE test result. In case a person has more than one POSITIVE test,
            should we consider the earliest?

            Args:
                return_:
                fields:
            Return:
                invalid_indexes:

        '''
        # In case the tests DataFrame is not loaded and processed yet, return -1.
        if self.testes_df is None:
            return -1
        # Default columns to define the unique names can be changed with the argument "fields".
        if fields is None:
            fields = ["PACIENTE NOME TRATADO", "Sexo", "Data de Nascimento"]
        
        # Store all indexes that should be removed from the original table.
        invalid_indexes = []
        df = self.testes_df
        df = df.reset_index()       
        
        indexes_of_person = defaultdict(lambda: [])
        result_of_person = defaultdict(lambda: [])
        # Each unique person generates a string joining info on name, sex and date of birth.
        # For each one, we generate a key in the hash tables below. The dataframe has no NaN
        # values at this point (processed).
        for j in range(0, df.shape[0]):
            compound_string = "".join([str(df[x].iloc[j]) for x in fields])
            indexes_of_person[compound_string].append(j)
            result_of_person[compound_string].append(df["RESULTADO FINAL"].iloc[j])
        
        # Now select the single testing record that should be representative of a person.
        unique_keys = list(indexes_of_person.keys())
        for key in unique_keys:
            bool_positive = False
            # If there is more than one testing record.
            if len(indexes_of_person[key])>1:
                if "POSITIVO" in result_of_person[key]:
                    for cur_index, table_index in enumerate(indexes_of_person[key]):
                        if result_of_person[cur_index]=="NEGATIVO" or result_of_person[cur_index]=="OUTROS":
                            invalid_indexes.append(table_index)
                        elif result_of_person[cur_index]=="POSITIVO" and bool_positive:
                            invalid_indexes.append(table_index)
                        else:
                            bool_positive = True
                else:
                    # If no "POSITIVO" for the person, then select the latest "NEGATIVO" result.
                    for cur_index, table_index in enumerate(indexes_of_person[key][:-1]):
                        invalid_indexes.append(table_index)
        return invalid_indexes
    
    def load_and_transform(self, return_=True, nrows=None):
        '''
            Open the data and perform the main operations to make the data ready
            for further analysis.

            Args:
                return_:
            Return:
                testes_df:
        '''
        colnames = ["Index tabela original", "Paciente", "Data de Nascimento", "Idade", "Bairro", "Sexo", 
                    "Municipio do Solicitante","Estado do Solicitante", "Data da Solicitação", "Data Notificação Sinan", 
                    "Data da Coleta", "1º Campo Resultado", "Descrição Finalidade", "Agravo Sinan"]
        self.testes_df = dutils.open_testes_gal(self.testes_fname, colnames=colnames, delimiter=",", nrows=nrows)

        # Apply filters
        # Include only "Fortaleza"
        self.testes_df = self.testes_df[self.testes_df["Municipio do Solicitante"]=="FORTALEZA"]
        # Include tests only for "Covid-19"
        self.testes_df = self.testes_df[self.testes_df["Descrição Finalidade"]=="COVID-19"]
        # Ascending order of "Data da Coleta"
        self.testes_df = self.testes_df.sort_values(by="Data da Solicitação", ascending=True)
        # Modify results column
        self.testes_df["RESULTADO FINAL"] = self.testes_df["1º Campo Resultado"].apply(lambda x: f_resultado(x))
        self.testes_df = self.testes_df.drop("1º Campo Resultado", axis=1)
        # Process the "Sexo" column
        self.testes_df["Sexo"] = self.testes_df["Sexo"].apply(lambda x: f_sexo(x))
        # Process names in "Paciente" column
        fields_for_nan = ["Paciente", "Sexo", "Data de Nascimento"]
        self.testes_df = self.testes_df.dropna(subset=fields_for_nan, axis=0)
        self.testes_df["PACIENTE NOME TRATADO"] = self.testes_df["Paciente"].apply(lambda x: utils.replace_string(x))
        # Remove duplicates (Taking into account that we cannot lose positive tests between who got several tests)
        invalid_indexes = self.remove_duplicates()
        self.testes_df = self.testes_df.reset_index()
        self.testes_df = self.testes_df.drop(invalid_indexes)
        
        if return_:
            return self.testes_df

    def compare_integraSUS(self, integra_df, return_=True):
        '''
            Using name, birth date and sex, we compare the negative results of GAL database with the 
            positive results of IntegraSUS. The ones we find in the IntegraSUS are considered positives
            in the new column "RESULTADO FINAL INTEGRASUS" of the GAL table.

            Args:
                integra_df:
                return_:
            Return:
                testes_df
        '''
        gal_cols = ["PACIENTE NOME TRATADO", "Sexo", "Data de Nascimento", "RESULTADO FINAL"]
        integra_cols = ["PACIENTE NOME TRATADO", "sexo_paciente", "data_nascimento", "RESULTADO FINAL"]
        gal_df = self.testes_df[gal_cols]

        # Consider only positive cases in the IntegraSUS.
        integra_df = integra_df[integra_df["RESULTADO FINAL"]=="POSITIVO"]
        key_string_integra = defaultdict(lambda: -1)
        for j in range(0, integra_df.shape[0]):
            nome_integra = integra_df[integra_cols[0]].iloc[j]
            sexo_integra = integra_df[integra_cols[1]].iloc[j]
            nasc_integra = integra_df[integra_cols[2]].iloc[j]
            nasc_integra_dt = nasc_integra.date()

            unique_string = "".join([nome_integra,sexo_integra,f"{nasc_integra_dt.year}-{nasc_integra_dt.month}-{nasc_integra_dt.day}"])
            key_string_integra[unique_string] = integra_df["RESULTADO FINAL"].iloc[j]

        new_col = []
        for j in range(0, gal_df.shape[0]):
            nome_gal = gal_df[gal_cols[0]].iloc[j]
            sexo_gal = gal_df[gal_cols[1]].iloc[j]
            nasc_gal = gal_df[gal_cols[2]].iloc[j]
            nasc_gal_dt = nasc_gal.date()

            unique_string = "".join([nome_gal,sexo_gal,f"{nasc_gal_dt.year}-{nasc_gal_dt.month}-{nasc_gal_dt.day}"])
            if key_string_integra[unique_string]!=-1:
                new_col.append(key_string_integra[unique_string])
            else:
                new_col.append("OUTRO")

        self.testes_df["RESULTADO FINAL INTEGRASUS"] = new_col
        subset_col = ["RESULTADO FINAL", "RESULTADO FINAL INTEGRASUS"]
        f = lambda x: "POSITIVO" if x[subset_col[0]]=="POSITIVO" or x[subset_col[1]]=="POSITIVO" else "OUTRO"
        self.testes_df["RESULTADO FINAL GAL-INTEGRASUS"] = self.testes_df[subset_col].apply(f, axis=1) 
        
        if return_:
            return self.testes_df

    def summary_countings(self):
        '''
            Perform some general countings regarding the COVID-19 tests records.
        '''
        SUMMARY_HASH_XAXIS = {
            "COUNT RESULTADOS": [],
            "COUNT NEGATIVO PER DAY": [],
            "COUNT NEG-OUTROS PER DAY": [],
            "COUNT POSITIVO PER DAY": []
        }
        SUMMARY_HASH_YAXIS = {
            "COUNT RESULTADOS": [],
            "COUNT NEGATIVO PER DAY": [],
            "COUNT NEG-OUTROS PER DAY": [],
            "COUNT POSITIVO PER DAY": []
        }
        # Count how many records there is for each test outcome.
        df = self.testes_df
        count_resultados = df["RESULTADO FINAL"].value_counts().reset_index()
        SUMMARY_HASH_XAXIS["COUNT RESULTADOS"] += count_resultados["index"].tolist()
        SUMMARY_HASH_YAXIS["COUNT RESULTADOS"] += count_resultados["RESULTADO FINAL"].tolist()

        # Count per day for records on each possible outcome.
        negative_count = defaultdict(lambda:0)
        positive_count = defaultdict(lambda:0)
        negative_outros_count = defaultdict(lambda:0)
        
        for j in range(0, self.testes_df.shape[0]):
            coleta_date = self.testes_df["Data da Solicitação"].iloc[j].date()
            if self.testes_df["RESULTADO FINAL"].iloc[j]=="NEGATIVO":
                negative_count[coleta_date] += 1
                negative_outros_count[coleta_date] += 1
            elif self.testes_df["RESULTADO FINAL"].iloc[j]=="POSITIVO":
                positive_count[coleta_date] += 1
            elif self.testes_df["RESULTADO FINAL"].iloc[j]=="OUTROS":
                negative_outros_count[coleta_date] += 1
        
        negative_keys = list(negative_count.keys())
        positive_keys = list(positive_count.keys())
        negative_outro_keys = list(negative_outros_count.keys())
        negative_x = utils.generate_date_list(min(negative_keys), max(negative_keys))
        negative_outros_x = utils.generate_date_list(min(negative_outro_keys), max(negative_outro_keys))
        positive_x = utils.generate_date_list(min(positive_keys), max(positive_keys))

        for key in negative_x:
            SUMMARY_HASH_XAXIS["COUNT NEGATIVO PER DAY"].append(key)
            SUMMARY_HASH_YAXIS["COUNT NEGATIVO PER DAY"].append(negative_count[key])
        for key in negative_outros_x:
            SUMMARY_HASH_XAXIS["COUNT NEG-OUTROS PER DAY"].append(key)
            SUMMARY_HASH_YAXIS["COUNT NEG-OUTROS PER DAY"].append(negative_outros_count[key])
        for key in positive_x:
            SUMMARY_HASH_XAXIS["COUNT POSITIVO PER DAY"].append(key)
            SUMMARY_HASH_YAXIS["COUNT POSITIVO PER DAY"].append(positive_count[key])
        
        return SUMMARY_HASH_XAXIS, SUMMARY_HASH_YAXIS
    

def f_resultado(x):
    res_str = ["Resultado: Não Detectável", "Resultado: Detectável"]
    if pd.isna(x): return "OUTROS"
    if x==res_str[0]:
        return "NEGATIVO"
    elif x==res_str[1]:
        return "POSITIVO"
    else:
        return "OUTROS"

def f_sexo(x):
    if pd.isna(x): return np.nan
    if x=="MASCULINO":
        return "M"
    elif x=="FEMININO":
        return "F"
    else:
        return np.nan