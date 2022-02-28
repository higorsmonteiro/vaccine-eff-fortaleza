'''
    ...
'''
import pandas as pd
import numpy as np
import lib.utils as utils
import lib.db_utils as dutils
import datetime as dt
from datetime import timedelta

class JoinVacinadosTestes:
    def __init__(self, gal_integra_df, vacinejaplus_df):
        '''
            Join operation between the processed data from testes (GAL and IntegraSUS)
            and the processed data from VacineJá (containing info about vaccination for
            who received any vaccine).

            These two databases are resulted from two pair of data analysis code:
                GAL-IntegraSUS <- "TestesGalTransform.py" and "TestesIntegraTransform.py".
                VacineJá <- "VacineJaTransform.py" and "VacinadosTransform.py"

            Args:
                final_testes_df:
                vacinejaplus_df:
        '''
        self.testes_df = gal_integra_df.copy()
        self.vacinejaplus_df = vacinejaplus_df.copy()
        self.final_df = None

    def integrate_vacineja_galintegra(self, return_=True):
        '''
            
        '''
        # Create column for join operation in the database of tests.
        col_testes = ["PACIENTE NOME TRATADO", "Data de Nascimento", "RESULTADO FINAL GAL-INTEGRASUS"]
        self.testes_df = self.testes_df.dropna(subset=col_testes[:-1], how="any", axis=0)
        self.testes_df["COL COMPARACAO"] = self.testes_df[col_testes].apply(lambda x: f_join(x, col_testes), axis=1)

        # Create column for join operation in the database of VacineJá.
        col_vacineja = ["NOME TRATADO_vacineja", "data_nascimento"]
        self.vacinejaplus_df = self.vacinejaplus_df.dropna(subset=col_vacineja, how="any", axis=0)
        self.vacinejaplus_df["COL COMPARACAO"] = self.vacinejaplus_df[col_vacineja].apply(lambda x: f_join(x, col_vacineja), axis=1)

        # Perform left join from VacineJá to GAL-IntegraSUS database.
        # Columns to include in the final table.
        col_subset_testes = ["Data da Solicitação", "Data da Coleta", "RESULTADO FINAL GAL-INTEGRASUS", "COL COMPARACAO"] 
        self.vacinejaplus_df = self.vacinejaplus_df.merge(self.testes_df[col_subset_testes], on="COL COMPARACAO", how="left")
        self.vacinejaplus_df = self.vacinejaplus_df.drop(["COL COMPARACAO"], axis=1)
        self.vacinejaplus_df = self.vacinejaplus_df.drop_duplicates(subset=["cpf"], keep="first")
        self.vacinejaplus_df = self.vacinejaplus_df.rename({"Data da Solicitação": "DATA SOLICITACAO(TESTES)", "Data da Coleta": "DATA COLETA(TESTES)"}, axis=1)

        if return_:
            return self.vacinejaplus_df

def f_join(x, colnames):
    '''
    
    '''
    info = colnames
    date_str = f"{x[info[1]].date().year}-{x[info[1]].date().month}-{x[info[1]].date().day}"
    return "".join([x[info[0]], date_str])
