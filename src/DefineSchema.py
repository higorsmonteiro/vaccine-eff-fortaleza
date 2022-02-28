'''
    After linkage is performed and databases are processed, create the final schema database as the
    input of the matching process.
'''
import os
import json
import numpy as np
import pandas as pd
from collections import defaultdict

class DefineSchema:
    def __init__(self, base_folder):
        '''
            Using the path to the files of processed databases and linkage data, create the final schema
            for the matching database.
        '''
        self.base_folder = base_folder

    def load_processed_data(self, folder=os.path.join("PARQUET_TRANSFORMED")):
        '''
            Description.
        '''
        self.vacineja_df = pd.read_parquet(os.path.join(self.base_folder, folder, "VACINEJA.parquet"))
        self.vacinados_df = pd.read_parquet(os.path.join(self.base_folder, folder, "VACINADOS.parquet"))
        self.tests_df = pd.read_parquet(os.path.join(self.base_folder, folder, "INTEGRASUS.parquet"))
        self.obito_covid_df = pd.read_parquet(os.path.join(self.base_folder, folder, "OBITO_COVID.parquet"))
        self.obito_cartorio_df = pd.read_parquet(os.path.join(self.base_folder, folder, "OBITO_CARTORIO.parquet"))
        self.sivep_df = pd.read_parquet(os.path.join(self.base_folder, folder, "SIVEP-GRIPE", "SIVEP_COVID19_2021.parquet"))
    
    def load_linked_data(self, folder=os.path.join("output", "data", "LINKAGE")):
        '''
            Description.
        '''
        self.vacineja_vacinados_df = pd.read_parquet(os.path.join(folder, "VACINEJA_VACINADOS.parquet"))
        self.vacineja_tests_df = pd.read_parquet(os.path.join(folder, "VACINEJA_INTEGRASUS.parquet"))
        self.vacineja_covid_df = pd.read_parquet(os.path.join(folder, "VACINEJA_OBITO_COVID.parquet"))
        self.vacineja_cartorio_df = pd.read_parquet(os.path.join(folder, "VACINEJA_OBITO_CARTORIO.parquet"))
        self.vacineja_sivep_df = pd.read_parquet(os.path.join(self.base_folder, "PARQUET_TRANSFORMED", "SIVEP-GRIPE", "SIVEP_VACINEJA_2021.parquet"))

    def join_keys(self):
        '''
            Join the primary keys to the processed 'Vacine Já' database.
        '''
        found_vacinados = self.vacineja_vacinados_df[pd.notna(self.vacineja_vacinados_df["cpf(VACINEJA)"])]
        found_tests = self.vacineja_tests_df[pd.notna(self.vacineja_tests_df["cpf(VACINEJA)"])]
        found_covid = self.vacineja_covid_df[pd.notna(self.vacineja_covid_df["cpf(VACINEJA)"])]
        found_cartorio = self.vacineja_cartorio_df[pd.notna(self.vacineja_cartorio_df["cpf(VACINEJA)"])]
        found_sivep = self.vacineja_sivep_df[pd.notna(self.vacineja_sivep_df["cpf(VACINEJA)"])]
        
        to_vacinados = defaultdict(lambda: np.nan, zip(found_vacinados["cpf(VACINEJA)"], found_vacinados["cpf(VACINADOS)"]))
        to_tests = defaultdict(lambda: np.nan, zip(found_tests["cpf(VACINEJA)"], found_tests["id"]))
        to_covid = defaultdict(lambda: np.nan, zip(found_covid["cpf(VACINEJA)"], found_covid["ORDEM(OBITO COVID)"]))
        to_cart = defaultdict(lambda: np.nan, zip(found_cartorio["cpf(VACINEJA)"], found_cartorio["cpf(CARTORIOS)"]))
        to_sivep = defaultdict(lambda: np.nan, zip(found_sivep["cpf(VACINEJA)"], found_sivep["SIVEP"]))

        pos_tests = self.tests_df[self.tests_df["resultado_final_exame"]=="POSITIVO"]
        id_coleta = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_coleta_exame"]))
        id_solicitacao = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_coleta_exame"]))

        # --> Join
        self.vacineja_df["cpf LINKAGE VACINADOS"] = self.vacineja_df["cpf"].apply(lambda x: to_vacinados[x])
        self.vacineja_df["id LINKAGE INTEGRASUS"] = self.vacineja_df["cpf"].apply(lambda x: to_tests[x])
        self.vacineja_df["ordem LINKAGE OBITO COVID"] = self.vacineja_df["cpf"].apply(lambda x: to_covid[x])
        self.vacineja_df["cpf LINKAGE CARTORIOS"] = self.vacineja_df["cpf"].apply(lambda x: to_cart[x])
        self.vacineja_df["primary key LINKAGE SIVEP"] = self.vacineja_df["cpf"].apply(lambda x: to_sivep[x])
        
        self.tests_df = self.tests_df.set_index("id")
        self.vacineja_df["TESTE POSITIVO ANTES COORTE"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: select_indexes(self.tests_df, x) if type(x)!=float else "NAO")
        self.vacineja_df["POSITIVOS COLETA DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [id_coleta[cur_id] for cur_id in x] if type(x)!=float else np.nan)
        self.vacineja_df["POSITIVOS SOLICITACAO DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [id_solicitacao[cur_id] for cur_id in x] if type(x)!=float else np.nan)
        
    def create_final_schema(self, return_=True):
        '''
            Description.
        '''
        col_vacinados = ["cpf(VACINADOS)", "vacina(VACINADOS)", "data D1(VACINADOS)", "data D2(VACINADOS)", "data D3(VACINADOS)", 
                         "data D4(VACINADOS)"]
        self.vacineja_df = self.vacineja_df.merge(self.vacinados_df[col_vacinados].dropna(subset=["cpf(VACINADOS)"], axis=0), left_on="cpf LINKAGE VACINADOS",
                                                  right_on="cpf(VACINADOS)", how="left").drop("cpf(VACINADOS)", axis=1)
        col_covid = ["ORDEM(OBITO COVID)", "numerodo", "data_pri_sintomas_nova(OBITO COVID)", "data_obito(OBITO COVID)"]
        self.vacineja_df = self.vacineja_df.merge(self.obito_covid_df[col_covid].dropna(subset=["ORDEM(OBITO COVID)"], axis=0), left_on="ordem LINKAGE OBITO COVID",
                                                  right_on="ORDEM(OBITO COVID)", how="left")
        col_cart = ["cpf(CARTORIOS)", "data falecimento(CARTORIOS)", "do(CARTORIOS)"]
        self.vacineja_df = self.vacineja_df.merge(self.obito_cartorio_df[col_cart].dropna(subset=["cpf(CARTORIOS)"], axis=0), left_on="cpf LINKAGE CARTORIOS", 
                                                  right_on="cpf(CARTORIOS)", how="left").drop("cpf(CARTORIOS)", axis=1)

        # --> Join SIVEP COLUMNS
        col_sivep = ["PRIMARY_KEY", "DT_NOTIFIC", "DT_INTERNA", "EVOLUCAO", "DT_EVOLUCA"]
        agg_sivep = self.sivep_df[col_sivep].astype(str).groupby("PRIMARY_KEY").agg(";".join).reset_index()
        self.vacineja_df = self.vacineja_df.merge(agg_sivep.dropna(subset=["PRIMARY_KEY"], axis=0), left_on="primary key LINKAGE SIVEP",
                                                                              right_on="PRIMARY_KEY", how="left").drop("PRIMARY_KEY", axis=1)
        f = lambda x: [pd.to_datetime(xx) for xx in x.split(";")] if pd.notna(x) else np.nan
        self.vacineja_df["DT_NOTIFIC"] = self.vacineja_df["DT_NOTIFIC"].apply(f)
        self.vacineja_df["DT_INTERNA"] = self.vacineja_df["DT_INTERNA"].apply(f)
        self.vacineja_df["DT_EVOLUCA"] = self.vacineja_df["DT_EVOLUCA"].apply(f)
        self.vacineja_df["EVOLUCAO"] = self.vacineja_df["EVOLUCAO"].apply(lambda x: x.split(";") if pd.notna(x) else np.nan)
        
        # --> Verify inconsistencies in tests dates
        col_tests = ["POSITIVOS COLETA DATA", "POSITIVOS SOLICITACAO DATA", "data_obito(OBITO COVID)"]
        f = lambda x: np.any([date_>=x[col_tests[2]] for date_ in  x[col_tests[0]] if not pd.isna(date_)]) if np.any(pd.notna(x[col_tests[0]])) else False
        self.vacineja_df["COLETA APOS OBITO"] = self.vacineja_df[col_tests].apply(f, axis=1)
        f = lambda x: np.any([date_>=x[col_tests[2]] for date_ in  x[col_tests[1]] if not pd.isna(date_)]) if np.any(pd.notna(x[col_tests[1]])) else False
        self.vacineja_df["SOLICITACAO APOS OBITO"] = self.vacineja_df[col_tests].apply(f, axis=1)

        #self.vacineja_df = self.vacineja_df.drop(["ordem LINKAGE OBITO COVID", "id LINKAGE INTEGRASUS", "cpf LINKAGE CARTORIOS", "cpf LINKAGE VACINADOS", "created_at"], axis=1)
        self.vacineja_df = self.vacineja_df.drop(["created_at"], axis=1)
        self.vacineja_df = self.vacineja_df.rename({"nome": "NOME", "nome_mae": "NOME MAE", "cpf": "CPF", "cns": "CNS", "data_nascimento": "DATA NASCIMENTO",
                                                    "cep": "CEP", "bairro": "BAIRRO", "sexo": "SEXO", "situacao": "SITUACAO VACINEJA",
                                                    "vacina(VACINADOS)": "VACINA APLICADA", "data D1(VACINADOS)": "DATA D1", 
                                                    "data D2(VACINADOS)": "DATA D2", "data D3(VACINADOS)": "DATA D3", "data D4(VACINADOS)": "DATA D4",
                                                    "numerodo": "NUMERODO", "data_pri_sintomas_nova(OBITO COVID)": "DATA PRI SINTOMAS (COVID)", 
                                                    "data_obito(OBITO COVID)": "DATA OBITO", "data falecimento(CARTORIOS)": "DATA FALECIMENTO(CARTORIOS)",
                                                    "do(CARTORIOS)": "NUMERODO(CARTORIOS)", "DT_NOTIFIC": "DATA NOTIFICACAO SIVEP", 
                                                    "DT_INTERNA": "DATA INTERNACAO", "DT_EVOLUCA": "DATA EVOLUCAO"}, axis=1) 
        
        if return_:
            return self.vacineja_df
    
# --> AUX
def select_indexes(integra_df, indexes):
    '''
    
    '''
    selected = integra_df.loc[indexes]
    if np.isin(["SIM"], selected["INFO COORTE SINTOMAS"].values)[0]:
        return "SIM"
    if np.isin(["SIM"], selected["INFO COORTE SOLICITACAO"].values)[0]:
        return "SIM"
    if np.isin(["SIM"], selected["INFO COORTE COLETA"].values)[0]:
        return "SIM"
    return "NAO"

def get_dates(integra_df, indexes, col="data_coleta_exame"):
    '''
        Description.
    '''
    selected = integra_df.loc[indexes]
    return json.dumps(dict(zip(selected[col].astype(str).values, selected["resultado_final_exame"].values)))


