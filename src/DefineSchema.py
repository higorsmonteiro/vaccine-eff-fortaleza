'''
    After linkage is performed and databases are processed, create the final schema database as the
    input of the matching process.
'''
import os
import numpy as np
import pandas as pd
import datetime as dt
from collections import defaultdict
from dateutil.relativedelta import relativedelta
import lib.schema_aux as aux

class DefineSchema:
    def __init__(self, base_folder):
        '''
            Using the path to the files of processed databases and linkage data, create the final schema
            for the matching database.

            Args:
                base_folder:
                    String. Path string to the data folder where the parquet folder is located.
        '''
        self.base_folder = base_folder

    def load_processed_data(self, folder=os.path.join("PARQUET_TRANSFORMED")):
        '''
            Load all processed parquet files generated from the previous processing step of the pipeline.

            Args:
                folder:
                    String.Path. Complement folder for 'self.base_folder' to locate the processed parquet files.
        '''
        self.vacineja_df = pd.read_parquet(os.path.join(self.base_folder, folder, "VACINEJA.parquet"))
        self.vacinados_df = pd.read_parquet(os.path.join(self.base_folder, folder, "VACINADOS.parquet"))
        self.tests_df = pd.read_parquet(os.path.join(self.base_folder, folder, "INTEGRASUS.parquet"))
        self.obito_covid_df = pd.read_parquet(os.path.join(self.base_folder, folder, "OBITO_COVID.parquet"))
        self.obito_cartorio_df = pd.read_parquet(os.path.join(self.base_folder, folder, "OBITO_CARTORIO.parquet"))
        self.bairro_df = pd.read_parquet(os.path.join(self.base_folder, folder, "BAIRRO_IDH.parquet"))
        self.sivep_df = pd.read_parquet(os.path.join(self.base_folder, folder, "SIVEP-GRIPE", "SIVEP_COVID19_2021.parquet"))
    
    def load_linked_data(self, folder=os.path.join("output", "data", "LINKAGE")):
        '''
            Load all linkage parquet files to merge with the final data.

            Args:
                folder: Complement folder (from the project folder) to locate the linkage parquet files.
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
        # Metadata of the Covid-19 tests
        id_resultado = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["resultado_final_exame"]))
        id_coleta = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_coleta_exame"]))
        id_solicitacao = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_solicitacao_exame"]))
        id_sintomas = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_inicio_sintomas_nova"]))
        id_sivep = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_internacao_sivep"]))
        id_uti = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_entrada_uti_sivep"]))
        id_obito = defaultdict(lambda: np.nan, zip(pos_tests["id"], pos_tests["data_obito"]))

        # --> Join
        self.vacineja_df["cpf LINKAGE VACINADOS"] = self.vacineja_df["cpf"].apply(lambda x: to_vacinados[x])
        self.vacineja_df["id LINKAGE INTEGRASUS"] = self.vacineja_df["cpf"].apply(lambda x: to_tests[x])
        self.vacineja_df["ordem LINKAGE OBITO COVID"] = self.vacineja_df["cpf"].apply(lambda x: to_covid[x])
        self.vacineja_df["cpf LINKAGE CARTORIOS"] = self.vacineja_df["cpf"].apply(lambda x: to_cart[x])
        self.vacineja_df["primary key LINKAGE SIVEP"] = self.vacineja_df["cpf"].apply(lambda x: to_sivep[x])
        self.vacineja_df = self.vacineja_df.merge(self.bairro_df[["NOME_BAIRRO", "IDH2010", "SR"]], left_on="bairro", right_on="NOME_BAIRRO", how="left")
        
        # --> Create columns on IntegraSUS
        self.tests_df = self.tests_df.set_index("id")
        self.vacineja_df["TESTE POSITIVO ANTES COORTE"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: aux.select_indexes(self.tests_df, x) if type(x)!=float else False)
        self.vacineja_df["POSITIVOS COLETA DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [ id_coleta[cur_id] for cur_id in x ] if type(x)!=float else np.nan)
        self.vacineja_df["POSITIVOS SOLICITACAO DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [ id_solicitacao[cur_id] for cur_id in x ] if type(x)!=float else np.nan)
        self.vacineja_df["INTEGRA PRI SINTOMAS DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [ id_sintomas[cur_id] for cur_id in x ] if type(x)!=float else np.nan)
        self.vacineja_df["INTEGRA HOSPITALIZACAO DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [ id_sivep[cur_id] for cur_id in x ] if type(x)!=float else np.nan)
        self.vacineja_df["INTEGRA UTI DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [ id_uti[cur_id] for cur_id in x ] if type(x)!=float else np.nan)
        self.vacineja_df["INTEGRA OBITO DATA"] = self.vacineja_df["id LINKAGE INTEGRASUS"].apply(lambda x: [ id_obito[cur_id] for cur_id in x ] if type(x)!=float else np.nan)
        self.vacineja_df["INTEGRA OBITO DATA"] = self.vacineja_df["INTEGRA OBITO DATA"].apply(lambda x: x[0] if np.any(pd.notna(x)) else np.nan)
        
    def create_final_schema(self, cohort=(dt.datetime(2021, 1, 21), dt.datetime(2021, 8, 31)), return_=True):
        '''
            Create final schema containing main information from the other databases and derived info.
        '''
        col_vacinados = ["cpf(VACINADOS)", "vacina(VACINADOS)", "data D1(VACINADOS)", "data D2(VACINADOS)", "data D3(VACINADOS)", 
                         "data D4(VACINADOS)", "grupo prioritario(VACINADOS)"]
        self.vacineja_df = self.vacineja_df.merge(self.vacinados_df[col_vacinados].dropna(subset=["cpf(VACINADOS)"], axis=0), left_on="cpf LINKAGE VACINADOS",
                                                  right_on="cpf(VACINADOS)", how="left").drop("cpf(VACINADOS)", axis=1)
        col_covid = ["ORDEM(OBITO COVID)", "numerodo", "data_pri_sintomas_nova(OBITO COVID)", "data_obito(OBITO COVID)"]
        self.vacineja_df = self.vacineja_df.merge(self.obito_covid_df[col_covid].dropna(subset=["ORDEM(OBITO COVID)"], axis=0), left_on="ordem LINKAGE OBITO COVID",
                                                  right_on="ORDEM(OBITO COVID)", how="left")
        col_cart = ["cpf(CARTORIOS)", "data falecimento(CARTORIOS)", "do(CARTORIOS)"]
        self.vacineja_df = self.vacineja_df.merge(self.obito_cartorio_df[col_cart].dropna(subset=["cpf(CARTORIOS)"], axis=0), left_on="cpf LINKAGE CARTORIOS", 
                                                  right_on="cpf(CARTORIOS)", how="left").drop("cpf(CARTORIOS)", axis=1)

        # --> Join SIVEP COLUMNS
        col_sivep = ["PRIMARY_KEY", "DT_NOTIFIC", "DT_INTERNA", "EVOLUCAO", "DT_EVOLUCA", "UTI", "DT_ENTUTI"]
        agg_sivep = self.sivep_df[col_sivep].astype(str).groupby("PRIMARY_KEY").agg(";".join).reset_index()
        self.vacineja_df = self.vacineja_df.merge(agg_sivep.dropna(subset=["PRIMARY_KEY"], axis=0), left_on="primary key LINKAGE SIVEP",
                                                                              right_on="PRIMARY_KEY", how="left").drop("PRIMARY_KEY", axis=1)
        
        f = lambda x: [pd.to_datetime(xx) for xx in x.split(";")] if pd.notna(x) else np.nan
        self.vacineja_df["DT_NOTIFIC"] = self.vacineja_df["DT_NOTIFIC"].apply(f)
        self.vacineja_df["DT_INTERNA"] = self.vacineja_df["DT_INTERNA"].apply(f)
        self.vacineja_df["DT_EVOLUCA"] = self.vacineja_df["DT_EVOLUCA"].apply(f)
        self.vacineja_df["EVOLUCAO"] = self.vacineja_df["EVOLUCAO"].apply(lambda x: x.split(";") if pd.notna(x) else np.nan)
        self.vacineja_df["DT_INTERNA"] = self.vacineja_df["DT_INTERNA"].apply(lambda x: x if not np.all(pd.isna(x)) else np.nan)
        self.vacineja_df["DT_NOTIFIC"] = self.vacineja_df["DT_NOTIFIC"].apply(lambda x: x if not np.all(pd.isna(x)) else np.nan)
        self.vacineja_df["DT_ENTUTI"] = self.vacineja_df["DT_ENTUTI"].apply(lambda x: [pd.to_datetime(xx) for xx in x.split(";")] if pd.notna(x) else np.nan)
        self.vacineja_df["DT_ENTUTI"] = self.vacineja_df["DT_ENTUTI"].apply(lambda x: x if not np.all(pd.isna(x)) else np.nan)
        self.vacineja_df["DATA UTI"] = self.vacineja_df["DT_ENTUTI"].apply(lambda x: aux.new_uti_date(x, cohort))
        
        # --> Verify inconsistencies in tests dates
        col_tests = ["POSITIVOS COLETA DATA", "POSITIVOS SOLICITACAO DATA", "data_obito(OBITO COVID)"]
        f = lambda x: np.any([date_>=x[col_tests[2]] for date_ in  x[col_tests[0]] if not pd.isna(date_)]) if np.any(pd.notna(x[col_tests[0]])) else False
        self.vacineja_df["COLETA APOS OBITO"] = self.vacineja_df[col_tests].apply(f, axis=1)
        f = lambda x: np.any([date_>=x[col_tests[2]] for date_ in  x[col_tests[1]] if not pd.isna(date_)]) if np.any(pd.notna(x[col_tests[1]])) else False
        self.vacineja_df["SOLICITACAO APOS OBITO"] = self.vacineja_df[col_tests].apply(f, axis=1)

        self.vacineja_df = self.vacineja_df.drop(["created_at"], axis=1)
        self.vacineja_df = self.vacineja_df.rename({"nome": "NOME", "nome_mae": "NOME MAE", "cpf": "CPF", "cns": "CNS", "data_nascimento": "DATA NASCIMENTO",
                                                    "cep": "CEP", "bairro": "BAIRRO", "sexo": "SEXO", "situacao": "SITUACAO VACINEJA",
                                                    "vacina(VACINADOS)": "VACINA APLICADA", "data D1(VACINADOS)": "DATA D1", "grupo prioritario(VACINADOS)": "GRUPO PRIORITARIO",
                                                    "data D2(VACINADOS)": "DATA D2", "data D3(VACINADOS)": "DATA D3", "data D4(VACINADOS)": "DATA D4",
                                                    "numerodo": "NUMERODO(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)": "DATA PRI SINTOMAS(OBITO COVID)", 
                                                    "data_obito(OBITO COVID)": "DATA OBITO", "data falecimento(CARTORIOS)": "DATA FALECIMENTO(CARTORIOS)",
                                                    "do(CARTORIOS)": "NUMERODO(CARTORIOS)", "DT_NOTIFIC": "DATA NOTIFICACAO SIVEP", 
                                                    "DT_INTERNA": "DATA INTERNACAO", "DT_EVOLUCA": "DATA EVOLUCAO"}, axis=1) 

        # --> Find inconsistencies between death date and vaccine dates.
        cols = {
            "OBITO": "DATA OBITO",
            "D1": "DATA D1",
            "D2": "DATA D2",
            "D3": "DATA D3",
            "D4": "DATA D4",
        }
        self.vacineja_df["OBITO INCONSISTENCIA COVID"] = self.vacineja_df[list(cols.values())].apply(lambda x: aux.compare_vaccine_death(x, cols), axis=1)
        cols = {
            "OBITO": "DATA FALECIMENTO(CARTORIOS)",
            "D1": "DATA D1",
            "D2": "DATA D2",
            "D3": "DATA D3",
            "D4": "DATA D4",
        }
        self.vacineja_df["OBITO INCONSISTENCIA CARTORIOS"] = self.vacineja_df[list(cols.values())].apply(lambda x: aux.compare_vaccine_death(x, cols), axis=1)
        
        # Save the dates of positive Covid-19 tests based on sampling date and solicitation date.
        self.vacineja_df["POSITIVOS COLETA DATA"] = self.vacineja_df["POSITIVOS COLETA DATA"].apply(lambda x: np.nan if np.all(pd.isna(x)) else x)
        self.vacineja_df["POSITIVOS SOLICITACAO DATA"] = self.vacineja_df["POSITIVOS SOLICITACAO DATA"].apply(lambda x: np.nan if np.all(pd.isna(x)) else x)
        self.vacineja_df["INTEGRA PRI SINTOMAS DATA"] = self.vacineja_df["INTEGRA PRI SINTOMAS DATA"].apply(lambda x: np.nan if np.all(pd.isna(x)) else x)
        self.vacineja_df["INTEGRA HOSPITALIZACAO DATA"] = self.vacineja_df["INTEGRA HOSPITALIZACAO DATA"].apply(lambda x: np.nan if np.all(pd.isna(x)) else x)
        self.vacineja_df["INTEGRA UTI DATA"] = self.vacineja_df["INTEGRA UTI DATA"].apply(lambda x: np.nan if np.all(pd.isna(x)) else x)
        self.vacineja_df["INTEGRA OBITO DATA"] = self.vacineja_df["INTEGRA OBITO DATA"].apply(lambda x: np.nan if np.all(pd.isna(x)) else x)

        # Define vaccination status during cohort.
        new_col_cohort = "STATUS VACINACAO DURANTE COORTE"
        subs = ["DATA D1", "DATA D2", "DATA D3", "DATA D4"]
        self.vacineja_df[new_col_cohort] = self.vacineja_df[subs].apply(lambda x: aux.vaccination_during_cohort(x, cohort[0], cohort[1]), axis=1)
        # Define vaccination status considering whole period of the database.
        new_col = "STATUS VACINACAO"
        subs = ["DATA D1", "DATA D2", "DATA D3", "DATA D4"]
        self.vacineja_df[new_col] = self.vacineja_df[subs].apply(lambda x: aux.vaccination_status(x), axis=1)

        # Calculate age of the individuals based on the end of the cohort.
        self.vacineja_df["IDADE"] = self.vacineja_df["DATA NASCIMENTO"].apply(lambda x: relativedelta(cohort[1], x).years)
        self.vacineja_df["VACINA APLICADA"] = self.vacineja_df["VACINA APLICADA"].fillna("NAO VACINADO")

        # Define hospitalization date
        col = ["DATA NOTIFICACAO SIVEP", "DATA INTERNACAO"]
        f = lambda x: x[col[1]] if np.any(pd.notna(x[col[1]])) else (x[col[0]] if np.any(pd.notna(x[col[0]])) else np.nan)
        self.vacineja_df["DATA HOSPITALIZACAO"] = self.vacineja_df[col].apply(f, axis=1)
        col = ["DATA HOSPITALIZACAO", "INTEGRA HOSPITALIZACAO DATA"]
        f = lambda x: x[col[0]]+x[col[1]] if np.any(pd.notna(x[col[0]])) and np.any(pd.notna(x[col[1]])) else (  (x[col[0]]) if np.any(pd.notna(x[col[0]])) and np.any(pd.isna(x[col[1]])) else ( x[col[1]] if np.any(pd.notna(x[col[1]]))and np.any(pd.isna(x[col[0]])) else np.nan ))
        self.vacineja_df["DATA HOSPITALIZACAO"] = self.vacineja_df[col].apply(f, axis=1)

        # Uniformize death date
        col = ["DATA OBITO", "INTEGRA OBITO DATA"]
        f = lambda x: x[col[0]] if np.any(pd.notna(x[col[0]])) else (x[col[1]] if np.any(pd.notna(x[col[1]])) else np.nan)
        self.vacineja_df["DATA OBITO"] = self.vacineja_df[col].apply(f, axis=1)

        # Uniformize ICU date
        col = ["DATA UTI", "INTEGRA UTI DATA"]
        self.vacineja_df["DATA UTI"] = self.vacineja_df["DATA UTI"].apply(lambda x: [x] if pd.notna(x) else np.nan)
        f = lambda x: x[col[0]]+x[col[1]] if np.any(pd.notna(x[col[0]])) and np.any(pd.notna(x[col[1]])) else ( x[col[0]] if np.any(pd.notna(x[col[0]])) and np.any(pd.isna(x[col[1]])) else ( x[col[1]] if np.any(pd.notna(x[col[1]]))and np.any(pd.isna(x[col[0]])) else np.nan ))
        self.vacineja_df["DATA UTI"] = self.vacineja_df[col].apply(f, axis=1)

        # Death before cohort
        subcol = ["DATA OBITO", "DATA FALECIMENTO(CARTORIOS)"]
        self.vacineja_df["OBITO ANTES COORTE"] = self.vacineja_df[subcol].apply(lambda x: True if pd.notna(x[subcol[0]]) and x[subcol[0]]<cohort[0] else ( True if pd.notna(x[subcol[1]]) and x[subcol[1]]<cohort[0] else False), axis=1)

        # Covid-19 hospitalization before cohort
        self.vacineja_df["HOSPITALIZACAO ANTES COORTE"] = self.vacineja_df["DATA HOSPITALIZACAO"].apply(lambda x: np.any([dts<cohort[0] for dts in x if pd.notna(dts)]) if np.any(pd.notna(x)) else False)

        # --> Treat JANSSEN vaccine as D1-D2.
        str_vac = "STATUS VACINACAO DURANTE COORTE"
        self.vacineja_df["DATA D1"] = self.vacineja_df[["DATA D1", "DATA D4", str_vac]].apply(lambda x: x["DATA D4"] if x[str_vac]=="(D4)" else x["DATA D1"], axis=1)
        self.vacineja_df["DATA D2"] = self.vacineja_df[["DATA D2", "DATA D4", str_vac]].apply(lambda x: x["DATA D4"] if x[str_vac]=="(D4)" else x["DATA D2"], axis=1)

        if return_:
            return self.vacineja_df
