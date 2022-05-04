'''
    ...
'''
import os
import pandas as pd
import datetime as dt
import numpy as np
import lib.utils as utils
import lib.transform_data as transf
import lib.linkage_data as linkage
from collections import defaultdict

class ProcessingAndLinkage:
    def __init__(self, fpaths, fnames):
        self.path_to_data = fpaths
        self.name_of_files = fnames
        self.vacineja_df = None         # People registered willing to take the vaccine.
        self.vacinados_df = None        # Separated file containing all vaccinated individuals.
        self.integra_df = None          # IntegraSUS Covid-19 tests.
        self.obitos_covid_df = None 
        self.obitos_cartorios_df = None
        self.bairro_df = None

    def set_paths(self):
        '''
            Create the variables holding the paths for all databases needed for the analysis.
        '''
        # --> Set the paths for each one of the databases
        self.vacineja_path = os.path.join(self.path_to_data["VACINACAO CADASTRO (VACINE JA)"],
                                          self.name_of_files["VACINACAO CADASTRO (VACINE JA)"])
        self.vacinados_path = os.path.join(self.path_to_data["VACINACAO POR PESSOA"], 
                                           self.name_of_files["VACINACAO POR PESSOA"])
        self.testes_integra_path = os.path.join(self.path_to_data["TESTES COVID-19 INTEGRA"], 
                                                self.name_of_files["TESTES COVID-19 INTEGRA"])
        self.cartorio_path = os.path.join(self.path_to_data["OBITOS CARTORIOS"], 
                                          self.name_of_files["OBITOS CARTORIOS"])
        self.obitos_covid_path = os.path.join(self.path_to_data["OBITOS COVID-19"], 
                                              self.name_of_files["OBITOS COVID-19"])
        self.bairro_path = os.path.join(self.path_to_data["BAIRROS IDH"], self.name_of_files["BAIRROS IDH"])

    def load_dbs(self, nrows_dict=None, cols_dict=None):
        '''

            Args:
                nrows_dict:
                    Dictionary. Keys of the dictionary -> 'Vacine Ja', 'Vacinados',
                    'IntegraSUS', 'Obito Covid', 'Obito Cartorio'.
                cols_dict:
                    Dictionary. Keys of the dictionary -> 'Vacine Ja', 'Vacinados',
                    'IntegraSUS', 'Obito Covid', 'Obito Cartorio'.
        '''
        if nrows_dict is None:
            nrows_dict = {
                "Vacine Ja": None,
                "Vacinados": None,
                "IntegraSUS": None,
                "Obito Covid": None,
                "Obito Cartorio": None,
            }
        if cols_dict is None:
            cols_dict = {
                "Vacine Ja": None,
                "Vacinados": None,
                "IntegraSUS": None,
                "Obito Covid": None,
                "Obito Cartorio": None,
            }
        # ----> Load data
        self.vacineja_df = pd.read_csv(self.vacineja_path, usecols=cols_dict["Vacine Ja"], nrows=nrows_dict["Vacine Ja"], 
                                       delimiter=";", encoding="utf-8")
        self.vacinados_df = pd.read_csv(self.vacinados_path, usecols=cols_dict["Vacinados"], nrows=nrows_dict["Vacinados"], 
                                        delimiter=',', encoding="utf-8")
        self.tests_df = pd.read_stata(self.testes_integra_path, convert_categoricals=False, columns=cols_dict["IntegraSUS"])
        if nrows_dict["IntegraSUS"] is not None:
            self.tests_df = self.tests_df[:nrows_dict["IntegraSUS"]]
        self.obitos_cartorios_df = pd.read_csv(self.cartorio_path, dtype={"cpf": str, "cpf novo str": str, "do_8": str})
        self.obitos_covid_df = pd.read_excel(self.obitos_covid_path, dtype={"ORDEM": str, "NUMERODO": str}, sheet_name="OBITOS")
        self.bairro_df = pd.read_excel(self.bairro_path, sheet_name="BAIRRO_IDH")

    def transform(self, init_cohort, save=False, fpath=None):
        '''
            
            Args:
                select:
                    List of Strings. To signal which database should be transformed.
        '''
        self.vacineja_df = transf.transform_vacineja(self.vacineja_df)
        self.vacinados_df = transf.transform_vacinados(self.vacinados_df)
        self.tests_df = transf.transform_integrasus(self.tests_df, init_cohort)
        self.obitos_cartorios_df = transf.transform_cartorios(self.obitos_cartorios_df)
        self.obitos_covid_df = transf.transform_obito_covid(self.obitos_covid_df)
        self.bairro_df = transf.transform_bairros(self.bairro_df)

        col_key = ["NOMENASCIMENTOCHAVE", "NOMENOMEMAECHAVE", "NOMEMAENASCIMENTOCHAVE",	"NOMEHASHNASCIMENTOCHAVE",	
                   "NOMEMAEHASHNASCIMENTOCHAVE"]
        if save and fpath is not None:
            self.vacineja_df.drop(col_key, axis=1).to_parquet(os.path.join(fpath, "VACINEJA.parquet"))
            self.vacinados_df.drop(col_key, axis=1).to_parquet(os.path.join(fpath, "VACINADOS.parquet"))
            self.tests_df.drop(col_key, axis=1).to_parquet(os.path.join(fpath, "INTEGRASUS.parquet"))
            self.obitos_cartorios_df.drop(col_key, axis=1).to_parquet(os.path.join(fpath, "OBITO_CARTORIO.parquet"))
            self.obitos_covid_df.drop(col_key, axis=1).to_parquet(os.path.join(fpath, "OBITO_COVID.parquet"))
            self.bairro_df.to_parquet(os.path.join(fpath, "BAIRRO_IDH.parquet"))

    def linkage(self, save=False, fpath=os.path.join("output", "data", "LINKAGE")):
        '''
            Perform linkage between "Vacine Já" and all databases.
        '''
        self.linkage_vacineja_vacinados = linkage.linkage_vacinados(self.vacineja_df, self.vacinados_df)
        self.linkage_vacineja_integrasus = linkage.linkage_integrasus(self.vacineja_df, self.tests_df)
        self.linkage_obitos_covid = linkage.linkage_obitos_covid(self.vacineja_df, self.obitos_covid_df, self.obitos_cartorios_df)
        self.linkage_obitos_cartorios = linkage.linkage_obito_cartorios(self.vacineja_df, self.obitos_covid_df, self.obitos_cartorios_df)

        if save:
            self.linkage_vacineja_vacinados.to_parquet(os.path.join(fpath, "VACINEJA_VACINADOS.parquet"))
            self.linkage_vacineja_integrasus.to_parquet(os.path.join(fpath, "VACINEJA_INTEGRASUS.parquet"))
            self.linkage_obitos_covid.to_parquet(os.path.join(fpath, "VACINEJA_OBITO_COVID.parquet"))
            self.linkage_obitos_cartorios.to_parquet(os.path.join(fpath, "VACINEJA_OBITO_CARTORIO.parquet"))

    # --> DELETE
    #def init_integrasus(self, init_cohort=dt.date(2021, 1, 21), return_=True):
    #    '''
    #        Load and transform IntegraSUS data and find all individuals from records in Vacine Já.
    #    '''
    #    testes_integra_path = os.path.join(self.path_to_data["TESTES COVID-19 INTEGRA"], self.name_of_files["TESTES COVID-19 INTEGRA"])
    #    self.tests_df = pd.read_stata(testes_integra_path, convert_categoricals=False)

    #    self.tests_df["cpf"] = self.tests_df["cpf"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not pd.isna(x) else np.nan)
    #    self.tests_df['nome_mae'] = self.tests_df['nome_mae'].apply(lambda x: x if len(x)>0 and not pd.isna(x) else np.nan)
    #    self.tests_df["nome tratado"] = self.tests_df["nome_paciente"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    #    self.tests_df["nome hashcode"] = self.tests_df["nome_paciente"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    #    self.tests_df["nome mae tratado"] = self.tests_df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    #    self.tests_df["nome mae hashcode"] = self.tests_df["nome_mae"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    #    self.tests_df["cns"] = self.tests_df["cns"].apply(lambda x: x if len(x)!=0 or not pd.isna(x) else np.nan)

        # Transform date fields
    #    self.tests_df["data_nascimento"] = pd.to_datetime(self.tests_df["data_nascimento"], errors="coerce")
    #    self.tests_df["data_coleta_exame"] = pd.to_datetime(self.tests_df["data_coleta_exame"], errors="coerce")
    #    self.tests_df["data_inicio_sintomas_nova"] = pd.to_datetime(self.tests_df["data_inicio_sintomas_nova"], errors="coerce")
    #    self.tests_df["data_internacao_sivep"] = pd.to_datetime(self.tests_df["data_internacao_sivep"], errors="coerce")
    #    self.tests_df["data_entrada_uti_sivep"] = pd.to_datetime(self.tests_df["data_entrada_uti_sivep"], format="%Y/%m/%d", errors="coerce")
    #    self.tests_df["data_evolucao_caso_sivep"] = pd.to_datetime(self.tests_df["data_entrada_uti_sivep"], format="%Y/%m/%d", errors="coerce")
    #    self.tests_df["data_obito"] = pd.to_datetime(self.tests_df["data_obito"], errors="coerce")
    #    self.tests_df["data_resultado_exame"] = pd.to_datetime(self.tests_df["data_resultado_exame"], errors="coerce")
    #    self.tests_df["data_solicitacao_exame"] = pd.to_datetime(self.tests_df["data_solicitacao_exame"], errors="coerce")
    #    self.tests_df["data_saida_uti_sivep"] = pd.to_datetime(self.tests_df["data_saida_uti_sivep"], format="%Y/%m/%d", errors="coerce")
    #    self.tests_df = self.tests_df.dropna(subset=["nome_paciente", "data_nascimento"], axis=0, how="all")

        # --> Create primary keys for person
    #    f_key = lambda x: x["cpf"] if not pd.isna(x["cpf"]) else str(x["nome tratado"])+str(x["data_nascimento"])
    #    self.tests_df["PRIMARY_KEY_PERSON"] = self.tests_df.apply(lambda x: f_key(x), axis=1)
    #    # --> Define primary keys for linkage: cpf, NOME+DATANASC, NOMEHASH+DATANASC, NOME+NOMEMAE
    #    self.tests_df["NOMENASCIMENTOCHAVE"] = self.tests_df["nome tratado"] + self.tests_df["data_nascimento"].astype(str)
    #    self.tests_df["NOMEHASHNASCIMENTOCHAVE"] = self.tests_df["nome hashcode"] + self.tests_df["data_nascimento"].astype(str)
    #    self.tests_df["NOMEMAENASCIMENTOCHAVE"] = self.tests_df["nome mae tratado"] + self.tests_df["data_nascimento"].astype(str)
    #    self.tests_df["NOMENOMEMAECHAVE"] = self.tests_df["nome tratado"] + self.tests_df["nome mae tratado"]
    #    self.tests_df["NOMEMAEHASHNASCIMENTOCHAVE"] = self.tests_df["nome mae hashcode"] + self.tests_df["data_nascimento"].astype(str)

    #    col_linkage1 = ["id", "cpf"]
    #    col_linkage2 = ["id", "NOMENASCIMENTOCHAVE"]
    #    col_linkage3 = ["id", "NOMENOMEMAECHAVE"]
    #    col_linkage4 = ["id", "NOMEMAENASCIMENTOCHAVE"]
    #    col_linkage5 = ["id", "NOMEHASHNASCIMENTOCHAVE"]
    #    col_linkage6 = ["id", "NOMEMAEHASHNASCIMENTOCHAVE"]

    #    linkage1 = self.tests_df[col_linkage1].dropna(subset=["cpf"], axis=0).merge(self.vacineja_df.add_suffix("(vj)")["cpf(vj)"], left_on="cpf", right_on="cpf(vj)", how="left").dropna(subset=["cpf(vj)"], axis=0)
    #    linkage2 = self.tests_df[col_linkage2].merge(self.vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]], on="NOMENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0).dropna(subset=["cpf"], axis=0)
    #    linkage3 = self.tests_df[col_linkage3].dropna(subset=["NOMENOMEMAECHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMENOMEMAECHAVE"]], on="NOMENOMEMAECHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #    linkage4 = self.tests_df[col_linkage4].dropna(subset=["NOMEMAENASCIMENTOCHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]], on="NOMEMAENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #    linkage5 = self.tests_df[col_linkage5].dropna(subset=["NOMEHASHNASCIMENTOCHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]], on="NOMEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #    linkage6 = self.tests_df[col_linkage6].dropna(subset=["NOMEMAEHASHNASCIMENTOCHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)

    #    self.id_to_vacinejacpf = defaultdict(lambda: np.nan)
    #    self.id_to_vacinejacpf.update(zip(linkage1["id"], linkage1["cpf"]))
    #    self.id_to_vacinejacpf.update(zip(linkage2["id"], linkage2["cpf"]))
    #    self.id_to_vacinejacpf.update(zip(linkage3["id"], linkage3["cpf"]))
    #    self.id_to_vacinejacpf.update(zip(linkage4["id"], linkage4["cpf"]))
    #    self.id_to_vacinejacpf.update(zip(linkage5["id"], linkage5["cpf"]))
    #    self.id_to_vacinejacpf.update(zip(linkage6["id"], linkage6["cpf"]))

    #    self.tests_df["cpf(VACINEJA)"] = self.tests_df["id"].apply(lambda x: self.id_to_vacinejacpf[x])

        # To verify if symptoms and testing (check whether is positive later) were done before cohort.
    #    self.tests_df["SINTOMAS ANTES COORTE"] = self.tests_df["data_inicio_sintomas_nova"].apply(lambda x: "SIM" if not pd.isna(x) and x<init_cohort else "NAO")
    #    self.tests_df["SOLICITACAO ANTES COORTE"] = self.tests_df["data_solicitacao_exame"].apply(lambda x: "SIM" if not pd.isna(x) and x<init_cohort else "NAO")
    #    self.tests_df["COLETA ANTES COORTE"] = self.tests_df["data_coleta_exame"].apply(lambda x: "SIM" if not pd.isna(x) and x<init_cohort else "NAO")
        
        # Now classify each individual as YES or NO depending if the person has positive test before cohort or not.
    #    self.pos_antes_cohort_sint = defaultdict(lambda:"NAO")
    #    self.pos_antes_cohort_sol = defaultdict(lambda:"NAO")
    #    self.pos_antes_cohort_col = defaultdict(lambda:"NAO")
        
    #    person_grouped = self.tests_df.groupby("PRIMARY_KEY_PERSON")
    #    n_records = person_grouped.count()["id"].reset_index()
    #    one_record_people = n_records[n_records["id"]==1]["PRIMARY_KEY_PERSON"]
    #    mult_record_people = n_records[n_records["id"]>1]["PRIMARY_KEY_PERSON"]

        # Individuals with a single record
    #    one_rec_tests = self.tests_df[self.tests_df["PRIMARY_KEY_PERSON"].isin(one_record_people)]
    #    subcol = ["resultado_final_exame", "SINTOMAS ANTES COORTE", "SOLICITACAO ANTES COORTE", "COLETA ANTES COORTE"]
    #    one_rec_tests["INFO COORTE SINTOMAS"] = one_rec_tests[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[1]]=="SIM" else "NAO", axis=1)
    #    one_rec_tests["INFO COORTE SOLICITACAO"] = one_rec_tests[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[2]]=="SIM" else "NAO", axis=1)
    #    one_rec_tests["INFO COORTE COLETA"] = one_rec_tests[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[3]]=="SIM" else "NAO", axis=1)

        # Individuals with multiple records
    #   subcol = ["resultado_final_exame", "SINTOMAS ANTES COORTE", "SOLICITACAO ANTES COORTE", "COLETA ANTES COORTE"]
    #   for pkey in mult_record_people:
    #       sub_df = person_grouped.get_group(pkey)
    #       sub_df["INFO COORTE SINTOMAS"] = sub_df[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[1]]=="SIM" else "NAO", axis=1)
    #       sub_df["INFO COORTE SOLICITACAO"] = sub_df[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[2]]=="SIM" else "NAO", axis=1)
    #       sub_df["INFO COORTE COLETA"] = sub_df[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[3]]=="SIM" else "NAO", axis=1)
    #       if np.isin(["SIM"], sub_df["INFO COORTE SINTOMAS"]):
    #           self.pos_antes_cohort_sint[pkey] = "SIM"
    #       if np.isin(["SIM"], sub_df["INFO COORTE SOLICITACAO"]):
    #           self.pos_antes_cohort_sol[pkey] = "SIM"
    #       if np.isin(["SIM"], sub_df["INFO COORTE COLETA"]):
    #           self.pos_antes_cohort_col[pkey] = "SIM"
        
    #    self.tests_df["INFO COORTE SINTOMAS"] = self.tests_df["PRIMARY_KEY_PERSON"].apply(lambda x: self.pos_antes_cohort_sint[x])
    #    self.tests_df["INFO COORTE SOLICITACAO"] = self.tests_df["PRIMARY_KEY_PERSON"].apply(lambda x: self.pos_antes_cohort_sol[x])
    #    self.tests_df["INFO COORTE COLETA"] = self.tests_df["PRIMARY_KEY_PERSON"].apply(lambda x: self.pos_antes_cohort_col[x])
        
    #    subcol = ["id", "cpf(VACINEJA)", 'SINTOMAS ANTES COORTE',
    #              'SOLICITACAO ANTES COORTE', 'COLETA ANTES COORTE',
    #              'INFO COORTE SINTOMAS', 'INFO COORTE SOLICITACAO',
    #              'INFO COORTE COLETA']
    #    self.tests_df = self.tests_df[subcol]

    #    if return_:
    #        return self.tests_df
#
#    