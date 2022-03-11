'''
    Main class to transform the Vacine Já database into its final form containing 
    the main information necessary for the matching and survival analysis.
'''
import os
import pandas as pd
import numpy as np
import lib.aux_utils as aux
import lib.utils as utils
import datetime as dt
from collections import defaultdict
from dateutil.relativedelta import relativedelta

class VacineJaTransform:
    def __init__(self, path_to, name_of):
        self.path_to_data = path_to
        self.name_of_files = name_of
        self.vacineja_df = None     # People registered willing to take the vaccine.
        self.vacinados_df = None    # Separated file containing all vaccinated individuals.
        self.gal_df = None          # GAL Covid-19 tests.
        self.integra_df = None      # IntegraSUS Covid-19 tests.
        self.obitos_covid_df = None 
        self.obitos_cartorios_df = None
        self.hospitalizados_df = None

    def init_vacineja(self, return_=False, colnames=None, nrows=None, cur_date=dt.date(2021, 9, 2)):
        '''
            Description.

            Args:
                return_:
                    Bool.
                colnames:
                    List of strings or String.
                nrows:
                    Integer or None.
                cur_date:
                    datetime.date.
            Return:
                self.vacineja_df:
                    pandas.DataFrame. If return_=True.
        '''
        # Define path for "Vacine Já" database.
        vacineja_path = os.path.join(self.path_to_data["VACINACAO CADASTRO (VACINE JA)"],
                                     self.name_of_files["VACINACAO CADASTRO (VACINE JA)"])
        # Load and transform the database.
        date_fmt = "%Y-%m-%d"
        date_cols = ["data_nascimento", "created_at"]
        if colnames is None:
            colnames = ["nome", "cpf", "data_nascimento", "bairro", "sexo", "created_at", "cns", "cep", "situacao", "nome_mae"]
        else:
            colnames = colnames + ["data_nascimento", "created_at"]
            colnames = list(dict.fromkeys(colnames))
        self.vacineja_df = pd.read_csv(vacineja_path, usecols=colnames, nrows=nrows, delimiter=";", encoding="utf-8")
        for j in date_cols:
            self.vacineja_df[j] = pd.to_datetime(self.vacineja_df[j], format=date_fmt, errors="coerce")
    
        # More specific filters and transformations
        self.vacineja_df = self.vacineja_df.dropna(subset=["cpf", "data_nascimento"], axis=0)
        # Include only the individuals with male and female sex (to avoid the problem of before)
        self.vacineja_df = self.vacineja_df[self.vacineja_df["sexo"].isin(["M", "F"])]
        self.vacineja_df["cpf"] = self.vacineja_df["cpf"].apply(lambda x:f"{x:11.0f}".replace(" ","0") if type(x)!=str else np.nan)
        self.vacineja_df = self.vacineja_df.drop_duplicates(subset=["cpf"], keep="first")
        #self.vacineja_df = self.vacineja_df[self.vacineja_df["cidade"]=="FORTALEZA"]
        #self.vacineja_df = self.vacineja_df.drop("cidade", axis=1)

        # --> Create custom primary keys to complement the linkage by 'cpf' with the other databases
        self.vacineja_df["nome tratado"] = self.vacineja_df["nome"].apply(lambda x: utils.replace_string(x))
        self.vacineja_df["nome mae tratado"] = self.vacineja_df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.vacineja_df["nome hashcode"] = self.vacineja_df["nome"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.vacineja_df["nome mae hashcode"] = self.vacineja_df["nome_mae"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        # ----> CUSTOM PRIMARY KEYS
        self.vacineja_df["NOMENASCIMENTOCHAVE"] = self.vacineja_df["nome tratado"].astype(str)+self.vacineja_df["data_nascimento"].astype(str)
        self.vacineja_df["NOMENOMEMAECHAVE"] = self.vacineja_df["nome tratado"].astype(str)+self.vacineja_df["nome mae tratado"].astype(str)
        self.vacineja_df["NOMEMAENASCIMENTOCHAVE"] = self.vacineja_df["nome mae tratado"].astype(str) + self.vacineja_df["data_nascimento"].astype(str)
        self.vacineja_df["NOMEHASHNASCIMENTOCHAVE"] = self.vacineja_df["nome hashcode"].astype(str) + self.vacineja_df["data_nascimento"].astype(str)
        self.vacineja_df["NOMEMAEHASHNASCIMENTOCHAVE"] = self.vacineja_df["nome mae hashcode"].astype(str) + self.vacineja_df["data_nascimento"].astype(str)
        self.vacineja_df = self.vacineja_df.drop(["nome tratado", "nome mae tratado", "nome hashcode", "nome mae hashcode"], axis=1)

        if return_:
            return self.vacineja_df

    def init_vacinados(self, return_=False, colnames=None, nrows=None, delimiter=",", encoding="utf-8"):
        '''
            Description.

            Args:
                return_:
                    Bool.
                colnames:
                    List of strings or String.
                nrows:
                    Integer or None.
                delimiter:
                    String.
                encoding:
                    String.
            Return:
                self.vacinados_df:
                    pandas.DataFrame. If return_=True.
        '''
        # Define path for "Vacinados" database.
        vacinados_path = os.path.join(self.path_to_data["VACINACAO POR PESSOA"], self.name_of_files["VACINACAO POR PESSOA"])
        
        date_fmt = "%Y-%m-%d"
        date_cols = ["data D1", "data D2", "data D3", "data D4", "data nascimento"]
        if colnames is None:
            colnames = ["nome", "cpf", "sexo", "bairro id", "vacina", "fornecedor", "data nascimento",
                        "data D1", "data D2", "data D3", "data D4", "idade anos", "faixa etaria", "bairro", 
                        "tipo atendimento", "tipo usuario", "grupo prioritario"]
        else:
            colnames = colnames + date_cols
            colnames = list(dict.fromkeys(colnames))
        self.vacinados_df = pd.read_csv(vacinados_path, usecols=colnames, nrows=nrows, delimiter=delimiter, encoding="utf-8")
        for j in date_cols:
            self.vacinados_df[j] = pd.to_datetime(self.vacinados_df[j], format=date_fmt, errors="coerce")
        
        # Apply filters
        # Remove records with the fields "cpf_usuario" and "data_nascimento" missing.
        self.vacinados_df = self.vacinados_df.dropna(subset=["cpf","data nascimento","sexo","vacina"], how="any", axis=0)
        # Format the field of "grupo prioritario" to reduce the rows with compound information.
        self.vacinados_df["grupo prioritario"] = self.vacinados_df["grupo prioritario"].apply(lambda x: x.split("-")[0] if not pd.isna(x) else x)
        # Process the CPF field
        self.vacinados_df["cpf"] = self.vacinados_df["cpf"].astype(float, errors="ignore")
        self.vacinados_df["cpf"] = self.vacinados_df["cpf"].apply(lambda x:f"{x:11.0f}".replace(" ","0") if type(x)!=str else np.nan)
        self.vacinados_df = self.vacinados_df.drop_duplicates(subset=["cpf"], keep="first")
        # Process the names of each person
        self.vacinados_df["nome tratado"] = self.vacinados_df["nome"].apply(lambda x: utils.replace_string(x))
        # Find all persons having an inconsistent set of vaccination dates.
        subset = ["data D1", "data D2"]
        self.vacinados_df["data aplicacao consistente"] = self.vacinados_df[subset].apply(lambda x: aux.f_d1d2(x["data D1"], x["data D2"]), axis=1)
        self.vacinados_df = self.vacinados_df.add_suffix("(VACINADOS)").rename({"cpf(VACINADOS)": "cpf"}, axis=1)

        if return_:
            return self.vacinados_df

    def init_obitos_cartorios(self, return_=False, save=None):
        '''
            Description.

            Args:
                return_:
                    Bool.
                save:
                    String.
            Return:
                self.obitos_cartorios_df:
                    pandas.DataFrame. If return_=True.
        '''
        cartorio_path = os.path.join(self.path_to_data["OBITOS CARTORIOS"], self.name_of_files["OBITOS CARTORIOS"])
        self.obitos_cartorios_df = pd.read_csv(cartorio_path, dtype={"cpf": str, "cpf novo str": str, "do_8": str})
        self.obitos_cartorios_df = self.obitos_cartorios_df.drop_duplicates(subset=["cpf novo str", "do_8"], keep="first")
        self.obitos_cartorios_df = self.obitos_cartorios_df.dropna(subset=["cpf novo str", "do_8"], axis=0, how="any")
        self.obitos_cartorios_df = self.obitos_cartorios_df.add_suffix("(CARTORIOS)").rename({"cpf novo str(CARTORIOS)": "cpf", "do_8(CARTORIOS)": "do_8"}, axis=1)
        self.obitos_cartorios_df["nome mae tratado"] = self.obitos_cartorios_df["genitor 2(CARTORIOS)"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.obitos_cartorios_df["nome tratado"] = self.obitos_cartorios_df["nome(CARTORIOS)"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.obitos_cartorios_df["nome hashcode"] = self.obitos_cartorios_df["nome(CARTORIOS)"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.obitos_cartorios_df["nome mae hashcode"] = self.obitos_cartorios_df["genitor 2(CARTORIOS)"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.obitos_cartorios_df["data nascimento(CARTORIOS)"] = pd.to_datetime(self.obitos_cartorios_df["data nascimento(CARTORIOS)"], format="%d/%m/%Y", errors="coerce")
        self.obitos_cartorios_df["data falecimento(CARTORIOS)"] = pd.to_datetime(self.obitos_cartorios_df["data falecimento(CARTORIOS)"], format="%d/%m/%Y", errors="coerce")

        self.obitos_cartorios_df["NOMENASCIMENTOCHAVE"] = self.obitos_cartorios_df["nome tratado"].astype(str) + self.obitos_cartorios_df["data nascimento(CARTORIOS)"].astype(str)
        self.obitos_cartorios_df["NOMENOMEMAECHAVE"] = self.obitos_cartorios_df["nome tratado"].astype(str) + self.obitos_cartorios_df["nome mae tratado"].astype(str)
        self.obitos_cartorios_df["NOMEMAENASCIMENTOCHAVE"] = self.obitos_cartorios_df["nome mae tratado"].astype(str) + self.obitos_cartorios_df["data nascimento(CARTORIOS)"].astype(str)
        self.obitos_cartorios_df["NOMEHASHNASCIMENTOCHAVE"] = self.obitos_cartorios_df["nome hashcode"].astype(str) + self.obitos_cartorios_df["data nascimento(CARTORIOS)"].astype(str)
        self.obitos_cartorios_df["NOMEMAEHASHNASCIMENTOCHAVE"] = self.obitos_cartorios_df["nome mae hashcode"].astype(str) + self.obitos_cartorios_df["data nascimento(CARTORIOS)"].astype(str)
        self.obitos_cartorios_df = self.obitos_cartorios_df.drop(["nome tratado", "nome mae tratado", "nome hashcode", "nome mae hashcode"], axis=1)
        
        if save is not None:
            self.obitos_cartorios_df.to_excel(save, index=False)
        if return_:
            return self.obitos_cartorios_df

    def init_obitos_covid(self, return_=False, save=None):
        '''
            Description.

            Args:
                return_:
                    Bool.
                save:
                    String.
            Return:
                self.obitos_covid_df:
                    pandas.DataFrame. If return_=True.
        '''
        obitos_covid_path = os.path.join(self.path_to_data["OBITOS COVID-19"], self.name_of_files["OBITOS COVID-19"])

        date_fmt = "%d/%m/%Y"
        col_obitos = ["ORDEM", "NUMERODO", "NOME", "DATA_NASCIMENTO", "SEXO", "EVOLUCAO", "NOME_MAE",
                      "IDADE_ANOS", "FX_ETARIA", 'DATA_PRI_SINTOMAS_NOVA', "BAIRRO_RESIDENCIA", "DATA_OBITO"]
        date_cols = ["DATA_NASCIMENTO", "DATA_OBITO", "DATA_PRI_SINTOMAS_NOVA"]
        col_obitos = col_obitos + date_cols
        col_obitos = list(dict.fromkeys(col_obitos))
        self.obitos_covid_df = pd.read_excel(obitos_covid_path, dtype={"ORDEM": str, "NUMERODO": str}, sheet_name="OBITOS")
        self.obitos_covid_df = self.obitos_covid_df[col_obitos]
        for j in date_cols:
            self.obitos_covid_df[j] = pd.to_datetime(self.obitos_covid_df[j], format=date_fmt, errors="coerce")
        
        self.obitos_covid_df["ANO OBITO"] = self.obitos_covid_df["DATA_OBITO"].apply(lambda x: f"{x.year:.0f}" if not pd.isna(x) else np.nan)
        self.obitos_covid_df = self.obitos_covid_df[self.obitos_covid_df["ANO OBITO"].isin(["2020", "2021"])]
        self.obitos_covid_df = self.obitos_covid_df[self.obitos_covid_df["EVOLUCAO"]=="OBITO"]

        self.obitos_covid_df["nome_tratado"] = self.obitos_covid_df["NOME"].apply(lambda x: utils.replace_string(x))
        self.obitos_covid_df["nome_mae_tratado"] = self.obitos_covid_df["NOME_MAE"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.obitos_covid_df["nome hashcode"] = self.obitos_covid_df["NOME"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.obitos_covid_df["nome mae hashcode"] = self.obitos_covid_df["NOME_MAE"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.obitos_covid_df = self.obitos_covid_df.drop_duplicates(subset=["NUMERODO"], keep="first")
        self.obitos_covid_df = self.obitos_covid_df.dropna(subset=["NOME", "NOME_MAE", "DATA_NASCIMENTO"], axis=0, how="any")
        self.obitos_covid_df = self.obitos_covid_df.add_suffix("(OBITO COVID)").rename({"NUMERODO(OBITO COVID)": "numerodo",
                                                                                        "DATA_OBITO(OBITO COVID)": "data_obito(OBITO COVID)",
                                                                                        "DATA_PRI_SINTOMAS_NOVA(OBITO COVID)": "data_pri_sintomas_nova(OBITO COVID)"}, axis=1)
        self.obitos_covid_df["numerodo"] = self.obitos_covid_df["numerodo"].apply(lambda x: x[:8] if not pd.isna(x) else np.nan)
        # PRIMARY KEYS
        self.obitos_covid_df["NOMENASCIMENTOCHAVE"] = self.obitos_covid_df["nome_tratado(OBITO COVID)"].astype(str) + self.obitos_covid_df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
        self.obitos_covid_df["NOMENOMEMAECHAVE"] = self.obitos_covid_df["nome_tratado(OBITO COVID)"].astype(str) + self.obitos_covid_df["nome_mae_tratado(OBITO COVID)"].astype(str)
        self.obitos_covid_df["NOMEMAENASCIMENTOCHAVE"] = self.obitos_covid_df["nome_mae_tratado(OBITO COVID)"].astype(str) + self.obitos_covid_df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
        self.obitos_covid_df["NOMEHASHNASCIMENTOCHAVE"] = self.obitos_covid_df["nome hashcode(OBITO COVID)"].astype(str) + self.obitos_covid_df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
        self.obitos_covid_df["NOMEMAEHASHNASCIMENTOCHAVE"] = self.obitos_covid_df["nome mae hashcode(OBITO COVID)"].astype(str) + self.obitos_covid_df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
        self.obitos_covid_df = self.obitos_covid_df.drop(["nome_tratado(OBITO COVID)", "nome_mae_tratado(OBITO COVID)", "nome hashcode(OBITO COVID)", "nome mae hashcode(OBITO COVID)"], axis=1)

        if save is not None:
            self.obitos_covid_df.to_excel(save, index=False)
        if return_:
            return self.obitos_covid_df
    
    def init_obitos_covid_old(self, return_=False, colnames=None, delimiter=";", encoding="latin"):
        '''
            Description.

            Args:
                return_:
                    Bool.
            Return:
                self.obitos_covid_df:
                    pandas.DataFrame. If return_=True.
        '''
        obitos_covid_path = os.path.join(self.path_to_data["OBITOS COVID-19"], self.name_of_files["OBITOS COVID-19"])
        
        date_fmt = "%d/%m/%Y"
        col_obitos = ["nome_tratado", "nome_mae_tratado", "data_nascimento", "numerodo"]
        date_cols = ["data_nascimento", "data_obito", "data_pri_sintomas_nova", "data_coleta"]
        if colnames is not None:
            colnames = colnames + date_cols
            colnames = list(dict.fromkeys(colnames))
        self.obitos_covid_df = pd.read_csv(obitos_covid_path, usecols=colnames, delimiter=delimiter, encoding=encoding)
        for j in date_cols:
            self.obitos_covid_df[j] = pd.to_datetime(self.obitos_covid_df[j], format=date_fmt, errors="coerce")
        self.obitos_covid_df["nome_tratado"] = self.obitos_covid_df["nome"].apply(lambda x: utils.replace_string(x))
        self.obitos_covid_df["nome_mae_tratado"] = self.obitos_covid_df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.obitos_covid_df = self.obitos_covid_df.dropna(subset=col_obitos, how="any", axis=0)
        self.obitos_covid_df = self.obitos_covid_df.drop_duplicates(subset=["numerodo"], keep="first")
        self.obitos_covid_df = self.obitos_covid_df.add_suffix("(OBITO COVID)").rename({"numerodo(OBITO COVID)": "numerodo"}, axis=1)
        self.obitos_covid_df["numerodo"] = self.obitos_covid_df["numerodo"].apply(lambda x: f"{x:8.0f}".replace(" ","0") if type(x)!=str else np.nan)
        self.obitos_covid_df["NOMENASCIMENTOCHAVE"] = self.obitos_covid_df["nome_tratado(OBITO COVID)"] + self.obitos_covid_df["data_nascimento(OBITO COVID)"].astype(str)
        self.obitos_covid_df["NOMENOMEMAECHAVE"] = self.obitos_covid_df["nome_tratado(OBITO COVID)"] + self.obitos_covid_df["nome_mae_tratado(OBITO COVID)"].astype(str)
        self.obitos_covid_df = self.obitos_covid_df.drop(["nome_tratado(OBITO COVID)", "nome_mae_tratado(OBITO COVID)"], axis=1)

        if return_:
            return self.obitos_covid_df
    
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
        if self.gal_df is None:
            return -1
        # Default columns to define the unique names can be changed with the argument "fields".
        if fields is None:
            fields = ["nome tratado", "Sexo(GAL)", "Data de Nascimento(GAL)"]
        
        # Store all indexes that should be removed from the original table.
        invalid_indexes = []
        df = self.gal_df
        df = df.reset_index()       
        
        indexes_of_person = defaultdict(lambda: [])
        result_of_person = defaultdict(lambda: [])
        # Each unique person generates a string joining info on name, sex and date of birth.
        # For each one, we generate a key in the hash tables below. The dataframe has no NaN
        # values at this point (processed).
        for j in range(0, df.shape[0]):
            compound_string = "".join([str(df[x].iloc[j]) for x in fields])
            indexes_of_person[compound_string].append(j)
            result_of_person[compound_string].append(df["resultado final(GAL)"].iloc[j])
        
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
    
    def init_integrasus(self, init_cohort=dt.date(2021, 1, 21), return_=True):
        '''
            Load and transform IntegraSUS data and find all individuals from records in Vacine Já.
        '''
        testes_integra_path = os.path.join(self.path_to_data["TESTES COVID-19 INTEGRA"], self.name_of_files["TESTES COVID-19 INTEGRA"])
        fname = "base_dados_integrasus_fortaleza_final.dta"
        self.tests_df = pd.read_stata(os.path.join(testes_integra_path, fname), convert_categoricals=False)

        self.tests_df["cpf"] = self.tests_df["cpf"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not pd.isna(x) else np.nan)
        self.tests_df['nome_mae'] = self.tests_df['nome_mae'].apply(lambda x: x if len(x)>0 and not pd.isna(x) else np.nan)
        self.tests_df["nome tratado"] = self.tests_df["nome_paciente"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.tests_df["nome hashcode"] = self.tests_df["nome_paciente"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.tests_df["nome mae tratado"] = self.tests_df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.tests_df["nome mae hashcode"] = self.tests_df["nome_mae"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
        self.tests_df["cns"] = self.tests_df["cns"].apply(lambda x: x if len(x)!=0 or not pd.isna(x) else np.nan)

        # Transform date fields
        self.tests_df["data_nascimento"] = pd.to_datetime(self.tests_df["data_nascimento"], errors="coerce")
        self.tests_df["data_coleta_exame"] = pd.to_datetime(self.tests_df["data_coleta_exame"], errors="coerce")
        self.tests_df["data_inicio_sintomas_nova"] = pd.to_datetime(self.tests_df["data_inicio_sintomas_nova"], errors="coerce")
        self.tests_df["data_internacao_sivep"] = pd.to_datetime(self.tests_df["data_internacao_sivep"], errors="coerce")
        self.tests_df["data_entrada_uti_sivep"] = pd.to_datetime(self.tests_df["data_entrada_uti_sivep"], format="%Y/%m/%d", errors="coerce")
        self.tests_df["data_evolucao_caso_sivep"] = pd.to_datetime(self.tests_df["data_entrada_uti_sivep"], format="%Y/%m/%d", errors="coerce")
        self.tests_df["data_obito"] = pd.to_datetime(self.tests_df["data_obito"], errors="coerce")
        self.tests_df["data_resultado_exame"] = pd.to_datetime(self.tests_df["data_resultado_exame"], errors="coerce")
        self.tests_df["data_solicitacao_exame"] = pd.to_datetime(self.tests_df["data_solicitacao_exame"], errors="coerce")
        self.tests_df["data_saida_uti_sivep"] = pd.to_datetime(self.tests_df["data_saida_uti_sivep"], format="%Y/%m/%d", errors="coerce")

        self.tests_df = self.tests_df.dropna(subset=["nome_paciente", "data_nascimento"], axis=0, how="all")

        # --> Create primary keys for person
        f_key = lambda x: x["cpf"] if not pd.isna(x["cpf"]) else str(x["nome tratado"])+str(x["data_nascimento"])
        self.tests_df["PRIMARY_KEY_PERSON"] = self.tests_df.apply(lambda x: f_key(x), axis=1)
        # --> Define primary keys for linkage: cpf, NOME+DATANASC, NOMEHASH+DATANASC, NOME+NOMEMAE
        self.tests_df["NOMENASCIMENTOCHAVE"] = self.tests_df["nome tratado"] + self.tests_df["data_nascimento"].astype(str)
        self.tests_df["NOMEHASHNASCIMENTOCHAVE"] = self.tests_df["nome hashcode"] + self.tests_df["data_nascimento"].astype(str)
        self.tests_df["NOMEMAENASCIMENTOCHAVE"] = self.tests_df["nome mae tratado"] + self.tests_df["data_nascimento"].astype(str)
        self.tests_df["NOMENOMEMAECHAVE"] = self.tests_df["nome tratado"] + self.tests_df["nome mae tratado"]
        self.tests_df["NOMEMAEHASHNASCIMENTOCHAVE"] = self.tests_df["nome mae hashcode"] + self.tests_df["data_nascimento"].astype(str)

        col_linkage1 = ["id", "cpf"]
        col_linkage2 = ["id", "NOMENASCIMENTOCHAVE"]
        col_linkage3 = ["id", "NOMENOMEMAECHAVE"]
        col_linkage4 = ["id", "NOMEMAENASCIMENTOCHAVE"]
        col_linkage5 = ["id", "NOMEHASHNASCIMENTOCHAVE"]
        col_linkage6 = ["id", "NOMEMAEHASHNASCIMENTOCHAVE"]

        linkage1 = self.tests_df[col_linkage1].dropna(subset=["cpf"], axis=0).merge(self.vacineja_df.add_suffix("(vj)")["cpf(vj)"], left_on="cpf", right_on="cpf(vj)", how="left").dropna(subset=["cpf(vj)"], axis=0)
        linkage2 = self.tests_df[col_linkage2].merge(self.vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]], on="NOMENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0).dropna(subset=["cpf"], axis=0)
        linkage3 = self.tests_df[col_linkage3].dropna(subset=["NOMENOMEMAECHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMENOMEMAECHAVE"]], on="NOMENOMEMAECHAVE", how="left").dropna(subset=["cpf"], axis=0)
        linkage4 = self.tests_df[col_linkage4].dropna(subset=["NOMEMAENASCIMENTOCHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]], on="NOMEMAENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
        linkage5 = self.tests_df[col_linkage5].dropna(subset=["NOMEHASHNASCIMENTOCHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]], on="NOMEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
        linkage6 = self.tests_df[col_linkage6].dropna(subset=["NOMEMAEHASHNASCIMENTOCHAVE"], axis=0).merge(self.vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)

        self.id_to_vacinejacpf = defaultdict(lambda: np.nan)
        self.id_to_vacinejacpf.update(zip(linkage1["id"], linkage1["cpf"]))
        self.id_to_vacinejacpf.update(zip(linkage2["id"], linkage2["cpf"]))
        self.id_to_vacinejacpf.update(zip(linkage3["id"], linkage3["cpf"]))
        self.id_to_vacinejacpf.update(zip(linkage4["id"], linkage4["cpf"]))
        self.id_to_vacinejacpf.update(zip(linkage5["id"], linkage5["cpf"]))
        self.id_to_vacinejacpf.update(zip(linkage6["id"], linkage6["cpf"]))

        self.tests_df["cpf(VACINEJA)"] = self.tests_df["id"].apply(lambda x: self.id_to_vacinejacpf[x])

        # To verify if symptoms and testing (check whether is positive later) were done before cohort.
        self.tests_df["SINTOMAS ANTES COORTE"] = self.tests_df["data_inicio_sintomas_nova"].apply(lambda x: "SIM" if not pd.isna(x) and x<init_cohort else "NAO")
        self.tests_df["SOLICITACAO ANTES COORTE"] = self.tests_df["data_solicitacao_exame"].apply(lambda x: "SIM" if not pd.isna(x) and x<init_cohort else "NAO")
        self.tests_df["COLETA ANTES COORTE"] = self.tests_df["data_coleta_exame"].apply(lambda x: "SIM" if not pd.isna(x) and x<init_cohort else "NAO")
        
        # Now classify each individual as YES or NO depending if the person has positive test before cohort or not.
        self.pos_antes_cohort = defaultdict(lambda:np.nan)
        person_grouped = self.tests_df.groupby("PRIMARY_KEY_PERSON")
        n_records = person_grouped.count()["id"].reset_index()

        one_record_people = n_records[n_records["id"]==1]["PRIMARY_KEY_PERSON"]
        mult_record_people = n_records[n_records["id"]>1]["PRIMARY_KEY_PERSON"]

        # Individuals with a single record
        one_rec_tests = self.tests_df[self.tests_df["PRIMARY_KEY_PERSON"].isin(one_record_people)]
        subcol = ["resultado_final_exame", "SINTOMAS ANTES COORTE", "SOLICITACAO ANTES COORTE", "COLETA ANTES COORTE"]
        one_rec_tests["INFO COORTE SINTOMAS"] = one_rec_tests.apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[1]]=="SIM" else "NAO", axis=1)
        one_rec_tests["INFO COORTE SOLICITACAO"] = one_rec_tests.apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[2]]=="SIM" else "NAO", axis=1)
        one_rec_tests["INFO COORTE COLETA"] = one_rec_tests.apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[3]]=="SIM" else "NAO", axis=1)

        # Individuals with multiple records
        for pkey in mult_record_people:
            sub_df = person_grouped.get_group(pkey)
            for j in range(sub_df.shape[0]):
                #sub_df.
                pass
        
        if return_:
            return self.tests_df

    
    def init_gal_integrasus(self, nrows_gal=None, nrows_integra=None, return_=True):
        '''
            Description.

            Args:
                return_:
                    Bool.
            Return:
                xxx:
        '''
        # --> GAL tests
        testes_gal_path = os.path.join(self.path_to_data["TESTES COVID-19"], self.name_of_files["TESTES COVID-19"])

        date_fmt = "%Y-%m-%d"
        date_cols = ["Data da Solicitação", "Data de Nascimento", "Data de Cadastro", 
                     "Data da Coleta", "Data do Recebimento"]
        
        colnames = ["Index tabela original", "Paciente", "Idade", "Bairro", "Sexo", "Nome da Mãe", 
                    "Municipio do Solicitante","Estado do Solicitante", "Data Notificação Sinan", 
                    "1º Campo Resultado", "Descrição Finalidade", "Agravo Sinan"] + date_cols

        self.gal_df = pd.read_csv(testes_gal_path, usecols=colnames, delimiter=",", encoding="utf-8", nrows=nrows_gal)
        for j in date_cols:
            self.gal_df[j] = pd.to_datetime(self.gal_df[j], format=date_fmt, errors="coerce")
        # GAL: Apply filters
        self.gal_df = self.gal_df[self.gal_df["Municipio do Solicitante"]=="FORTALEZA"]
        # Include tests only for "Covid-19"
        self.gal_df = self.gal_df[self.gal_df["Descrição Finalidade"]=="COVID-19"]
        # Ascending order of "Data da Coleta"
        self.gal_df = self.gal_df.sort_values(by="Data da Solicitação", ascending=True)
        # Modify results column
        self.gal_df["resultado final"] = self.gal_df["1º Campo Resultado"].apply(lambda x: aux.f_resultado(x))
        self.gal_df = self.gal_df.drop("1º Campo Resultado", axis=1)
        # Process the "Sexo" column
        self.gal_df["Sexo"] = self.gal_df["Sexo"].apply(lambda x: aux.f_sexo_gal(x))
        # Process names in "Paciente" column
        fields_for_nan = ["Paciente", "Nome da Mãe", "Sexo", "Data de Nascimento"]
        self.gal_df = self.gal_df.dropna(subset=fields_for_nan, axis=0)
        self.gal_df["nome tratado"] = self.gal_df["Paciente"].apply(lambda x: utils.replace_string(x))
        self.gal_df["nome mae tratado"] = self.gal_df["Nome da Mãe"].apply(lambda x: utils.replace_string(x))
        self.gal_df = self.gal_df.add_suffix("(GAL)").rename({"nome tratado(GAL)": "nome tratado", 
                                                              "nome mae tratado(GAL)": "nome mae tratado"}, axis=1)
        # Remove duplicates (Taking into account that we cannot lose positive tests between who got several tests)
        invalid_indexes = self.remove_duplicates()
        self.gal_df = self.gal_df.reset_index()
        self.gal_df = self.gal_df.drop(invalid_indexes)

        # --> INTEGRASUS tests
        testes_integra_path = os.path.join(self.path_to_data["TESTES COVID-19 INTEGRA"], self.name_of_files["TESTES COVID-19 INTEGRA"])
        
        date_fmt = "%Y-%m-%d"
        date_cols = ["data_nascimento", "data_notificacao", "data_inicio_sintomas_nova", "data_solicitacao_exame"]
        colnames = ["nome_paciente", "idade_anos", "bairro_ajustado", "sexo_paciente", "nome_mae",
                    "municipio_paciente", "fx_etaria", "data_resultado_exame", "resultado_final_exame"] + date_cols

        self.integra_df = pd.read_csv(testes_integra_path, usecols=colnames, delimiter=",", encoding="utf-8", nrows=nrows_integra)
        for j in date_cols:
            self.integra_df[j] = pd.to_datetime(self.integra_df[j], format=date_fmt, errors="coerce")
        self.integra_df["sexo_paciente"] = self.integra_df["sexo_paciente"].apply(lambda x: aux.f_sexo_integra(x))
        # Ascending order of "Data da Solicitação"
        self.integra_df = self.integra_df.sort_values(by="data_solicitacao_exame", ascending=True)
        # Modify results column
        self.integra_df["resultado final"] = self.integra_df["resultado_final_exame"].apply(lambda x: aux.f_resultado_integra(x))
        # Consider only the positive cases.
        self.integra_df = self.integra_df[self.integra_df["resultado final"]=="POSITIVO"]
        # Process names in "Paciente" column
        #self.integra_df = self.integra_df.dropna(subset=fields_for_nan, axis=0)
        self.integra_df["nome tratado"] = self.integra_df["nome_paciente"].apply(lambda x: utils.replace_string(x))
        self.integra_df["nome mae tratado"] = self.integra_df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
        self.integra_df = self.integra_df.dropna(subset=["nome tratado", "data_nascimento", "sexo_paciente"], how="any")
        self.integra_df = self.integra_df.add_suffix("(INTEGRASUS)").rename({"nome tratado(INTEGRASUS)": "nome tratado",
                                                                             "nome mae tratado(INTEGRASUS)": "nome mae tratado"}, axis=1)
        
        if return_:
            return (self.gal_df, self.integra_df)

    # --> JOIN OPERATIONS
    def join_vacineja_vacinados(self, return_=True):
        '''
            Description.

            Args:
                return_:
                    Bool.
            Return:
                self.vacineja_df:
                    pandas.DataFrame. If return_=True.
        '''
        # Check if the two datasets exists
        if self.vacineja_df is not None and self.vacinados_df is not None:
            remove_cols = ["nome(VACINADOS)", "sexo(VACINADOS)", "data nascimento(VACINADOS)", "nome tratado(VACINADOS)"]
            self.vacineja_df = self.vacineja_df.merge(self.vacinados_df.drop(remove_cols, axis=1), on="cpf", how="left")
        else:
            return -1
        
        if return_:
            return self.vacineja_df

    def join_obitos(self, return_=True, cur_date=dt.date(2021, 9, 2)):
        '''
            Description.

            Args:
                return_:
                    Bool.
                cur_date:
                    datetime.date.
            Return:
                self.vacineja_df:
                    pandas.DataFrame if return_=True. 
        '''
        if self.obitos_covid_df is not None and self.obitos_cartorios_df is not None:
            # --> Linkage to found deaths from COVID-19 in 'Vacine Já'
            col_include = ["cpf", "do_8"]
            self.obitos_covid_df = self.obitos_covid_df.merge(self.obitos_cartorios_df[col_include], left_on="numerodo", right_on="do_8", how="left")

            # ----> Columns for each linkage.
            cols_obito1 = ["cpf", "data_obito(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)", "do_8"]
            cols_obito2 = ["NOMENASCIMENTOCHAVE", "data_obito(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)", "do_8"]
            cols_obito3 = ["NOMENOMEMAECHAVE", "data_obito(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)", "do_8"]
            cols_obito4 = ["NOMEMAENASCIMENTOCHAVE", "data_obito(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)", "do_8"]
            cols_obito5 = ["NOMEHASHNASCIMENTOCHAVE", "data_obito(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)", "do_8"]
            cols_obito6 = ["NOMEMAEHASHNASCIMENTOCHAVE", "data_obito(OBITO COVID)", "data_pri_sintomas_nova(OBITO COVID)", "do_8"]

            linkage_1 = self.vacineja_df[["cpf"]].merge(self.obitos_covid_df[cols_obito1], on="cpf", how="left")
            linkage_2 = self.vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]].merge(self.obitos_covid_df[cols_obito2], on="NOMENASCIMENTOCHAVE", how="left")
            linkage_3 = self.vacineja_df[["cpf", "NOMENOMEMAECHAVE"]].merge(self.obitos_covid_df[cols_obito3], on="NOMENOMEMAECHAVE", how="left")
            linkage_4 = self.vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]].merge(self.obitos_covid_df[cols_obito4], on="NOMEMAENASCIMENTOCHAVE", how="left")
            linkage_5 = self.vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]].merge(self.obitos_covid_df[cols_obito5], on="NOMEHASHNASCIMENTOCHAVE", how="left")
            linkage_6 = self.vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]].merge(self.obitos_covid_df[cols_obito6], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left")

            linkage_2 = linkage_2.drop("NOMENASCIMENTOCHAVE", axis=1)
            linkage_3 = linkage_3.drop("NOMENOMEMAECHAVE", axis=1)
            linkage_4 = linkage_4.drop("NOMEMAENASCIMENTOCHAVE", axis=1)
            linkage_5 = linkage_5.drop("NOMEHASHNASCIMENTOCHAVE", axis=1)
            linkage_6 = linkage_6.drop("NOMEMAEHASHNASCIMENTOCHAVE", axis=1)
            
            obito_str = "data_obito(OBITO COVID)"
            found = pd.concat([linkage_1[pd.notna(linkage_1[obito_str])], linkage_2[pd.notna(linkage_2[obito_str])], 
                               linkage_3[pd.notna(linkage_3[obito_str])], linkage_4[pd.notna(linkage_4[obito_str])],
                               linkage_5[pd.notna(linkage_5[obito_str])], linkage_6[pd.notna(linkage_6[obito_str])]], axis=0)
            found = found.drop_duplicates(subset=["cpf"], keep="first")
            self.vacineja_df = self.vacineja_df.merge(found, on="cpf", how="left")
            self.vacineja_df = self.vacineja_df.drop_duplicates(subset=["cpf"], keep="first")
            
            # --> Linkage to found deaths due to any cause in 'Vacine Já'
            cols_obito1 = ["cpf", "data falecimento(CARTORIOS)", "do_8"]
            cols_obito2 = ["NOMENASCIMENTOCHAVE", "data falecimento(CARTORIOS)", "do_8"]
            cols_obito3 = ["NOMENOMEMAECHAVE", "data falecimento(CARTORIOS)", "do_8"]
            cols_obito4 = ["NOMEMAENASCIMENTOCHAVE", "data falecimento(CARTORIOS)", "do_8"]
            cols_obito5 = ["NOMEHASHNASCIMENTOCHAVE", "data falecimento(CARTORIOS)", "do_8"]
            cols_obito6 = ["NOMEMAEHASHNASCIMENTOCHAVE", "data falecimento(CARTORIOS)", "do_8"]

            linkage_1 = self.vacineja_df[["cpf"]].merge(self.obitos_cartorios_df[cols_obito1], on="cpf", how="left")
            linkage_2 = self.vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]].merge(self.obitos_cartorios_df[cols_obito2], on="NOMENASCIMENTOCHAVE", how="left")
            linkage_3 = self.vacineja_df[["cpf", "NOMENOMEMAECHAVE"]].merge(self.obitos_cartorios_df[cols_obito3], on="NOMENOMEMAECHAVE", how="left")
            linkage_4 = self.vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]].merge(self.obitos_cartorios_df[cols_obito4], on="NOMEMAENASCIMENTOCHAVE", how="left")
            linkage_5 = self.vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]].merge(self.obitos_cartorios_df[cols_obito5], on="NOMEHASHNASCIMENTOCHAVE", how="left")
            linkage_6 = self.vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]].merge(self.obitos_cartorios_df[cols_obito6], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left")

            linkage_2 = linkage_2.drop("NOMENASCIMENTOCHAVE", axis=1)
            linkage_3 = linkage_3.drop("NOMENOMEMAECHAVE", axis=1)
            linkage_4 = linkage_4.drop("NOMEMAENASCIMENTOCHAVE", axis=1)
            linkage_5 = linkage_5.drop("NOMEHASHNASCIMENTOCHAVE", axis=1)
            linkage_6 = linkage_6.drop("NOMEMAEHASHNASCIMENTOCHAVE", axis=1)

            obito_str = "data falecimento(CARTORIOS)"
            found = pd.concat([linkage_1[pd.notna(linkage_1[obito_str])], linkage_2[pd.notna(linkage_2[obito_str])], 
                               linkage_3[pd.notna(linkage_3[obito_str])], linkage_4[pd.notna(linkage_4[obito_str])],
                               linkage_5[pd.notna(linkage_5[obito_str])], linkage_6[pd.notna(linkage_6[obito_str])]], axis=0)
            found = found.drop_duplicates(subset=["cpf"], keep="first")
            self.vacineja_df = self.vacineja_df.merge(found, on="cpf", how="left", suffixes=("","(CARTORIOS)"))
            self.vacineja_df = self.vacineja_df.drop_duplicates(subset=["cpf"], keep="first")
            
            # REMOVE INITIAL COLUMNS FOR CUSTOM PRIMARY KEYS
            self.vacineja_df = self.vacineja_df.drop(['NOMENASCIMENTOCHAVE','NOMENOMEMAECHAVE','NOMEMAENASCIMENTOCHAVE','NOMEHASHNASCIMENTOCHAVE','NOMEMAEHASHNASCIMENTOCHAVE'], axis=1)
            
            sbst = ["data D1(VACINADOS)", "data D2(VACINADOS)", "data D3(VACINADOS)", "data D4(VACINADOS)", "data_obito(OBITO COVID)"]
            self.vacineja_df["OBITO INCONSISTENCIA COVID"] = self.vacineja_df[sbst].apply(lambda x: aux.f_vaccination_death(x[sbst[4]], x[sbst[0]], x[sbst[1]], x[sbst[2]], x[sbst[3]]), axis=1)
            sbst = ["data D1(VACINADOS)", "data D2(VACINADOS)", "data D3(VACINADOS)", "data D4(VACINADOS)", "data falecimento(CARTORIOS)"]
            self.vacineja_df["OBITO INCONSISTENCIA CARTORIOS"] = self.vacineja_df[sbst].apply(lambda x: aux.f_vaccination_death(x[sbst[4]], x[sbst[0]], x[sbst[1]], x[sbst[2]], x[sbst[3]]), axis=1)
            if return_:
                return self.vacineja_df
        else:
            return -1

    def join_GAL_integraSUS(self, return_=True):
        '''

        '''
        gal_cols = ["nome tratado", "Sexo(GAL)", "Data de Nascimento(GAL)", "nome mae tratado", "resultado final(GAL)"]
        integra_cols = ["nome tratado", "sexo_paciente(INTEGRASUS)", "data_nascimento(INTEGRASUS)", "nome mae tratado", "resultado final(INTEGRASUS)"]
        gal_df = self.gal_df[gal_cols]
        integra_df = self.integra_df[integra_cols]

        integra_df = integra_df[integra_df["resultado final(INTEGRASUS)"]=="POSITIVO"]
        key_string_integra = defaultdict(lambda: -1)
        for j in range(0, integra_df.shape[0]):
            nome_integra = integra_df[integra_cols[0]].iloc[j]
            nasc_integra = integra_df[integra_cols[2]].iloc[j]
            nasc_integra_dt = nasc_integra.date()

            unique_string = "".join([nome_integra,f"{nasc_integra_dt.year}-{nasc_integra_dt.month}-{nasc_integra_dt.day}"])
            key_string_integra[unique_string] = integra_df["resultado final(INTEGRASUS)"].iloc[j]

        new_col = []
        for j in range(0, gal_df.shape[0]):
            nome_gal = gal_df[gal_cols[0]].iloc[j]
            nasc_gal = gal_df[gal_cols[2]].iloc[j]
            nasc_gal_dt = nasc_gal.date()

            unique_string = "".join([nome_gal, f"{nasc_gal_dt.year}-{nasc_gal_dt.month}-{nasc_gal_dt.day}"])
            if key_string_integra[unique_string]!=-1:
                new_col.append(key_string_integra[unique_string])
            else:
                new_col.append("OUTRO")
        self.gal_df["RESULTADO FINAL INTEGRASUS"] = new_col
        subset_col = ["resultado final(GAL)", "RESULTADO FINAL INTEGRASUS"]
        f = lambda x: "POSITIVO" if x[subset_col[0]]=="POSITIVO" or x[subset_col[1]]=="POSITIVO" else "OUTRO"
        self.gal_df["RESULTADO FINAL GAL-INTEGRASUS"] = self.gal_df[subset_col].apply(f, axis=1)

        if return_:
            return self.gal_df

    def join_vacineja_galintegra(self, return_=True):
        '''
            Description.

            Args:
                return_:
                    Bool.
            Return:
                self.vacineja_df:
                    pandas.DataFrame. If return_=True
        '''
        if "NOMENASCIMENTOCHAVE" not in self.vacineja_df.columns or "NOMENOMEMAECHAVE" not in self.vacineja_df.columns:
            self.vacineja_df["NOMENASCIMENTOCHAVE"] = self.vacineja_df["nome tratado"]+self.vacineja_df["data_nascimento"].astype(str)
            self.vacineja_df["NOMENOMEMAECHAVE"] = self.vacineja_df["nome tratado"]+self.vacineja_df["nome mae tratado"]

        # Filter NaN values from the columns used to create primary keys -> then, create the keys.
        col_testes = ["nome tratado", "nome mae tratado", "Data de Nascimento(GAL)"]
        self.gal_df = self.gal_df.dropna(subset=col_testes, how="any", axis=0)
        self.gal_df["NOMENASCIMENTOCHAVE"] = self.gal_df["nome tratado"] + self.gal_df["Data de Nascimento(GAL)"].astype(str)
        self.gal_df["NOMENOMEMAECHAVE"] = self.gal_df["nome tratado"] + self.gal_df["nome mae tratado"]

        # Create two dataframes holding the JOIN over the two primary keys, and then join them to the original "Vacine Já" database. 
        col_testes = ["NOMENASCIMENTOCHAVE", "Data da Solicitação(GAL)", "Data da Coleta(GAL)", "RESULTADO FINAL GAL-INTEGRASUS"]
        name_birth_cols = self.vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]].merge(self.gal_df[col_testes], on="NOMENASCIMENTOCHAVE", how="left")
        col_testes = ["NOMENOMEMAECHAVE", "Data da Solicitação(GAL)", "Data da Coleta(GAL)", "RESULTADO FINAL GAL-INTEGRASUS"]
        name_mother_cols = self.vacineja_df[["cpf", "NOMENOMEMAECHAVE"]].merge(self.gal_df[col_testes], on="NOMENOMEMAECHAVE", how="left")
        name_birth_cols = name_birth_cols.drop("NOMENASCIMENTOCHAVE", axis=1)
        name_mother_cols = name_mother_cols.drop("NOMENOMEMAECHAVE", axis=1)
        name_birth_cols = name_birth_cols.add_suffix("(TESTE_NASC)").rename({"cpf(TESTE_NASC)": "cpf"}, axis=1)
        name_mother_cols = name_mother_cols.add_suffix("(TESTE_MAE)").rename({"cpf(TESTE_MAE)": "cpf"}, axis=1)
        
        self.vacineja_df = self.vacineja_df.merge(name_birth_cols, on="cpf", how="left")
        self.vacineja_df = self.vacineja_df.merge(name_mother_cols, on="cpf", how="left")

        sbst1 = ["Data da Solicitação(GAL)(TESTE_NASC)", "Data da Solicitação(GAL)(TESTE_MAE)"]
        self.vacineja_df["Data da Solicitação(GAL)"] = self.vacineja_df[sbst1].apply(lambda x: aux.f_testes(x[sbst1[0]], x[sbst1[1]]), axis=1)
        sbst2 = ["Data da Coleta(GAL)(TESTE_NASC)", "Data da Coleta(GAL)(TESTE_MAE)"]
        self.vacineja_df["Data da Coleta(GAL)"] = self.vacineja_df[sbst2].apply(lambda x: aux.f_testes(x[sbst2[0]], x[sbst2[1]]), axis=1)
        self.vacineja_df = self.vacineja_df.drop(sbst1+sbst2, axis=1)

        sbst1 = ["RESULTADO FINAL GAL-INTEGRASUS(TESTE_NASC)", "RESULTADO FINAL GAL-INTEGRASUS(TESTE_MAE)"]
        f_temp = lambda x: "POSITIVO" if x[sbst1[0]]=="POSITIVO" or x[sbst1[1]]=="POSITIVO" else "OUTRO"
        self.vacineja_df["RESULTADO FINAL GAL-INTEGRASUS"] = self.vacineja_df[sbst1].apply(f_temp, axis=1)
        self.vacineja_df = self.vacineja_df.drop(sbst1, axis=1)

        if return_:
            return self.vacineja_df

    
    

