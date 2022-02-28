import os
import numpy as np
import pandas as pd
import datetime as dt
import lib.utils as utils

def data_hash(vacinas_foldername="vacinado_obito", 
              testes_foldername="testes_covid19",
              hospital_foldername="hospitalizacao_covid19",
              obitos_foldername="obitos_covid19"):
    '''
        Returns two dictionaries: the first containing the absolute path to the folders of
        each one of the databases used for the analysis of the project. The second containing
        the name of the files of each one of the files. Both dictionaries are accessed through
        the same keys.

        Args:
            vacinas_foldername:
            testes_foldername:
            hospital_foldername:
            obitos_foldername:
        Return:
            loc_data:
                Tuple containing the two dictionaries.
    '''
    data_folder = os.path.join("Documents", "data")
    init_folder = os.environ["USERPROFILE"]
    loc_names = {
        "VACINACAO POR PESSOA": os.path.abspath(os.path.join(init_folder, data_folder, vacinas_foldername)),
        "VACINACAO CADASTRO (VACINE JA)": os.path.abspath(os.path.join(init_folder, data_folder, vacinas_foldername)),
        "TESTES COVID-19": os.path.abspath(os.path.join(init_folder, data_folder, testes_foldername)),
        "TESTES COVID-19 INTEGRA": os.path.abspath(os.path.join(init_folder, data_folder, testes_foldername)),
        "OBITOS COVID-19": os.path.abspath(os.path.join(init_folder, data_folder, obitos_foldername)),
        "HOSPITALIZACAO COVID-19": os.path.abspath(os.path.join(init_folder, data_folder, hospital_foldername)),
        "VACINAS APLICADAS": os.path.abspath(os.path.join(init_folder, data_folder, vacinas_foldername))
    }
    data_names = {
        "VACINACAO POR PESSOA": "vacinados_d1d2_update_Sep2_2021.csv",
        "VACINACAO CADASTRO (VACINE JA)": "cadastrados_vacineja_202110111044.csv",
        "TESTES COVID-19": "testes_gal_JAN_AGO2021.csv",
        "TESTES COVID-19 INTEGRA": "base_dados_integrasus_fortaleza_final.csv",
        "OBITOS COVID-19": "base_dados_obitos_cevepi.csv",
        "HOSPITALIZACAO COVID-19": "hospitalizados_covid19.csv",
        "VACINAS APLICADAS": "VACINADOS2Sep2021.csv"
    }
    loc_data = (loc_names, data_names)
    return loc_data

def print_filenames(vacinas_foldername="vacinado_obito", 
                    testes_foldername="testes_covid19",
                    hospital_foldername="hospitalizacao_covid19",
                    obitos_foldername="obitos_covid19"):
    '''
        Print all files contained in the folder holding the main databases.
    '''
    data_folder = os.path.join("Documents", "data")
    vacinacao_folder = os.path.abspath(os.path.join(os.environ["USERPROFILE"], data_folder, vacinas_foldername))
    testes_folder = os.path.abspath(os.path.join(os.environ["USERPROFILE"], data_folder, testes_foldername))
    obitos_folder = os.path.abspath(os.path.join(os.environ["USERPROFILE"], data_folder, obitos_foldername))
    hospitalizacao_folder = os.path.abspath(os.path.join(os.environ["USERPROFILE"], data_folder, hospital_foldername))

    # -----> Print all files
    print("Vacinas - Covid-19:")
    print(os.listdir(vacinacao_folder))
    print("Testes - Covid-19:")
    print(os.listdir(testes_folder))
    print("Óbitos - Covid-19:")
    print(os.listdir(obitos_folder))
    print("Hospitalizacao - Covid-19:")
    print(os.listdir(hospitalizacao_folder))

def open_vacinados(vacinados_fname, colnames=None, delimiter=",", encoding="utf-8", nrows=None, date_fmt="%Y-%m-%d"):
    '''
    
    '''
    date_cols = ["data D1", "data D2", "data nascimento"]
    if colnames is not None:
        colnames = colnames + date_cols
        colnames = list(dict.fromkeys(colnames))
    # Open dataset considering only a subset of columns
    # return the DataFrame
    df = pd.read_csv(vacinados_fname, usecols=colnames, nrows=nrows, delimiter=delimiter, encoding=encoding)
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def open_vacinas(vacinas_fname, colnames=None, delimiter=",", encoding="utf-8", nrows=None, date_fmt="%d/%m/%Y"):
    '''
    
    '''
    date_cols = ["data_aplicacao_ajustada", "data_nascimento"]
    if colnames is not None:
        colnames = colnames + date_cols

    df = pd.read_csv(vacinas_fname, usecols=colnames, nrows=nrows, dtype={"cpf_usuario": str}, delimiter=delimiter, encoding=encoding)
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def open_vacineja(vacineja_fname, colnames=None, delimiter=",", encoding="utf-8", nrows=None, date_fmt="%Y-%m-%d"):
    '''
    
    '''
    date_cols = ["data_nascimento", "created_at"]
    if colnames is not None:
        colnames = colnames + date_cols
        colnames = list(dict.fromkeys(colnames))

    df = pd.read_csv(vacineja_fname, usecols=colnames, nrows=nrows, delimiter=delimiter, encoding=encoding)
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def open_testes_gal(testes_fname, colnames=None, delimiter=",", encoding="utf-8", date_fmt="%Y-%m-%d", nrows=None):
    '''
    
    '''
    date_cols = ["Data da Solicitação", "Data de Nascimento", "Data de Cadastro", "Data da Coleta", "Data do Recebimento"]
    if colnames is not None:
        colnames = colnames + date_cols
        colnames = list(dict.fromkeys(colnames))
    
    df = pd.read_csv(testes_fname, usecols=colnames, delimiter=delimiter, encoding=encoding, index_col=0, nrows=nrows)
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def open_testes_integra(testes_fname, colnames=None, delimiter=",", encoding="utf-8", date_fmt="%Y-%m-%d", nrows=None):
    '''
    
    '''
    date_cols = ["data_nascimento", "data_notificacao", "data_inicio_sintomas_nova", "data_solicitacao_exame"]
    if colnames is not None:
        colnames = colnames + date_cols
        colnames = list(dict.fromkeys(colnames))

    df = pd.read_csv(testes_fname, usecols=colnames, delimiter=delimiter, encoding=encoding, nrows=nrows)
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def open_obitos(obitos_fname, colnames=None, delimiter=";", encoding="latin", date_fmt="%d/%m/%Y"):
    '''
    
    '''
    # Open dataset considering only a subset of columns
    # return the DataFrame
    df = pd.read_csv(obitos_fname, delimiter=delimiter, usecols=colnames, encoding=encoding, index_col=0, dtype={"numerodo": str})
    df["nome_tratado"] = df["nome"].apply(lambda x: utils.replace_string(x))
    df["nome_mae_tratado"] = df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    
    date_cols = ["data_nascimento", "data_obito", "data_pri_sintomas_nova", "data_coleta"]
    if colnames is not None:
        colnames = colnames + date_cols
        colnames = list(dict.fromkeys(colnames))
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def open_hospitalizacao(hospitalizacao_fname, colnames=None, delimiter=",", encoding="utf-8", date_fmt="%d/%m/%Y"):
    '''
    
    '''
    # Open dataset considering only a subset of columns
    # retuirn the DataFrame
    date_cols = ["DT_INTERNA", "DT_NOTIFIC", "DT_SIN_PRI", "DT_NASC", "DT_ENCERRA"]
    if colnames is not None:
        colnames = colnames + date_cols
        colnames = list(dict.fromkeys(colnames))

    df = pd.read_csv(hospitalizacao_fname, delimiter=delimiter, usecols=colnames, encoding=encoding)
    df["nome_tratado_hosp"] = df["NM_PACIENT"].apply(lambda x: utils.replace_string(x))
    df["nome_mae_tratado"] = df["NM_MAE_PAC"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["DT_INTERNA"] = pd.to_datetime(df["DT_INTERNA"], errors="coerce", format="%Y-%m-%d")
    for j in date_cols[1:]:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    return df

def obtain_eligible_exposed(vacinados_df, testes_df, vaccine="CORONAVAC"):
    '''
        Get all the persons who were vaccinated between the start and the end of 
        the cohort study. Also, include other criteria, like age, PCR tests, health
        workers, etc.

        Args:
            vacinados_df:
            testes_df:
        Return:
            
    '''
    start_date = dt.date(2021, 1, 21)
    final_date = dt.date(2021, 7, 31)
    age_threshold = 16
    # Include more conditions
    # Open the dataset - banco de vacinação Covid-19
    # Open the dataset - banco de testes Covid-19
    # Apply all the eligibility criteria
    
    # Create a DataFrame containing some primary key such that we can 
    # recover the subset from the original DataFrames
    pass

def obtain_matching(exposed_df, vacinados_df, testes_df, vaccine="CORONAVAC"):
    '''
        Starting from the list of the exposed group, select all the control persons
        from the unexposed group to define the matching for each exposed person.

        Args:
            exposed_df:
            vacinados_df:
            testes_df:
        Return:
            control_df:
    '''
    pass

def generate_testes_df(output=None, return_=False):
    '''
        GAL files are dispersed by each month. This function join all these tables
        into a single one containing all the COVID-19 tests.
    '''
    loc, data = data_hash()
    month_lst = ["janeiro_2021", "fevereiro_2021", "marco_2021", "abril_2021", "maio_2021",
                 "junho_2021", "julho_2021", "agosto_2021"]
    
    all_tables = []
    for fname in month_lst:
        path = os.path.join(loc["TESTES COVID-19"], fname+".csv")
        df = pd.read_csv(path, index_col=0)
        all_tables.append(df)
    new_df = pd.concat(all_tables)
    new_df.reset_index(inplace=True)
    new_df.rename({"index": "Index tabela original"}, axis=1, inplace=True)

    if output is not None:
        new_df.to_csv(output)
    if return_:
        return new_df

def test_functions():
    '''
    
    '''
    loc, data = data_hash()
    vacinados_fname = os.path.abspath(os.path.join(loc["VACINACAO POR PESSOA"], data["VACINACAO POR PESSOA"]))
    obitos_fname = os.path.abspath(os.path.join(loc["OBITOS COVID-19"], data["OBITOS COVID-19"]))
    df = open_vacinados(vacinados_fname)
    return df




