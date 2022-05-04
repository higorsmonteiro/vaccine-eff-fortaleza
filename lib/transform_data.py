import numpy as np
import pandas as pd
import lib.utils as utils
import lib.aux_utils as aux
from collections import defaultdict

def transform_vacineja(df):
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
    date_fmt = "%Y-%m-%d"
    date_cols = ["data_nascimento", "created_at"]

    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
    
    # More specific filters and transformations
    df = df.dropna(subset=["cpf", "data_nascimento"], axis=0)
    # Include only the individuals with male and female sex (to avoid the problem of before)
    df = df[df["sexo"].isin(["M", "F"])]
    df["cpf"] = df["cpf"].apply(lambda x:f"{x:11.0f}".replace(" ","0") if type(x)!=str else np.nan)
    df = df.drop_duplicates(subset=["cpf"], keep="first")
    df['cns'] = df['cns'].apply(lambda x: f'{x}')
    #df = df[df["cidade"]=="FORTALEZA"]
    #df = df.drop("cidade", axis=1)

    # --> Create custom primary keys to complement the linkage by 'cpf' with the other databases
    df["nome tratado"] = df["nome"].apply(lambda x: utils.replace_string(x))
    df["nome mae tratado"] = df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome hashcode"] = df["nome"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["nome mae hashcode"] = df["nome_mae"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    # ----> CUSTOM PRIMARY KEYS
    df["NOMENASCIMENTOCHAVE"] = df["nome tratado"].astype(str)+df["data_nascimento"].astype(str)
    df["NOMENOMEMAECHAVE"] = df["nome tratado"].astype(str)+df["nome mae tratado"].astype(str)
    df["NOMEMAENASCIMENTOCHAVE"] = df["nome mae tratado"].astype(str) + df["data_nascimento"].astype(str)
    df["NOMEHASHNASCIMENTOCHAVE"] = df["nome hashcode"].astype(str) + df["data_nascimento"].astype(str)
    df["NOMEMAEHASHNASCIMENTOCHAVE"] = df["nome mae hashcode"].astype(str) + df["data_nascimento"].astype(str)
    df = df.drop(["nome tratado", "nome mae tratado", "nome hashcode", "nome mae hashcode"], axis=1)

    return df

def transform_vacinados(df):
    '''
    
    '''
    date_fmt = "%Y-%m-%d"
    date_cols = ["data D1", "data D2", "data D3", "data D4", "data nascimento"]
    
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
        
    # Apply filters
    # Remove records with the fields "cpf_usuario" and "data_nascimento" missing.
    df = df.dropna(subset=["cpf","data nascimento","sexo","vacina"], how="any", axis=0)
    # Format the field of "grupo prioritario" to reduce the rows with compound information.
    df["grupo prioritario"] = df["grupo prioritario"].apply(lambda x: x.split("-")[0] if not pd.isna(x) else x)
    # Process the CPF field
    df["cpf"] = df["cpf"].astype(float, errors="ignore")
    df["cpf"] = df["cpf"].apply(lambda x:f"{x:11.0f}".replace(" ","0") if type(x)!=str else np.nan)
    df = df.drop_duplicates(subset=["cpf"], keep="first")
    # Process the names of each person
    # Find all persons having an inconsistent set of vaccination dates.
    subset = ["data D1", "data D2"]
    df["data aplicacao consistente"] = df[subset].apply(lambda x: aux.f_d1d2(x["data D1"], x["data D2"]), axis=1)

    # --> Create custom primary keys to complement the linkage by 'cpf' with the other databases
    df["nome tratado"] = df["nome"].apply(lambda x: utils.replace_string(x))
    df["nome mae tratado"] = df["nome mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome hashcode"] = df["nome"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["nome mae hashcode"] = df["nome mae"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df = df.add_suffix("(VACINADOS)")
    # ----> CUSTOM PRIMARY KEYS
    df["NOMENASCIMENTOCHAVE"] = df["nome tratado(VACINADOS)"].astype(str)+df["data nascimento(VACINADOS)"].astype(str)
    df["NOMENOMEMAECHAVE"] = df["nome tratado(VACINADOS)"].astype(str)+df["nome mae tratado(VACINADOS)"].astype(str)
    df["NOMEMAENASCIMENTOCHAVE"] = df["nome mae tratado(VACINADOS)"].astype(str) + df["data nascimento(VACINADOS)"].astype(str)
    df["NOMEHASHNASCIMENTOCHAVE"] = df["nome hashcode(VACINADOS)"].astype(str) + df["data nascimento(VACINADOS)"].astype(str)
    df["NOMEMAEHASHNASCIMENTOCHAVE"] = df["nome mae hashcode(VACINADOS)"].astype(str) + df["data nascimento(VACINADOS)"].astype(str)
    df = df.drop(["nome tratado(VACINADOS)", "nome mae tratado(VACINADOS)", "nome hashcode(VACINADOS)", "nome mae hashcode(VACINADOS)"], axis=1)
    return df

def transform_integrasus(df, init_cohort):
    '''
    
    '''
    df["cpf"] = df["cpf"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not pd.isna(x) else np.nan)
    df['nome_mae'] = df['nome_mae'].apply(lambda x: x if len(x)>0 and not pd.isna(x) else np.nan)
    df["nome tratado"] = df["nome_paciente"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome hashcode"] = df["nome_paciente"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["nome mae tratado"] = df["nome_mae"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome mae hashcode"] = df["nome_mae"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["cns"] = df["cns"].apply(lambda x: x if len(x)>0 or not pd.isna(x) else "INDETERMINADO")

    # Transform date fields
    df["data_nascimento"] = pd.to_datetime(df["data_nascimento"], errors="coerce")
    df["data_coleta_exame"] = pd.to_datetime(df["data_coleta_exame"], errors="coerce")
    df["data_inicio_sintomas_nova"] = pd.to_datetime(df["data_inicio_sintomas_nova"], errors="coerce")
    df["data_internacao_sivep"] = pd.to_datetime(df["data_internacao_sivep"], errors="coerce")
    df["data_entrada_uti_sivep"] = pd.to_datetime(df["data_entrada_uti_sivep"], format="%Y/%m/%d", errors="coerce")
    df["data_evolucao_caso_sivep"] = pd.to_datetime(df["data_entrada_uti_sivep"], format="%Y/%m/%d", errors="coerce")
    df["data_obito"] = pd.to_datetime(df["data_obito"], errors="coerce")
    df["data_resultado_exame"] = pd.to_datetime(df["data_resultado_exame"], errors="coerce")
    df["data_solicitacao_exame"] = pd.to_datetime(df["data_solicitacao_exame"], errors="coerce")
    df["data_saida_uti_sivep"] = pd.to_datetime(df["data_saida_uti_sivep"], format="%Y/%m/%d", errors="coerce")
    df = df.dropna(subset=["nome_paciente", "data_nascimento"], axis=0, how="all")

    # --> Create primary keys for person
    f_key = lambda x: x["cpf"] if not pd.isna(x["cpf"]) else str(x["nome tratado"])+str(x["data_nascimento"])
    df["PRIMARY_KEY_PERSON"] = df.apply(lambda x: f_key(x), axis=1)
    # --> Define primary keys for linkage: cpf, NOME+DATANASC, NOMEHASH+DATANASC, NOME+NOMEMAE
    df["NOMENASCIMENTOCHAVE"] = df["nome tratado"] + df["data_nascimento"].astype(str)
    df["NOMEHASHNASCIMENTOCHAVE"] = df["nome hashcode"] + df["data_nascimento"].astype(str)
    df["NOMEMAENASCIMENTOCHAVE"] = df["nome mae tratado"] + df["data_nascimento"].astype(str)
    df["NOMENOMEMAECHAVE"] = df["nome tratado"] + df["nome mae tratado"]
    df["NOMEMAEHASHNASCIMENTOCHAVE"] = df["nome mae hashcode"] + df["data_nascimento"].astype(str)
    
    # To verify if symptoms and testing were done before cohort (check whether test is positive later).
    df["SINTOMAS ANTES COORTE"] = df["data_inicio_sintomas_nova"].apply(lambda x: "SIM" if pd.notna(x) and x<init_cohort else "NAO")
    df["SOLICITACAO ANTES COORTE"] = df["data_solicitacao_exame"].apply(lambda x: "SIM" if pd.notna(x) and x<init_cohort else "NAO")
    df["COLETA ANTES COORTE"] = df["data_coleta_exame"].apply(lambda x: "SIM" if pd.notna(x) and x<init_cohort else "NAO")
    
    # Now classify each individual as YES or NO depending if the person has positive test before cohort or not.
    pos_antes_cohort_sint, pos_antes_cohort_sol, pos_antes_cohort_col = defaultdict(lambda:"NAO"), defaultdict(lambda: "NAO"), defaultdict(lambda: "NAO")
    person_grouped = df.groupby("PRIMARY_KEY_PERSON")
    n_records = person_grouped.count()["id"].reset_index()
    one_record_people = n_records[n_records["id"]==1]["PRIMARY_KEY_PERSON"]
    mult_record_people = n_records[n_records["id"]>1]["PRIMARY_KEY_PERSON"]

    # Individuals with a single record
    one_rec_tests = df[df["PRIMARY_KEY_PERSON"].isin(one_record_people)]
    subcol = ["resultado_final_exame", "SINTOMAS ANTES COORTE", "SOLICITACAO ANTES COORTE", "COLETA ANTES COORTE"]
    one_rec_tests["INFO COORTE SINTOMAS"] = one_rec_tests[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[1]]=="SIM" else "NAO", axis=1)
    one_rec_tests["INFO COORTE SOLICITACAO"] = one_rec_tests[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[2]]=="SIM" else "NAO", axis=1)
    one_rec_tests["INFO COORTE COLETA"] = one_rec_tests[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[3]]=="SIM" else "NAO", axis=1)

    # Individuals with multiple records
    subcol = ["resultado_final_exame", "SINTOMAS ANTES COORTE", "SOLICITACAO ANTES COORTE", "COLETA ANTES COORTE"]
    for pkey in mult_record_people:
        sub_df = person_grouped.get_group(pkey)
        sub_df["INFO COORTE SINTOMAS"] = sub_df[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[1]]=="SIM" else "NAO", axis=1)
        sub_df["INFO COORTE SOLICITACAO"] = sub_df[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[2]]=="SIM" else "NAO", axis=1)
        sub_df["INFO COORTE COLETA"] = sub_df[subcol].apply(lambda x: "SIM" if x[subcol[0]]=="POSITIVO" and x[subcol[3]]=="SIM" else "NAO", axis=1)
        if np.isin(["SIM"], sub_df["INFO COORTE SINTOMAS"]):
            pos_antes_cohort_sint[pkey] = "SIM"
        if np.isin(["SIM"], sub_df["INFO COORTE SOLICITACAO"]):
            pos_antes_cohort_sol[pkey] = "SIM"
        if np.isin(["SIM"], sub_df["INFO COORTE COLETA"]):
            pos_antes_cohort_col[pkey] = "SIM"
        
    # ====> ERRO - OVERWRITING SOME "SIM"
    df["INFO COORTE SINTOMAS"] = df["PRIMARY_KEY_PERSON"].apply(lambda x: pos_antes_cohort_sint[x])
    df["INFO COORTE SOLICITACAO"] = df["PRIMARY_KEY_PERSON"].apply(lambda x: pos_antes_cohort_sol[x])
    df["INFO COORTE COLETA"] = df["PRIMARY_KEY_PERSON"].apply(lambda x: pos_antes_cohort_col[x])
    return df

def transform_cartorios(df):
    '''
    
    '''
    df = df.drop_duplicates(subset=["cpf novo str", "do_8"], keep="first")
    df = df.dropna(subset=["cpf novo str", "do_8"], axis=0, how="any")
    df = df.add_suffix("(CARTORIOS)").rename({"cpf novo str(CARTORIOS)": "cpf", "do_8(CARTORIOS)": "do_8"}, axis=1).drop("cpf str(CARTORIOS)", axis=1)
    df["nome mae tratado"] = df["genitor 2(CARTORIOS)"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome tratado"] = df["nome(CARTORIOS)"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome hashcode"] = df["nome(CARTORIOS)"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["nome mae hashcode"] = df["genitor 2(CARTORIOS)"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["data nascimento(CARTORIOS)"] = pd.to_datetime(df["data nascimento(CARTORIOS)"], format="%d/%m/%Y", errors="coerce")
    df["data falecimento(CARTORIOS)"] = pd.to_datetime(df["data falecimento(CARTORIOS)"], format="%d/%m/%Y", errors="coerce")

    df["NOMENASCIMENTOCHAVE"] = df["nome tratado"].astype(str) + df["data nascimento(CARTORIOS)"].astype(str)
    df["NOMENOMEMAECHAVE"] = df["nome tratado"].astype(str) + df["nome mae tratado"].astype(str)
    df["NOMEMAENASCIMENTOCHAVE"] = df["nome mae tratado"].astype(str) + df["data nascimento(CARTORIOS)"].astype(str)
    df["NOMEHASHNASCIMENTOCHAVE"] = df["nome hashcode"].astype(str) + df["data nascimento(CARTORIOS)"].astype(str)
    df["NOMEMAEHASHNASCIMENTOCHAVE"] = df["nome mae hashcode"].astype(str) + df["data nascimento(CARTORIOS)"].astype(str)
    df = df.drop(["nome tratado", "nome mae tratado", "nome hashcode", "nome mae hashcode"], axis=1)
    return df

def transform_obito_covid(df):
    '''
    
    '''
    date_fmt = "%d/%m/%Y"
    col_obitos = ["ORDEM", "NUMERODO", "NOME", "DATA_NASCIMENTO", "SEXO", "EVOLUCAO", "NOME_MAE",
                 "IDADE_ANOS", "FX_ETARIA", 'DATA_PRI_SINTOMAS_NOVA', "BAIRRO_RESIDENCIA", "DATA_OBITO"]
    date_cols = ["DATA_NASCIMENTO", "DATA_OBITO", "DATA_PRI_SINTOMAS_NOVA"]
    
    df = df[col_obitos]
    for j in date_cols:
        df[j] = pd.to_datetime(df[j], format=date_fmt, errors="coerce")
        
    df["ANO OBITO"] = df["DATA_OBITO"].apply(lambda x: f"{x.year:.0f}" if not pd.isna(x) else np.nan)
    df = df[df["ANO OBITO"].isin(["2020", "2021"])]
    df = df[df["EVOLUCAO"]=="OBITO"]

    df["nome_tratado"] = df["NOME"].apply(lambda x: utils.replace_string(x))
    df["nome_mae_tratado"] = df["NOME_MAE"].apply(lambda x: utils.replace_string(x) if not pd.isna(x) else np.nan)
    df["nome hashcode"] = df["NOME"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df["nome mae hashcode"] = df["NOME_MAE"].apply(lambda x: utils.replace_string_hash(x) if not pd.isna(x) else np.nan)
    df = df.drop_duplicates(subset=["NUMERODO"], keep="first")
    df = df.dropna(subset=["NOME", "NOME_MAE", "DATA_NASCIMENTO"], axis=0, how="any")
    df = df.add_suffix("(OBITO COVID)").rename({"NUMERODO(OBITO COVID)": "numerodo",
                                                "DATA_OBITO(OBITO COVID)": "data_obito(OBITO COVID)",
                                                "DATA_PRI_SINTOMAS_NOVA(OBITO COVID)": "data_pri_sintomas_nova(OBITO COVID)"}, axis=1)
    df["numerodo"] = df["numerodo"].apply(lambda x: x[:8] if not pd.isna(x) else np.nan)
    # PRIMARY KEYS
    df["NOMENASCIMENTOCHAVE"] = df["nome_tratado(OBITO COVID)"].astype(str) + df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
    df["NOMENOMEMAECHAVE"] = df["nome_tratado(OBITO COVID)"].astype(str) + df["nome_mae_tratado(OBITO COVID)"].astype(str)
    df["NOMEMAENASCIMENTOCHAVE"] = df["nome_mae_tratado(OBITO COVID)"].astype(str) + df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
    df["NOMEHASHNASCIMENTOCHAVE"] = df["nome hashcode(OBITO COVID)"].astype(str) + df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
    df["NOMEMAEHASHNASCIMENTOCHAVE"] = df["nome mae hashcode(OBITO COVID)"].astype(str) + df["DATA_NASCIMENTO(OBITO COVID)"].astype(str)
    df = df.drop(["nome_tratado(OBITO COVID)", "nome_mae_tratado(OBITO COVID)", "nome hashcode(OBITO COVID)", "nome mae hashcode(OBITO COVID)"], axis=1)
    return df

def transform_bairros(bairro_df):
    '''
    
    '''
    bairro_df = bairro_df[["NOME_BAIRRO", "SR", "idhmb2010"]]
    bairro_df = bairro_df.set_index("NOME_BAIRRO").rename({"BAIRRO DE LOURDES": "DE LOURDES",
                                                    "CIDADE 2000": "CIDADE 2.000",
                                                    "LUCIANO CAVALCANTE": "ENGENHEIRO LUCIANO CAVALCANTE",
                                                    "PARQUE GENIBAU": "GENIBAU",
                                                    "SAPIRANGA COITE": "SAPIRANGA / COITE",
                                                    "VICENTE PINZON": "VINCENTE PINZON"}, axis=0).reset_index()
    bairro_df = bairro_df.rename({"idhmb2010": "IDH"}, axis=1)
    return bairro_df


