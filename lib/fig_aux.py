import pandas as pd
import numpy as np
import lib.utils as utils
from collections import defaultdict

def summary_fschema(df):
    '''
        Summary of the counting of the basic quantities in the final schema file.
    '''
    summary = {
        "n_records": None,
        "missing_sex": None,
        "missing_age": None,
        "missing_hdi": None,
        "only_d1_cohort": None,
        "d1d2_cohort": None,
        "d4_cohort": None,
        "covid_death": None,
        "covid_hospitalization": None,
        "covid_icu": None,
    }
    summary["n_records"] = df.shape[0]
    summary["missing_sex"] = df["SEXO"].isnull().sum()
    summary["missing_age"] = df["IDADE"].isnull().sum()
    summary["missing_hdi"] = df["IDH2010"].isnull().sum()
    summary["only_d1_cohort"] = df["STATUS VACINACAO DURANTE COORTE"].value_counts().loc["(D1)"]
    summary["d1d2_cohort"] = df["STATUS VACINACAO DURANTE COORTE"].value_counts().loc["(D1)(D2)"]
    try:
        summary["d4_cohort"] = df["STATUS VACINACAO DURANTE COORTE"].value_counts().loc["(D4)"]
    except:
        pass
    summary["d1d2_cohort"] += summary["d4_cohort"]
    summary["covid_death"] = df[pd.notna(df["DATA OBITO"])].shape[0]
    summary["covid_hospitalization"] = df[pd.notna(df["DATA HOSPITALIZACAO"])].shape[0]
    summary["covid_icu"] = df[pd.notna(df["DATA UTI"])].shape[0]
    df["ANO OBITO"] = df["DATA OBITO"].apply(lambda x: x.year if pd.notna(x) else np.nan)
    df["ANO UTI"] = df["DATA UTI"].apply(lambda x: x.year if pd.notna(x) else np.nan)
    df["ANOS HOSPITAL"] = df["DATA HOSPITALIZACAO"].apply(lambda dt_lst: [pd.to_datetime(date_).year for date_ in dt_lst if pd.notna(date_)] if np.any(pd.notna(dt_lst)) else np.nan)
    df["HOSPITAL 2021"] = df["ANOS HOSPITAL"].apply(lambda x: 2021 in x if np.any(pd.notna(x)) else False)
    summary["covid_death_2021"] = df[(pd.notna(df["DATA OBITO"])) & (df["ANO OBITO"]==2021)].shape[0]
    summary["covid_hospitalization_2021"] = df[df["HOSPITAL 2021"]==True].shape[0]
    summary["covid_icu_2021"] = df[(pd.notna(df["DATA UTI"])) & (df["ANO UTI"]==2021)].shape[0]
    return summary

def recruitment_cohort(fschema, events_df, vaccine, cohort, hdi_index, age_range=(60,200)):
    '''
    
    '''
    date_index = pd.DataFrame({
        "index": utils.generate_date_list(cohort[0], cohort[1], interval=1),
    })
    
    # --> Found who is eligible
    df = fschema[ fschema["VACINA APLICADA"]==vaccine ]
    df1 = fschema[ fschema["VACINA APLICADA"]==vaccine ]
    # Zeroth rule: Filter by age range.
    df = df[(df["IDADE"]>=age_range[0]) & (df["IDADE"]<=age_range[1])]
    # First rule: Filter out uncommon vaccination schemes.
    ruleout = ["(D2)", "(D1)(D4)", "(D1)(D2)(D4)"]
    df = df[~df["STATUS VACINACAO DURANTE COORTE"].isin(ruleout)]
    df1 = df1[~df1["STATUS VACINACAO DURANTE COORTE"].isin(ruleout)]
    # Second rule: Remove everyone with positive test before cohort.
    df = df[df["TESTE POSITIVO ANTES COORTE"]==False]
    # Third rule: Remove all inconsistencies between death date and vaccination.
    df = df[(df["OBITO INCONSISTENCIA CARTORIOS"]==False) & (df["OBITO INCONSISTENCIA COVID"]==False)]
    # Fourth rule: Remove all deaths before cohort.
    df = df[df["OBITO ANTES COORTE"]==False]
    # Fifth rule(for hospitalization): Remove all hospitalization before cohort.
    df = df[df["HOSPITALIZACAO ANTES COORTE"]==False]
    # Sixth rule: Remove health workers and health profissionals
    df = df[~df["GRUPO PRIORITARIO"].isin(["PROFISSIONAL DE SAUDE", "TRABALHADOR DA SAUDE"])]
    # Seventh rule: Remove records without info on matching variables.
    df = df[(pd.notna(df["IDADE"])) & (pd.notna(df["SEXO"])) & (pd.notna(df[f"IDH {hdi_index}"]))]

    total_df = df1["DATA D1"].value_counts().sort_index().reset_index().rename({"DATA D1": "D1 TOTAL"}, axis=1)
    eligible_df = df["DATA D1"].value_counts().sort_index().reset_index()
    matched_df = events_df[events_df["TIPO"]=="CASO"]["DATA D1"].value_counts().sort_index().reset_index().rename({"DATA D1": "D1 PAREADO"}, axis=1)

    date_index = date_index.merge(total_df, on="index", how="left").fillna(0)
    date_index = date_index.merge(eligible_df, on="index", how="left").fillna(0)
    date_index = date_index.merge(matched_df, on="index", how="left").fillna(0)
    return date_index

def cohort_diagram(fschema, pairs_df, cohort_end, vaccine, include_hdi=False):
    '''
        ...
    '''
    output_lst = []

    df = fschema
    # Complete database
    output_lst.append(f"TOTAL OF INDIVIDUALS IN THE DATABASE: {df.shape[0]}")
    # Total participants >= 60 yo without positive test before cohort, and with no hospitalization and death recorded before cohort.
    condition1 = (df["IDADE"]>=60) & (df["IDADE"]<=200)
    condition2 = (df["TESTE POSITIVO ANTES COORTE"]==False)
    condition3 = (df["HOSPITALIZACAO ANTES COORTE"]==False)
    condition4 = (df["OBITO ANTES COORTE"]==False)
    df_total = df[condition1 & condition2 & condition3 & condition4]
    output_lst.append(f"TOTAL OF PARTICIPANTS IN THE VACCINE COHORT: {df_total.shape[0]}")
    # Total of vaccinated individuals (any vaccine) as Jan 20, 2022.
    vaccinated_total = df_total[df_total["VACINA APLICADA"]!="NAO VACINADO"].shape[0]
    unvaccinated_total = df_total[df_total["VACINA APLICADA"]=="NAO VACINADO"].shape[0]
    output_lst.append(f"TOTAL OF VACCINATED AND NON VACCINATED (ANY VACCINE) AS JAN 20, 2022: {vaccinated_total}, {unvaccinated_total}")
    # Missing data and exclusion criteria
    output_lst.append("MISSING DATA AND EXCLUSION CRITERIA: ")
    # First rule: Filter out uncommon vaccination schemes.
    ruleout = ["(D2)", "(D1)(D4)", "(D1)(D2)(D4)"]
    irregular_vac = df_total[df_total["STATUS VACINACAO DURANTE COORTE"].isin(ruleout)].shape[0]
    output_lst.append(f"\tIRREGULAR VACCINATION STATUS: {irregular_vac}")
    # Third rule: Remove all inconsistencies between death date and vaccination.
    irregular_death = df_total[(df_total["OBITO INCONSISTENCIA CARTORIOS"]==True) | (df["OBITO INCONSISTENCIA COVID"]==True)].shape[0]
    output_lst.append(f"\tIRREGULAR DEATH DATES: {irregular_death}")
    # Sixth rule: Remove health workers and health profissionals
    health_workers = df_total[df_total["GRUPO PRIORITARIO"].isin(["PROFISSIONAL DE SAUDE", "TRABALHADOR DA SAUDE"])].shape[0]
    output_lst.append(f"\tHEALTH WORKERS: {health_workers}")
    # Seventh rule: Remove records without info on matching variables.
    missing_age = df_total["IDADE"].isnull().sum()
    missing_sex = df_total["SEXO"].isnull().sum()
    missing_hdi = df_total["IDH2010"].isnull().sum()
    output_lst.append(f"\tMISSING AGE: {missing_age}")
    output_lst.append(f"\tMISSING SEX: {missing_sex}")
    output_lst.append(f"\tMISSING HDI: {missing_hdi}")

    # Eighth rule: 
    test_irregular_dts = df[(df["COLETA APOS OBITO"]==True) | (df["SOLICITACAO APOS OBITO"]==True)]
    output_lst.append(f"\tIRREGULAR DATES BETWEEN TESTS AND DATES (TESTS AFTER DEATH)  {test_irregular_dts.shape[0]}")

    df_total1 = df_total[~df_total["STATUS VACINACAO DURANTE COORTE"].isin(ruleout)]
    df_total1 = df_total1[(df_total1["OBITO INCONSISTENCIA CARTORIOS"]==False) & (df["OBITO INCONSISTENCIA COVID"]==False)]
    df_total1 = df_total1[~df_total1["GRUPO PRIORITARIO"].isin(["PROFISSIONAL DE SAUDE", "TRABALHADOR DA SAUDE"])]
    df_total1_hdi = df_total1[pd.notna(df_total1["IDH2010"])]
    output_lst.append(f"DATA CONSIDERING HDI AND OTHERWISE: {df_total1.shape}, {df_total1_hdi.shape}")

    if include_hdi:
        dfe = df_total1_hdi
    else:
        dfe = df_total1
    output_lst.append(f"Total eligible: {dfe.shape[0]}")
    # How many individuals were vaccinated with CoronaVac before the end of the cohort?
    cols = ["DATA D1", "VACINA APLICADA"]
    f = lambda x: True if pd.notna(x["DATA D1"]) and x["DATA D1"]<cohort_end and x["VACINA APLICADA"]==vaccine else False
    dfe[f"{vaccine} ANTES FIM COORTE"] = dfe[cols].apply(f, axis=1)
    dfe[f"QUALQUER VACINA ANTES FIM COORTE"] = dfe["DATA D1"].apply(lambda x: True if pd.notna(x) and x<cohort_end else False)
    corona_vac_antes_fim_cohort = dfe[dfe[f"{vaccine} ANTES FIM COORTE"]==True].shape[0]
    corona_unvac_antes_fim_cohort = dfe[dfe[f"{vaccine} ANTES FIM COORTE"]==False].shape[0]
    output_lst.append(f"VACCINATED WITH {vaccine} BEFORE END OF COHORT: {corona_vac_antes_fim_cohort}")
    output_lst.append(f"NOT VACCINATED WITH {vaccine} BEFORE END OF COHORT: {corona_unvac_antes_fim_cohort}")
    # Among the unvaccinated with CORONAVAC, how many took other vaccine?
    lst_unvac = dfe[dfe[f"{vaccine} ANTES FIM COORTE"]==False]["VACINA APLICADA"].value_counts()
    condition = (dfe[f"{vaccine} ANTES FIM COORTE"]==False) & (dfe["QUALQUER VACINA ANTES FIM COORTE"]==True)
    lst_unvac_antes = dfe[condition]["VACINA APLICADA"].value_counts()
    output_lst.append(f"\tAMONG THEM: ")
    for index, value in lst_unvac.iteritems():
        output_lst.append(f"\t\t{index}:{value}")
    output_lst.append(f"\tFROM WHICH THOSE WHO TOOK OTHER VACCINE DURING COHORT ARE: ")
    for index, value in lst_unvac_antes.iteritems():
        output_lst.append(f"\t\t{index}:{value}")

    # Info about matched pairs
    # How many individuals were matched? How many were not?
    cases_matched = pairs_df[pairs_df["TIPO"]=="CASO"].shape[0]
    output_lst.append(f"MATCHED AND NON MATCHED: {cases_matched}, {corona_vac_antes_fim_cohort-cases_matched}")
    # How many of the matched cases were matched as control before vaccination? 
    cases = pairs_df[pairs_df["TIPO"]=="CASO"]
    controls = pairs_df[pairs_df["TIPO"]=="CONTROLE"]
    cpf_caso = defaultdict(lambda: np.nan, zip(cases["CPF"], cases["TIPO"]))
    cpf_controle = defaultdict(lambda: np.nan, zip(controls["CPF"], controls["TIPO"]))
    n = 0
    for key in cpf_caso.keys():
        if pd.notna(cpf_controle[key]):
            n+=1
    output_lst.append(f"NUMBER OF CASES MATCHED AS CONTROL BEFORE VACCINATION: {n}")
    return output_lst


