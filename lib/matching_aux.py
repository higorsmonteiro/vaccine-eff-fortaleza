import pandas as pd
from collections import defaultdict
import lib.matching_utils as utils

def initial_filtering(df, vaccine, age_range, cohort):
    '''
        First rule to consider when performing matching.
    '''
    # Zeroth rule: Filter by age range.
    df = df[(df["IDADE"]>=age_range[0]) & (df["IDADE"]<=age_range[1])]

    # First rule: Filter out uncommon vaccination schemes.
    ruleout = ["(D2)", "(D4)", "(D1)(D4)", "(D1)(D2)(D4)"]
    df = df[~df["STATUS VACINACAO DURANTE COORTE"].isin(ruleout)]

    # Second rule: Remove everyone with positive test before cohort.
    df = df[df["TESTE POSITIVO ANTES COORTE"]=="NAO"]

    # Third rule: Remove all inconsistencies between death date and vaccination.
    df = df[(df["OBITO INCONSISTENCIA CARTORIOS"]==False) & (df["OBITO INCONSISTENCIA COVID"]==False)]

    # Fourth rule: Remove all positive tests done after death. (REVIEW)
    #df = df[(df["COLETA APOS OBITO"]==False) & (df["SOLICITACAO APOS OBITO"]==False)]

    # Fourth rule(alternative): Remove all deaths before cohort.
    subcol = ["DATA OBITO", "DATA FALECIMENTO(CARTORIOS)"]
    df["OBITO ANTES COORTE"] = df[subcol].apply(lambda x: True if pd.notna(x[subcol[0]]) and x[subcol[0]]<cohort[0] else ( True if pd.notna(x[subcol[1]]) and x[subcol[1]]<cohort[0] else False), axis=1)
    df[df["OBITO ANTES COORTE"]==False]

    # Fourth rule(for hospitalization): Remove all hospitalization before cohort.
    #df = df[(df["DATA HOSPITALIZACAO"]<cohort[0])]

    # Fifth rule: Select vaccine and nonvaccinated individuals
    df_vaccinated = df[df["VACINA APLICADA"]==vaccine]
    df_nonvaccinated = df[df["VACINA APLICADA"]=="NAO VACINADO"]

    return df, df_vaccinated, df_nonvaccinated

def setup_scheme(df_vac, df_nonvac, datelst, seed):
    '''
    
    '''
    control_used = defaultdict(lambda: False)
    control_reservoir = defaultdict(lambda: [])
    control_dates = {
        "D1": defaultdict(lambda: -1),
        "D2": defaultdict(lambda: -1),
        "DEATH COVID": defaultdict(lambda: -1),
        "DEATH GENERAL": defaultdict(lambda: -1),
        "HOSPITALIZATION COVID": defaultdict(lambda: -1),
    }
    df_pop = pd.concat([df_vac, df_nonvac])
    # Get the main outcomes' dates for each eligible individual of the cohort.
    col_names = {
        "D1": "DATA D1",
        "D2": "DATA D2",
        "OBITO COVID": "DATA OBITO",
        "OBITO GERAL": "DATA FALECIMENTO(CARTORIOS)",
    }
    # Coletando datas para toda população considerada
    utils.collect_dates_for_cohort(df_pop, control_reservoir, control_dates, col_names)
    utils.rearrange_controls(control_reservoir, seed)
    pareados, matched = utils.perform_matching(datelst, df_vac, control_reservoir, control_used, control_dates, col_names)
    
    events_df = utils.get_events(df_pop, pareados, matched, col_names)
    df_pop["PAREADO"] = df_pop["CPF"].apply(lambda x: True if matched[x] else False)
    return pareados, events_df, matched

