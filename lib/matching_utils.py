import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt
from collections import defaultdict
from dateutil.relativedelta import relativedelta

def collect_dates_for_cohort(df_pop, control_reservoir, control_dates, col_names=None):
    '''
        Fill 'control_used' dictionary with the dates (specified in 'control_dates') of each person
        (represented by their CPF) regarding the main events considered in the analysis.

        Args:
            df_pop:
                pandas.DataFrame.
            control_reservoir:
                collections.defaultdict.
            control_used:
                collections.defaultdict.
            control_dates:
                collections.defaultdict.
            col_names:
                dictionary.
        Return:
            None.
    '''
    if col_names is None:
        col_names = {
            "D1": "data D1(VACINADOS)",
            "D2": "data D2(VACINADOS)",
            "OBITO COVID": "data_obito(OBITO COVID)",
            "OBITO GERAL": "data falecimento(CARTORIOS)",
            "HOSPITALIZACAO COVID": "DATA HOSPITALIZACAO",
        }

    for j in tqdm(range(df_pop.shape[0])):
        cpf = df_pop["CPF"].iat[j]
        sex, age = df_pop["SEXO"].iat[j], df_pop["IDADE"].iat[j]

        # Different outcomes' dates
        dt_d1 = df_pop[col_names["D1"]].iat[j]
        dt_d2 = df_pop[col_names["D2"]].iat[j]
        dt_death = df_pop[col_names["OBITO COVID"]].iat[j]
        dt_death_general = df_pop[col_names["OBITO GERAL"]].iat[j]
        dt_hosp_covid = df_pop[col_names["HOSPITALIZACAO COVID"]].iat[j]

        control_reservoir[(age,sex)].append(cpf)
        if pd.notna(dt_d1):
            control_dates["D1"][cpf] = dt_d1
        if pd.notna(dt_d2):
            control_dates["D2"][cpf] = dt_d2
        if pd.notna(dt_death):
            control_dates["DEATH COVID"][cpf] = dt_death
        if pd.notna(dt_death_general):
            control_dates["DEATH GENERAL"][cpf] = dt_death_general
        if pd.notna(dt_hosp_covid):
            control_dates["HOSPITALIZATION COVID"][cpf] = dt_hosp_covid

def rearrange_controls(control_reservoir, seed):
    '''
        Shuffle the order of the controls in the structure containing all
        control candidates.

        Args:
            control_reservoir:
                collections.defaultdict.
            seed:
                Integer.
        Return:
            None.
    '''
    np.random.seed(seed)
    for key in control_reservoir.keys():
        np.random.shuffle(control_reservoir[key])

def perform_matching(datelst, df_vac, control_reservoir, control_used, control_dates, col_names):
    '''
        Description.

        Args:
            datelst:
                List of datetime.date.
            df_vac:
                pandas.DataFrame.
            control_reservoir:
                collections.defaultdict.
            control_used:
                collections.defaultdict.
            control_dates:
                collections.defaultdict.
            col_names:
                dictionary.
        Return:
            pareados:
                pandas.DataFrame.
            matched:
                dictionary.
    '''
    if col_names is None:
        col_names = {
            "D1": "data D1(VACINADOS)",
            "D2": "data D2(VACINADOS)",
            "OBITO COVID": "data_obito(OBITO COVID)",
            "OBITO GERAL": "data falecimento(CARTORIOS)"
        }

    matchings = defaultdict(lambda:-1)
    matched = defaultdict(lambda:False)
    for current_date in tqdm(datelst):
        # Select all people who was vaccinated at the current date
        df_vac["compare_date"] = df_vac[col_names["D1"]].apply(lambda x: True if x==current_date else False)
        current_vaccinated = df_vac[df_vac["compare_date"]==True]
        
        cpf_list = current_vaccinated["CPF"].tolist()
        age_list = current_vaccinated["IDADE"].tolist()
        sex_list = current_vaccinated["SEXO"].tolist()

        # For each person vaccinated at the current date, check if there is a control for he/she.
        for j in range(0, len(cpf_list)):
            pair = find_pair(current_date, age_list[j], sex_list[j], control_reservoir, control_used, control_dates)
            if pair!=-1:
                matchings[cpf_list[j]] = pair
    
    items_matching = matchings.items()
    pareados = pd.DataFrame({"CPF CASO": [ x[0] for x in items_matching ], "CPF CONTROLE": [ x[1] for x in items_matching ]})
    for cpf in [ x[0] for x in items_matching ]+[ x[1] for x in items_matching ]:
        matched[cpf]=True
    return pareados, matched

def get_events(df_pop, pareados, matched, col_names):
    '''
        Description.

        Args:
            df_pop:
            pareados:
            matched:
            col_names:
        Return:
            datas:
                pandas.DataFrame.
    '''
    if col_names is None:
        col_names = {
            "D1": "data D1(VACINADOS)",
            "D2": "data D2(VACINADOS)",
            "OBITO COVID": "data_obito(OBITO COVID)",
            "OBITO GERAL": "data falecimento(CARTORIOS)"
        }

    data_obito = defaultdict(lambda:np.nan)
    data_obito_geral = defaultdict(lambda:np.nan)
    data_d1 = defaultdict(lambda:np.nan)
    data_d2 = defaultdict(lambda:np.nan)
    for j in range(df_pop.shape[0]):
        cpf = df_pop["CPF"].iat[j]
        d1_dt = df_pop[col_names["D1"]].iat[j]
        d2_dt = df_pop[col_names["D2"]].iat[j]
        obito = df_pop[col_names["OBITO COVID"]].iat[j]
        obito_geral = df_pop[col_names["OBITO GERAL"]].iat[j]
        #teste = df_pop["DATA SOLICITACAO(TESTES)"].iat[j]
        if not pd.isna(obito):
            data_obito[cpf] = obito
        elif not pd.isna(obito_geral):
            data_obito_geral[cpf] = obito_geral
        if not pd.isna(d1_dt):
            data_d1[cpf] = d1_dt
        if not pd.isna(d2_dt):
            data_d2[cpf] = d2_dt

    # -- create cols with dates --
    datas = {
        "CPF": [], "DATA D1": [], "DATA D2": [],
        "DATA OBITO COVID": [], "DATA OBITO GERAL": [],
        "TIPO": [], "PAR": [], "PAREADO": []
    }
    print("Criando tabela de eventos ...") 
    for j in tqdm(range(0, pareados.shape[0])):
        cpf_caso = pareados["CPF CASO"].iat[j]
        cpf_control = pareados["CPF CONTROLE"].iat[j]
        # Fill new columns
        datas["CPF"] += [cpf_caso, cpf_control]
        datas["DATA D1"] += [data_d1[cpf_caso], data_d1[cpf_control]]
        datas["DATA D2"] += [data_d2[cpf_caso], data_d2[cpf_control]]
        datas["DATA OBITO COVID"] += [data_obito[cpf_caso], data_obito[cpf_control]]
        datas["DATA OBITO GERAL"] += [data_obito_geral[cpf_caso], data_obito_geral[cpf_control]]
        #datas["DATA HOSPITALIZACAO"] += [data_hospitalizado[cpf_caso], data_hospitalizado[cpf_control]]
        #datas["DATA TESTE"] += [data_teste[cpf_caso], data_teste[cpf_control]]
        datas["TIPO"] += ["CASO", "CONTROLE"]
        datas["PAR"] += [cpf_control, cpf_caso]
        datas["PAREADO"] += [True, True]
    print("Criando tabela de eventos ... Concluído") 
    
    print("Incluindo não pareados ...")
    for j in tqdm(range(df_pop.shape[0])):
        cpf = df_pop["CPF"].iat[j]
        if matched[cpf]==False:
            datas["CPF"] += [cpf]
            datas["DATA D1"] += [data_d1[cpf]]
            datas["DATA D2"] += [data_d2[cpf]]
            datas["DATA OBITO COVID"] += [data_obito[cpf]]
            datas["DATA OBITO GERAL"] += [data_obito_geral[cpf]]
            #datas["DATA HOSPITALIZACAO"] += [data_hospitalizado[cpf]]
            #datas["DATA TESTE"] += [data_teste[cpf]]
            datas["TIPO"] += ["NAO PAREADO"]
            datas["PAR"] += [np.nan]
            datas["PAREADO"] += [False]
    print("Incluindo não pareados ... Concluído.")
    datas = pd.DataFrame(datas)
    return datas

def get_events_per_pair(df_pop, pareados, col_names):
    '''
        Description.

        Args:
            df_pop:
            pareados:
            matched:
            col_names:
        Return:
            datas:
                pandas.DataFrame.
    '''
    if col_names is None:
        col_names = {
            "D1": "data D1(VACINADOS)",
            "D2": "data D2(VACINADOS)",
            "OBITO COVID": "data_obito(OBITO COVID)",
            "OBITO GERAL": "data falecimento(CARTORIOS)"
        }
    data_obito = defaultdict(lambda:np.nan)
    data_obito_geral = defaultdict(lambda:np.nan)
    data_d1 = defaultdict(lambda:np.nan)
    data_d2 = defaultdict(lambda:np.nan)
    for j in range(df_pop.shape[0]):
        cpf = df_pop["cpf"].iat[j]
        d1_dt = df_pop[col_names["D1"]].iat[j]
        d2_dt = df_pop[col_names["D2"]].iat[j]
        obito = df_pop[col_names["OBITO COVID"]].iat[j]
        obito_geral = df_pop[col_names["OBITO GERAL"]].iat[j]
        #teste = df_pop["DATA SOLICITACAO(TESTES)"].iat[j]
        if not pd.isna(obito):
            data_obito[cpf] = obito
        elif not pd.isna(obito_geral):
            data_obito_geral[cpf] = obito_geral
        if not pd.isna(d1_dt):
            data_d1[cpf] = d1_dt
        if not pd.isna(d2_dt):
            data_d2[cpf] = d2_dt
    
    # -- create cols with dates --
    datas = {
        "CPF CASO": [], "DATA D1 CASO": [], "DATA D2 CASO": [],
        "DATA OBITO COVID CASO": [], "DATA OBITO GERAL CASO": [],
        "CPF CONTROLE": [], "DATA D1 CONTROLE": [], "DATA D2 CONTROLE": [],
        "DATA OBITO COVID CONTROLE": [], "DATA OBITO GERAL CONTROLE": []
    }
    print("Criando tabela de eventos por par ...") 
    for j in tqdm(range(0, pareados.shape[0])):
        cpf_caso = pareados["CPF CASO"].iat[j]
        cpf_control = pareados["CPF CONTROLE"].iat[j]
        # Fill new columns
        datas["CPF CASO"] += [cpf_caso]
        datas["CPF CONTROLE"] += [cpf_control]
        datas["DATA D1 CASO"] += [data_d1[cpf_caso]]
        datas["DATA D1 CONTROLE"] += [data_d1[cpf_control]]
        datas["DATA D2 CASO"] += [data_d2[cpf_caso]]
        datas["DATA D2 CONTROLE"] += [data_d2[cpf_control]]
        datas["DATA OBITO COVID CASO"] += [data_obito[cpf_caso]]
        datas["DATA OBITO COVID CONTROLE"] += [data_obito[cpf_control]]
        datas["DATA OBITO GERAL CASO"] += [data_obito_geral[cpf_caso]]
        datas["DATA OBITO GERAL CONTROLE"] += [data_obito_geral[cpf_control]]
    print("Criando tabela de eventos por par ... Concluído")
    datas = pd.DataFrame(datas)
    return datas

def get_intervals_events(events_pair_df, final_cohort, which="D1"):
    '''
        Calculate the intervals between the start of the pair's cohort and all
        possible events for the case and control.

        For both case and control individuals, there 4 possible events:
            - Death by Covid (Outcome)
            - Death due to another cause (Censored)
            - Control vaccination (Censored)
            - End of the cohort (Censored)
        
        The intervals are calculated for all events, and for the survival analysis
        only the earliest event should be considered. 

        Args:
            events_pair_df:
                pandas.DataFrame.
            Return:
                data:
                    pandas.DataFrame
        Return:
            ...
    '''
    # Column names translator.
    colname = {
        "CPF CASO": "CPF CASO", "D1 CASO": "DATA D1 CASO",
        "D2 CASO": "DATA D2 CASO", "OBITO CASO": "DATA OBITO GERAL CASO",
        "OBITO COVID CASO": "DATA OBITO COVID CASO",
        "CPF CONTROLE": "CPF CONTROLE", "D1 CONTROLE": "DATA D1 CONTROLE",
        "D2 CONTROLE": "DATA D2 CONTROLE", "OBITO CONTROLE": "DATA OBITO GERAL CONTROLE",
        "OBITO COVID CONTROLE": "DATA OBITO COVID CONTROLE",
    }
    
    # Calculate intervals for case.
    sbst1 = [colname["OBITO COVID CASO"], colname[f"{which} CASO"]]
    sbst2 = [colname["OBITO CASO"], colname[f"{which} CASO"]]
    events_pair_df[f"INTV OBITO COVID CASO({which})"] = events_pair_df[sbst1].apply(lambda x: calc_interval(x,sbst1), axis=1)
    events_pair_df[f"INTV OBITO GERAL CASO({which})"] = events_pair_df[sbst2].apply(lambda x: calc_interval(x,sbst2), axis=1)
    sbst_d1d2 = [colname[f"D2 CASO"], colname[f"D1 CASO"]]
    if which=="D1":
        events_pair_df[f"INTV D2-D1 CASO"] = events_pair_df[sbst_d1d2].apply(lambda x: calc_interval(x, sbst_d1d2), axis=1)
    # Calculate intervals for control
    sbst1 = [colname["OBITO COVID CONTROLE"], colname[f"{which} CASO"]]
    sbst2 = [colname["OBITO CONTROLE"], colname[f"{which} CASO"]]
    events_pair_df[f"INTV OBITO COVID CONTROLE({which})"] = events_pair_df[sbst1].apply(lambda x: calc_interval(x,sbst1), axis=1)
    events_pair_df[f"INTV OBITO GERAL CONTROLE({which})"] = events_pair_df[sbst2].apply(lambda x: calc_interval(x,sbst2), axis=1)
    sbst_d1d2 = [colname[f"D2 CONTROLE"], colname[f"D1 CONTROLE"]]
    if which=="D1":
        events_pair_df[f"INTV D2-D1 CONTROLE"] = events_pair_df[sbst_d1d2].apply(lambda x: calc_interval(x, sbst_d1d2), axis=1)
    
    # Interval in common for both individuals
    sbst_d1 = [colname["D1 CONTROLE"], colname[f"{which} CASO"]]
    events_pair_df[f"INTV D1 CASO CONTROLE({which})"] = events_pair_df[sbst_d1].apply(lambda x: calc_interval(x,sbst_d1), axis=1)
    events_pair_df[f"INTV FIM COORTE({which})"] = events_pair_df[colname[f"{which} CASO"]].apply(lambda x: (final_cohort-x.date()).days if not pd.isna(x) else np.nan)
    return events_pair_df

def get_intervals(events_pair_df, final_cohort=dt.date(2021, 8, 31)):
    '''
        Description.

        Args:
            events_df:
                pandas.DataFrame.
        Return:
            data:
                pandas.DataFrame.
    '''
    colname = {
        "CPF CASO": "CPF CASO",
        "D1 CASO": "DATA D1 CASO",
        "D2 CASO": "DATA D2 CASO",
        "OBITO CASO": "DATA OBITO GERAL CASO",
        "OBITO COVID CASO": "DATA OBITO COVID CASO",
        "CPF CONTROLE": "CPF CONTROLE",
        "D1 CONTROLE": "DATA D1 CONTROLE",
        "D2 CONTROLE": "DATA D2 CONTROLE",
        "OBITO CONTROLE": "DATA OBITO GERAL CONTROLE",
        "OBITO COVID CONTROLE": "DATA OBITO COVID CONTROLE",
    }

    data = {
        "CPF": [], "DATA D1": [], "DATA D2": [], "DATA OBITO COVID": [],
        "DATA OBITO GERAL": [], "TIPO": [], "PAR": [], "PAREADO": [],
        "OBITO COVID DURACAO": [], "COM DESFECHO - OBITO COVID": []
    }
    # --> Go through each pair
    for j in tqdm(range(events_pair_df.shape[0])):
        cpf_caso = events_pair_df[colname["CPF CASO"]].iat[j]
        cpf_controle = events_pair_df[colname["CPF CONTROLE"]].iat[j]
        d2_caso = events_pair_df[colname["D2 CASO"]].iat[j]
        d2_controle = events_pair_df[colname["D2 CONTROLE"]].iat[j]

        init = events_pair_df[colname["D1 CASO"]].iat[j].date()
        events_caso = {
            "OBITO CASO": events_pair_df[colname["OBITO CASO"]].iat[j],
            "OBITO COVID CASO": events_pair_df[colname["OBITO COVID CASO"]].iat[j],
            "COORTE FINAL": final_cohort
        }
        events_controle = {
            "D1 CONTROLE": events_pair_df[colname["D1 CONTROLE"]].iat[j],
            "OBITO CONTROLE": events_pair_df[colname["OBITO CONTROLE"]].iat[j],
            "OBITO COVID CONTROLE": events_pair_df[colname["OBITO COVID CONTROLE"]].iat[j],
            "COORTE FINAL": final_cohort
        }
        # Convert date strings to date formats.
        for key in events_caso.keys():
            if not pd.isna(events_caso[key]) and type(events_caso[key])!=dt.date:
                events_caso[key] = events_caso[key].date()
        for key in events_controle.keys():
            if not pd.isna(events_controle[key]) and type(events_controle[key])!=dt.date:
                events_controle[key] = events_controle[key].date()
        
        # Determine final day of each person of the pair.
        # --> For case:
        timeline_namecaso = ["D1 CONTROLE", "OBITO COVID CASO", "OBITO CASO", "COORTE FINAL"]
        timeline_caso = [events_controle["D1 CONTROLE"], events_caso["OBITO COVID CASO"],
                         events_caso["OBITO CASO"], events_caso["COORTE FINAL"]]
        # replace NaN for any date later than "COORTE FINAL"
        timeline_caso = [x if not pd.isna(x) else dt.date(2050, 1, 1) for x in timeline_caso ]
        sorted_tp_caso = sorted(zip(timeline_caso, timeline_namecaso))
        final_namecaso = sorted_tp_caso[0][1]
        final_caso = sorted_tp_caso[0][0]
        interval_caso = (final_caso-init).days
        #print(sorted_tp_caso, interval_caso, final_namecaso, final_caso)
        if final_namecaso!="OBITO COVID CASO":
            type_caso = False
        else:
            type_caso = True
        
        # --> For control:
        timeline_namecontrole = ["D1 CONTROLE", "OBITO COVID CONTROLE", "OBITO CONTROLE", "COORTE FINAL"]
        timeline_controle = [events_controle["D1 CONTROLE"], events_controle["OBITO COVID CONTROLE"],
                             events_controle["OBITO CONTROLE"], events_controle["COORTE FINAL"]]
        timeline_controle = [x if not pd.isna(x) else dt.date(2050, 1, 1) for x in timeline_controle ]
        sorted_tp_controle = sorted(zip(timeline_controle, timeline_namecontrole))
        final_namecontrole = sorted_tp_controle[0][1]
        final_controle = sorted_tp_controle[0][0]
        interval_controle = (final_controle-init).days
        #print(sorted_tp_caso, interval_caso, final_namecaso, final_caso)
        if final_namecontrole!="OBITO COVID CONTROLE":
            type_controle = False
        else:
            type_controle = True

        # --> Organize values
        data["CPF"] += [cpf_caso, cpf_controle]
        data["DATA D1"] += [init, events_pair_df[colname["D1 CONTROLE"]].iat[j]]
        data["DATA D2"] += [d2_caso, d2_controle]
        data["DATA OBITO COVID"] += [events_caso["OBITO COVID CASO"], events_controle["OBITO COVID CONTROLE"]]
        data["DATA OBITO GERAL"] += [events_caso["OBITO CASO"], events_controle["OBITO CONTROLE"]]
        data["TIPO"] += ["CASO", "CONTROLE"]
        data["PAR"] += [cpf_controle, cpf_caso]
        data["PAREADO"] += [True, True]
        data["OBITO COVID DURACAO"] += [interval_caso, interval_controle]
        data["COM DESFECHO - OBITO COVID"] += [type_caso, type_controle]
    
    data = pd.DataFrame(data)
    return data


'''
    AUXILIAR FUNCTIONS.
'''
def calc_interval(x, sbst):
    '''
    
    '''
    if pd.isna(x[sbst[0]]) or pd.isna(x[sbst[1]]):
        return np.nan
    else:
        return (x[sbst[0]].date() - x[sbst[1]].date()).days

def find_pair(cur_date, age_case, sex_case, control_reservoir, control_used, control_dates, age_interval=1):
    '''
        Based on the features of the exposed individual, find a control to match.

        Args:
            cur_date:
                Date of vaccination of the exposed individual. It cannot be after
                the vaccination of the control (if vaccinated).
            age_case:
                Age of the exposed individual.
            sex_case:
                Sex of the exposed individual.
            control_reservoir:
                Hash table holding lists of control candidates for each tuple (age,sex).
            control_used:
                Hash table to signal whether an individual (through CPF) was already
                used as a control.
            control_dates:
                Dates of relevance of the control individual: D1, death and hospitalization 
                (if appliable).
            age_interval:
                Age interval to search for a control.
        Return:
            cpf_control:
                Either -1 or a CPF number (for the found control).
    '''
    eligible_controls = []
    for j in np.arange(age_case-age_interval, age_case+age_interval+1, 1):
        eligible_controls += control_reservoir[(j, sex_case)]
    
    for cpf_control in eligible_controls:
        if not control_used[cpf_control]:
            condition_d1 = control_dates["D1"][cpf_control]==-1 or control_dates["D1"][cpf_control]>cur_date
            condition_death = control_dates["DEATH COVID"][cpf_control]==-1 or control_dates["DEATH COVID"][cpf_control]>cur_date
            condition_death_geral = control_dates["DEATH GENERAL"][cpf_control]==-1 or control_dates["DEATH GENERAL"][cpf_control]>cur_date
            #condition_hospt = control_dates["HOSPITAL"][cpf_control]==-1 or control_dates["HOSPITAL"][cpf_control]>cur_date
            if condition_d1 and condition_death and condition_death_geral:
                control_used[cpf_control] = True
                return cpf_control
    return -1