from re import sub
import numpy as np
import pandas as pd
from tqdm import tqdm
import lib.utils as utils
from datetime import date, timedelta
from collections import defaultdict

def initial_filtering(df, vaccine, age_range, hdi_index=0, pop_test="ALL"):
    '''
        Rules to consider when performing matching - Exclusion criteria.

        Args:
            df:
                pandas.DataFrame. Population data for the vaccine cohort.
            vaccine:
                String. Vaccine selected for cohort.
            age_range:
                2-tuple of integers. Range of a ge to consider during cohort.
            HDI_index:
                Integer. To signal whether to use HDI variable during the matching 
                process.
            pop_test:
                String. For running tests.
        Return:
            df:
                pandas.DataFrame.
            df_vaccinated:
                pandas.DataFrame.
            df_unvaccinated:
                pandas.DataFrame. 
    '''
    # Zeroth rule: Filter by age range. (OK)
    df = df[(df["IDADE"]>=age_range[0]) & (df["IDADE"]<=age_range[1])]

    # First rule: Filter out uncommon vaccination schemes. (OK)
    ruleout = ["(D2)", "(D1)(D4)", "(D1)(D2)(D4)"]
    df = df[~df["STATUS VACINACAO DURANTE COORTE"].isin(ruleout)]

    # Second rule: Remove everyone with positive test before cohort. (OK)
    df = df[df["TESTE POSITIVO ANTES COORTE"]==False]

    # Third rule: Remove all inconsistencies between death date and vaccination. (OK)
    df = df[(df["OBITO INCONSISTENCIA CARTORIOS"]==False) & (df["OBITO INCONSISTENCIA COVID"]==False)]

    # Fourth rule: Remove all deaths before cohort. (OK)
    df = df[df["OBITO ANTES COORTE"]==False]

    # Fifth rule(for hospitalization): Remove all hospitalization before cohort. (OK)
    df = df[df["HOSPITALIZACAO ANTES COORTE"]==False]

    # Sixth rule: Remove health workers and health profissionals (OK)
    df = df[~df["GRUPO PRIORITARIO"].isin(["PROFISSIONAL DE SAUDE", "TRABALHADOR DA SAUDE"])]

    # Seventh rule: Remove records without info on matching variables. (OK)
    df = df[(pd.notna(df["IDADE"])) & (pd.notna(df["SEXO"])) & (pd.notna(df[f"IDH {hdi_index}"]))]

    # Eighth rule: (NOT OKAY -> TO BE FIXED DURING MATCHING)
    #df = df[(df["COLETA APOS OBITO"]==False) & (df["SOLICITACAO APOS OBITO"]==False)]
    
    # Ninth rule: Select vaccine and nonvaccinated individuals
    df_vaccinated = df[df["VACINA APLICADA"]==vaccine]
    if pop_test=="ALL":
        df_nonvaccinated = df[df["VACINA APLICADA"]!=vaccine]
    elif pop_test=="VACCINE":
        df_nonvaccinated = df[df["VACINA APLICADA"]=="NAO VACINADO"]
    else:
        pass
    return df_vaccinated, df_nonvaccinated

def collect_dates_for_cohort(df_pop, control_reservoir, control_dates, HDI_index=0, col_names=None):
    '''
        Fill 'control_dates' dictionary with the dates (specified in 'control_dates') of each person
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
    for j in tqdm(range(df_pop.shape[0])):
        cpf = df_pop["CPF"].iat[j]
        sex, age = df_pop["SEXO"].iat[j], df_pop["IDADE"].iat[j]
        idh = df_pop[f"IDH {HDI_index}"].iat[j]

        # Different events' dates
        dt_d1 = df_pop[col_names["D1"]].iat[j]
        dt_d2 = df_pop[col_names["D2"]].iat[j]
        dt_death = df_pop[col_names["OBITO COVID"]].iat[j]
        dt_death_general = df_pop[col_names["OBITO GERAL"]].iat[j]
        dt_hosp_covid = df_pop[col_names["HOSPITALIZACAO COVID"]].iat[j]
        dt_uti_covid = df_pop[col_names["UTI COVID"]].iat[j]
        dt_pri = df_pop[col_names["PRI SINTOMAS"]].iat[j]

        control_reservoir[(age,sex,idh)].append(cpf)
        if pd.notna(dt_d1):
            control_dates["D1"][cpf] = dt_d1
        if pd.notna(dt_d2):
            control_dates["D2"][cpf] = dt_d2
        if pd.notna(dt_death):
            control_dates["DEATH COVID"][cpf] = dt_death
        if pd.notna(dt_death_general):
            control_dates["DEATH GENERAL"][cpf] = dt_death_general
        if np.any(pd.notna(dt_hosp_covid)):
            control_dates["HOSPITALIZATION COVID"][cpf] = dt_hosp_covid
        if np.any(pd.notna(dt_uti_covid)):
            control_dates["UTI COVID"][cpf] = dt_uti_covid
        if np.any(pd.notna(dt_pri)):
            control_dates["PRI SINTOMAS"][cpf] = dt_pri


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
        control_reservoir[key] = list(control_reservoir[key])
        np.random.shuffle(control_reservoir[key])

def perform_matching_new(df_vac, df_pop, control_reservoir, control_used, person_dates, hdi_index, cohort, days_after=0):
    '''
        Description.

        Args:
            df_vac:

            control_reservoir:

            control_used:

            person_dates:

            hdi_index:

            cohort:

            days_after:

    '''
    '''
        'matchings': Pairs -> key-value:vaccinated-unvaccinated.
        'matched': Whether an individual was matched.
    '''
    matchings = defaultdict(lambda:-1) 
    matched = defaultdict(lambda:False) 

    # --> Vaccinated during cohort 
    df_vac = df_vac[(pd.notna(df_vac["DATA D1"])) & (df_vac["DATA D1"]>=cohort[0]) & (df_vac["DATA D1"]<=cohort[1])]
    # --> Apply callbacks
    # ----> First symptoms eligibility
    f_pri = lambda x: True if np.all(pd.isna(x["INTEGRA PRI SINTOMAS DATA"])) else ( 
                      False if np.any([ pd.notna(pri_date) and pri_date<(x["DATA D1"]+timedelta(days=days_after)) for pri_date in x["INTEGRA PRI SINTOMAS DATA"] ]) else True )
    # ----> Hospitalization occurrence eligibility
    f_hosp = lambda x: True if np.all(pd.isna(x["DATA HOSPITALIZACAO"])) else ( 
                       False if np.any([ pd.notna(hosp_date) and hosp_date<(x["DATA D1"]+timedelta(days=days_after)) for hosp_date in x["DATA HOSPITALIZACAO"] ]) else True )
    # ----> ICU occurrence eligibility
    f_icu = lambda x: True if np.all(pd.isna(x["DATA UTI"])) else ( 
                      False if np.any([ pd.notna(icu_date) and icu_date<(x["DATA D1"]+timedelta(days=days_after)) for icu_date in x["DATA UTI"] ]) else True )
    # ----> Death by Covid-19 occurrence eligibility
    f_death = lambda x: True if pd.isna(x["DATA OBITO"]) else ( 
                        False if x["DATA OBITO"]<(x["DATA D1"]+timedelta(days=days_after)) else True )
    # --> Apply conditions
    df_vac["PRI CONDICAO"] = df_vac[["INTEGRA PRI SINTOMAS DATA", "DATA D1"]].apply(f_pri, axis=1)
    df_vac["HOSPITAL CONDICAO"] = df_vac[["DATA HOSPITALIZACAO", "DATA D1"]].apply(f_hosp, axis=1)
    df_vac["UTI CONDICAO"] = df_vac[["DATA UTI", "DATA D1"]].apply(f_icu, axis=1)
    df_vac["OBITO CONDICAO"] = df_vac[["DATA OBITO", "DATA D1"]].apply(f_death, axis=1)
    # --> Consider only eligible vaccinated
    df_vac = df_vac[(df_vac["PRI CONDICAO"]==True) & (df_vac["HOSPITAL CONDICAO"]==True) & (df_vac["UTI CONDICAO"]==True) & (df_vac["OBITO CONDICAO"]==True)]

    for row in tqdm(range(df_vac.shape[0])):
        # --> Metadata on case individual
        cpf_vaccinated = df_vac.index[row]
        d1_date = df_vac["DATA D1"].iat[row]
        # --> Matching variables
        matching_vars = df_vac[f"MATCHING {hdi_index}"].iat[row]
        # --> Find unvaccinated pair 
        pair = find_pair_fix(d1_date, matching_vars, control_reservoir, control_used, person_dates, cohort, days_after)
        if pair!=-1:
            matchings[cpf_vaccinated] = pair
        #pair = find_unvaccinated_pair(d1_date, df_pop, matching_vars, control_reservoir, control_used, days_after)
        #if pair!=-1:
        #    matchings[cpf_vaccinated] = pair

        # --> Main dates to assess eligibility
        #pri_vaccinated = df_vac["INTEGRA PRI SINTOMAS DATA"].iat[row]
        #obito_vaccinated = df_vac["DATA OBITO"].iat[row]
        #hosp_vaccinated = df_vac["DATA HOSPITALIZACAO"].iat[row]
        #uti_vaccinated = df_vac["DATA UTI"].iat[row]

        #condition_pri = df_vac["PRI CONDICAO"].iat[row]
        #condition_death = df_vac["OBITO CONDICAO"].iat[row]
        #condition_hosp = df_vac["HOSPITAL CONDICAO"].iat[row]
        #condition_icu = df_vac["UTI CONDICAO"].iat[row]

        # For each person vaccinated at the current date, check if there is a control for he/she.
        # But first check if the vaccinated is eligible.
        #condition_pri = True # True as eligible 
        #if np.any(pd.notna(pri_vaccinated)):
        #    for pri_date in pri_vaccinated:
        #        # Drop any case who had symptoms during the cohort before the current day
        #        if pd.notna(pri_date) and pri_date<(d1_date+timedelta(days=days_after)):
        #            condition_pri = False
        #            break

        ## --> Verify if there is an outcome before the current day 
        #condition_death = True
        #if pd.notna(obito_vaccinated):
        #    if obito_vaccinated<(d1_date+timedelta(days=days_after)):
        #        condition_death = False
        ## --> Verify hospitalization before the current day
        #condition_hosp = True
        #if np.any(pd.notna(hosp_vaccinated)):
        #    for hosp_date in hosp_vaccinated:
        #        if pd.notna(hosp_date) and hosp_date<(d1_date+timedelta(days=days_after)):
        #            condition_hosp = False
        #            break
        ## --> Verify ICU before the current day
        #condition_icu = True
        #if np.any(pd.notna(uti_vaccinated)):
        #    for uti_date in uti_vaccinated:
        #        if pd.notna(uti_date) and uti_date<(d1_date+timedelta(days=days_after)):
        #            condition_icu = False
        #            break

        #if condition_pri==False or condition_death==False or condition_hosp==False or condition_icu==False:
        #    continue

    items_matching = matchings.items()
    pareados = pd.DataFrame({"CPF CASO": [ x[0] for x in items_matching ], "CPF CONTROLE": [ x[1] for x in items_matching ]})
    for cpf in [ x[0] for x in items_matching ]+[ x[1] for x in items_matching ]:
        matched[cpf]=True
    return pareados, matched



def perform_matching_new2(df_vac, df_pop, control_reservoir, control_used, person_dates, hdi_index, cohort, date_str, days_after=0):
    '''

    '''
    # --> Vaccinated during cohort 
    df_vac = df_vac[(pd.notna(df_vac[date_str])) & (df_vac[date_str]>=cohort[0]) & (df_vac[date_str]<=cohort[1])]
    # --> Apply callbacks
    # ----> First symptoms eligibility
    f_pri = lambda x: True if np.all(pd.isna(x["INTEGRA PRI SINTOMAS DATA"])) else ( 
                      False if np.any([ pd.notna(pri_date) and pri_date<(x[date_str]+timedelta(days=days_after)) for pri_date in x["INTEGRA PRI SINTOMAS DATA"] ]) else True )
    # ----> Hospitalization occurrence eligibility
    f_hosp = lambda x: True if np.all(pd.isna(x["DATA HOSPITALIZACAO"])) else ( 
                       False if np.any([ pd.notna(hosp_date) and hosp_date<(x[date_str]+timedelta(days=days_after)) for hosp_date in x["DATA HOSPITALIZACAO"] ]) else True )
    # ----> ICU occurrence eligibility
    f_icu = lambda x: True if np.all(pd.isna(x["DATA UTI"])) else ( 
                      False if np.any([ pd.notna(icu_date) and icu_date<(x[date_str]+timedelta(days=days_after)) for icu_date in x["DATA UTI"] ]) else True )
    # ----> Death by Covid-19 occurrence eligibility
    f_death = lambda x: True if pd.isna(x["DATA OBITO"]) else ( 
                        False if x["DATA OBITO"]<(x[date_str]+timedelta(days=days_after)) else True )
    # --> Apply conditions
    df_vac["PRI CONDICAO"] = df_vac[["INTEGRA PRI SINTOMAS DATA", date_str]].apply(f_pri, axis=1)
    df_vac["HOSPITAL CONDICAO"] = df_vac[["DATA HOSPITALIZACAO", date_str]].apply(f_hosp, axis=1)
    df_vac["UTI CONDICAO"] = df_vac[["DATA UTI", date_str]].apply(f_icu, axis=1)
    df_vac["OBITO CONDICAO"] = df_vac[["DATA OBITO", date_str]].apply(f_death, axis=1)
    # --> Consider only eligible vaccinated
    df_vac = df_vac[(df_vac["PRI CONDICAO"]==True) & (df_vac["HOSPITAL CONDICAO"]==True) & (df_vac["UTI CONDICAO"]==True) & (df_vac["OBITO CONDICAO"]==True)]
    # --> Create a partition of the data regarding the date of vaccination (D1 or D2). 
    datelst = utils.generate_date_list(cohort[0], cohort[1], interval=1)
    sub_dfs = [ df_vac[df_vac[date_str]==date_] for date_ in datelst ]

    '''
        'matchings': Pairs -> key-value:vaccinated-unvaccinated.
        'matched': Whether an individual was matched.
    '''
    matchings = defaultdict(lambda:-1) 
    matched = defaultdict(lambda:False) 
    for sub_df in tqdm(sub_dfs):
        if sub_df.shape[0]==0:
            continue
        
        current_date = sub_df[date_str].iat[0]
        cpf_vaccinated_lst = list(sub_df.index)
        matching_vars_lst = sub_df[f"MATCHING {hdi_index}"].tolist()

        # Find the pair for all individuals who took the vaccine at the current date.
        for j in range(len(cpf_vaccinated_lst)):
            cpf_vaccinated = cpf_vaccinated_lst[j]
            matching_vars = matching_vars_lst[j]

            #pair = find_unvaccinated_pair(current_date, matching_vars, control_reservoir, person_dates, days_after)
            pair = find_pair_fix2(current_date, matching_vars, control_reservoir, control_used, person_dates, days_after)
            if pair!=-1:
                matchings[cpf_vaccinated] = pair
    
    items_matching = matchings.items()
    pareados = pd.DataFrame({"CPF CASO": [ x[0] for x in items_matching ], "CPF CONTROLE": [ x[1] for x in items_matching ]})
    for cpf in [ x[0] for x in items_matching ]+[ x[1] for x in items_matching ]:
        matched[cpf]=True
    return pareados, matched


def perform_matching(datelst, df_vac, control_reservoir, control_used, control_dates, HDI_index, cohort, col_names):
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
            HDI_index:
                Integer.
            col_names:
                dictionary.
        Return:
            pareados:
                pandas.DataFrame.
            matched:
                dictionary.
    '''
    matchings = defaultdict(lambda:-1) # Pairs -> key-value:case-control.
    matched = defaultdict(lambda:False) # Whether an individual was matched.
    for current_date in tqdm(datelst):
        # Select all people who was vaccinated at the current date
        df_vac["compare_date"] = df_vac[col_names["D1"]].apply(lambda x: True if x==current_date else False)
        current_vaccinated = df_vac[df_vac["compare_date"]==True]
        
        # Metadata on case individual
        cpf_list = current_vaccinated["CPF"].tolist()
        age_list = current_vaccinated["IDADE"].tolist()
        sex_list = current_vaccinated["SEXO"].tolist()
        idh_list = current_vaccinated[f"IDH {HDI_index}"].tolist()
        # --> Main dates to assess eligibility
        pri_list = current_vaccinated["INTEGRA PRI SINTOMAS DATA"].tolist()
        obito_list = current_vaccinated["DATA OBITO"].tolist()
        hosp_list = current_vaccinated["DATA HOSPITALIZACAO"].tolist()
        uti_list = current_vaccinated["DATA UTI"].tolist()

        # For each person vaccinated at the current date, check if there is a control for he/she.
        # But first check if the vaccinated is eligible.
        for j in range(0, len(cpf_list)):
            # Eligibility
            # --> Verify first symptoms
            condition_pri = True # True as eligible 
            if np.any(pd.notna(pri_list[j])):
                for pri_date in pri_list[j]:
                    # Drop any case who had symptoms during the cohort before the current day
                    if pd.notna(pri_date) and pri_date<current_date and pri_date>cohort[0]:
                        condition_pri = False
                        break
            # --> Verify if there is an outcome before the current day 
            condition_death = True
            if pd.notna(obito_list[j]):
                if obito_list[j]<current_date:
                    condition_death = False
            # --> Verify hospitalization before the current day
            condition_hosp = True
            if np.any(pd.notna(hosp_list[j])):
                for hosp_date in hosp_list[j]:
                    if pd.notna(hosp_date) and hosp_date<current_date:
                        condition_hosp = False
                        break
            # --> Verify ICU before the current day
            condition_icu = True
            if np.any(pd.notna(uti_list[j])):
                for uti_date in uti_list[j]:
                    if pd.notna(uti_date) and uti_date<current_date:
                        condition_icu = False
                        break

            if condition_pri==False or condition_death==False or condition_hosp==False or condition_icu==False:
                continue

            matching_vars = (age_list[j], sex_list[j], idh_list[j])
            #pair = find_pair(current_date, matching_vars, control_reservoir, control_used, control_dates)
            pair = find_pair_fix(current_date, matching_vars, control_reservoir, control_used, control_dates, cohort)
            if pair!=-1:
                matchings[cpf_list[j]] = pair
    
    items_matching = matchings.items()
    pareados = pd.DataFrame({"CPF CASO": [ x[0] for x in items_matching ], "CPF CONTROLE": [ x[1] for x in items_matching ]})
    for cpf in [ x[0] for x in items_matching ]+[ x[1] for x in items_matching ]:
        matched[cpf]=True
    return pareados, matched

def perform_matching1(cohort, df_vac, control_reservoir, control_used, person_dates, hdi_index, date_str, days_after):
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
            person_dates:
                collections.defaultdict.
            hdi_index:
                Integer.
            col_names:
                dictionary.
        Return:
            pareados:
                pandas.DataFrame.
            matched:
                dictionary.
    '''
    df_vac = df_vac.reset_index().copy()
    datelst = utils.generate_date_list(cohort[0], cohort[1], interval=1)
    matchings = defaultdict(lambda:-1) # Pairs -> key-value:case-control.
    matched = defaultdict(lambda:False) # Whether an individual was matched.
    for current_date in tqdm(datelst):
        # Select all people who was vaccinated at the current date
        current_vaccinated = df_vac[df_vac[date_str]==current_date]
        
        # Metadata on case individual
        cpf_list = current_vaccinated["CPF"].array
        matching_vars_list = current_vaccinated[f"MATCHING {hdi_index}"].array
        # --> Main dates to assess eligibility
        pri_list = current_vaccinated["INTEGRA PRI SINTOMAS DATA"].array
        obito_list = current_vaccinated["DATA OBITO"].array
        hosp_list = current_vaccinated["DATA HOSPITALIZACAO"].array
        uti_list = current_vaccinated["DATA UTI"].array

        # For each person vaccinated at the current date, check if there is a control for he/she.
        # But first check if the vaccinated is eligible.
        for j in range(0, len(cpf_list)):
            # Eligibility
            # --> Verify first symptoms
            condition_pri = True # True as eligible 
            if np.any(pd.notna(pri_list[j])):
                for pri_date in pri_list[j]:
                    # Drop any case who had symptoms during the cohort before the current day
                    if pd.notna(pri_date) and pri_date<=(current_date+timedelta(days=days_after)):
                        condition_pri = False
                        break
            if not condition_pri: continue
            # --> Verify if there is an outcome before the current day 
            condition_death = True
            if pd.notna(obito_list[j]):
                if obito_list[j]<=(current_date+timedelta(days=days_after)):
                    condition_death = False
            if not condition_death: continue
            # --> Verify hospitalization before the current day
            condition_hosp = True
            if np.any(pd.notna(hosp_list[j])):
                for hosp_date in hosp_list[j]:
                    if pd.notna(hosp_date) and hosp_date<=(current_date+timedelta(days=days_after)):
                        condition_hosp = False
                        break
            if not condition_hosp: continue
            # --> Verify ICU before the current day
            condition_icu = True
            if np.any(pd.notna(uti_list[j])):
                for uti_date in uti_list[j]:
                    if pd.notna(uti_date) and uti_date<=(current_date+timedelta(days=days_after)):
                        condition_icu = False
                        break
            if not condition_icu: continue

            matching_vars = matching_vars_list[j]
            pair = find_pair_fix(current_date, matching_vars, control_reservoir, control_used, person_dates, days_after, age_interval=1)
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
    data_obito = defaultdict(lambda:np.nan)
    data_obito_geral = defaultdict(lambda:np.nan)
    data_d1 = defaultdict(lambda:np.nan)
    data_d2 = defaultdict(lambda:np.nan)
    data_hospitalizado = defaultdict(lambda: np.nan)
    data_uti = defaultdict(lambda: np.nan)
    for j in range(df_pop.shape[0]):
        cpf = df_pop["CPF"].iat[j]
        d1_dt = df_pop[col_names["D1"]].iat[j]
        d2_dt = df_pop[col_names["D2"]].iat[j]
        obito = df_pop[col_names["OBITO COVID"]].iat[j]
        obito_geral = df_pop[col_names["OBITO GERAL"]].iat[j]
        hosp = df_pop["DATA HOSPITALIZACAO"].iat[j]
        uti = df_pop["DATA UTI"].iat[j]
        if pd.notna(obito):
            data_obito[cpf] = obito
        elif pd.notna(obito_geral):
            data_obito_geral[cpf] = obito_geral
        if pd.notna(d1_dt):
            data_d1[cpf] = d1_dt
        if pd.notna(d2_dt):
            data_d2[cpf] = d2_dt
        if np.any(pd.notna(hosp)): # it is a list of dates.
            data_hospitalizado[cpf] = hosp
        if np.any(pd.notna(uti)): # it is a list of dates.
            data_uti[cpf] = uti

    # -- create cols with dates --
    datas = {
        "CPF": [], "DATA D1": [], "DATA D2": [],
        "DATA OBITO COVID": [], "DATA OBITO GERAL": [],
        "DATA HOSPITALIZACAO": [], "DATA UTI": [],
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
        datas["DATA HOSPITALIZACAO"] += [data_hospitalizado[cpf_caso], data_hospitalizado[cpf_control]]
        datas["DATA UTI"] += [data_uti[cpf_caso], data_uti[cpf_control]]
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
            datas["DATA HOSPITALIZACAO"] += [data_hospitalizado[cpf]]
            datas["DATA UTI"] += [data_uti[cpf]]
            datas["TIPO"] += ["NAO PAREADO"]
            datas["PAR"] += [np.nan]
            datas["PAREADO"] += [False]
    print("Incluindo não pareados ... Concluído.")
    datas = pd.DataFrame(datas)
    return datas

def find_pair(cur_date, matching_vars, control_reservoir, control_used, control_dates, age_interval=1):
    '''
        Based on the features of the exposed individual, find a control to match.

        Args:
            cur_date:
                Date of vaccination of the exposed individual. It cannot be after
                the vaccination of the control (if vaccinated).
            matching_vars:
                3-Tuple of values. age, sex and HDI(categorical) of the case.
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
    age_case, sex_case, idh_case = matching_vars[0], matching_vars[1], matching_vars[2]
    eligible_controls = []
    for j in np.arange(age_case-age_interval, age_case+age_interval+1, 1):
        eligible_controls += control_reservoir[(j, sex_case, idh_case)]
    
    for cpf_control in eligible_controls:
        if not control_used[cpf_control]:
            condition_d1 = control_dates["D1"][cpf_control]==-1 or control_dates["D1"][cpf_control]>cur_date
            #condition_death = control_dates["DEATH COVID"][cpf_control]==-1 or control_dates["DEATH COVID"][cpf_control]>cur_date
            #condition_death_geral = control_dates["DEATH GENERAL"][cpf_control]==-1 or control_dates["DEATH GENERAL"][cpf_control]>cur_date
            #if condition_d1 and condition_death and condition_death_geral:
            #    control_used[cpf_control] = True
            #    return cpf_control
            if condition_d1:
                control_used[cpf_control] = True
                return cpf_control
    return -1

def find_pair_fix(cur_date, matching_vars, control_reservoir, control_used, control_dates, days_after=0, age_interval=1):
    '''
        Based on the features of the exposed individual, find a control to match. Also,
        the control needs to fill some requirements to be considered as eligible: no
        symptoms during the cohort before the current date, no outcome before the current
        day (in case there is any missing data on first symptoms, this verification should
        be useful).

        Args:
            cur_date:
                Date of vaccination of the exposed individual. It cannot be after
                the vaccination of the control (if vaccinated).
            matching_vars:
                3-Tuple of values. age, sex and HDI(categorical) of the case.
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
    age_case, sex_case, idh_case = matching_vars[0], matching_vars[1], matching_vars[2]
    
    # Select potential controls with age within range, same sex and same HDI code.
    eligible_controls = []
    for j in np.arange(age_case-age_interval, age_case+age_interval+1, 1):
        eligible_controls += [control for control in control_reservoir[(j, sex_case, idh_case)] if not control_used[control]]
    
    for cpf_control in eligible_controls:
        if not control_used[cpf_control]:
            # If vaccinated, vaccinated only after current date.
            condition_d1 = pd.isna(control_dates[cpf_control]["DATA D1"]) or control_dates[cpf_control]["DATA D1"]>cur_date
            if not condition_d1: continue
            # If died by Covid-19, only after current date.
            condition_death = pd.isna(control_dates[cpf_control]["DATA OBITO"]) or control_dates[cpf_control]["DATA OBITO"]>(cur_date+timedelta(days=days_after))
            if not condition_death: continue
            # If the individual had first symptoms before matching and after start of cohort.
            condition_pri = True
            if np.any(pd.notna(control_dates[cpf_control]["INTEGRA PRI SINTOMAS DATA"])):
                for pri in control_dates[cpf_control]["INTEGRA PRI SINTOMAS DATA"]:
                    if pd.notna(pri) and pri<=(cur_date+timedelta(days=days_after)):
                        condition_pri = False
                        break
            if not condition_pri: continue
            # If hospitalized before matching, hospitalized only after the current date.
            condition_hosp = True
            if np.any(pd.notna(control_dates[cpf_control]["DATA HOSPITALIZACAO"])):
                for hosp in control_dates[cpf_control]["DATA HOSPITALIZACAO"]:
                    if pd.notna(hosp) and hosp<=(cur_date+timedelta(days=days_after)):
                        condition_hosp = False
                        break
            if not condition_hosp: continue
            # If attended ICU before matching, ICU only after the current date.
            condition_uti = True
            if np.any(pd.notna(control_dates[cpf_control]["DATA UTI"])):
                for uti in control_dates[cpf_control]["DATA UTI"]:
                    if pd.notna(uti) and uti<=(cur_date+timedelta(days=days_after)):
                        condition_uti = False
                        break
            if not condition_uti: continue

            control_used[cpf_control] = True
            return cpf_control                
    return -1

def find_pair_fix2(cur_date, matching_vars, control_reservoir, control_used, control_dates, days_after=0, age_interval=1):
    '''
        Based on the features of the exposed (vaccinated) individual, find a control to match. 
        Also, the control needs to fulfill some requirements to be considered as eligible: no
        symptoms before the current date, no outcome before the current day (in case there is
        any missing data on first symptoms, this verification should be useful).

        Args:
            cur_date:
                Date of vaccination of the exposed individual. It cannot be after
                the vaccination of the control (if vaccinated).
            matching_vars:
                3-Tuple of values. age, sex and HDI(categorical) of the case.
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
    age_case, sex_case, idh_case = matching_vars[0], matching_vars[1], matching_vars[2]
    
    # --> Select potential controls with age within range, same sex and same HDI code.
    eligible_controls = []
    for j in np.arange(age_case-age_interval, age_case+age_interval+1, 1):
        eligible_controls += control_reservoir[(j, sex_case, idh_case)]
    
    for cpf_control in eligible_controls:
        if not control_used[cpf_control]:
            # If vaccinated, vaccinated only after current date.
            condition_d1 = pd.isna(control_dates[cpf_control]["DATA D1"]) or control_dates[cpf_control]["DATA D1"]>cur_date
            # If died by Covid-19, only after current date.
            condition_death = pd.isna(control_dates[cpf_control]["DATA OBITO"]) or control_dates[cpf_control]["DATA OBITO"]>cur_date
            # If the individual had first symptoms before matching and after start of cohort.
            condition_pri = True
            if np.any(pd.notna(control_dates[cpf_control]["INTEGRA PRI SINTOMAS DATA"])):
                for pri in control_dates[cpf_control]["INTEGRA PRI SINTOMAS DATA"]:
                    if pd.notna(pri) and pri<=(cur_date+timedelta(days=days_after)):
                        condition_pri = False
                        break
            # If hospitalized before matching, hospitalized only after the current date.
            condition_hosp = True
            if np.any(pd.notna(control_dates[cpf_control]["DATA HOSPITALIZACAO"])):
                for hosp in control_dates[cpf_control]["DATA HOSPITALIZACAO"]:
                    if pd.notna(hosp) and hosp<=(cur_date+timedelta(days=days_after)):
                        condition_hosp = False
                        break
            # If attended ICU before matching, ICU only after the current date.
            condition_uti = True
            if np.any(pd.notna(control_dates[cpf_control]["DATA UTI"])):
                for uti in control_dates[cpf_control]["DATA UTI"]:
                    if pd.notna(uti) and uti<=(cur_date+timedelta(days=days_after)):
                        condition_uti = False
                        break

            if condition_d1 and condition_pri and condition_death and condition_hosp and condition_uti:
                control_used[cpf_control] = True
                return cpf_control
    return -1

def find_unvaccinated_pair(ref_date, matching_vars, control_reservoir, person_dates, 
                           days_after, age_interval=1):
    '''
        Based on the features of the exposed (vaccinated) individual, find a control to match. 
        Also, the control needs to fulfill some requirements to be considered as eligible: no
        symptoms before the current date, no outcome before the current day (in case there is
        any missing data on first symptoms, this verification should be useful).

        Args:
            ref_date:
                Date of vaccination of the exposed individual. It cannot be after
                the vaccination of the control (if vaccinated).
            matching_vars:
                3-Tuple of values. age, sex and HDI(categorical) of the case.
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
    matching_vars_control = None
    control_index = None
    control_found = False
    age_case, sex_case, idh_case = matching_vars[0], matching_vars[1], matching_vars[2]
    for age in np.arange(age_case-age_interval, age_case+age_interval+1, 1):
        control_found = False
        for index, cpf_control in enumerate(control_reservoir[(age, sex_case, idh_case)]):
            # --> Verify eligibility conditions
            # ----> D1 date
            condition_d1 = pd.notna(person_dates[cpf_control]["DATA D1"]) or person_dates[cpf_control]["DATA D1"]<=ref_date
            if condition_d1:
                continue

            # ----> Death by Covid-19
            condition_death = pd.notna(person_dates[cpf_control]["DATA OBITO"]) and person_dates[cpf_control]["DATA OBITO"]<=(ref_date+timedelta(days_after))
            if condition_death:
                continue
            
            # ----> First symptoms
            pri_dates = np.array(person_dates[cpf_control]["INTEGRA PRI SINTOMAS DATA"])
            pri_dates = pri_dates[pd.notna(pri_dates)]
            condition_pri = np.any(pri_dates<=(ref_date+timedelta(days=days_after)))
            if condition_pri:
                continue

            # ----> Hospitalization
            hosp_dates = np.array(person_dates[cpf_control]["DATA HOSPITALIZACAO"])
            hosp_dates = hosp_dates[pd.notna(hosp_dates)]
            condition_hosp = np.any(hosp_dates<=(ref_date+timedelta(days=days_after)))
            if condition_hosp:
                continue

            # ----> Admission to ICU
            icu_dates = np.array(person_dates[cpf_control]["DATA UTI"])
            icu_dates = icu_dates[pd.notna(icu_dates)]
            condition_icu = np.any(icu_dates<=(ref_date+timedelta(days=days_after)))
            if condition_icu:
                continue

            control_found = True
            control_index = index
            matching_vars_control = (age, sex_case, idh_case)
            break
        
        if control_found:
            cpf_control = control_reservoir[matching_vars_control][control_index]
            cpf_endlist = control_reservoir[matching_vars_control][-1]
            control_reservoir[matching_vars_control][control_index] = cpf_endlist
            control_reservoir[matching_vars_control].pop()
            return cpf_control
    return -1

def extract_pair_dict(cpf_caso, cpf_controle, eventos_df):
    '''
    
    '''
    sub_caso = eventos_df.loc[cpf_caso]
    sub_controle = eventos_df.loc[cpf_controle]
    if type(sub_caso)==pd.Series:
        caso_dict = [sub_caso.to_dict()]
    else:
        caso_dict = sub_caso[sub_caso["TIPO"]=="CASO"].to_dict(orient="records")
    if type(sub_controle)==pd.Series:
        controle_dict = [sub_controle.to_dict()]
    else:
        controle_dict = sub_controle[sub_controle["TIPO"]=="CONTROLE"].to_dict(orient='records')
    return caso_dict, controle_dict

def compare_pair_survival(caso_hash, controle_hash, events_col, final_cohort, col_event="OBITO COVID"):
    '''
        Calculate the intervals between different events of vaccinated-unvaccinated pairs. 
        
        Args:
            caso_hash:
                dictionary. Dictionary containing the main info of the vaccinated individual, 
                like cpf identifier, D1 and D2 dates and other events, including the outcomes
                of interest. 
            controle_hash:
                dictionary. Dictionary containing the main info of the unvaccinated individual, 
                like cpf identifier, D1 and D2 dates and other events, including the outcomes
                of interest. 
            events_col:
                dictionary. Dictionary containing the column names of the events of interest for
                the analysis.
            final_cohort:
                datetime.date.
            dose:
                String.
            col_event:
                String.
        Return:
            res:
                dictionary.
    '''
    cpf_caso = caso_hash["CPF"]
    cpf_controle = controle_hash["CPF"]
    # Get events of case
    caso_d1_date = caso_hash[events_col["D1"]]
    caso_d2_date = caso_hash[events_col["D2"]]
    caso_covid_date = caso_hash[events_col[col_event]]
    caso_geral_date = caso_hash[events_col["OBITO GERAL"]]
    # Get events of control
    control_d1_date = controle_hash[events_col["D1"]]
    control_d2_date = controle_hash[events_col["D2"]]
    control_covid_date = controle_hash[events_col[col_event]]
    control_geral_date = controle_hash[events_col["OBITO GERAL"]]
    
    f = lambda x: x if pd.notna(x) else np.nan
    g = lambda x,y: (x-y).days if not pd.isna(x) and not pd.isna(y) else np.nan
            
    # --> D1
    start_date = caso_d1_date
    caso_diff = {
        "D1 to D2": g(f(caso_d2_date),start_date),
        "D1 to D1_CONTROL": g(f(control_d1_date),start_date),
        "D1 to COVID": g(f(caso_covid_date), start_date),
        "D1 to GERAL": g(f(caso_geral_date), start_date),
        "D1 to FIM": g(final_cohort, start_date)
    }
    control_diff = {
        "D1 to D1_CONTROL": g(f(control_d1_date),start_date),
        "D1 to COVID_CONTROL": g(f(control_covid_date),start_date),
        "D1 to GERAL_CONTROL": g(f(control_geral_date), start_date),
        "D1 to D2": g(f(caso_d2_date),start_date), # test, think
        "D1 to FIM": g(final_cohort,start_date)
    }
    
    # --> D2
    start_date = caso_d2_date
    caso_diff_d2 = {
        "D2 to D1_CONTROL": g(f(control_d1_date),start_date),
        "D2 to COVID": g(f(caso_covid_date), start_date),
        "D2 to GERAL": g(f(caso_geral_date), start_date),
        "D2 to FIM": g(final_cohort, start_date)
    }
    control_diff_d2 = {
        "D2 to D1_CONTROL": g(f(control_d1_date),start_date),
        "D2 to COVID_CONTROL": g(f(control_covid_date),start_date),
        "D2 to GERAL_CONTROL": g(f(control_geral_date), start_date),
        "D2 to FIM": g(final_cohort,start_date)
    }
    
    caso_events_d1 = [ (key, caso_diff[key]) for key in caso_diff.keys() ]
    control_events_d1 = [ (key, control_diff[key]) for key in control_diff.keys() ]
    caso_events_d2 = [ (key, caso_diff_d2[key]) for key in caso_diff_d2.keys() ]
    control_events_d2 = [ (key, control_diff_d2[key]) for key in control_diff_d2.keys() ]
    res = {
        "CPF CASO": cpf_caso,
        "CPF CONTROLE": cpf_controle,
        "D1": (caso_events_d1, control_events_d1),
        "D2": (caso_events_d2, control_events_d2)
    }
    return res

def compare_pair_survival_v2(caso_hash, controle_hash, events_col, final_cohort, dose="DATA D1", col_event="OBITO COVID"):
    '''
        Calculate the intervals between different events of vaccinated-unvaccinated pairs. 
        
        Args:
            caso_hash:
                dictionary. Dictionary containing the main info of the vaccinated individual, 
                like cpf identifier, D1 and D2 dates and other events, including the outcomes
                of interest. 
            controle_hash:
                dictionary. Dictionary containing the main info of the unvaccinated individual, 
                like cpf identifier, D1 and D2 dates and other events, including the outcomes
                of interest. 
            events_col:
                dictionary. Dictionary containing the column names of the events of interest for
                the analysis.
            final_cohort:
                datetime.date.
            dose:
                String.
            col_event:
                String.
        Return:
            res:
                dictionary.
    '''
    cpf_caso = caso_hash["CPF"]
    cpf_controle = controle_hash["CPF"]
    # Get events of case
    caso_d1_date = caso_hash[events_col["D1"]]
    caso_d2_date = caso_hash[events_col["D2"]]
    caso_covid_date = caso_hash[events_col[col_event]]
    caso_geral_date = caso_hash[events_col["OBITO GERAL"]]
    # Get events of control
    control_d1_date = controle_hash[events_col["D1"]]
    control_d2_date = controle_hash[events_col["D2"]]
    control_covid_date = controle_hash[events_col[col_event]]
    control_geral_date = controle_hash[events_col["OBITO GERAL"]]
    
    f = lambda x: x if pd.notna(x) else np.nan
    g = lambda x,y: (x-y).days if not pd.isna(x) and not pd.isna(y) else np.nan

    caso_diff, control_diff = None, None
    if dose=="DATA D1":
        start_date = caso_d1_date
        caso_diff = {
            "D1 to D2": g(f(caso_d2_date),start_date),
            "D1 to D1_CONTROL": g(f(control_d1_date),start_date),
            "D1 to COVID": g(f(caso_covid_date), start_date),
            "D1 to GERAL": g(f(caso_geral_date), start_date),
            "D1 to FIM": g(final_cohort, start_date)
        }
        control_diff = {
            "D1 to D1_CONTROL": g(f(control_d1_date),start_date),
            "D1 to COVID_CONTROL": g(f(control_covid_date),start_date),
            "D1 to GERAL_CONTROL": g(f(control_geral_date), start_date),
            "D1 to D2": g(f(caso_d2_date),start_date), # test, think
            "D1 to FIM": g(final_cohort,start_date)
        }
    elif dose=="DATA D2":
        start_date = caso_d2_date
        caso_diff = {
            "D2 to D1_CONTROL": g(f(control_d1_date),start_date),
            "D2 to COVID": g(f(caso_covid_date), start_date),
            "D2 to GERAL": g(f(caso_geral_date), start_date),
            "D2 to FIM": g(final_cohort, start_date)
        }
        control_diff = {
            "D2 to D1_CONTROL": g(f(control_d1_date),start_date),
            "D2 to COVID_CONTROL": g(f(control_covid_date),start_date),
            "D2 to GERAL_CONTROL": g(f(control_geral_date), start_date),
            "D2 to FIM": g(final_cohort,start_date)
        }
    else: pass
            
    caso_events = [ (key, caso_diff[key]) for key in caso_diff.keys() ]
    control_events = [ (key, control_diff[key]) for key in control_diff.keys() ]

    res = {
        "CPF CASO": cpf_caso,
        "CPF CONTROLE": cpf_controle, 
        "DOSE": (caso_events, control_events)
    }
    return res

def define_interval_type(info):
    '''
        Used with .apply over 'RESULT' column of intervals
    '''
    new_df = {"CPF CASO": None, "CPF CONTROLE": None, "CASO D1 INTERVALO": None, 
              "CONTROLE D1 INTERVALO": None, "CASO D1 CENSURADO": None,
              "CONTROLE D1 CENSURADO": None, "CASO D2 INTERVALO": None,
              "CONTROLE D2 INTERVALO": None, "CASO D2 CENSURADO": None,
              "CONTROLE D2 CENSURADO": None}
    
    new_df["CPF CASO"] = info["CPF CASO"]
    new_df["CPF CONTROLE"] = info["CPF CONTROLE"]

    info_d1_caso = info["D1"][0]
    info_d1_controle = info["D1"][1]
    info_d1_caso = [ x for x in info_d1_caso if not pd.isna(x[1]) ]
    info_d1_controle = [ x for x in info_d1_controle if not pd.isna(x[1]) ]
    info_d1_caso = sorted(info_d1_caso, key=lambda tup: tup[1])
    info_d1_controle = sorted(info_d1_controle, key=lambda tup: tup[1])
    if info_d1_caso[0][0]=="D1 to COVID":
        new_df["CASO D1 INTERVALO"] = info_d1_caso[0][1]
        new_df["CASO D1 CENSURADO"] = False
    else:
        new_df["CASO D1 INTERVALO"] = info_d1_caso[0][1]
        new_df["CASO D1 CENSURADO"] = True
    if info_d1_controle[0][0]=="D1 to COVID_CONTROL":
        new_df["CONTROLE D1 INTERVALO"] = info_d1_controle[0][1]
        new_df["CONTROLE D1 CENSURADO"] = False
    else:
        new_df["CONTROLE D1 INTERVALO"] = info_d1_controle[0][1]
        new_df["CONTROLE D1 CENSURADO"] = True

    # --> D2
    info_d2_caso = info["D2"][0]
    info_d2_controle = info["D2"][1]
    info_d2_caso = [ x for x in info_d2_caso if not pd.isna(x[1]) ]
    info_d2_controle = [ x for x in info_d2_controle if not pd.isna(x[1]) ]
    info_d2_caso = sorted(info_d2_caso, key=lambda tup: tup[1])
    info_d2_controle = sorted(info_d2_controle, key=lambda tup: tup[1])
    if len(info_d2_caso)==0 or len(info_d2_controle)==0:
        new_df["CASO D2 INTERVALO"] = np.nan
        new_df["CASO D2 CENSURADO"] = np.nan
        new_df["CONTROLE D2 INTERVALO"] = np.nan
        new_df["CONTROLE D2 CENSURADO"] = np.nan
        return new_df
    if info_d2_caso[0][0]=="D2 to COVID":
        new_df["CASO D2 INTERVALO"] = info_d2_caso[0][1]
        new_df["CASO D2 CENSURADO"] = False
    else:
        new_df["CASO D2 INTERVALO"] = info_d2_caso[0][1]
        new_df["CASO D2 CENSURADO"] = True
    if info_d2_controle[0][0]=="D2 to COVID_CONTROL":
        new_df["CONTROLE D2 INTERVALO"] = info_d2_controle[0][1]
        new_df["CONTROLE D2 CENSURADO"] = False
    else:
        new_df["CONTROLE D2 INTERVALO"] = info_d2_controle[0][1]
        new_df["CONTROLE D2 CENSURADO"] = True

    # If case individual does not have second dose, remove pair
    if pd.isna(new_df["CASO D2 INTERVALO"]):
        new_df["CONTROLE D2 INTERVALO"] = np.nan
    # If event occurred before starting of the pair cohort, remove pair 
    if pd.notna(new_df["CASO D2 INTERVALO"]) and pd.notna(new_df["CONTROLE D2 INTERVALO"]):
        if new_df["CASO D2 INTERVALO"]<0 or new_df["CONTROLE D2 INTERVALO"]<0:
            new_df["CASO D2 INTERVALO"] = np.nan
            new_df["CONTROLE D2 INTERVALO"] = np.nan
    if pd.notna(new_df["CASO D1 INTERVALO"]) and pd.notna(new_df["CONTROLE D1 INTERVALO"]):
        if new_df["CASO D1 INTERVALO"]<0 or new_df["CONTROLE D1 INTERVALO"]<0:
            new_df["CASO D1 INTERVALO"] = np.nan
            new_df["CONTROLE D1 INTERVALO"] = np.nan

    return new_df

def define_interval_type_v2(info, date_str="DATA D1"):
    '''
        Used with .apply over 'RESULT' column of intervals
    '''
    new_df = {"CPF CASO": None, "CPF CONTROLE": None, "CASO INTERVALO": None,
              "CONTROLE INTERVALO": None, "CASO CENSURADO": None, "CONTROLE CENSURADO": None }
    
    new_df["CPF CASO"] = info["CPF CASO"]
    new_df["CPF CONTROLE"] = info["CPF CONTROLE"]

    if date_str=="DATA D1":
        info_d1_caso = info["DOSE"][0]
        info_d1_controle = info["DOSE"][1]
        info_d1_caso = [ x for x in info_d1_caso if not pd.isna(x[1]) ]
        info_d1_controle = [ x for x in info_d1_controle if not pd.isna(x[1]) ]
        info_d1_caso = sorted(info_d1_caso, key=lambda tup: tup[1])
        info_d1_controle = sorted(info_d1_controle, key=lambda tup: tup[1])
        if info_d1_caso[0][0]=="D1 to COVID":
            new_df["CASO INTERVALO"] = info_d1_caso[0][1]
            new_df["CASO CENSURADO"] = False
        else:
            new_df["CASO INTERVALO"] = info_d1_caso[0][1]
            new_df["CASO CENSURADO"] = True
        if info_d1_controle[0][0]=="D1 to COVID_CONTROL":
            new_df["CONTROLE INTERVALO"] = info_d1_controle[0][1]
            new_df["CONTROLE CENSURADO"] = False
        else:
            new_df["CONTROLE INTERVALO"] = info_d1_controle[0][1]
            new_df["CONTROLE CENSURADO"] = True
    elif date_str=="DATA D2":
        info_d2_caso = info["DOSE"][0]
        info_d2_controle = info["DOSE"][1]
        info_d2_caso = [ x for x in info_d2_caso if not pd.isna(x[1]) ]
        info_d2_controle = [ x for x in info_d2_controle if not pd.isna(x[1]) ]
        info_d2_caso = sorted(info_d2_caso, key=lambda tup: tup[1])
        info_d2_controle = sorted(info_d2_controle, key=lambda tup: tup[1])
        if len(info_d2_caso)==0 or len(info_d2_controle)==0:
            new_df["CASO INTERVALO"] = np.nan
            new_df["CASO CENSURADO"] = np.nan
            new_df["CONTROLE INTERVALO"] = np.nan
            new_df["CONTROLE CENSURADO"] = np.nan
            return new_df
        if info_d2_caso[0][0]=="D2 to COVID":
            new_df["CASO INTERVALO"] = info_d2_caso[0][1]
            new_df["CASO CENSURADO"] = False
        else:
            new_df["CASO INTERVALO"] = info_d2_caso[0][1]
            new_df["CASO CENSURADO"] = True
        if info_d2_controle[0][0]=="D2 to COVID_CONTROL":
            new_df["CONTROLE INTERVALO"] = info_d2_controle[0][1]
            new_df["CONTROLE CENSURADO"] = False
        else:
            new_df["CONTROLE INTERVALO"] = info_d2_controle[0][1]
            new_df["CONTROLE CENSURADO"] = True

    # If event occurred before starting of the pair cohort, remove pair 
    if pd.notna(new_df["CASO INTERVALO"]) and pd.notna(new_df["CONTROLE INTERVALO"]):
        if new_df["CASO INTERVALO"]<0 or new_df["CONTROLE INTERVALO"]<0:
            new_df["CASO INTERVALO"] = np.nan
            new_df["CONTROLE INTERVALO"] = np.nan

    return new_df

def organize_table_for_survival(df, event_string="OBITO"):
    '''
    
    '''
    tb = {"CPF": [], "TIPO": [], f"t - D1 {event_string}": [], f"E - D1 {event_string}": [], f"t - D2 {event_string}": [], f"E - D2 {event_string}": []}
    for j in range(df.shape[0]):
        res = df["FINAL SURVIVAL"].iat[j]
        tb["CPF"].append(res["CPF CASO"])
        tb["TIPO"].append("CASO")
        tb[f"t - D1 {event_string}"].append(res['CASO D1 INTERVALO'])
        tb[f"t - D2 {event_string}"].append(res['CASO D2 INTERVALO'])
        tb[f"E - D1 {event_string}"].append(not res['CASO D1 CENSURADO'])
        tb[f"E - D2 {event_string}"].append(not res['CASO D2 CENSURADO'])
        tb["CPF"].append(res["CPF CONTROLE"])
        tb["TIPO"].append("CONTROLE")
        tb[f"t - D1 {event_string}"].append(res['CONTROLE D1 INTERVALO'])
        tb[f"t - D2 {event_string}"].append(res['CONTROLE D2 INTERVALO'])
        tb[f"E - D1 {event_string}"].append(not res['CONTROLE D1 CENSURADO'])
        tb[f"E - D2 {event_string}"].append(not res['CONTROLE D2 CENSURADO'])
    return pd.DataFrame(tb)

def organize_table_for_survival_v2(df, event_string="OBITO"):
    '''
    
    '''
    tb = {"CPF": [], "TIPO": [], f"t - {event_string}": [], f"E - {event_string}": [], f"t - {event_string}": [], f"E - {event_string}": []}
    for j in range(df.shape[0]):
        res = df["FINAL SURVIVAL"].iat[j]
        tb["CPF"].append(res["CPF CASO"])
        tb["TIPO"].append("CASO")
        tb[f"t - {event_string}"].append(res['CASO INTERVALO'])
        tb[f"E - {event_string}"].append(not res['CASO CENSURADO'])
        tb["CPF"].append(res["CPF CONTROLE"])
        tb["TIPO"].append("CONTROLE")
        tb[f"t - {event_string}"].append(res['CONTROLE INTERVALO'])
        tb[f"E - {event_string}"].append(not res['CONTROLE CENSURADO'])
    return pd.DataFrame(tb)

def new_hospitalization_date(x, cohort):
    '''
    
    '''
    if not np.any(pd.notna(x)):
        return np.nan
    x = np.sort([xx for xx in x if pd.notna(xx)]) 
    condition = (x>=cohort[0]) & (x<=cohort[1])
    if x[condition].shape[0]>0:
        return x[condition][0]
    else:
        return np.nan

def f_hdi_range(x, irange, include_nans=False):
    '''
        Auxiliary function for .apply() to define categorical variables
        for the HDI variable.
    '''
    for k in range(len(irange)-1):
        if include_nans and pd.isna(x):
            return k
        if x>irange[k] and x<=irange[k+1]:
            return k
