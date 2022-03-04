from ast import Return
import numpy as np
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

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
    df = df[df["TESTE POSITIVO ANTES COORTE"]==False]

    # Third rule: Remove all inconsistencies between death date and vaccination.
    df = df[(df["OBITO INCONSISTENCIA CARTORIOS"]==False) & (df["OBITO INCONSISTENCIA COVID"]==False)]

    # Fourth rule: Remove all deaths before cohort.
    #df["OBITO ANTES COORTE"] = df[subcol].apply(lambda x: True if pd.notna(x[subcol[0]]) and x[subcol[0]]<cohort[0] else ( True if pd.notna(x[subcol[1]]) and x[subcol[1]]<cohort[0] else False), axis=1)
    df = df[df["OBITO ANTES COORTE"]==False]

    # Fifth rule(for hospitalization): Remove all hospitalization before cohort.
    #df["HOSPITALIZACAO ANTES COORTE"] = df["DATA HOSPITALIZACAO"].apply(lambda x: np.any([dts<cohort[0] for dts in x if pd.notna(dts)]) if np.any(pd.notna(x)) else False)
    df = df[df["HOSPITALIZACAO ANTES COORTE"]==False]

    # Sixth rule: Select vaccine and nonvaccinated individuals
    df_vaccinated = df[df["VACINA APLICADA"]==vaccine]
    df_nonvaccinated = df[df["VACINA APLICADA"]=="NAO VACINADO"]
    return df, df_vaccinated, df_nonvaccinated

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
        if np.any(pd.notna(dt_hosp_covid)):
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
    data_hospitalizado = defaultdict(lambda: np.nan)
    for j in range(df_pop.shape[0]):
        cpf = df_pop["CPF"].iat[j]
        d1_dt = df_pop[col_names["D1"]].iat[j]
        d2_dt = df_pop[col_names["D2"]].iat[j]
        obito = df_pop[col_names["OBITO COVID"]].iat[j]
        obito_geral = df_pop[col_names["OBITO GERAL"]].iat[j]
        hosp = df_pop["DATA HOSPITALIZACAO"].iat[j]
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

    # -- create cols with dates --
    datas = {
        "CPF": [], "DATA D1": [], "DATA D2": [],
        "DATA OBITO COVID": [], "DATA OBITO GERAL": [],
        "DATA HOSPITALIZACAO": [],
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
            datas["TIPO"] += ["NAO PAREADO"]
            datas["PAR"] += [np.nan]
            datas["PAREADO"] += [False]
    print("Incluindo não pareados ... Concluído.")
    datas = pd.DataFrame(datas)
    return datas

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

def compare_pair_survival(caso_hash, controle_hash, events_col, final_cohort):
    '''
        Description.
        
        Args:
            caso_hash:
                dictionary.
            controle_hash:
                dictionary.
            events_col:
                dictionary.
            final_cohort:
                datetime.date.
        Return:
            res:
                dictionary.
    '''
    cpf_caso = caso_hash["CPF"]
    cpf_controle = controle_hash["CPF"]
    # Get events of case
    caso_d1_date = caso_hash[events_col["D1"]]
    caso_d2_date = caso_hash[events_col["D2"]]
    caso_covid_date = caso_hash[events_col["OBITO COVID"]]
    caso_geral_date = caso_hash[events_col["OBITO GERAL"]]
    # Get events of control
    control_d1_date = controle_hash[events_col["D1"]]
    control_d2_date = controle_hash[events_col["D2"]]
    control_covid_date = controle_hash[events_col["OBITO COVID"]]
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

    return new_df

def organize_table_for_survival(df):
    '''
    
    '''
    tb = {"CPF": [], "TIPO": [], f"t - D1 OBITO": [], f"E - D1 OBITO": [], f"t - D2 OBITO": [], f"E - D2 OBITO": []}
    for j in range(df.shape[0]):
        res = df["FINAL SURVIVAL"].iat[j]
        tb["CPF"].append(res["CPF CASO"])
        tb["TIPO"].append("CASO")
        tb["t - D1 OBITO"].append(res['CASO D1 INTERVALO'])
        tb["t - D2 OBITO"].append(res['CASO D2 INTERVALO'])
        tb["E - D1 OBITO"].append(not res['CASO D1 CENSURADO'])
        tb["E - D2 OBITO"].append(not res['CASO D2 CENSURADO'])
        tb["CPF"].append(res["CPF CONTROLE"])
        tb["TIPO"].append("CONTROLE")
        tb["t - D1 OBITO"].append(res['CONTROLE D1 INTERVALO'])
        tb["t - D2 OBITO"].append(res['CONTROLE D2 INTERVALO'])
        tb["E - D1 OBITO"].append(not res['CONTROLE D1 CENSURADO'])
        tb["E - D2 OBITO"].append(not res['CONTROLE D2 CENSURADO'])
    return pd.DataFrame(tb)
