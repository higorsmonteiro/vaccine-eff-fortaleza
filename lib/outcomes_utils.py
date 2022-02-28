import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt
from collections import defaultdict

def format_events(events_df, event_str="OBITO", 
                init_cohort=dt.date(2021, 1, 21), final_cohort=dt.date(2021, 8, 31)):
        '''
            Format the complete event table to perform a survival analysis.
            
            Considering a DataFrame "events_df" following the schema imposed 
            by the function self.get_intervals(), we quantify the durations 
            and types of an event defined by the string "event_str".

            Args:
                events_df:
                    DataFrame containing the dates of each event during the 
                    cohort (if applicable). Events are: date of vaccination
                    (D1 and D2), date of death by Covid-19, date of positive 
                    Covid-19 test and date of hospitalization by Covid-19. In
                    case, an individual did not experience an event, the date 
                    is replaced by a NaN value.
                event_str:
                    {"OBITO", "TESTE", "HOSPITALIZACAO"} - String representing 
                    the name of the event to consider.
            Return:
                duration_df:
                    DataFrame containing the durations and types of each event.
        '''
        subset_cols = ["DATA D1 CASO", "DATA D1 CONTROLE", "DATA D2 CASO", 
                       "DATA D2 CONTROLE", f"{event_str} CASO", f"{event_str} CONTROLE"]
        df = events_df[subset_cols]
        duration_df = events_df[["CPF CASO", "CPF CONTROLE"]+subset_cols].copy()
        
        duration_case = []
        duration_control = []
        event_type_case = []
        event_type_control = []
        # Calculate duration and type of event for each pair.
        for j in range(0, df.shape[0]):
            d1_case = df["DATA D1 CASO"].iat[j]
            d1_control = df["DATA D1 CONTROLE"].iat[j]
            d2_case = df["DATA D2 CASO"].iat[j]
            d2_control = df["DATA D2 CONTROLE"].iat[j]
            event_case = df[f"{event_str} CASO"].iat[j]
            event_control = df[f"{event_str} CONTROLE"].iat[j]

            # Verify all possible events: Outcome for case or control, 
            # end of cohort for both, vaccination of the matched control
            # for the case, etc.
            start_date = d1_case.date()
            if not pd.isna(d1_control):
                d1_control = d1_control.date()
            if not pd.isna(event_case):
                event_case = event_case.date()
            if not pd.isna(event_control):
                event_control = event_control.date()

            event_name_case = ["D1 CONTROLE", "EVENTO CASO"]
            event_dates_case = [d1_control, event_case]
            event_name_control = ["D1 CONTROLE", "EVENTO CONTROLE"]
            event_dates_control = [d1_control, event_control]

            # Eliminate any NaN value for the exposed individual.
            to_include_indexes = []
            for j in range(len(event_dates_case)):
                if not pd.isna(event_dates_case[j]):
                    to_include_indexes.append(j)
            event_name_case = [ event_name_case[j] for j in to_include_indexes ]
            event_dates_case = [ event_dates_case[j] for j in to_include_indexes ]
            # Eliminate any NaN value for unexposed individual (control).
            to_include_indexes = []
            for j in range(len(event_dates_control)):
                if not pd.isna(event_dates_control[j]):
                    to_include_indexes.append(j)
            event_name_control = [ event_name_control[j] for j in to_include_indexes ]
            event_dates_control = [ event_dates_control[j] for j in to_include_indexes ]

            # Sort dates for exposed individual.
            sorted_arg = np.argsort(event_dates_case)
            event_name_case = [ event_name_case[k] for k in sorted_arg ]
            event_dates_case = [ event_dates_case[k] for k in sorted_arg ]
            # Sort dates for unexposed individual (control).
            sorted_arg = np.argsort(event_dates_control)
            event_name_control = [ event_name_control[k] for k in sorted_arg ]
            event_dates_control = [ event_dates_control[k] for k in sorted_arg ]

            # Calculate time duration and event type - Exposed (outcome or censoring)
            if len(event_dates_case)==0:
                # Event: end of cohort
                cur_duration_case = (final_cohort-start_date).days
                cur_type_case = False # Right censored
            elif event_name_case[0]=="D1 CONTROLE":
                # Vaccination of the matched control
                cur_duration_case = (event_dates_case[0]-start_date).days
                cur_type_case = False # Right censored
            elif event_name_case[0]=="EVENTO CASO":
                cur_duration_case = (event_dates_case[0]-start_date).days
                cur_type_case = True # Outcome

            # Calculate time duration and event type - Unexposed (outcome or censoring)
            if len(event_dates_control)==0:
                # Event: end of cohort
                cur_duration_control = (final_cohort-start_date).days
                cur_type_control = False # Right censored
            elif event_name_control[0]=="D1 CONTROLE":
                # Vaccination of the matched control
                cur_duration_control = (event_dates_control[0]-start_date).days
                cur_type_control = False # Right censored
            elif event_name_control[0]=="EVENTO CONTROLE":
                cur_duration_control = (event_dates_control[0]-start_date).days
                cur_type_control = True # Outcome

            duration_case.append(cur_duration_case)
            duration_control.append(cur_duration_control)
            event_type_case.append(cur_type_case)
            event_type_control.append(cur_type_control)
        duration_df["DURACAO CASO"] = duration_case
        duration_df["DURACAO CONTROLE"] = duration_control
        duration_df["TIPO EVENTO CASO"] = event_type_case
        duration_df["TIPO EVENTO CONTROLE"] = event_type_control
        #duration_df = duration_df[(duration_df["DURACAO CASO"]>=0) & (duration_df["DURACAO CONTROLE"]>=0)]
        return duration_df

def format_events1(events_df, event_str, init_cohort=dt.date(2021, 1, 21),
                  final_cohort=dt.date(2021, 8, 31)):
        '''
            Format the complete event table to perform a survival analysis.

            Considering a DataFrame "events_df" following the schema imposed by
            the function self.get_intervals(), we quantify the durations and types
            of an event defined by the string "event_str".

            Args:
                events_df:
                    DataFrame containing the dates of each event during the 
                    cohort (if applicable). Events are: date of vaccination
                    (D1 and D2), date of death by Covid-19, date of positive 
                    Covid-19 test and date of hospitalization by Covid-19. In
                    case an individual did not experience an event, the date 
                    is replaced by a NaN value.
                events_str:
                    {"OBITO", "TESTE", "HOSPITALIZACAO"} - String representing
                    the name of the event to consider.
            Return:
                duration_df:
                    DataFrame containing the durations and types of each event.
        '''
        df = events_df[events_df["PAREADO"]==True]

        df_cases = df[df["TIPO"]=="CASO"]
        df_control = df[df["TIPO"]=="CONTROLE"]
        for j in range(0, df_cases.shape[0]):
            cpf_case = df_cases["CPF"].iat[j]
            cpf_control = df_cases["PAR"].iat[j]

            d1_case = df_cases["DATA D1"].iat[j]
            d2_case = df_cases["DATA D2"].iat[j]
            death_case = df_cases["DATA OBITO"].iat[j]
            hospitalization_case = df_cases["DATA HOSPITALIZACAO"].iat[j]
            tests_case = df_cases["DATA TESTE"].iat[j]

            row_control = df_control[df_control["CPF"]==cpf_control]
            d1_control = row_control["DATA D1"].iat[0]
            d2_control = row_control["DATA D2"].iat[0]
            death_control = row_control["DATA OBITO"].iat[0]
            hospitalization_control = row_control["DATA HOSPITALIZACAO"].iat[0]
            tests_control = row_control["DATA TESTE"].iat[0]

            # Verify all possible events: Outcome for case or control, 
            # end of cohort for both, vaccination of the matched control
            # for the case, etc.
            start_date = d1_case.date()
            if not pd.isna(d1_control):
                d1_control = d1_control.date()
            if not pd.isna(death_case):
                death_case = death_case.date()
            if not pd.isna(death_control):
                death_control = death_control.date()
            if not pd.isna(hospitalization_case):
                hospitalization_case = hospitalization_case.date()
            if not pd.isna(hospitalization_control):
                hospitalization_control = hospitalization_control.date()
            if not pd.isna(tests_case):
                tests_case = tests_case.date()
            if not pd.isna(tests_control):
                tests_control = tests_control.date()

            start_date = d1_case
            dur_death = defaultdict(lambda:np.nan)
            type_death = defaultdict(lambda:np.nan)
            res1 = calc_duration(start_date,d1_case,d1_control,death_case,death_control,final_cohort)
            duration_death_case, duration_death_control = res1[0], res1[1]
            type_case_death, type_control_death = res1[2], res1[3]
            dur_death[cpf_case] = duration_death_case
            dur_death[cpf_control] = duration_death_control
            type_death[cpf_case] = type_case_death
            type_death[cpf_control] = type_control_death

            dur_hospt = defaultdict(lambda:np.nan)
            type_hospt = defaultdict(lambda:np.nan)
            res2 = calc_duration(start_date,d1_case,d1_control,hospitalization_case,hospitalization_control,final_cohort)
            duration_hospt_case, duration_hospt_control = res2[0], res2[1]
            type_case_hospt, type_control_hospt = res2[2], res2[3]
            dur_hospt[cpf_case] = duration_hospt_case
            dur_hospt[cpf_control] = duration_hospt_control
            type_hospt[cpf_case] = type_case_hospt
            type_hospt[cpf_control] = type_control_hospt

            dur_test = defaultdict(lambda:np.nan)
            type_test = defaultdict(lambda:np.nan)
            res3 = calc_duration(start_date,d1_case,d1_control,tests_case,tests_control,final_cohort)
            duration_test_case, duration_test_control = res3[0], res3[1]
            type_case_test, type_control_test = res3[2], res3[3]
            dur_test[cpf_case] = duration_test_case
            dur_test[cpf_control] = duration_test_control
            type_test[cpf_case] = type_case_test
            type_test[cpf_control] = type_control_test

        survival_df = {
            "DURACAO OBITO": [],
            "DURACAO HOSPITALIZACAO": [],
            "DURACAO TESTES": [],
            "NAO CENSURADO OBITO": [],
            "NAO CENSURADO HOSPITALIZACAO": [],
            "NAO CENSURADO TESTES": []
        }
        count = 0
        for j in range(0, events_df.shape[0]):
            cpf = events_df["CPF"].iat[j]
            if pd.isna(dur_death[cpf]):
                count+=1
            survival_df["DURACAO OBITO"].append(dur_death[cpf])
            survival_df["DURACAO HOSPITALIZACAO"].append(dur_hospt[cpf])
            survival_df["DURACAO TESTES"].append(dur_test[cpf])
            survival_df["NAO CENSURADO OBITO"].append(type_death[cpf])
            survival_df["NAO CENSURADO HOSPITALIZACAO"].append(type_hospt[cpf])
            survival_df["NAO CENSURADO TESTES"].append(type_test[cpf])
        print(count) 
        survival_df = pd.DataFrame(survival_df)
        survival_df = pd.concat([events_df, survival_df], axis=1)
        return survival_df


def calculate_riskdays(events_df, final_cohort=dt.date(2021, 8, 31)):
    '''
        Description.

        Args:
            events_df:
                pandas.DataFrame. It contains the dates of each event during the 
                cohort (if applicable). Events are: date of vaccination (D1 and 
                D2), date of death by Covid-19, date of positive Covid-19 test and 
                date of hospitalization by Covid-19. In case, an individual did 
                not experience an event, the date is replaced by a NaN value.
            final_cohort:
                datetime.date.
        Return:
            survival_tb:
                DataFrame.
    '''
    col_d1, col_d2 = "DATA D1", "DATA D2"
    col_obito_covid = "DATA OBITO COVID"
    col_obito_geral = "DATA OBITO GERAL"
    col_hospt = "DATA HOSPITALIZACAO",
    col_teste = "DATA TESTE"
    # Include only the columns present in the events table.
    date_cols = [col_d1, col_d2, col_obito_covid, col_obito_geral]
    for j in date_cols:
        events_df[j] = pd.to_datetime(events_df[j], format="%Y-%m-%d", errors="coerce")

    # Add hospitalization and tests if required.
    HASH_NAMES = {
        col_d1: defaultdict(lambda:-1),
        col_obito_covid: defaultdict(lambda:-1),
        col_obito_geral: defaultdict(lambda:-1)
    }
    for j in range(0, events_df.shape[0]): 
        HASH_NAMES[col_d1][events_df["CPF"].iat[j]] = events_df[col_d1].iat[j]
        HASH_NAMES[col_obito_geral][events_df["CPF"].iat[j]] = events_df[col_obito_geral].iat[j]
        HASH_NAMES[col_obito_covid][events_df["CPF"].iat[j]] = events_df[col_obito_covid].iat[j]

    new_cols = {
        "OBITO COVID DURACAO": defaultdict(lambda:np.nan),
        "COM DESFECHO - OBITO COVID": defaultdict(lambda:False),
        "HOSPITALIZACAO DURACAO": defaultdict(lambda:np.nan),
        "COM DESFECHO - HOSPITALIZACAO": defaultdict(lambda:False),
        "TESTE DURACAO": defaultdict(lambda:np.nan),
        "COM DESFECHO - TESTE": defaultdict(lambda:False)
    }
    casos_df = events_df[events_df["TIPO"]=="CASO"]
    for j in tqdm(range(0, casos_df.shape[0])):
        cpf_caso = casos_df["CPF"].iat[j]
        cpf_controle = casos_df["PAR"].iat[j]
        
        d1_caso = HASH_NAMES[col_d1][cpf_caso]
        d1_controle = HASH_NAMES[col_d1][cpf_controle]
        obito_caso_geral = HASH_NAMES[col_obito_geral][cpf_caso]
        obito_controle_geral = HASH_NAMES[col_obito_geral][cpf_controle]
        # OBITO COVID
        obito_caso = HASH_NAMES[col_obito_covid][cpf_caso]
        obito_controle = HASH_NAMES[col_obito_covid][cpf_controle]
        res = calc_duration((d1_caso, d1_controle), (obito_caso_geral, obito_controle_geral), (obito_caso, obito_controle), final_cohort)
        caso_dur, controle_dur = res[0], res[1]
        caso_type, controle_type = res[2], res[3]
        new_cols["OBITO COVID DURACAO"][cpf_caso] = caso_dur
        new_cols["OBITO COVID DURACAO"][cpf_controle] = controle_dur
        new_cols["COM DESFECHO - OBITO COVID"][cpf_caso] = caso_type
        new_cols["COM DESFECHO - OBITO COVID"][cpf_controle] = controle_type
        # HOSPITALIZACAO (OLD)
        #hospt_caso = hospt_hash[cpf_caso]
        #hospt_controle = hospt_hash[cpf_controle]
        #res = calc_duration(d1_caso, d1_controle, hospt_caso, hospt_controle, final_cohort)
        #caso_dur, controle_dur = res[0], res[1]
        #caso_type, controle_type = res[2], res[3]
        #new_cols["HOSPITALIZACAO DURACAO"][cpf_caso] = caso_dur
        #new_cols["HOSPITALIZACAO DURACAO"][cpf_controle] = controle_dur
        #new_cols["COM DESFECHO - HOSPITALIZACAO"][cpf_caso] = caso_type
        #new_cols["COM DESFECHO - HOSPITALIZACAO"][cpf_controle] = controle_type

    for key in new_cols.keys():
        events_df[key] = events_df["CPF"].apply(lambda x: new_cols[key][x])
    return events_df

def calc_duration(d1_caso_controle, obito_geral_caso_controle, event_caso_controle, final_cohort):
    '''
        Description.

        Args:
            d1_caso_controle:
                Tuple of dates.
            obito_geral_caso_controle:
                Tuple of dates.
            event_caso_controle:
                Tuple of dates.
    '''
    d1_caso = d1_caso_controle[0]
    d1_controle = d1_caso_controle[1]
    obito_geral_caso = obito_geral_caso_controle[0]
    obito_geral_controle = obito_geral_caso_controle[1]
    event_caso = event_caso_controle[0]
    event_controle = event_caso_controle[1]

    start_date = d1_caso.date()
    if not pd.isna(d1_controle):
        d1_controle = d1_controle.date()
    if not pd.isna(obito_geral_caso):
        obito_geral_caso = obito_geral_caso.date()
    if not pd.isna(obito_geral_controle):
        obito_geral_controle = obito_geral_controle.date()
    if not pd.isna(event_caso):
        event_caso = event_caso.date()
    if not pd.isna(event_controle):
        event_controle = event_controle.date()

    event_name_case = ["D1 CONTROLE", "OBITO GERAL CASO", "EVENTO CASO"]
    event_dates_case = [d1_controle, obito_geral_caso, event_caso]
    event_name_control = ["D1 CONTROLE", "OBITO GERAL CONTROLE", "EVENTO CONTROLE"]
    event_dates_control = [d1_controle, obito_geral_controle, event_controle]

    # Eliminate any NaN value for the exposed individual.
    to_include_indexes = []
    for j in range(len(event_dates_case)):
        if not pd.isna(event_dates_case[j]):
            to_include_indexes.append(j)
    event_name_case = [ event_name_case[j] for j in to_include_indexes ]
    event_dates_case = [ event_dates_case[j] for j in to_include_indexes ]
    # Eliminate any NaN value for unexposed individual (control).
    to_include_indexes = []
    for j in range(len(event_dates_control)):
        if not pd.isna(event_dates_control[j]):
            to_include_indexes.append(j)
    event_name_control = [ event_name_control[j] for j in to_include_indexes ]
    event_dates_control = [ event_dates_control[j] for j in to_include_indexes ]

    # Sort dates for exposed individual.
    sorted_arg = np.argsort(event_dates_case)
    event_name_case = [ event_name_case[k] for k in sorted_arg ]
    event_dates_case = [ event_dates_case[k] for k in sorted_arg ]
    # Sort dates for unexposed individual (control).
    sorted_arg = np.argsort(event_dates_control)
    event_name_control = [ event_name_control[k] for k in sorted_arg ]
    event_dates_control = [ event_dates_control[k] for k in sorted_arg ]

    # Calculate time duration and event type - Exposed (outcome or censoring)
    if len(event_dates_case)==0:
        # Event: end of cohort
        cur_duration_case = (final_cohort-start_date).days
        cur_type_case = False # Right censored
    elif event_name_case[0]=="D1 CONTROLE":
        # Vaccination of the matched control
        cur_duration_case = (event_dates_case[0]-start_date).days
        cur_type_case = False # Right censored
    elif event_name_case[0]=="OBITO GERAL CASO":
        # Vaccination of the matched control
        cur_duration_case = (event_dates_case[0]-start_date).days
        cur_type_case = False # Right censored
    elif event_name_case[0]=="EVENTO CASO":
        cur_duration_case = (event_dates_case[0]-start_date).days
        cur_type_case = True # Outcome

    # Calculate time duration and event type - Unexposed (outcome or censoring)
    if len(event_dates_control)==0:
        # Event: end of cohort
        cur_duration_control = (final_cohort-start_date).days
        cur_type_control = False # Right censored
    elif event_name_control[0]=="D1 CONTROLE":
        # Vaccination of the matched control
        cur_duration_control = (event_dates_control[0]-start_date).days
        cur_type_control = False # Right censored
    elif event_name_control[0]=="OBITO GERAL CONTROLE":
        # Vaccination of the matched control
        cur_duration_control = (event_dates_control[0]-start_date).days
        cur_type_control = False # Right censored
    elif event_name_control[0]=="EVENTO CONTROLE":
        cur_duration_control = (event_dates_control[0]-start_date).days
        cur_type_control = True # Outcome

    return (cur_duration_case, cur_duration_control, cur_type_case, cur_type_control)