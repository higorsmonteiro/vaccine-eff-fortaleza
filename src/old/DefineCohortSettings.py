'''
    ...
'''
import os
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
import lib.utils as utils
import lib.db_utils as dutils
from datetime import timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta

class DefineCohortSettings:
    def __init__(self, vacineja2plus_df, init_cohort, final_cohort):
        '''
            Description.

            Args:
                vacineja2plus_df:
        '''
        self.vacineja2plus_df = vacineja2plus_df.copy()
        self.init_cohort = init_cohort
        self.final_cohort = final_cohort

    def define_eligibility(self, partial=14, fully=14, return_=True):
        '''
        
        '''
        subset = ["DATA D1(VACINADOS)", "DATA D2(VACINADOS)"]
        self.vacineja2plus_df["VACINA STATUS - COORTE"] = self.vacineja2plus_df[subset].apply(lambda x: f_when_vaccine(x, self.init_cohort, self.final_cohort), axis=1)
        self.vacineja2plus_df["IMUNIZACAO MAXIMA ATE FIM DA COORTE"] = self.vacineja2plus_df[subset].apply(lambda x: f_immunization(x, self.init_cohort, self.final_cohort, partial, fully), axis=1)

        # --> Eligibility by tests
        subset = ["DATA SOLICITACAO(TESTES)", "DATA COLETA(TESTES)", "RESULTADO FINAL GAL-INTEGRASUS"]
        self.vacineja2plus_df["ELIGIBILIDADE TESTE"] = self.vacineja2plus_df[subset].apply(lambda x: f_eligible_test(x, self.init_cohort, self.final_cohort), axis=1)
        
        subset = "IMUNIZACAO MAXIMA ATE FIM DA COORTE"
        aptos = ["NAO VACINADO", "PARCIALMENTE IMUNIZADO", "TOTALMENTE IMUNIZADO", "VACINADO SEM IMUNIZACAO"]
        self.vacineja2plus_df["ELIGIBILIDADE COORTE GERAL"] = self.vacineja2plus_df[subset].apply(lambda x: "APTO" if x in aptos else "NAO APTO")
        # --> Eligibility for cases partial
        self.vacineja2plus_df["ELIGIBILIDADE EXPOSTO PARCIAL"] = self.vacineja2plus_df[subset].apply(lambda x: "APTO" if x=="PARCIALMENTE IMUNIZADO" else "NAO APTO")
        # --> Eligibility for cases fully
        self.vacineja2plus_df["ELIGIBILIDADE EXPOSTO TOTAL"] = self.vacineja2plus_df[subset].apply(lambda x: "APTO" if x=="TOTALMENTE IMUNIZADO" else "NAO APTO")
        # --> Create column with age based on the final of cohort.
        self.vacineja2plus_df["IDADE"] = self.vacineja2plus_df["DATA NASCIMENTO(VACINEJA)"].apply(lambda x: relativedelta(self.final_cohort, x.date()).years)
        self.vacineja2plus_df = self.vacineja2plus_df.drop_duplicates(subset=["CPF"], keep="first")

        if return_:
            return self.vacineja2plus_df

    def dynamical_matching(self, vaccine="CORONAVAC", return_=True, verbose=False, age_thr=18, seed=0):
        '''
            Description.

            Args:
                return_:
            Return:

        '''
        if "ELIGIBILIDADE TESTE" not in self.vacineja2plus_df.columns:
            return -1

        datelst = utils.generate_date_list(self.init_cohort, self.final_cohort)
        # --> Apply essential filters
        # First, consider only people with age older or equal to 18 years old.
        df = self.vacineja2plus_df[self.vacineja2plus_df["IDADE"]>=age_thr]
        df = df[df["OBITO INCONSISTENCIA"]!="S"]
        df = df[df["DATA VACINA CONSISTENCIA"]!="N"]
        # Filter by eligibility
        df = df[(df["ELIGIBILIDADE TESTE"]=="APTO") & (df["ELIGIBILIDADE COORTE GERAL"]=="APTO")]
        # Obtain set of vaccinated and unvaccinated.
        df_vaccinated = df[df["VACINA(VACINADOS)"]==vaccine]
        df_vaccinated = df_vaccinated.dropna(subset=["DATA D1(VACINADOS)"], axis=0)
        df_unvaccinated = df[pd.isna(df["VACINA(VACINADOS)"])]
        if verbose:
            print(f"Dimensão de elegíveis após aplicacão das condições: {df.shape}")
            print(f"Número restante de óbitos: {df['DATA OBITO'].notnull().sum()}")
            print(f"Número restante de hospitalizados: {df['DATA HOSPITALIZACAO'].notnull().sum()}")
            print(f"Número restante de testes: {df['DATA SOLICITACAO(TESTES)'].notnull().sum()}")
            print(f"Número de vacinados elegíveis para {vaccine}: {df_vaccinated.shape[0]}")
        
        #condition_exposed1 = df_vaccinated["ELIGIBILIDADE TESTE"]=="APTO"
        #condition_exposed2 = df_vaccinated["ELIGIBILIDADE COORTE GERAL"]=="APTO" 
        #df_vaccinated = df_vaccinated[(condition_exposed1) & (condition_exposed2)]
        #condition_unexposed1 = df_unvaccinated["ELIGIBILIDADE TESTE"]=="APTO"
        #condition_unexposed2 = df_unvaccinated["ELIGIBILIDADE COORTE GERAL"]=="APTO"
        #df_unvaccinated = df_unvaccinated[(condition_unexposed1) & (condition_unexposed2)]

        # -- CREATE CONTROL RESERVOIR --
        control_dates = {
            "D1": defaultdict(lambda:-1),
            "DEATH": defaultdict(lambda:-1),
            "HOSPITAL": defaultdict(lambda:-1)
        }
        control_reservoir = defaultdict(lambda:[])
        control_used = defaultdict(lambda: False)
        df_join = pd.concat([df_vaccinated, df_unvaccinated])
        print("Criando reservatório de controles ...")
        for j in tqdm(range(0, df_join.shape[0])):
            cpf = df_join["CPF"].iat[j]
            age = df_join["IDADE"].iat[j]
            sex = df_join["SEXO(VACINEJA)"].iat[j]
            d1 = df_join["DATA D1(VACINADOS)"].iat[j]
            dt_death = df_join["DATA OBITO"].iat[j]
            dt_hospt = df_join["DATA HOSPITALIZACAO"].iat[j]
            control_reservoir[(age,sex)].append(cpf)
            if not pd.isna(d1):
                control_dates["D1"][cpf] = d1.date()
            if not pd.isna(dt_death):
                control_dates["DEATH"][cpf] = dt_death.date()
            if not pd.isna(dt_hospt):
                control_dates["HOSPITAL"][cpf] = dt_hospt.date()

        if seed!=0:
            np.random.seed(seed)
            for key in control_reservoir.keys():
                np.random.shuffle(control_reservoir[key])

        matchings = defaultdict(lambda:-1)
        print("Executando pareamento ...")
        for cur_date in tqdm(datelst):
            # Select all people who was vaccinated at the current date
            df_vaccinated["compare_date"] = df_vaccinated["DATA D1(VACINADOS)"].apply(lambda x: "TRUE" if x.date()==cur_date else "FALSE")
            current_vaccinated = df_vaccinated[df_vaccinated["compare_date"]=="TRUE"]
            #print(current_vaccinated.shape)
            cpf_list = current_vaccinated["CPF"].tolist()
            age_list = current_vaccinated["IDADE"].tolist()
            sex_list = current_vaccinated["SEXO(VACINEJA)"].tolist()
            date_list = current_vaccinated["DATA D1(VACINADOS)"].tolist()
            # For each person vaccinated at the current date, check if there is a control for he/she.
            for j in range(0, len(cpf_list)):
                pair = find_pair(cur_date, age_list[j], sex_list[j], control_reservoir, control_used, control_dates)
                if pair!=-1:
                    matchings[cpf_list[j]] = pair
        
        items_matching = matchings.items()
        pareados = pd.DataFrame({"CPF CASO": [ x[0] for x in items_matching ], "CPF CONTROLE": [ x[1] for x in items_matching ]})
        events_df = self.get_intervals(pareados, df_vaccinated, df_unvaccinated)
        matched = defaultdict(lambda:False)
        for cpf in [ x[0] for x in items_matching ]+[ x[1] for x in items_matching ]:
            matched[cpf]=True
        df_join["PAREADO"] = df_join["CPF"].apply(lambda x: "SIM" if matched[x] else "NAO")
        return events_df, df_join

    def get_intervals(self, df_pairs, df_vac, df_unvac):
        '''
            Description.
            
            Args:
                df_pairs:
                df_vac:
                df_unvac:
        '''
        pareado = defaultdict(lambda: False)
        matched_cpfs = df_pairs["CPF CASO"].tolist()+df_pairs["CPF CONTROLE"].tolist()
        [ pareado.update({cpf:True}) for cpf in matched_cpfs ]

        data_teste = defaultdict(lambda: np.nan)
        data_hospitalizado = defaultdict(lambda:np.nan)
        data_obito = defaultdict(lambda:np.nan)
        data_d1 = defaultdict(lambda:np.nan)
        data_d2 = defaultdict(lambda:np.nan)
        df_join = pd.concat([df_vac, df_unvac])
        for j in range(0, df_join.shape[0]):
            cpf = df_join["CPF"].iat[j]
            obito = df_join["DATA OBITO"].iat[j]
            teste = df_join["DATA SOLICITACAO(TESTES)"].iat[j]
            hospitalizacao = df_join["DATA HOSPITALIZACAO"].iat[j]
            d1_dt = df_join["DATA D1(VACINADOS)"].iat[j]
            d2_dt = df_join["DATA D2(VACINADOS)"].iat[j]
            if not pd.isna(obito):
                data_obito[cpf] = obito
            if not pd.isna(d1_dt):
                data_d1[cpf] = d1_dt
            if not pd.isna(d2_dt):
                data_d2[cpf] = d2_dt
            if not pd.isna(teste):
                data_teste[cpf] = teste
            if not pd.isna(hospitalizacao):
                data_hospitalizado[cpf] = hospitalizacao

        # -- create cols with dates --
        datas = {
            "CPF": [],
            "DATA D1": [],
            "DATA D2": [],
            "DATA OBITO": [],
            "DATA TESTE": [],
            "DATA HOSPITALIZACAO": [],
            "TIPO": [],
            "PAR": [],
            "PAREADO": []
        }
        print("Criando tabela de eventos ...") 
        for j in tqdm(range(0, df_pairs.shape[0])):
            cpf_caso = df_pairs["CPF CASO"].iloc[j]
            cpf_control = df_pairs["CPF CONTROLE"].iloc[j]

            # Fill new columns
            datas["CPF"] += [cpf_caso, cpf_control]
            datas["DATA D1"] += [data_d1[cpf_caso], data_d1[cpf_control]]
            datas["DATA D2"] += [data_d2[cpf_caso], data_d2[cpf_control]]
            datas["DATA OBITO"] += [data_obito[cpf_caso], data_obito[cpf_control]]
            datas["DATA HOSPITALIZACAO"] += [data_hospitalizado[cpf_caso], data_hospitalizado[cpf_control]]
            datas["DATA TESTE"] += [data_teste[cpf_caso], data_teste[cpf_control]]
            datas["TIPO"] += ["CASO", "CONTROLE"]
            datas["PAR"] += [cpf_control, cpf_caso]
            datas["PAREADO"] += [True, True]
        print("Incluindo não pareados ...")
        for cpf in tqdm(pareado.keys()):
            if pareado[cpf]==False:
                datas["CPF"] += [cpf]
                datas["DATA D1"] += [data_d1[cpf]]
                datas["DATA D2"] += [data_d2[cpf]]
                datas["DATA OBITO"] += [data_obito[cpf]]
                datas["DATA HOSPITALIZACAO"] += [data_hospitalizado[cpf]]
                datas["DATA TESTE"] += [data_teste[cpf]]
                datas["TIPO"] += ["NAO PAREADO"]
                datas["PAR"] += [np.nan]
                datas["PAREADO"] += [False]

        datas = pd.DataFrame(datas)
        return datas
        
def f_eligible_test(x, init_cohort, final_cohort):
    '''
    
    '''
    result = x["RESULTADO FINAL GAL-INTEGRASUS"]
    coleta = x["DATA COLETA(TESTES)"]
    solicit = x["DATA SOLICITACAO(TESTES)"]
    if pd.isna(result):
        return "APTO"
    elif result=="POSITIVO":
        if not pd.isna(solicit):
            solicit = solicit.date()
            if solicit<init_cohort:
                return "NAO APTO"
            else:
                return "APTO"
        elif not pd.isna(coleta):
            coleta = coleta.date()
            if coleta<init_cohort:
                return "NAO APTO"
            else:
                return "APTO"
        else:
            return "NAO APTO"
    else:
        return "APTO"

def f_when_vaccine(x, init_cohort, final_cohort):
    '''
        
    '''
    d1 = x["DATA D1(VACINADOS)"]
    d2 = x["DATA D2(VACINADOS)"]

    if pd.isna(d1): d1 = -1
    else: d1 = d1.date()
    if pd.isna(d2): d2 = -1
    else: d2 = d2.date()

    if d1!=-1 and d1<init_cohort:
        return "D1 ANTES DA COORTE"
    elif d1!=-1 and d1>=init_cohort and d1<=final_cohort:
        return "D1 DURANTE COORTE"
    elif d1!=-1 and d1>final_cohort:
        return "D1 APOS COORTE"
    elif d2!=-1:
        return "INVALIDO - D2 ANTES DA D1"
    else:
        return "NAO VACINADO"

def f_immunization(x, init_cohort, final_cohort, partial=14, fully=14):
    '''
        Classify the maximum immunization status during the cohort. 
    '''
    d1 = x["DATA D1(VACINADOS)"]
    d2 = x["DATA D2(VACINADOS)"]

    if pd.isna(d1): d1 = -1
    else: d1 = d1.date()
    if pd.isna(d2): d2 = -1
    else: d2 = d2.date()

    if d1!=-1 and (d1<init_cohort or d1>final_cohort):
        return "D1 FORA DA COORTE"
    if d1==-1 and d2!=-1:
        return "INVALIDO - D2 ANTES DA D1"

    if d2!=-1 and d2+timedelta(days=fully)<=final_cohort:
        return "TOTALMENTE IMUNIZADO"
    if d2!=-1 and d2+timedelta(days=partial)>final_cohort:
        return "PARCIALMENTE IMUNIZADO"
    if d1!=-1 and d1>=init_cohort and d1+timedelta(days=partial)<=final_cohort:
        return "PARCIALMENTE IMUNIZADO"
    if d1!=-1 and d1<=final_cohort and d1+timedelta(days=partial)>final_cohort:
        return "VACINADO SEM IMUNIZACAO"
    else:
        return "NAO VACINADO"

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
            condition_death = control_dates["DEATH"][cpf_control]==-1 or control_dates["DEATH"][cpf_control]>cur_date
            condition_hospt = control_dates["HOSPITAL"][cpf_control]==-1 or control_dates["HOSPITAL"][cpf_control]>cur_date
            if condition_d1 and condition_death and condition_hospt:
                control_used[cpf_control] = True
                return cpf_control
    return -1
