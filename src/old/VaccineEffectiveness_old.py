'''
    After the data is generated from the definition of the cohort and the matching process,
    this class performs the calculation of the survival table and the statistical analysis
    regarding the survival analysis.
'''

import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt
import lib.survival_f as surv
from collections import defaultdict
from lifelines import CoxPHFitter, KaplanMeierFitter

class VaccineEffectiveness:
    def __init__(self, df_pairs, df_info):
        '''
            Initialize object by parsing the two main outputs of the survival pipeline:
            the dataframe of matched individuals and the dataframe containing all eligible 
            population of a given vaccine containing general info about the individuals.

            Args:
                df_pairs:
                    pandas.DataFrame.
                df_info:
                    pandas.DataFrame.
        '''
        self.df_pairs = df_pairs.copy()
        self.df_info = df_info.copy()
        self.df_matched = self.df_pairs[self.df_pairs["PAREADO"]==True].copy().drop("PAREADO", axis=1)
        self.survival_info = None
        self.casos_hash = defaultdict(dict)
        self.controles_hash = defaultdict(dict)

    def define_intervals(self, final_cohort, events_col=None, return_=False):
        '''
            For each of the events considered in the cohort, define the size of the interval 
            (in days) for each individual.

            Args:
                events_col:
                    Dictionary.
                return_:
                    Bool.
        '''
        # Define the columns of the events.
        cols = None
        if events_col is None:
            cols = {
                "D1": "DATA D1", "D2": "DATA D2",
                "OBITO COVID": "DATA OBITO COVID",
                "OBITO GERAL": "DATA OBITO GERAL"
            }
        else:
            cols = events_col
        
        # If date columns are defined as strings, convert them to datetimes
        for name in [cols["D1"], cols["D2"], cols["OBITO COVID"], cols["OBITO GERAL"]]:
            if self.df_matched.dtypes.loc[name]=='O':
                self.df_matched[name] = pd.to_datetime(self.df_matched[name], errors="coerce", format="%Y-%m-%d")

        # Fill two dictionaries (for cases and controls) with events' info of each individual.
        n_individuals = self.df_matched.shape[0]
        for j in range(0, n_individuals):
            person_info = self.df_matched.iloc[j].to_dict()
            if person_info["TIPO"] == "CONTROLE":
                self.controles_hash[person_info["CPF"]] = person_info
            elif person_info["TIPO"] == "CASO":
                self.casos_hash[person_info["CPF"]] = person_info

        # Calculate the size of the intervals for each pair
        self.intervals = []
        for key in self.casos_hash.keys():
            caso_hash = self.casos_hash[key]
            controle_hash = self.controles_hash[caso_hash["PAR"]]
            res = compare_pair_survival(caso_hash, controle_hash, cols, final_cohort)
            self.intervals.append(res)

        new_df = {"CPF CASO": [], "CPF CONTROLE": [], "CASO D1 INTERVALO": [], 
                  "CONTROLE D1 INTERVALO": [], "CASO D1 CENSURADO": [],
                  "CONTROLE D1 CENSURADO": [], "CASO D2 INTERVALO": [],
                  "CONTROLE D2 INTERVALO": [], "CASO D2 CENSURADO": [],
                  "CONTROLE D2 CENSURADO": []}
        for info in tqdm(self.intervals):
            new_df["CPF CASO"].append(info["CPF CASO"])
            new_df["CPF CONTROLE"].append(info["CPF CONTROLE"])
            # --> D1
            info_d1_caso = info["D1"][0]
            info_d1_controle = info["D1"][1]
            info_d1_caso = [ x for x in info_d1_caso if not pd.isna(x[1]) ]
            info_d1_controle = [ x for x in info_d1_controle if not pd.isna(x[1]) ]
            info_d1_caso = sorted(info_d1_caso, key=lambda tup: tup[1])
            info_d1_controle = sorted(info_d1_controle, key=lambda tup: tup[1])
            if info_d1_caso[0][0]=="D1 to COVID":
                new_df["CASO D1 INTERVALO"].append(info_d1_caso[0][1])
                new_df["CASO D1 CENSURADO"].append(False)
            else:
                new_df["CASO D1 INTERVALO"].append(info_d1_caso[0][1])
                new_df["CASO D1 CENSURADO"].append(True)
            if info_d1_controle[0][0]=="D1 to COVID_CONTROL":
                new_df["CONTROLE D1 INTERVALO"].append(info_d1_controle[0][1])
                new_df["CONTROLE D1 CENSURADO"].append(False)
            else:
                new_df["CONTROLE D1 INTERVALO"].append(info_d1_controle[0][1])
                new_df["CONTROLE D1 CENSURADO"].append(True)

            # --> D2
            info_d2_caso = info["D2"][0]
            info_d2_controle = info["D2"][1]
            info_d2_caso = [ x for x in info_d2_caso if not pd.isna(x[1]) ]
            info_d2_controle = [ x for x in info_d2_controle if not pd.isna(x[1]) ]
            info_d2_caso = sorted(info_d2_caso, key=lambda tup: tup[1])
            info_d2_controle = sorted(info_d2_controle, key=lambda tup: tup[1])
            if len(info_d2_caso)==0 or len(info_d2_controle)==0:
                new_df["CASO D2 INTERVALO"].append(np.nan)
                new_df["CASO D2 CENSURADO"].append(np.nan)
                new_df["CONTROLE D2 INTERVALO"].append(np.nan)
                new_df["CONTROLE D2 CENSURADO"].append(np.nan)
                continue
            if info_d2_caso[0][0]=="D2 to COVID":
                new_df["CASO D2 INTERVALO"].append(info_d2_caso[0][1])
                new_df["CASO D2 CENSURADO"].append(False)
            else:
                new_df["CASO D2 INTERVALO"].append(info_d2_caso[0][1])
                new_df["CASO D2 CENSURADO"].append(True)
            if info_d2_controle[0][0]=="D2 to COVID_CONTROL":
                new_df["CONTROLE D2 INTERVALO"].append(info_d2_controle[0][1])
                new_df["CONTROLE D2 CENSURADO"].append(False)
            else:
                new_df["CONTROLE D2 INTERVALO"].append(info_d2_controle[0][1])
                new_df["CONTROLE D2 CENSURADO"].append(True)
            
        self.survival_info = pd.DataFrame(new_df)
        self.survival_info_transf = surv.transform_observations(self.survival_info, self.df_info)
        if return_:
            return self.survival_info_transf

    def create_survival_table(self, age=None, sex=None, dose="D1", t_0=0):
        '''
        
        '''
        df = push_info_to_survival(self.survival_info, self.df_info)

        df_caso = df.copy()
        df_controle = df.copy()
        if age is not None:
            df_caso = df_caso[(df_caso[f"idade(CASO)"]>=age[0]) & (df_caso[f"idade(CASO)"]<=age[1])]
            df_controle = df_controle[(df_controle[f"idade(CONTROLE)"]>=age[0]) & (df_controle[f"idade(CONTROLE)"]<=age[1])]
        if sex is not None:
            df_caso = df_caso[df_caso[f"sexo(CASO)"]==sex]
            df_controle = df_controle[df_controle[f"sexo(CONTROLE)"]==sex]

        survival_tb_caso = surv.generate_survival_table(df_caso, group="CASO", dose=dose)
        survival_tb_controle = surv.generate_survival_table(df_controle, group="CONTROLE", dose=dose)
        
        final = surv.get_casocontrole_survival(survival_tb_caso, survival_tb_controle, t_0=t_0)
        return final

    def resume_survival_ve(self, t_0=0):
        '''

        '''
        HASH_RES = {
            "D1": None, "D2": None, "D1_M": None, "D1_F": None,
            "D2_M": None, "D2_F": None, "D1_6069": None, "D1_7079": None,
            "D1_80+": None, "D2_6069": None, "D2_7079": None, "D2_80+": None
        }
        # D1 -> from zero.
        HASH_RES["D1"] = self.create_survival_table(t_0=t_0, dose="D1")
        # D2 -> from zero.
        HASH_RES["D2"] = self.create_survival_table(t_0=t_0, dose="D2")
        # D1 and Male -> from zero.
        HASH_RES["D1_M"] = self.create_survival_table(t_0=t_0, sex="M", dose="D1")
        # D1 and Female -> from zero.
        HASH_RES["D1_F"] = self.create_survival_table(t_0=t_0, sex="F", dose="D1")
        # D2 and Male -> from zero.
        HASH_RES["D2_M"] = self.create_survival_table(t_0=t_0, sex="M", dose="D2")
        # D2 and Female -> from zero.
        HASH_RES["D2_F"] = self.create_survival_table(t_0=t_0, sex="F", dose="D2")
        # D1 and 60-69 -> from zero.
        HASH_RES["D1_6069"] = self.create_survival_table(t_0=t_0, age=(60,69), dose="D1")
        # D1 and 70-79 -> from zero.
        HASH_RES["D1_7079"] = self.create_survival_table(t_0=t_0, age=(60,69), dose="D1")
        # D1 and 80+ -> from zero.
        HASH_RES["D1_80+"] = self.create_survival_table(t_0=t_0, age=(80,110), dose="D1")
        # D2 and 60-69 -> from zero.
        HASH_RES["D2_6069"] = self.create_survival_table(t_0=t_0, age=(60,69), dose="D2")
        # D2 and 70-79 -> from zero.
        HASH_RES["D2_7079"] = self.create_survival_table(t_0=t_0, age=(70,79), dose="D2")
        # D2 and 80+ -> from zero.
        HASH_RES["D2_80+"] = self.create_survival_table(t_0=t_0, age=(80,110), dose="D2")
        return HASH_RES

    def get_hazards_coxPH(self, age=None, sex=None, period_d1=None, period_d2=None):
        '''
        
        '''
        df = self.survival_info_transf.copy()
        if age is not None:
            df = df[(df[f"idade"]>=age[0]) & (df[f"idade"]<=age[1])]
        if sex is not None:
            df = df[df[f"sexo"]==sex]

        cph_d1 = CoxPHFitter()
        cph_d2 = CoxPHFitter()

        df_d1 = df[["TEMPO PARA EVENTO D1", "EVENTO D1", "VACINADO"]].dropna(subset=["TEMPO PARA EVENTO D1", "EVENTO D1", "VACINADO"], axis=0)
        df_d2 = df[["TEMPO PARA EVENTO D2", "EVENTO D2", "VACINADO"]].dropna(subset=["TEMPO PARA EVENTO D2", "EVENTO D2", "VACINADO"], axis=0)

        if period_d1 is not None and period_d2 is not None:
            df_d1 = df_d1[(df_d1["TEMPO PARA EVENTO D1"]>=period_d1[0]) & (df_d1["TEMPO PARA EVENTO D1"]<=period_d1[1])]
            df_d2 = df_d2[(df_d2["TEMPO PARA EVENTO D2"]>=period_d2[0]) & (df_d2["TEMPO PARA EVENTO D2"]<=period_d2[1])]
        
        res = {
            "D1": [np.nan, [np.nan, np.nan]],
            "D2": [np.nan, [np.nan, np.nan]]
        }
        try:
            cph_d1.fit(df_d1, duration_col="TEMPO PARA EVENTO D1", event_col="EVENTO D1")
            res["D1"] = [1-cph_d1.hazard_ratios_.loc["VACINADO"], 1-np.exp(cph_d1.confidence_intervals_.loc["VACINADO"].array)]
        except:
            cph_d1 = np.nan
        try:
            cph_d2.fit(df_d2, duration_col="TEMPO PARA EVENTO D2", event_col="EVENTO D2")
            res["D2"] = [1-cph_d2.hazard_ratios_.loc["VACINADO"], 1-np.exp(cph_d2.confidence_intervals_.loc["VACINADO"].array)]
        except:
            cph_d2 = np.nan
        
        return res

    def resume_hazard_ve(self, period_d1=[14,27], period_d2=[14,150]):
        '''

        '''
        HASH_RES = {
            "D1D2": None, "D1D2_M": None, "D1D2_F": None,
            "D1D2_6069": None, "D1D2_7079": None, "D1D2_80+": None
        }
        # D1 -> from zero.
        HASH_RES["D1D2"] = self.get_hazards_coxPH(period_d1=period_d1, period_d2=period_d2)
        # D1 and Male -> from zero.
        HASH_RES["D1D2_M"] = self.get_hazards_coxPH(sex="M", period_d1=period_d1, period_d2=period_d2)
        # D1 and Female -> from zero.
        HASH_RES["D1D2_F"] = self.get_hazards_coxPH(sex="F", period_d1=period_d1, period_d2=period_d2)
        # D1 and 60-69 -> from zero.
        HASH_RES["D1D2_6069"] = self.get_hazards_coxPH(age=(60,69), period_d1=period_d1, period_d2=period_d2)
        # D1 and 70-79 -> from zero.
        HASH_RES["D1D2_7079"] = self.get_hazards_coxPH(age=(70,79), period_d1=period_d1, period_d2=period_d2)
        # D1 and 80+ -> from zero.
        HASH_RES["D1D2_80+"] = self.get_hazards_coxPH(age=(80,110), period_d1=period_d1, period_d2=period_d2)
        
        return HASH_RES

    def get_risks_KM(self, age=None, sex=None, period_d1=None, period_d2=None):
        '''
        
        '''
        df = self.survival_info_transf.copy()
        if age is not None:
            df = df[(df[f"idade"]>=age[0]) & (df[f"idade"]<=age[1])]
        if sex is not None:
            df = df[df[f"sexo"]==sex]

        km_d1_caso, km_d1_controle = KaplanMeierFitter(label="d1_caso"), KaplanMeierFitter(label="d1_controle")
        km_d2_caso, km_d2_controle = KaplanMeierFitter(label="d2_caso"), KaplanMeierFitter(label="d2_controle")

        df_d1_caso = df[df["VACINADO"]==1][["TEMPO PARA EVENTO D1", "EVENTO D1", "VACINADO"]].dropna(subset=["TEMPO PARA EVENTO D1", "EVENTO D1", "VACINADO"], axis=0)
        df_d1_controle = df[df["VACINADO"]==0][["TEMPO PARA EVENTO D1", "EVENTO D1", "VACINADO"]].dropna(subset=["TEMPO PARA EVENTO D1", "EVENTO D1", "VACINADO"], axis=0)
        df_d2_caso = df[df["VACINADO"]==1][["TEMPO PARA EVENTO D2", "EVENTO D2", "VACINADO"]].dropna(subset=["TEMPO PARA EVENTO D2", "EVENTO D2", "VACINADO"], axis=0)
        df_d2_controle = df[df["VACINADO"]==0][["TEMPO PARA EVENTO D2", "EVENTO D2", "VACINADO"]].dropna(subset=["TEMPO PARA EVENTO D2", "EVENTO D2", "VACINADO"], axis=0)

        if period_d1 is not None and period_d2 is not None:
            df_d1_caso = df_d1_caso[(df_d1_caso["TEMPO PARA EVENTO D1"]>=period_d1[0]) & (df_d1_caso["TEMPO PARA EVENTO D1"]<=period_d1[1])]
            df_d1_controle = df_d1_controle[(df_d1_controle["TEMPO PARA EVENTO D1"]>=period_d1[0]) & (df_d1_controle["TEMPO PARA EVENTO D1"]<=period_d1[1])]
            df_d2_caso = df_d2_caso[(df_d2_caso["TEMPO PARA EVENTO D2"]>=period_d2[0]) & (df_d2_caso["TEMPO PARA EVENTO D2"]<=period_d2[1])]
            df_d2_controle = df_d2_controle[(df_d2_controle["TEMPO PARA EVENTO D2"]>=period_d2[0]) & (df_d2_controle["TEMPO PARA EVENTO D2"]<=period_d2[1])]

        km_d1_caso.fit(df_d1_caso["TEMPO PARA EVENTO D1"], df_d1_caso["EVENTO D1"])
        km_d1_controle.fit(df_d1_controle["TEMPO PARA EVENTO D1"], df_d1_controle["EVENTO D1"])
        km_d2_caso.fit(df_d2_caso["TEMPO PARA EVENTO D2"], df_d2_caso["EVENTO D2"])
        km_d2_controle.fit(df_d2_controle["TEMPO PARA EVENTO D2"], df_d2_controle["EVENTO D2"])
        
        # --------------------IIIIIII
        # FIXXXXXXXXXXX BELOW VVVVVVV
        res = {
            "D1": [np.nan, [np.nan, np.nan]],
            "D2": [np.nan, [np.nan, np.nan]]
        }
        try:
            cph_d1.fit(df_d1, duration_col="TEMPO PARA EVENTO D1", event_col="EVENTO D1")
            res["D1"] = [1-cph_d1.hazard_ratios_.loc["VACINADO"], 1-np.exp(cph_d1.confidence_intervals_.loc["VACINADO"].array)]
        except:
            cph_d1 = np.nan
        try:
            cph_d2.fit(df_d2, duration_col="TEMPO PARA EVENTO D2", event_col="EVENTO D2")
            res["D2"] = [1-cph_d2.hazard_ratios_.loc["VACINADO"], 1-np.exp(cph_d2.confidence_intervals_.loc["VACINADO"].array)]
        except:
            cph_d2 = np.nan
        
        return res


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
    
    f = lambda x: x.date() if not pd.isna(x) else np.nan
    g = lambda x,y: (x-y).days if not pd.isna(x) and not pd.isna(y) else np.nan
            
    # --> D1
    start_date = caso_d1_date.date()
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
    start_date = caso_d2_date.date()
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

def push_info_to_survival(df_survival, df_pop):
    '''
    
    '''
    col_info = ["cpf", "data_nascimento", "bairro", "sexo", "idade", "faixa etaria(VACINADOS)", "bairro id(VACINADOS)"]
    df_pop_caso = df_pop[col_info].add_suffix("(CASO)")
    df_pop_controle = df_pop[col_info].add_suffix("(CONTROLE)")

    df_survival = df_survival.merge(df_pop_caso, left_on="CPF CASO", right_on="cpf(CASO)", how='left')
    df_survival = df_survival.merge(df_pop_controle, left_on="CPF CONTROLE", right_on="cpf(CONTROLE)", how='left')
    return df_survival

    


        