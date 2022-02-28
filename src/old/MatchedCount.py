'''
    Class to handle the file of matched pairs.
'''

import os
import pandas as pd
import numpy as np
import datetime as dt
from collections import defaultdict
from lifelines import KaplanMeierFitter

class MatchedCount:
    def __init__(self, fname, colname):
        self.fname = fname
        self.df = pd.read_csv(fname)
        self.df["DATA D1"] = pd.to_datetime(self.df["DATA D1"], format="%Y-%m-%d", errors="coerce")
        self.df["DATA D2"] = pd.to_datetime(self.df["DATA D2"], format="%Y-%m-%d", errors="coerce")
        self.kmf_caso = KaplanMeierFitter()
        self.kmf_controle = KaplanMeierFitter()
        self.colname = colname

    def describe_outcomes(self, colname="OBITO"):
        '''
        
        '''
        df = self.df
        df_d1 = df[pd.notna(df["DATA D1 CASO"]) & (pd.isna(df["DATA D2 CASO"]))]
        df_d1d2 = df[pd.notna(df["DATA D1 CASO"]) & (pd.notna(df["DATA D2 CASO"]))]
        stats = pd.DataFrame({})
        stats.index = ["VACINADOS", "NAO VACINADOS"]
        stats.insert(0, "D1 aplicado", [df["DATA D1 CASO"].notnull().sum(), df["DATA D1 CONTROLE"].notnull().sum()])
        stats.insert(1, "D2 aplicado", [df["DATA D2 CASO"].notnull().sum(), df["DATA D2 CONTROLE"].notnull().sum()])
        stats.insert(2, f"{colname}", [df[f"{colname} CASO"].notnull().sum(), df[f"{colname} CONTROLE"].notnull().sum()])
        stats.insert(3, f"{colname} (D1)", [df_d1[f"{colname} CASO"].notnull().sum(), df_d1[f"{colname} CONTROLE"].notnull().sum()])
        stats.insert(4, f"{colname} (D1+D2)", [df_d1d2[f"{colname} CASO"].notnull().sum(), df_d1d2[f"{colname} CONTROLE"].notnull().sum()])
        return stats

    def fit_km(self, negative_intervals=False, return_curves=True):
        '''
        
        '''
        if not negative_intervals:
            self.df = self.df[self.df[f"{self.colname} DURACAO"]>=0]

        df_casos = self.df[self.df["TIPO"]=="CASO"]
        df_controle = self.df[self.df["TIPO"]=="CONTROLE"]
        self.kmf_caso.fit(df_casos[f"{self.colname} DURACAO"], df_casos[f"COM DESFECHO - {self.colname}"])
        self.kmf_controle.fit(df_controle[f"{self.colname} DURACAO"], df_controle[f"COM DESFECHO - {self.colname}"])

        if return_curves:
            KM_CASO = pd.DataFrame({
                "day": self.kmf_caso.cumulative_density_.index,
                "KM_estimate": self.kmf_caso.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_caso.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_caso.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_caso.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_caso.confidence_interval_survival_function_.iloc[:,0] 
            })

            KM_CONTROLE = pd.DataFrame({
                "day": self.kmf_controle.cumulative_density_.index,
                "KM_estimate": self.kmf_controle.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_controle.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_controle.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_controle.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_controle.confidence_interval_survival_function_.iloc[:,0] 
            })
            return (KM_CASO, KM_CONTROLE)

    def fit_km_period(self, init_period=0, negative_intervals=False, return_curves=True):
        '''
        
        '''
        if not negative_intervals:
            self.df = self.df[self.df[f"{self.colname} DURACAO"]>=0]

        df = self.df[self.df[f"{self.colname} DURACAO"]>=init_period]
        
        df_casos = df[df["TIPO"]=="CASO"]
        df_controle = df[df["TIPO"]=="CONTROLE"]
        self.kmf_caso.fit(df_casos[f"{self.colname} DURACAO"], df_casos[f"COM DESFECHO - {self.colname}"])
        self.kmf_controle.fit(df_controle[f"{self.colname} DURACAO"], df_controle[f"COM DESFECHO - {self.colname}"])
        event_caso = self.kmf_caso.event_table
        event_controle = self.kmf_controle.event_table
        event_caso["observed"] = event_caso["observed"].cumsum()
        event_controle["observed"] = event_controle["observed"].cumsum()
        n1 = event_caso["at_risk"].iloc[0]
        n2 = event_controle["at_risk"].iloc[0]
        if return_curves:
            KM_CASO = pd.DataFrame({
                "day": self.kmf_caso.cumulative_density_.index,
                "KM_estimate": self.kmf_caso.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_caso.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_caso.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_caso.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_caso.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_caso.cumulative_density_["KM_estimate"])/(n1*self.kmf_caso.cumulative_density_["KM_estimate"])
            })

            KM_CONTROLE = pd.DataFrame({
                "day": self.kmf_controle.cumulative_density_.index,
                "KM_estimate": self.kmf_controle.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_controle.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_controle.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_controle.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_controle.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_controle.cumulative_density_["KM_estimate"])/(n2*self.kmf_controle.cumulative_density_["KM_estimate"])
            })
            return (KM_CASO, KM_CONTROLE)

    def second_dose_km(self, init_period=0, negative_intervals=False, return_curves=True, final_cohort=dt.date(2021, 8, 31)):
        '''
            Generate the Kaplan-Meier survival curves and the Kaplan-Meier estimates for
            the survival data considering only second dose for the cases.

            Args:
                negative_intervals:
                return_curves:
                final_cohort:
            Return:
                KM_CASO:
                KM_CONTROLE:
        '''
        if not negative_intervals:
            self.df = self.df[self.df[f"{self.colname} DURACAO"]>=0]

        df = self.df[self.df[f"{self.colname} DURACAO"]>=init_period]
        df_casos = df[df["TIPO"]=="CASO"]
        df_controle = df[df["TIPO"]=="CONTROLE"]

        df_casos, df_controle = filter_second_dose(df_casos, df_controle, self.colname, final_cohort, True)
        
        self.kmf_caso.fit(df_casos[f"{self.colname} DURACAO"], df_casos[f"COM DESFECHO - {self.colname}"])
        self.kmf_controle.fit(df_controle[f"{self.colname} DURACAO"], df_controle[f"COM DESFECHO - {self.colname}"])
        event_caso = self.kmf_caso.event_table
        event_controle = self.kmf_controle.event_table
        n1 = event_caso["at_risk"].iloc[0]
        n2 = event_controle["at_risk"].iloc[0]
        if return_curves:
            KM_CASO = pd.DataFrame({
                "day": self.kmf_caso.cumulative_density_.index,
                "KM_estimate": self.kmf_caso.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_caso.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_caso.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_caso.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_caso.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_caso.confidence_interval_survival_function_.iloc[:,0], 
                "Factor for CI of RR": (1-self.kmf_caso.cumulative_density_["KM_estimate"])/(n1*self.kmf_caso.cumulative_density_["KM_estimate"])
            })

            KM_CONTROLE = pd.DataFrame({
                "day": self.kmf_controle.cumulative_density_.index,
                "KM_estimate": self.kmf_controle.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_controle.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_controle.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_controle.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_controle.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_controle.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_controle.cumulative_density_["KM_estimate"])/(n2*self.kmf_controle.cumulative_density_["KM_estimate"])
            })
            return (KM_CASO, KM_CONTROLE)

    def sex_km(self, pop_vaccine, init_period=0, second_dose_filter=False, negative_intervals=False, 
               return_curves=True, final_cohort=dt.date(2021, 8, 31)):
        '''
        
        '''
        if not negative_intervals:
            self.df = self.df[self.df[f"{self.colname} DURACAO"]>=0]

        df = self.df[self.df[f"{self.colname} DURACAO"]>=init_period]
        df = df.merge(pop_vaccine[["CPF", "SEXO(VACINEJA)"]], on="CPF", suffixes=("", "_caso"), how="left")

        df_masc = df[df["SEXO(VACINEJA)"]=="M"]
        df_masc_caso = df_masc[df_masc["TIPO"]=="CASO"]
        df_masc_controle = df_masc[df_masc["TIPO"]=="CONTROLE"]
        df_fem = df[df["SEXO(VACINEJA)"]=="F"]
        df_fem_caso = df_fem[df_fem["TIPO"]=="CASO"]
        df_fem_controle = df_fem[df_fem["TIPO"]=="CONTROLE"]

        if second_dose_filter:
            df_masc_caso, df_masc_controle = filter_second_dose(df_masc_caso, df_masc_controle, self.colname, final_cohort, True)
            df_fem_caso, df_fem_controle = filter_second_dose(df_fem_caso, df_fem_controle, self.colname, final_cohort, True)

        self.kmf_caso_masc = KaplanMeierFitter()
        self.kmf_controle_masc = KaplanMeierFitter()
        self.kmf_caso_fem = KaplanMeierFitter()
        self.kmf_controle_fem = KaplanMeierFitter()
        
        self.kmf_caso_masc.fit(df_masc_caso[f"{self.colname} DURACAO"], df_masc_caso[f"COM DESFECHO - {self.colname}"])
        self.kmf_controle_masc.fit(df_masc_controle[f"{self.colname} DURACAO"], df_masc_controle[f"COM DESFECHO - {self.colname}"])

        self.kmf_caso_fem.fit(df_fem_caso[f"{self.colname} DURACAO"], df_fem_caso[f"COM DESFECHO - {self.colname}"])
        self.kmf_controle_fem.fit(df_fem_controle[f"{self.colname} DURACAO"], df_fem_controle[f"COM DESFECHO - {self.colname}"])
        
        event_caso_masc = self.kmf_caso_masc.event_table
        event_controle_masc = self.kmf_controle_masc.event_table
        n1_masc = event_caso_masc["at_risk"].iloc[0]
        n2_masc = event_controle_masc["at_risk"].iloc[0]
        event_caso_fem = self.kmf_caso_fem.event_table
        event_controle_fem = self.kmf_controle_fem.event_table
        n1_fem = event_caso_fem["at_risk"].iloc[0]
        n2_fem = event_controle_fem["at_risk"].iloc[0]

        if return_curves:
            KM_CASO_MASC = pd.DataFrame({
                "day": self.kmf_caso_masc.cumulative_density_.index,
                "KM_estimate": self.kmf_caso_masc.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_caso_masc.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_caso_masc.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_caso_masc.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_caso_masc.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_caso_masc.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_caso_masc.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_caso_masc.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_caso_masc.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_caso_masc.cumulative_density_["KM_estimate"])/(n1_masc*self.kmf_caso_masc.cumulative_density_["KM_estimate"]) 
            })

            KM_CONTROLE_MASC = pd.DataFrame({
                "day": self.kmf_controle_masc.cumulative_density_.index,
                "KM_estimate": self.kmf_controle_masc.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_controle_masc.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_controle_masc.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_controle_masc.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_controle_masc.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_controle_masc.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_controle_masc.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_controle_masc.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_controle_masc.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_controle_masc.cumulative_density_["KM_estimate"])/(n2_masc*self.kmf_controle_masc.cumulative_density_["KM_estimate"]) 
            })

            KM_CASO_FEM = pd.DataFrame({
                "day": self.kmf_caso_fem.cumulative_density_.index,
                "KM_estimate": self.kmf_caso_fem.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_caso_fem.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_caso_fem.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_caso_fem.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_caso_fem.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_caso_fem.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_caso_fem.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_caso_fem.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_caso_fem.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_caso_fem.cumulative_density_["KM_estimate"])/(n1_fem*self.kmf_caso_fem.cumulative_density_["KM_estimate"]) 
            })

            KM_CONTROLE_FEM = pd.DataFrame({
                "day": self.kmf_controle_fem.cumulative_density_.index,
                "KM_estimate": self.kmf_controle_fem.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_controle_fem.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_controle_fem.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_controle_fem.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_controle_fem.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_controle_fem.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_controle_fem.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_controle_fem.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_controle_fem.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_controle_fem.cumulative_density_["KM_estimate"])/(n2_fem*self.kmf_controle_fem.cumulative_density_["KM_estimate"]) 
            })
            return {"M": (KM_CASO_MASC, KM_CONTROLE_MASC),
                    "F": (KM_CASO_FEM, KM_CONTROLE_FEM)}

    def age_km(self, pop_vaccine, init_period=0, age_interval=[50,59], second_dose_filter=False, negative_intervals=False, 
               return_curves=True, final_cohort=dt.date(2021, 8, 31)):
        '''
        
        '''
        if not negative_intervals:
            self.df = self.df[self.df[f"{self.colname} DURACAO"]>=0]

        df = self.df[self.df[f"{self.colname} DURACAO"]>=init_period]
        df = df.merge(pop_vaccine[["CPF", "IDADE"]], on="CPF", suffixes=("", "_caso"), how="left")

        df_age = df[(df["IDADE"]>=age_interval[0]) & (df["IDADE"]<=age_interval[1])]
        df_caso = df_age[df_age["TIPO"]=="CASO"]
        df_controle = df_age[df_age["TIPO"]=="CONTROLE"]

        if second_dose_filter:
            df_caso, df_controle = filter_second_dose(df_caso, df_controle, self.colname, final_cohort, True)
        
        self.kmf_caso_age = KaplanMeierFitter()
        self.kmf_controle_age = KaplanMeierFitter()
        
        self.kmf_caso_age.fit(df_caso[f"{self.colname} DURACAO"], df_caso[f"COM DESFECHO - {self.colname}"])
        self.kmf_controle_age.fit(df_controle[f"{self.colname} DURACAO"], df_controle[f"COM DESFECHO - {self.colname}"])
        event_caso_age = self.kmf_caso_age.event_table
        event_controle_age = self.kmf_controle_age.event_table
        n1_age = event_caso_age["at_risk"].iloc[0]
        n2_age = event_controle_age["at_risk"].iloc[0]

        if return_curves:
            KM_CASO = pd.DataFrame({
                "day": self.kmf_caso_age.cumulative_density_.index,
                "KM_estimate": self.kmf_caso_age.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_caso_age.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_caso_age.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_caso_age.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_caso_age.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_caso_age.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_caso_age.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_caso_age.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_caso_age.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_caso_age.cumulative_density_["KM_estimate"])/(n1_age*self.kmf_caso_age.cumulative_density_["KM_estimate"]) 
            })

            KM_CONTROLE = pd.DataFrame({
                "day": self.kmf_controle_age.cumulative_density_.index,
                "KM_estimate": self.kmf_controle_age.cumulative_density_["KM_estimate"],
                "KM_estimate_CI_lower": self.kmf_controle_age.confidence_interval_cumulative_density_.iloc[:,1],
                "KM_estimate_CI_upper": self.kmf_controle_age.confidence_interval_cumulative_density_.iloc[:,0],
                "KM_estimate_porc": self.kmf_controle_age.cumulative_density_["KM_estimate"]*100,
                "KM_estimate_CI_lower_porc":  self.kmf_controle_age.confidence_interval_cumulative_density_.iloc[:,1]*100,
                "KM_estimate_CI_upper_porc": self.kmf_controle_age.confidence_interval_cumulative_density_.iloc[:,0]*100,
                "KM_survival": self.kmf_controle_age.survival_function_["KM_estimate"],
                "KM_survival_CI_lower": self.kmf_controle_age.confidence_interval_survival_function_.iloc[:,1],
                "KM_survival_CI_upper": self.kmf_controle_age.confidence_interval_survival_function_.iloc[:,0],
                "Factor for CI of RR": (1-self.kmf_controle_age.cumulative_density_["KM_estimate"])/(n2_age*self.kmf_controle_age.cumulative_density_["KM_estimate"]) 
            })
            return (KM_CASO, KM_CONTROLE)



def filter_second_dose(df_casos, df_controle, colname, final_cohort, remove_negatives=True):
    '''
    
    '''
    df_casos["D2 APTO"] = df_casos["DATA D2"].apply(lambda x: "APTO" if not pd.isna(x) and x.date()<=final_cohort else "NAO APTO")
    df_casos = df_casos[df_casos["D2 APTO"]=="APTO"]
    sbst = ["DATA D1", "DATA D2"]
    df_casos["INTERVALO D2-D1"] = df_casos[sbst].apply(lambda x: (x[sbst[1]].date() - x[sbst[0]].date()).days, axis=1)
    # 'df_casos' contains all vaccinated that should be analyzed for the VE for second dose.
    caso_d2d1_interval = defaultdict(lambda: np.nan, zip(df_casos["CPF"], df_casos["INTERVALO D2-D1"]))
    controle_caso_dict = defaultdict(lambda:-1, zip(df_casos["PAR"], df_casos["CPF"]))
    df_controle["INTERVALO D2-D1"] = df_controle["CPF"].apply(lambda x: caso_d2d1_interval[controle_caso_dict[x]])
    df_controle = df_controle.dropna(subset=["INTERVALO D2-D1"], axis=0)

    sbst = [f"{colname} DURACAO", "INTERVALO D2-D1"]
    if remove_negatives:
        df_casos[sbst[0]] = df_casos[sbst].apply(lambda x: x[sbst[0]]-x[sbst[1]] if x[sbst[0]]>=x[sbst[1]] else np.nan, axis=1)
        df_controle[sbst[0]] = df_controle[sbst].apply(lambda x: x[sbst[0]]-x[sbst[1]] if x[sbst[0]]>=x[sbst[1]] else np.nan, axis=1)
        casos_nan1 = df_casos["CPF"][pd.notna(df_casos[f"{colname} DURACAO"])]
        casos_nan2 = df_casos["PAR"][pd.notna(df_casos[f"{colname} DURACAO"])]
        controle_nan1 = df_controle["CPF"][pd.notna(df_controle[f"{colname} DURACAO"])]
        controle_nan2 = df_controle["PAR"][pd.notna(df_controle[f"{colname} DURACAO"])]

        df_casos = df_casos[(df_casos["CPF"].isin(casos_nan1)) & (df_casos["CPF"].isin(controle_nan2))]
        df_controle = df_controle[(df_controle["CPF"].isin(casos_nan2)) & (df_controle["CPF"].isin(controle_nan1))]
    else:
        df_casos[sbst[0]] = df_casos[sbst].apply(lambda x: x[sbst[0]]-x[sbst[1]] if x[sbst[0]]>=x[sbst[1]] else 0, axis=1)
        df_controle[sbst[0]] = df_controle[sbst].apply(lambda x: x[sbst[0]]-x[sbst[1]] if x[sbst[0]]>=x[sbst[1]] else 0, axis=1)
    return df_casos, df_controle