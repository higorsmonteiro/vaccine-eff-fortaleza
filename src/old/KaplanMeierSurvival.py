'''

'''

import os
from typing import SupportsBytes
from numpy.lib.shape_base import column_stack
import pandas as pd
import numpy as np
import datetime as dt
from collections import defaultdict
from lifelines import KaplanMeierFitter

class KM_analysis:
    def __init__(self, survival_df):
        '''
        
        '''
        self.df = None
        self.survival_df = survival_df.copy()
        self.survival_df["DATA D1"] = pd.to_datetime(self.survival_df["DATA D1"], format="%Y-%m-%d", errors="coerce")
        self.survival_df["DATA D2"] = pd.to_datetime(self.survival_df["DATA D2"], format="%Y-%m-%d", errors="coerce")

    def fit_dose(self, pop_vaccine, colname, d2=False, from_day=0, final_cohort=dt.date(2021, 8, 31)):
        '''
        
        '''
        df = self.survival_df.copy()
        df = df.merge(pop_vaccine[["cpf", "sexo"]], left_on="CPF", right_on="cpf", suffixes=("", "_caso"), how="left")
        df = df.dropna(subset=["sexo"], axis=0)
        #df = df[df[f"{colname} DURACAO"]>=from_day]
        df_caso = df[df["TIPO"]=="CASO"]
        df_controle = df[df["TIPO"]=="CONTROLE"]
        if d2:
            df_caso, df_controle = only_D2_filter(df_caso, df_controle, colname, final_cohort, remove_negatives=True)
            #df_caso, df_controle = filter_second_dose(df_caso, df_controle, colname, final_cohort, remove_negatives=True)

        df_caso = df_caso[df_caso[f"{colname} DURACAO"]>=from_day]
        df_controle = df_controle[df_controle[f"{colname} DURACAO"]>=from_day]

        kmf_caso = KaplanMeierFitter()
        kmf_controle = KaplanMeierFitter()
        kmf_caso.fit(df_caso[f"{colname} DURACAO"], df_caso[f"COM DESFECHO - {colname}"])
        kmf_controle.fit(df_controle[f"{colname} DURACAO"], df_controle[f"COM DESFECHO - {colname}"])
        
        KM_CASO, KM_CONTROLE = fill_km_table(kmf_caso, kmf_controle)
        return (KM_CASO, KM_CONTROLE)

    def fit_sex(self, pop_vaccine, colname, d2=False, from_day=0, final_cohort=dt.date(2021, 8, 31)):
        '''
        
        '''
        df = self.survival_df.copy()
        print(df.shape)
        df = df.merge(pop_vaccine[["cpf", "sexo"]], left_on="CPF", right_on="cpf", suffixes=("", "_caso"), how="left")
        df = df.dropna(subset=["sexo"], axis=0)
        print(df.shape)
        df_masc_caso = df[(df["sexo"]=="M") & (df["TIPO"]=="CASO")]
        df_masc_controle = df[(df["sexo"]=="M") & (df["TIPO"]=="CONTROLE")]
        df_fem_caso = df[(df["sexo"]=="F") & (df["TIPO"]=="CASO")]
        df_fem_controle = df[(df["sexo"]=="F") & (df["TIPO"]=="CONTROLE")]
        df_masc = df[df["sexo"]=="M"]
        df_fem = df[df["sexo"]=="F"]
        print(df_masc.shape, df_fem.shape)
        #df_masc_caso = df_masc[df_masc["TIPO"]=="CASO"]
        #df_masc_controle = df_masc[df_masc["TIPO"]=="CONTROLE"]
        #df_fem_caso = df_fem[df_fem["TIPO"]=="CASO"]
        #df_fem_controle = df_fem[df_fem["TIPO"]=="CONTROLE"]

        if d2:
            df_masc_caso, df_masc_controle = only_D2_filter(df_masc_caso, df_masc_controle, colname, final_cohort, remove_negatives=True)
            df_fem_caso, df_fem_controle = only_D2_filter(df_fem_caso, df_fem_controle, colname, final_cohort, remove_negatives=True)

        df_masc_caso = df_masc_caso[df_masc_caso[f"{colname} DURACAO"]>=from_day]
        df_masc_controle = df_masc_controle[df_masc_controle[f"{colname} DURACAO"]>=from_day]
        df_fem_caso = df_fem_caso[df_fem_caso[f"{colname} DURACAO"]>=from_day]
        df_fem_controle = df_fem_controle[df_fem_controle[f"{colname} DURACAO"]>=from_day]

        kmf_masc_caso = KaplanMeierFitter()
        kmf_masc_controle = KaplanMeierFitter()
        kmf_fem_caso = KaplanMeierFitter()
        kmf_fem_controle = KaplanMeierFitter()

        kmf_masc_caso.fit(df_masc_caso[f"{colname} DURACAO"], df_masc_caso[f"COM DESFECHO - {colname}"])
        kmf_masc_controle.fit(df_masc_controle[f"{colname} DURACAO"], df_masc_controle[f"COM DESFECHO - {colname}"])
        kmf_fem_caso.fit(df_fem_caso[f"{colname} DURACAO"], df_fem_caso[f"COM DESFECHO - {colname}"])
        kmf_fem_controle.fit(df_fem_controle[f"{colname} DURACAO"], df_fem_controle[f"COM DESFECHO - {colname}"])

        KM_CASO_M, KM_CONTROLE_M = fill_km_table(kmf_masc_caso, kmf_masc_controle)
        KM_CASO_F, KM_CONTROLE_F = fill_km_table(kmf_fem_caso, kmf_fem_controle)

        return (KM_CASO_M, KM_CONTROLE_M, KM_CASO_F, KM_CONTROLE_F)

    def fit_age(self, pop_vaccine, colname, age_interval=[60,69], d2=False, from_day=0, final_cohort=dt.date(2021, 8, 31)):
        '''
        
        '''
        df = self.survival_df.copy()
        df = df.merge(pop_vaccine[["cpf", "idade"]], left_on="CPF", right_on="cpf", suffixes=("", "_caso"), how="left")

        df_age = df[(df["idade"]>=age_interval[0]) & (df["idade"]<=age_interval[1])]
        df_caso = df_age[df_age["TIPO"]=="CASO"]
        df_controle = df_age[df_age["TIPO"]=="CONTROLE"]
        if d2:
            df_caso, df_controle = only_D2_filter(df_caso, df_controle, colname, final_cohort, remove_negatives=True)

        df_caso = df_caso[df_caso[f"{colname} DURACAO"]>=from_day]
        df_controle = df_controle[df_controle[f"{colname} DURACAO"]>=from_day]

        kmf_caso = KaplanMeierFitter()
        kmf_controle = KaplanMeierFitter()
        kmf_caso.fit(df_caso[f"{colname} DURACAO"], df_caso[f"COM DESFECHO - {colname}"])
        kmf_controle.fit(df_controle[f"{colname} DURACAO"], df_controle[f"COM DESFECHO - {colname}"])

        KM_CASO, KM_CONTROLE = fill_km_table(kmf_caso, kmf_controle)
        return (KM_CASO, KM_CONTROLE)
        
def fill_km_table(kmf_caso, kmf_controle):
    '''
        AUX.
    '''
    event_caso = kmf_caso.event_table
    event_controle = kmf_controle.event_table
    #event_caso["observed"] = event_caso["observed"].cumsum()
    #event_controle["observed"] = event_controle["observed"].cumsum()
    n1 = event_caso["at_risk"].iloc[0]
    n2 = event_controle["at_risk"].iloc[0]

    KM_CASO = pd.DataFrame({
        "day": kmf_caso.cumulative_density_.index,
        "KM_estimate": kmf_caso.cumulative_density_["KM_estimate"],
        "KM_estimate_CI_lower": kmf_caso.confidence_interval_cumulative_density_.iloc[:,1],
        "KM_estimate_CI_upper": kmf_caso.confidence_interval_cumulative_density_.iloc[:,0],
        "KM_estimate_porc": kmf_caso.cumulative_density_["KM_estimate"]*100,
        "KM_estimate_CI_lower_porc":  kmf_caso.confidence_interval_cumulative_density_.iloc[:,1]*100,
        "KM_estimate_CI_upper_porc": kmf_caso.confidence_interval_cumulative_density_.iloc[:,0]*100,
        "KM_survival": kmf_caso.survival_function_["KM_estimate"],
        "KM_survival_CI_lower": kmf_caso.confidence_interval_survival_function_.iloc[:,1],
        "KM_survival_CI_upper": kmf_caso.confidence_interval_survival_function_.iloc[:,0],
        "Factor for CI of RR": (1-kmf_caso.cumulative_density_["KM_estimate"])/(n1*kmf_caso.cumulative_density_["KM_estimate"]) 
    })

    KM_CONTROLE = pd.DataFrame({
        "day": kmf_controle.cumulative_density_.index,
        "KM_estimate": kmf_controle.cumulative_density_["KM_estimate"],
        "KM_estimate_CI_lower": kmf_controle.confidence_interval_cumulative_density_.iloc[:,1],
        "KM_estimate_CI_upper": kmf_controle.confidence_interval_cumulative_density_.iloc[:,0],
        "KM_estimate_porc": kmf_controle.cumulative_density_["KM_estimate"]*100,
        "KM_estimate_CI_lower_porc":  kmf_controle.confidence_interval_cumulative_density_.iloc[:,1]*100,
        "KM_estimate_CI_upper_porc": kmf_controle.confidence_interval_cumulative_density_.iloc[:,0]*100,
        "KM_survival": kmf_controle.survival_function_["KM_estimate"],
        "KM_survival_CI_lower": kmf_controle.confidence_interval_survival_function_.iloc[:,1],
        "KM_survival_CI_upper": kmf_controle.confidence_interval_survival_function_.iloc[:,0],
        "Factor for CI of RR": (1-kmf_controle.cumulative_density_["KM_estimate"])/(n2*kmf_controle.cumulative_density_["KM_estimate"]) 
    })
    return (KM_CASO, KM_CONTROLE)

def filter_second_dose_new(df_casos, df_controle, colname, final_cohort, remove_negatives=True):
    '''
        TO CHECK VALUES.
    '''
    df_casos["D2 APTO"] = df_casos["DATA D2"].apply(lambda x: "APTO" if not pd.isna(x) and x.date()<=final_cohort else "NAO APTO")
    df_casos_apto = df_casos[df_casos["D2 APTO"]=="APTO"]
    sbst = ["DATA D1", "DATA D2"]
    df_casos_apto["INTERVALO D2-D1"] = df_casos_apto[sbst].apply(lambda x: (x[sbst[1]].date() - x[sbst[0]].date()).days, axis=1)
    caso_d2d1_interval = defaultdict(lambda: np.nan, zip(df_casos_apto["CPF"], df_casos_apto["INTERVALO D2-D1"]))
    controle_caso_dict = defaultdict(lambda:-1, zip(df_casos_apto["PAR"], df_casos_apto["CPF"]))

def filter_second_dose(df_casos, df_controle, colname, final_cohort, remove_negatives=True):
    '''
        TO CHECK VALUES.
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

'''
    data -> Sobrevida de casos, Sobrevida de controles
    1. Na tabela de casos, selecionar somente aqueles que tomaram a segunda dose antes do fim da coorte.
    2. Calcular intervalo de diferença entre data da D2 e a data da D1: D2-D1.
    3. Para cada caso, associa o intervalo D2-D1 ao seu controle.
    4. Da sobrevida do caso e do controle, subtrai D2-D1.
    5. Remove um par se ou a sobrevida do caso ou a sobrevida do controle forem negativos.
'''
def only_D2_filter(df_casos, df_controles, colname, final_cohort, remove_negatives=True):
    '''
        Description.

        Args:
            df_casos:
                DataFrame.
            df_controles:
                DataFrame.
            colname:
                String.
            final_cohort:
                datetime.date.
            remove_negative:
                Bool.
        Return:
            df_casos:
                DataFrame.
            df_controles:
                DataFrame.
    '''
    my_time = dt.datetime.min.time()
    # Na tabela de casos, selecionar somente aqueles que tomaram a segunda dose antes do fim da coorte.
    df_casos_d2 = df_casos[(pd.notna(df_casos["DATA D2"])) & (df_casos["DATA D2"]<=dt.datetime.combine(final_cohort, my_time))]
    # Calcular intervalo de diferença entre data da D2 e a data da D1: D2-D1.
    df_casos_d2["D2-D1"] = df_casos_d2[["DATA D1", "DATA D2"]].apply(lambda x: x["DATA D2"]-x["DATA D1"], axis=1)
    df_casos_d2["D2-D1"] = df_casos_d2["D2-D1"].apply(lambda x: x.days)

    # Associa os intervalos dos casos aos seus respectivos controles
    controle_intervalo = defaultdict(lambda: np.nan, zip(df_casos_d2["PAR"], df_casos_d2["D2-D1"]))
    df_controles["D2-D1"] = df_controles["CPF"].apply(lambda x: controle_intervalo[x])
    df_controles_d2 = df_controles.dropna(subset=["D2-D1"], axis=0)

    # Subtrai os intervalos da duração total das sobrevidas (mesmo que definir D2 como t = 0.)
    subset = [f"{colname} DURACAO", "D2-D1"]
    df_casos_d2[f"{colname} DURACAO"] = df_casos_d2[subset].apply(lambda x: x[subset[0]]-x[subset[1]], axis=1)
    df_controles_d2[f"{colname} DURACAO"] = df_controles_d2[subset].apply(lambda x: x[subset[0]]-x[subset[1]], axis=1)

    # Remover pares se a sobrevida do caso ou controle for negativo.
    df_casos_d2 = df_casos_d2.merge(df_controles_d2[["CPF", f"{colname} DURACAO"]], left_on="PAR", right_on="CPF", suffixes=("","_"))
    df_controles_d2 = df_controles_d2.merge(df_casos_d2[["CPF", f"{colname} DURACAO"]], left_on="PAR", right_on="CPF", suffixes=("","_"))

    df_casos_d2 = df_casos_d2[(df_casos_d2[f"{colname} DURACAO"]>=0) & (df_casos_d2[f"{colname} DURACAO_"]>=0)]
    df_controles_d2 = df_controles_d2[(df_controles_d2[f"{colname} DURACAO"]>=0) & (df_controles_d2[f"{colname} DURACAO_"]>=0)]
    return df_casos_d2, df_controles_d2









    