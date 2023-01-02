# -- Essential
import os
import pandas as pd
import numpy as np
import datetime as dt
from collections import defaultdict
import lib.utils as utils
from src.FigUtils import FigUtils

# -- Filter warnings
import warnings
warnings.filterwarnings("ignore")

# -- Plotting 
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns


######################################################################################################################
#                                       OPEN KM AND VE FILES
######################################################################################################################
def open_km(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, event, day_risk, seed, strat=None):
    '''
        Open a file regarding to a Kaplan-Meier curve.
        
        Here we identify a curve by: the vaccine, the prefix for the experiment used, HDI stratification used,
        dose to consider, cohort string related to the start and end of cohort, which event to consider, minimum
        number of days in risk to consider, and seed of the running. 
    '''
    fname = os.path.join(pareamento_folder, vaccine, f"{test_prefix}_HDI_{hdi_index}_{cohort_str}", "SURVIVAL", f"SEED{seed}", f"KM_events_{dose}_{event}_DAY{day_risk}.xlsx")
    hdf = pd.read_excel(fname, sheet_name=strat)
    return hdf

def open_ve(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, event, day_risk, seed, strat=None):
    '''
        Open a file regarding to a Vaccine Effectiveness curve.
        
        Here we identify a curve by: the vaccine, the prefix for the experiment used, HDI stratification used,
        dose to consider, cohort string related to the start and end of cohort, which event to consider, minimum
        number of days in risk to consider, and seed of the running.
    '''
    fname = os.path.join(pareamento_folder, vaccine, f"{test_prefix}_HDI_{hdi_index}_{cohort_str}", "SURVIVAL", f"SEED{seed}", f"VE_{dose}_{event}_DAY{day_risk}.xlsx")
    hdf = pd.read_excel(fname, sheet_name=strat)
    return hdf


######################################################################################################################
#                                       OPEN PAIRS AND PAIRS-EVENTS FILES
######################################################################################################################
def get_pairs(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, day_risk, seed):
    '''
        Open a file containing the matched pairs' CPFs.
        
        Here we identify an 'experiment' by: the vaccine, the prefix for the experiment used, HDI stratification used,
        dose to consider, cohort string related to the start and end of cohort, which event to consider, minimum
        number of days in risk to consider, and random number seed of the experiment. 
    '''
    fname = os.path.join(pareamento_folder, vaccine, f"{test_prefix}_HDI_{hdi_index}_{cohort_str}", f"PAREADOS_CPF_{dose}_DAY{day_risk}_{seed}.parquet")
    df = pd.read_parquet(fname)
    return df

def get_pairs_events_x(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, day_risk, seed):
    '''
        Open a file containing the matched pairs' CPFs.
        
        Here we identify an 'experiment' by: the vaccine, the prefix for the experiment used, HDI stratification used,
        dose to consider, cohort string related to the start and end of cohort, which event to consider, minimum
        number of days in risk to consider, and random number seed of the experiment. 
    '''
    fname = os.path.join(pareamento_folder, vaccine, f"{test_prefix}_HDI_{hdi_index}_{cohort_str}", f"EVENTOS_PAREADOS_{dose}_DAY{day_risk}_{seed}.parquet")
    df = pd.read_parquet(fname)
    return df

def get_pairs_events(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, day_risk, seed):
    '''
        Open a file containing the matched pairs' CPFs together with information on events.
        
        Here we identify an 'experiment' by: the vaccine, the prefix for the experiment used, HDI stratification used,
        dose to consider, cohort string related to the start and end of cohort, which event to consider, minimum
        number of days in risk to consider, and random number seed of the experiment. 
    '''
    fname = os.path.join(pareamento_folder, vaccine, f"{test_prefix}_HDI_{hdi_index}_{cohort_str}", f"PAREADOS_COM_INTERVALOS_{dose}_OBITO_DAY{day_risk}_{seed}.parquet")
    df = pd.read_parquet(fname)
    return df

def open_survival(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, event, day_risk, seed):
    '''
    
    '''
    fname = os.path.join(pareamento_folder, vaccine, f"{test_prefix}_HDI_{hdi_index}_{cohort_str}", "SURVIVAL", f"SURVIVAL_{dose}_{event}_DAY{day_risk}_{seed}.parquet")
    df = pd.read_parquet(fname)
    return df

def ve_mean(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, event, day_risk, strat="DOSE", seeds=np.arange(1,21,1), mode="VE_original"):
    '''
    
    '''
    ve_dfs = []
    for seed in seeds:
        cur_df = open_ve(pareamento_folder, vaccine, test_prefix, hdi_index, dose, cohort_str, event, day_risk, seed, strat=f"{strat}")
        cur_df = cur_df.set_index("t")
        ve_dfs.append(cur_df[f"{mode}"])
    ve_seeds = pd.concat(ve_dfs, axis=1)
    mean_df = ve_seeds.mean(axis=1)
    std_df = ve_seeds.std(axis=1)
    sem_df = ve_seeds.sem(axis=1)
    ve_seeds['VE_mean'] = mean_df
    ve_seeds['std'] = std_df
    ve_seeds['SE'] = sem_df
    ve_seeds['VE_mean_lower'] = ve_seeds["VE_mean"]-ve_seeds["SE"]
    ve_seeds['VE_mean_upper'] = ve_seeds["VE_mean"]+ve_seeds["SE"]
    ve_seeds['VE_mean_lower_std'] = ve_seeds["VE_mean"]-(ve_seeds["std"]/2)
    ve_seeds['VE_mean_upper_std'] = ve_seeds["VE_mean"]+(ve_seeds["std"]/2)
    ve_seeds = ve_seeds.reset_index()
    print(ve_seeds['VE_mean'].loc[90])
    return ve_seeds