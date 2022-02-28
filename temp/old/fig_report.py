'''
    Code for automation of the generation of main figures and calculations.

    TO DO: Create command line interface to facilitate processing.
'''

# --> Lib
import os
import sys
import numpy as np
import pandas as pd
import seaborn as sns
import datetime as dt
import lib.utils as utils
import lib.fig_utils as fig_utils
import matplotlib.pyplot as plt

from src.MatchedCount import MatchedCount

import warnings
warnings.filterwarnings("ignore")

# TEMPORARY --> Simple command line interface.
vaccine_name = sys.argv[1]
if vaccine_name not in ["CORONAVAC", "ASTRAZENECA"]:
    sys.exit("Vaccine name provided not known.")

# --> Define period of cohort.
init_cohort = dt.datetime(2021, 1, 21)
final_cohort = dt.datetime(2021, 8, 31)

# --> Define data and metadata.
path_to = os.path.join("output")
data_name = {
    "GENERAL POPULATION": "ELIGIBILIDADE_CADASTRADOS_VACINAS_COORTE_FORTALEZA_1634587254.csv",
    "MATCHED CORONAVAC": "pareados_corona.csv",
    "MATCHED ASTRAZENECA": "pareados_astra.csv",
    "MATCHED PFIZER": "pareados_pfizer.csv",
    "SURVIVAL DATA CORONA": "sobrevida_coronavac.csv",
    "SURVIVAL DATA ASTRA": "sobrevida_astrazeneca.csv",
    "POPULATION FOR CORONA": "control_reservoir_corona.csv",
    "POPULATION FOR ASTRA": "control_reservoir_astra.csv"
}

general_pop = pd.read_csv(os.path.join(path_to, data_name["GENERAL POPULATION"]), index_col=0)
if vaccine_name=="CORONAVAC":
    matched_vac = pd.read_csv(os.path.join(path_to, data_name["MATCHED CORONAVAC"]), index_col=0)
    survival_vac = pd.read_csv(os.path.join(path_to, data_name["SURVIVAL DATA CORONA"]))
    pop_vac = pd.read_csv(os.path.join(path_to, data_name["POPULATION FOR CORONA"]), index_col=0)
elif vaccine_name=="ASTRAZENECA":
    matched_vac = pd.read_csv(os.path.join(path_to, data_name["MATCHED ASTRAZENECA"]), index_col=0)
    survival_vac = pd.read_csv(os.path.join(path_to, data_name["SURVIVAL DATA ASTRA"]))
    pop_vac = pd.read_csv(os.path.join(path_to, data_name["POPULATION FOR ASTRA"]), index_col=0)

# ----> Vaccine REPORT
# --> Dynamics of matching.
#fig_1, ax_1 = fig_utils.included_per_day(general_pop, pop_vac, init_cohort, final_cohort, vaccine_name=vaccine_name)
# save fig here.

# --> Basic demographics
#frame_demo = fig_utils.demogr_table(general_pop, pop_vac, vaccine_name=vaccine_name)

# --> Define life tables
survival_fname = os.path.join("output", f"sobrevida_{vaccine_name.lower()}.csv")
#lifetb_death = fig_utils.create_lifetables(survival_fname, survival_vac, init_cohort, final_cohort, outcome_str="OBITO")
#lifetb_hospt = fig_utils.create_lifetables(survival_fname, survival_vac, init_cohort, final_cohort, outcome_str="HOSPITALIZACAO")

# --> Time windows
fig_2, ax_2 = fig_utils.time_window_plot(pop_vac, f"Death by Covid-19 - {vaccine_name}", mode="OBITO")
fig_3, ax_3 = fig_utils.time_window_plot(pop_vac, f"Hospitalization due to Covid-19 - {vaccine_name}", mode="HOSPT")

# --> Survival analysis
# D1 as t_0
obj_deaths = MatchedCount(survival_fname, "OBITO")
obj_hospt = MatchedCount(survival_fname, "HOSPITALIZACAO")

KM_CASO_DEATH, KM_CONTROLE_DEATH = obj_deaths.fit_km_period(init_period=0, negative_intervals=False)
KM_CASO_HOSPT, KM_CONTROLE_HOSPT = obj_hospt.fit_km_period(init_period=0, negative_intervals=False)
KM_CASO_DEATH = KM_CASO_DEATH.iloc[:100, :]
KM_CASO_HOSPT = KM_CASO_HOSPT.iloc[:100, :]
KM_CONTROLE_DEATH = KM_CONTROLE_DEATH.iloc[:100, :]
KM_CONTROLE_HOSPT = KM_CONTROLE_HOSPT.iloc[:100, :]

fig_4, ax_4 = fig_utils.KM_plotting(KM_CASO_DEATH, KM_CONTROLE_DEATH, KM_CASO_HOSPT, KM_CONTROLE_HOSPT)
fig_4.suptitle(f"{vaccine_name} - D1 and D1+D2", fontsize=14, y = 0.92)

# Daily hazard ratio
fig_41, ax_41 = fig_utils.daily_hazard(KM_CASO_DEATH, KM_CONTROLE_DEATH, KM_CASO_HOSPT, KM_CONTROLE_HOSPT)
fig_41.suptitle(f"{vaccine_name} - D1 and D1+D2", fontsize=14)

# D2 as t_0
obj_deaths = MatchedCount(survival_fname, "OBITO")
obj_hospt = MatchedCount(survival_fname, "HOSPITALIZACAO")

KM_CASO_DEATH, KM_CONTROLE_DEATH = obj_deaths.second_dose_km(negative_intervals=False)
KM_CASO_HOSPT, KM_CONTROLE_HOSPT = obj_hospt.second_dose_km(negative_intervals=False)
KM_CASO_DEATH = KM_CASO_DEATH.iloc[:100, :]
KM_CASO_HOSPT = KM_CASO_HOSPT.iloc[:100, :]
KM_CONTROLE_DEATH = KM_CONTROLE_DEATH.iloc[:100, :]
KM_CONTROLE_HOSPT = KM_CONTROLE_HOSPT.iloc[:100, :]

fig_5, ax_5 = fig_utils.KM_plotting(KM_CASO_DEATH, KM_CONTROLE_DEATH, KM_CASO_HOSPT, KM_CONTROLE_HOSPT)
fig_5.suptitle(f"{vaccine_name} - D1+D2", fontsize=14, y=0.92)

# Daily hazard ratio
fig_51, ax_51 = fig_utils.daily_hazard(KM_CASO_DEATH, KM_CONTROLE_DEATH, KM_CASO_HOSPT, KM_CONTROLE_HOSPT)
fig_51.suptitle(f"{vaccine_name} - D1+D2", fontsize=14)

# --> Calculate VE for different periods
file_death = os.path.join("output", f"sobrevida_{vaccine_name.lower()}.csv")
file_hospt = os.path.join("output", f"sobrevida_{vaccine_name.lower()}.csv")
df = fig_utils.survival_effectiveness_period(file_death, file_hospt) # Modify arguments handling

# --> Calculate VE for different stratifications

