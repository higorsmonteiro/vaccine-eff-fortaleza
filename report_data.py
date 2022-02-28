'''
    Script for reporting the results obtained.
'''

import os 
import pandas as pd
import numpy as np
import datetime as dt
from src.ReportVE import ReportVE

data_path = {
    "seed": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15],
    "survival_folder": os.path.join("output", "data", "SURVIVAL_21JAN2021_31AUG2021"),
    "cohort_folder": os.path.join("output", "data", "COHORT_21JAN2021_31AUG2021"),
    "seed folder": lambda seed: os.path.join("output", "data", "SURVIVAL_21JAN2021_31AUG2021", f"seed_{seed}"),
    "pares": lambda vac, seed: f"pareados_{vac}_{seed}.csv",
    "pop info": lambda vac: f"pop_reservoir_{vac}.csv",
    "prefix": "corona",
    "init_cohort": dt.date(2021, 1, 21),
    "final_cohort": dt.date(2021, 8, 31)
}

report_ve = ReportVE(data_path)
forplot = report_ve.get_survival_curves()
x_axis, recruited_axis, total_axis = report_ve.get_recruitment(data_path["init_cohort"], data_path["final_cohort"], vaccine=data_path["prefix"])
caso_morte, controle_morte = report_ve.count_deaths(mode="D2")
