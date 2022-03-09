import os
import pandas as pd
import datetime as dt
from src.VaccineEffectiveness import VaccineEffectiveness

import warnings
warnings.filterwarnings("ignore")

# --> Config
seed = 1
vaccine = "CORONAVAC"
init_cohort = dt.datetime(2021, 1, 21)
final_cohort = dt.datetime(2021, 8, 31)
fname = "SCHEMA_21JAN2021_31AUG2021.parquet"
base_path = os.path.join(os.environ["USERPROFILE"], "Documents", "projects", "vaccine-eff-fortaleza")

# --> Main data to be parsed to the class
pairs = pd.read_parquet(os.path.join(base_path, "output", "PAREAMENTO", "CORONAVAC", f"PAREADOS_CPF_{seed}.parquet"))
fschema = pd.read_parquet(os.path.join(base_path, "output", "data", fname))

survival_folder = os.path.join(base_path, "output", "PAREAMENTO", vaccine, "SURVIVAL")
obito = VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), event="OBITO")
hospital = VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), event="HOSPITAL")

obito.initialize_objects()
hospital.initialize_objects()

obito.bootstrap_ve(survival_folder, 200, seed)
hospital.bootstrap_ve(survival_folder, 200, seed)
 
obito.load_survival_data(survival_folder)
hospital.load_survival_data(survival_folder)

obito.fit_data()
hospital.fit_data()

obito.generate_tables()
hospital.generate_tables()

obito.generate_curves()
hospital.generate_curves()

