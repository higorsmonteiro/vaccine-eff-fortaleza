import os
import sys
import pandas as pd
import datetime as dt
from src.VaccineEffectiveness import VaccineEffectiveness

import warnings
warnings.filterwarnings("ignore")

# --> Config
seed = 1
t_min = 14
vaccine = "CORONAVAC"
suffix = sys.argv[1]
init_cohort = dt.datetime(2021, 1, 21)
final_cohort = dt.datetime(2021, 8, 31)
fname = "SCHEMA_21JAN2021_31AUG2021.parquet"
base_path = os.path.join(os.environ["USERPROFILE"], "Documents", "projects", "vaccine-eff-fortaleza")

# --> Main data to be parsed to the class
pairs = pd.read_parquet(os.path.join(base_path, "output", "PAREAMENTO", "CORONAVAC", f"PAREADOS_CPF_{seed}{suffix}.parquet"))
fschema = pd.read_parquet(os.path.join(base_path, "output", "data", fname))

survival_folder = os.path.join(base_path, "output", "PAREAMENTO", vaccine, "SURVIVAL")
obito = VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), event="OBITO", suffix=suffix)
hospital = VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), event="HOSPITAL", suffix=suffix)
uti = VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), event="UTI", suffix=suffix)

obito.initialize_objects()
hospital.initialize_objects()
uti.initialize_objects()

obito.bootstrap_ve(survival_folder, 20, seed, t_min)
hospital.bootstrap_ve(survival_folder, 20, seed, t_min)
uti.bootstrap_ve(survival_folder, 20, seed, t_min)

# --> OUTPUT
seed_folder = os.path.join(survival_folder, f"SEED{seed}{suffix}")
if not os.path.isdir(seed_folder):
    os.mkdir(seed_folder)

#obito.generate_output(seed_folder, t_min, "OBITO")
#hospital.generate_output(seed_folder, t_min, "HOSPITAL")
#uti.generate_output(seed_folder, t_min, "UTI")

# T = 0 - CORONAPOP 1
# T = 14 - CORONAPOP 1
# T = 0 - TODAPOP 1
# T = 14 - TODAPOP 1

