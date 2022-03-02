import os
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
from src.PerformMatching import PerformMatching

# --> config
vaccine = "CORONAVAC"
init_cohort = dt.datetime(2021, 1, 21)
final_cohort = dt.datetime(2021, 8, 31)
fname = "SCHEMA_21JAN2021_31AUG2021.parquet"
fschema = pd.read_parquet(os.path.join("output", "data", fname))
output_folder = os.path.join("output", "PAREAMENTO", vaccine)

m_obj = PerformMatching(fschema, (init_cohort, final_cohort))
for seed in [1,2,3,4,5,6,7,8,9,10]:
    m_obj.perform_matching(output_folder, vaccine="CORONAVAC", age_range=(60,200), seed=seed)