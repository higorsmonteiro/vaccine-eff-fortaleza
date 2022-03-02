import os
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
from src.DefineSchema import DefineSchema

# --> config
init_cohort = dt.datetime(2021, 1, 21)
final_cohort = dt.datetime(2021, 8, 31)

output_folder = os.path.join("output", "data")
base_folder = os.path.join(os.environ["USERPROFILE"], "Documents", "data")
schema = DefineSchema(base_folder)

print("Load ...")
schema.load_processed_data()
schema.load_linked_data()
print("Join ...")
schema.join_keys()
print("Create ...")
fschema = schema.create_final_schema(cohort=(init_cohort, final_cohort))

init_cohort_str = "21JAN2021"
final_cohort_str = "31AUG2021"
fschema.to_parquet(os.path.join(output_folder, f"SCHEMA_{init_cohort_str}_{final_cohort_str}.parquet"))