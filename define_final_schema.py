import os
import warnings
warnings.filterwarnings("ignore")
from src.DefineSchema import DefineSchema

output_folder = os.path.join("output", "data")
base_folder = os.path.join(os.environ["USERPROFILE"], "Documents", "data")
schema = DefineSchema(base_folder)

print("Load ...")
schema.load_processed_data()
schema.load_linked_data()
print("Join ...")
schema.join_keys()
print("Create ...")
fschema = schema.create_final_schema()

init_cohort_str = "21JAN2021"
final_cohort_str = "31AUG2021"
fschema.to_parquet(os.path.join(output_folder, f"SCHEMA_{init_cohort_str}_{final_cohort_str}.parquet"))