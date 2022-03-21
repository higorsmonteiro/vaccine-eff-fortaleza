import os
import argparse
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
from src.DefineSchema import DefineSchema

# --> argument parser 
parser = argparse.ArgumentParser(description="Create final structured schema for further analysis based on the cohort period.")
parser.add_argument("--start", type=str, help=r"Start of the cohort - Format: YY-MM-DD")
parser.add_argument("--end", type=str, help=r"End of the cohort - Format: YY-MM-DD")
args = parser.parse_args()

# --> config
init_cohort = dt.datetime.strptime(args.start, "%Y-%m-%d")
final_cohort = dt.datetime.strptime(args.end, "%Y-%m-%d")
init_cohort_str = f"{init_cohort.day}{init_cohort.strftime('%b').upper()}{init_cohort.year}"
final_cohort_str = f"{final_cohort.day}{final_cohort.strftime('%b').upper()}{final_cohort.year}"

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

fschema.to_parquet(os.path.join(output_folder, f"SCHEMA_{init_cohort_str}_{final_cohort_str}.parquet"))