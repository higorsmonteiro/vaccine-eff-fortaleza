import os
import argparse
import pandas as pd
import datetime as dt
from src.PerformMatching import PerformMatching
import warnings
warnings.filterwarnings("ignore")

# --> argument parser 
parser = argparse.ArgumentParser(description="Arguments to set up the matching mechanism for a given vaccine during a defined cohort.")
parser.add_argument("--start", type=str, help=r"Start of the cohort - Format: YY-MM-DD.")
parser.add_argument("--end", type=str, help=r"End of the cohort - Format: YY-MM-DD.")
parser.add_argument("--suffix", type=str, help=r"Suffix string to contrast between data - Empty string is not valid.")
parser.add_argument("--vaccine", type=str, help=r"Which vaccine to assess. Valid strings: 'CORONAVAC', 'ASTRAZENECA'.")
parser.add_argument("--hdi_index", type=str, help=r'''Which categorical division to use for the HDI variable. Valid options 
                                                      are 0, 1 and 2. Option 0 is the same as no using HDI.''')
parser.add_argument("--pop_test", type=str, help=r'''Variable for testing which global population to use for matching. Valid 
                                                    options are 'ALL' and 'VACCINE'. ''', default="ALL")
parser.add_argument("--seed", type=int, help=r'''Seed for controls.''')
parser.add_argument("--age_range", type=int, nargs="+", default=(60,200), help=r"Minimum and maximum age to consider when doing the matching.")
args = parser.parse_args()

# --> config
seed = args.seed
vaccine = args.vaccine
suffix = args.suffix
init_cohort = dt.datetime.strptime(args.start, "%Y-%m-%d")
final_cohort = dt.datetime.strptime(args.end, "%Y-%m-%d")
pop_test = args.pop_test
hdi_index = args.hdi_index
age_range = tuple(args.age_range)

# --> Simple checks
if len(age_range)>2:
    raise ValueError("More than two values were parsed to the age range.")
if pop_test not in ["ALL", "VACCINE"]:
    raise ValueError("'pop_test' value does not correspond to a valid option.")
if vaccine not in ["CORONAVAC", "ASTRAZENECA"]:
    raise ValueError("'vaccine' value does not correspond to a valid option.")

# --> Set up
init_str = f"{init_cohort.day}{init_cohort.strftime('%b').upper()}{init_cohort.year}"
final_str = f"{final_cohort.day}{final_cohort.strftime('%b').upper()}{final_cohort.year}"
fname = f"SCHEMA_{init_str}_{final_str}.parquet"
fschema = pd.read_parquet(os.path.join("output", "data", fname))

# --> Create folder with suffix+HDI name
output_folder = os.path.join("output", "PAREAMENTO", vaccine, f"{suffix}_HDI_{hdi_index}_{init_str}_{final_str}")
if not os.path.isdir(output_folder):
    os.mkdir(output_folder)
    os.mkdir(os.path.join(output_folder, "SURVIVAL"))

m_obj = PerformMatching(fschema, (init_cohort, final_cohort))
# --> Perform matching
m_obj.perform_matching(output_folder, vaccine=vaccine, age_range=age_range, HDI_index=hdi_index, seed=seed, pop_test=pop_test)
# --> Calculate survival intervals
m_obj.generate_survival_info(output_folder, seed=seed) 
