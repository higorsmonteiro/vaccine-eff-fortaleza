import os
import argparse
from nbformat import ValidationError
import pandas as pd
import datetime as dt
from src.VaccineEffectiveness import VaccineEffectiveness
import warnings
warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(description=r"Entries for calculating vaccine effectiveness using the Kaplan-Meier estimator.")
parser.add_argument("--start", type=str, help=r"Start of the cohort - Format: YY-MM-DD.")
parser.add_argument("--end", type=str, help=r"End of the cohort - Format: YY-MM-DD.")
parser.add_argument("--vaccine", type=str, help=r"Which vaccine to assess. Valid strings: 'CORONAVAC', 'ASTRAZENECA'.")
parser.add_argument("--seed", type=int, help=r"Seed value to access the right file regarding the matching pairs and their events.")
parser.add_argument("--t_min", type=int, help=r"To consider only those individuals still at risk at day 't_min'")
parser.add_argument("--suffix", type=str, help=r"Suffix used to signal how the global population was defined during the matching mechanism.")
parser.add_argument("--hdi_index", type=str, help=r'''Which categorical division to use for the HDI variable. Valid options 
                                                      are 0, 1 and 2. Option 0 is the same as no using HDI.''')
parser.add_argument("--events", type=str, default="ALL", help=r'''Whether to run all the events (Death, hospitalization and ICU) or
                                                                  only a specific event: 'OBITO', 'HOSPITALIZACAO', 'UTI'.''')
parser.add_argument("--bootstrap_n", type=int, default=10, help=r'''Number of simulations to perform during the percentile bootstrap 
                                                                    for confidence intervals of the vaccine effectiveness.''')
parser.add_argument("--dose", type=str, nargs="+", help=r'''Dose to consider when performing the calculation ('D1' or 'D2')''')
parser.add_argument("--days_after", type=int, default=0, help=r'''Number of days from day zero of an individual's cohort to consider when doing the matching.''')
#parser.add_argument("--delay_vaccine", type=int, default=0, help=r'''Number of days extra for censoring a pair due to vaccination of the control individual.''')
args = parser.parse_args()

# --> Config
seed = args.seed
#t_min = args.t_min
vaccine = args.vaccine
suffix = args.suffix
init_cohort = dt.datetime.strptime(args.start, "%Y-%m-%d")
final_cohort = dt.datetime.strptime(args.end, "%Y-%m-%d")
hdi_index = args.hdi_index
events = args.events
bootstrap_n = args.bootstrap_n
dose = ' '.join(args.dose)
days_after = args.days_after
#delay_vaccine = args.delay_vaccine

# --> Set up
init_str = f"{init_cohort.day}{init_cohort.strftime('%b').upper()}{init_cohort.year}"
final_str = f"{final_cohort.day}{final_cohort.strftime('%b').upper()}{final_cohort.year}"
fname = f"SCHEMA_{init_str}_{final_str}.parquet"

base_path = os.path.join(os.environ["USERPROFILE"], "Documents", "projects", "vaccine-eff-fortaleza")
matching_folder = os.path.join(base_path, "output", "PAREAMENTO", vaccine, f"{suffix}_HDI_{hdi_index}_{init_str}_{final_str}")
survival_folder = os.path.join(matching_folder, "SURVIVAL")

# ----> Simple error checks
if not os.path.isdir(matching_folder):
    raise ValueError("No matching files for this configuration.")
if not os.path.isdir(survival_folder):
    raise ValueError("No folder for survival intervals found.")
if not os.path.isfile(os.path.join(base_path, "output", "data", fname)):
    raise FileNotFoundError("No file for this cohort period.")
if not os.path.isfile(os.path.join(matching_folder, f"PAREADOS_CPF_{dose}_DAY{days_after}_{seed}.parquet")):
    raise FileNotFoundError("No file for matched pairs.")

# ----> Main data to be parsed: pairs file and schema file.
fschema = pd.read_parquet(os.path.join(base_path, "output", "data", fname))
pairs = pd.read_parquet(os.path.join(matching_folder, f"PAREADOS_CPF_{dose}_DAY{days_after}_{seed}.parquet"))

if events=="ALL":
    event_lst = [VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), dose=dose, days_after=days_after, event=x) for x in ["OBITO", "HOSPITAL", "UTI"]]
else:
    if events not in ["OBITO", "HOSPITALIZACAO", "UTI"]:
        raise ValidationError("Input for variable 'events' is not valid.")
    event_lst = [VaccineEffectiveness(fschema, pairs, (init_cohort, final_cohort), dose=dose, days_after=days_after, event=events)]

# --> Initialize objects to hold the information of the fitter.
for cur_event in event_lst:
    cur_event.initialize_objects()

# --> Calculate the vaccine effectiveness by using the percentile bootstrap over the results of the Kaplan-Meier estimator.
for cur_event in event_lst:
    if hdi_index!=0:
        cur_event.bootstrap_ve(survival_folder, bootstrap_n, seed, True)
        cur_event.km_curves(survival_folder, seed, True)
    else:
        cur_event.bootstrap_ve(survival_folder, bootstrap_n, seed, False)
        cur_event.km_curves(survival_folder, seed, False)

# --> Create folder to store output
seed_folder = os.path.join(survival_folder, f"SEED{seed}")
if not os.path.isdir(seed_folder):
    os.mkdir(seed_folder)
    os.mkdir(os.path.join(seed_folder, "FIGS"))

# --> Save output
for cur_event in event_lst:
    cur_event.generate_output(seed_folder)

