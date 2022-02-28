import os
import json
import datetime as dt
import numpy as np
import pandas as pd
from tqdm import tqdm
from collections import defaultdict
from src.VaccineEffectiveness import VaccineEffectiveness

config = {
    "seed": [1,2,3,4,5,6,7,8,9,10],
    "final cohort": dt.date(2021, 8, 31),
    "cohort folder": "COHORT_21JAN2021_31AUG2021",
    "survival folder": "SURVIVAL_21JAN2021_31AUG2021",
    "output folder": os.path.join("output", "data"),
    "prefix": "astra",
}

data_dframe = {
    "df_pair": lambda seed: os.path.join(config["output folder"], config["cohort folder"], f"pareados_{config['prefix']}_{seed}.csv"),
    "df_info": os.path.join(config["output folder"], config["cohort folder"], f"pop_reservoir_{config['prefix']}.csv"),
}

survival_folder = os.path.join(config["output folder"], config["survival folder"], config["prefix"])
if not os.path.isdir(survival_folder):
    os.mkdir(survival_folder)

for seed in config["seed"]:
    print(f"Seed: {seed}")
    df_pair = pd.read_csv(data_dframe["df_pair"](seed))
    df_pair["CPF"] = df_pair["CPF"].apply(lambda x: f"{x:11.0f}".replace(" ","0") if not pd.isna(x) else np.nan)
    df_pair["PAR"] = df_pair["PAR"].apply(lambda x: f"{x:11.0f}".replace(" ","0") if not pd.isna(x) else np.nan)
    df_info = pd.read_csv(data_dframe["df_info"])
    df_info["cpf"] = df_info["cpf"].apply(lambda x: f"{x:11.0f}".replace(" ","0") if not pd.isna(x) else np.nan)

    ve_obj = VaccineEffectiveness(df_pair, df_info)
    survival_obs = ve_obj.define_intervals(config["final cohort"], return_=True)

    # For monitoring and validation
    data_intervals = defaultdict(lambda: np.nan)
    intervals = ve_obj.intervals
    for cur_interval in intervals:
        data_intervals[cur_interval["CPF CASO"]] = {
            "CONTROLE": cur_interval["CPF CONTROLE"],
            "D1": cur_interval["D1"],
            "D2": cur_interval["D2"]
        }

    # Create the final storage folders 
    seed_folder = os.path.join(config["output folder"], config["survival folder"], config["prefix"], f"seed_{seed}")
    if not os.path.isdir(seed_folder):
        os.mkdir(seed_folder)
        for t in ["t_0", "t_13"]:
            if not os.path.isdir(os.path.join(seed_folder, t)):
                os.mkdir(os.path.join(seed_folder, t))
    
    # Save JSON containing the calculated intervals for each pair.
    survival_obs.to_csv(os.path.join(seed_folder, "survival_individual.csv"), index=False) 
    with open(os.path.join(seed_folder, "intervals.json"), "w") as f:
        json.dump(data_intervals, f)

    # Manually create the survival tables.
    RESULTS = {
        "t_0": ve_obj.resume_survival_ve(t_0=0),
        "t_13": ve_obj.resume_survival_ve(t_0=13)
    }
    # --> Save results
    keys = ["D1", "D2", "D1_M", "D1_F", "D2_M", "D2_F", "D1_6069",
            "D1_7079", "D1_80+", "D2_6069", "D2_7079", "D2_80+"]
    for t in ["t_0", "t_13"]:
        folder = os.path.join(config["output folder"], config["survival folder"], config["prefix"], f"seed_{seed}", t)
        for key in keys:
            cur_df = RESULTS[t][key]
            cur_df.to_csv(os.path.join(folder, f"survival_tb_{key}_{t}_{seed}.csv"), index=False)

    res_1 = ve_obj.resume_hazard_ve(period_d1=[0,27], period_d2=[14,150])
    res_2 = ve_obj.resume_hazard_ve(period_d1=[14,27], period_d2=[14,150])
    res_3 = ve_obj.resume_hazard_ve(period_d1=[20,27], period_d2=[14,150])
    res_4 = ve_obj.resume_hazard_ve(period_d1=[14,27], period_d2=[0,14])
    res_5 = ve_obj.resume_hazard_ve(period_d1=[14,20], period_d2=[14,150])

    resume_cox = {
        "1-HR(CI-)":[], "1-HR": [], "1-HR(CI+)":[], 
        "1-HR(CI-) MASC": [], "1-HR MASC": [], "1-HR(CI+) MASC": [],
        "1-HR(CI-) FEM": [], "1-HR FEM": [], "1-HR(CI+) FEM": [],
        "1-HR(CI-) 60-69": [], "1-HR 60-69": [], "1-HR(CI+) 60-69": [],
        "1-HR(CI-) 70-79": [], "1-HR 70-79": [], "1-HR(CI+) 70-79": [],
        "1-HR(CI-) 80+": [], "1-HR 80+": [], "1-HR(CI+) 80+": [],
    }
    cox_hr = pd.DataFrame(index=["1st 0-27", "1st 14-20", "1st 14-27", "1st 20-27", "2nd 0-13", "2nd 14+"],
                          columns=resume_cox.keys())

    first_row = [res_1["D1D2"]["D1"][1][1], res_1["D1D2"]["D1"][0], res_1["D1D2"]["D1"][1][0],
                 res_1["D1D2_M"]["D1"][1][1], res_1["D1D2_M"]["D1"][0], res_1["D1D2_M"]["D1"][1][0],
                 res_1["D1D2_F"]["D1"][1][1], res_1["D1D2_F"]["D1"][0], res_1["D1D2_F"]["D1"][1][0],
                 res_1["D1D2_6069"]["D1"][1][1], res_1["D1D2_6069"]["D1"][0], res_1["D1D2_6069"]["D1"][1][0],
                 res_1["D1D2_7079"]["D1"][1][1], res_1["D1D2_7079"]["D1"][0], res_1["D1D2_7079"]["D1"][1][0],
                 res_1["D1D2_80+"]["D1"][1][1], res_1["D1D2_80+"]["D1"][0], res_1["D1D2_80+"]["D1"][1][0]]
    second_row = [res_5["D1D2"]["D1"][1][1], res_5["D1D2"]["D1"][0], res_5["D1D2"]["D1"][1][0],
                 res_5["D1D2_M"]["D1"][1][1], res_5["D1D2_M"]["D1"][0], res_5["D1D2_M"]["D1"][1][0],
                 res_5["D1D2_F"]["D1"][1][1], res_5["D1D2_F"]["D1"][0], res_5["D1D2_F"]["D1"][1][0],
                 res_5["D1D2_6069"]["D1"][1][1], res_5["D1D2_6069"]["D1"][0], res_5["D1D2_6069"]["D1"][1][0],
                 res_5["D1D2_7079"]["D1"][1][1], res_5["D1D2_7079"]["D1"][0], res_5["D1D2_7079"]["D1"][1][0],
                 res_5["D1D2_80+"]["D1"][1][1], res_5["D1D2_80+"]["D1"][0], res_5["D1D2_80+"]["D1"][1][0]]
    third_row = [res_2["D1D2"]["D1"][1][1], res_2["D1D2"]["D1"][0], res_2["D1D2"]["D1"][1][0],
                 res_2["D1D2_M"]["D1"][1][1], res_2["D1D2_M"]["D1"][0], res_2["D1D2_M"]["D1"][1][0],
                 res_2["D1D2_F"]["D1"][1][1], res_2["D1D2_F"]["D1"][0], res_2["D1D2_F"]["D1"][1][0],
                 res_2["D1D2_6069"]["D1"][1][1], res_2["D1D2_6069"]["D1"][0], res_2["D1D2_6069"]["D1"][1][0],
                 res_2["D1D2_7079"]["D1"][1][1], res_2["D1D2_7079"]["D1"][0], res_2["D1D2_7079"]["D1"][1][0],
                 res_2["D1D2_80+"]["D1"][1][1], res_2["D1D2_80+"]["D1"][0], res_2["D1D2_80+"]["D1"][1][0]]
    fourth_row = [res_3["D1D2"]["D1"][1][1], res_3["D1D2"]["D1"][0], res_3["D1D2"]["D1"][1][0],
                 res_3["D1D2_M"]["D1"][1][1], res_3["D1D2_M"]["D1"][0], res_3["D1D2_M"]["D1"][1][0],
                 res_3["D1D2_F"]["D1"][1][1], res_3["D1D2_F"]["D1"][0], res_3["D1D2_F"]["D1"][1][0],
                 res_3["D1D2_6069"]["D1"][1][1], res_3["D1D2_6069"]["D1"][0], res_3["D1D2_6069"]["D1"][1][0],
                 res_3["D1D2_7079"]["D1"][1][1], res_3["D1D2_7079"]["D1"][0], res_3["D1D2_7079"]["D1"][1][0],
                 res_3["D1D2_80+"]["D1"][1][1], res_3["D1D2_80+"]["D1"][0], res_3["D1D2_80+"]["D1"][1][0]]
    fifth_row = [res_4["D1D2"]["D2"][1][1], res_4["D1D2"]["D2"][0], res_4["D1D2"]["D2"][1][0],
                 res_4["D1D2_M"]["D2"][1][1], res_4["D1D2_M"]["D2"][0], res_4["D1D2_M"]["D2"][1][0],
                 res_4["D1D2_F"]["D2"][1][1], res_4["D1D2_F"]["D2"][0], res_4["D1D2_F"]["D2"][1][0],
                 res_4["D1D2_6069"]["D2"][1][1], res_4["D1D2_6069"]["D2"][0], res_4["D1D2_6069"]["D2"][1][0],
                 res_4["D1D2_7079"]["D2"][1][1], res_4["D1D2_7079"]["D2"][0], res_4["D1D2_7079"]["D2"][1][0],
                 res_4["D1D2_80+"]["D2"][1][1], res_4["D1D2_80+"]["D2"][0], res_4["D1D2_80+"]["D2"][1][0]]
    sixth_row = [res_1["D1D2"]["D2"][1][1], res_1["D1D2"]["D2"][0], res_1["D1D2"]["D2"][1][0],
                 res_1["D1D2_M"]["D2"][1][1], res_1["D1D2_M"]["D2"][0], res_1["D1D2_M"]["D2"][1][0],
                 res_1["D1D2_F"]["D2"][1][1], res_1["D1D2_F"]["D2"][0], res_1["D1D2_F"]["D2"][1][0],
                 res_1["D1D2_6069"]["D2"][1][1], res_1["D1D2_6069"]["D2"][0], res_1["D1D2_6069"]["D2"][1][0],
                 res_1["D1D2_7079"]["D2"][1][1], res_1["D1D2_7079"]["D2"][0], res_1["D1D2_7079"]["D2"][1][0],
                 res_1["D1D2_80+"]["D2"][1][1], res_1["D1D2_80+"]["D2"][0], res_1["D1D2_80+"]["D2"][1][0]]

    cox_hr.loc["1st 0-27"] = first_row
    cox_hr.loc["1st 14-20"] = second_row
    cox_hr.loc["1st 14-27"] = third_row
    cox_hr.loc["1st 20-27"] = fourth_row
    cox_hr.loc["2nd 0-13"] = fifth_row
    cox_hr.loc["2nd 14+"] = sixth_row
    cox_hr.to_csv(os.path.join(seed_folder, f"cox_ve_{seed}.csv"))


    

