import pandas as pd
import numpy as np
from lib.fig_utils import calc_ci, pointplot_VE
from src.KaplanMeierSurvival import KM_analysis

vaccine = "corona"
seed = 2
pos_d1 = 20
pos_d2 = 49
df = pd.read_csv(f"output/data/sobrevida_{vaccine}_{seed}.csv")
df_pop = pd.read_csv("output/eligibility_population.csv")

obj = KM_analysis(df)
caso_d1_14, controle_d1_14 = obj.fit_dose(df_pop, "OBITO COVID", from_day=0, d2=False)
caso_d2_14, controle_d2_14 = obj.fit_dose(df_pop, "OBITO COVID", from_day=0, d2=True)

caso_d1_14m, controle_d1_14m, caso_d1_14f, controle_d1_14f = obj.fit_sex(df_pop, "OBITO COVID", from_day=0, d2=False)
caso_d2_14m, controle_d2_14m, caso_d2_14f, controle_d2_14f = obj.fit_sex(df_pop, "OBITO COVID", from_day=0, d2=True)

caso_d1_14_6069, controle_d1_14_6069 = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=False, age_interval=[60,69])
caso_d1_14_7079, controle_d1_14_7079 = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=False, age_interval=[70,79])
caso_d1_14_8089, controle_d1_14_8089 = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=False, age_interval=[80,120])
caso_d1_14_90plus, controle_d1_14_90plus = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=False, age_interval=[90,200])

caso_d2_14_6069, controle_d2_14_6069 = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=True, age_interval=[60,69])
caso_d2_14_7079, controle_d2_14_7079 = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=True, age_interval=[70,79])
caso_d2_14_8089, controle_d2_14_8089 = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=True, age_interval=[80,120])
caso_d2_14_90plus, controle_d2_14_90plus = obj.fit_age(df_pop, "OBITO COVID", from_day=14, d2=True, age_interval=[90,200])

RESULTS = {
    "D1": {
        "GERAL->14d+": None,
        "MALE->14d+": None,
        "FEMALE->14d+": None,
        "60-69->14d+": None,
        "70-79->14d+": None,
        "80+->14d+": None,
        "90+->14d+": None
    },
    "D2": {
        "GERAL->14d+": None,
        "MALE->14d+": None,
        "FEMALE->14d+": None,
        "60-69->14d+": None,
        "70-79->14d+": None,
        "80+->14d+": None,
        "90+->14d+": None
    }
}

RESULTS_CI = {
    "D1(CI)": {
        "GERAL->14d+": None,
        "MALE->14d+": None,
        "FEMALE->14d+": None,
        "60-69->14d+": None,
        "70-79->14d+": None,
        "80+->14d+": None,
        "90+->14d+": None
    },
    "D2(CI)": {
        "GERAL->14d+": None,
        "MALE->14d+": None,
        "FEMALE->14d+": None,
        "60-69->14d+": None,
        "70-79->14d+": None,
        "80+->14d+": None,
        "90+->14d+": None
    }
}

RESULTS["D1"]["GERAL->14d+"] = (1-caso_d1_14["KM_estimate"]/controle_d1_14["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14["Factor for CI of RR"] + controle_d1_14["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["GERAL->14d+"] = calc_ci(RESULTS["D1"]["GERAL->14d+"], CI_AUX)

RESULTS["D1"]["MALE->14d+"] = (1-caso_d1_14m["KM_estimate"]/controle_d1_14m["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14m["Factor for CI of RR"] + controle_d1_14m["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["MALE->14d+"] = calc_ci(RESULTS["D1"]["MALE->14d+"], CI_AUX)

RESULTS["D1"]["FEMALE->14d+"] = (1-caso_d1_14f["KM_estimate"]/controle_d1_14f["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14f["Factor for CI of RR"] + controle_d1_14f["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["FEMALE->14d+"] = calc_ci(RESULTS["D1"]["FEMALE->14d+"], CI_AUX)

RESULTS["D1"]["60-69->14d+"] = (1-caso_d1_14_6069["KM_estimate"]/controle_d1_14_6069["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14_6069["Factor for CI of RR"] + controle_d1_14_6069["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["60-69->14d+"] = calc_ci(RESULTS["D1"]["60-69->14d+"], CI_AUX)

RESULTS["D1"]["70-79->14d+"] = (1-caso_d1_14_7079["KM_estimate"]/controle_d1_14_7079["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14_7079["Factor for CI of RR"] + controle_d1_14_7079["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["70-79->14d+"] = calc_ci(RESULTS["D1"]["70-79->14d+"], CI_AUX)

RESULTS["D1"]["80+->14d+"] = (1-caso_d1_14_8089["KM_estimate"]/controle_d1_14_8089["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14_8089["Factor for CI of RR"] + controle_d1_14_8089["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["80+->14d+"] = calc_ci(RESULTS["D1"]["80+->14d+"], CI_AUX)

RESULTS["D1"]["90+->14d+"] = (1-caso_d1_14_90plus["KM_estimate"]/controle_d1_14_90plus["KM_estimate"]).loc[pos_d1]
CI_AUX = 1.96*np.sqrt((caso_d1_14_90plus["Factor for CI of RR"] + controle_d1_14_90plus["Factor for CI of RR"]).loc[pos_d1])
RESULTS_CI["D1(CI)"]["90+->14d+"] = calc_ci(RESULTS["D1"]["90+->14d+"], CI_AUX)

RESULTS["D2"]["GERAL->14d+"] = (1-caso_d2_14["KM_estimate"]/controle_d2_14["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14["Factor for CI of RR"] + controle_d2_14["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["GERAL->14d+"] = calc_ci(RESULTS["D2"]["GERAL->14d+"], CI_AUX)

RESULTS["D2"]["MALE->14d+"] = (1-caso_d2_14m["KM_estimate"]/controle_d2_14m["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14m["Factor for CI of RR"] + controle_d2_14m["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["MALE->14d+"] = calc_ci(RESULTS["D2"]["MALE->14d+"], CI_AUX)

RESULTS["D2"]["FEMALE->14d+"] = (1-caso_d2_14f["KM_estimate"]/controle_d2_14f["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14f["Factor for CI of RR"] + controle_d2_14f["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["FEMALE->14d+"] = calc_ci(RESULTS["D2"]["FEMALE->14d+"], CI_AUX)

RESULTS["D2"]["60-69->14d+"] = (1-caso_d2_14_6069["KM_estimate"]/controle_d2_14_6069["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14_6069["Factor for CI of RR"] + controle_d2_14_6069["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["60-69->14d+"] = calc_ci(RESULTS["D2"]["60-69->14d+"], CI_AUX)

RESULTS["D2"]["70-79->14d+"] = (1-caso_d2_14_7079["KM_estimate"]/controle_d2_14_7079["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14_7079["Factor for CI of RR"] + controle_d2_14_7079["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["70-79->14d+"] = calc_ci(RESULTS["D2"]["70-79->14d+"], CI_AUX)

RESULTS["D2"]["80+->14d+"] = (1-caso_d2_14_8089["KM_estimate"]/controle_d2_14_8089["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14_8089["Factor for CI of RR"] + controle_d2_14_8089["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["80+->14d+"] = calc_ci(RESULTS["D2"]["80+->14d+"], CI_AUX)

RESULTS["D2"]["90+->14d+"] = (1-caso_d2_14_90plus["KM_estimate"]/controle_d2_14_90plus["KM_estimate"]).loc[pos_d2]
CI_AUX = 1.96*np.sqrt((caso_d2_14_90plus["Factor for CI of RR"] + controle_d2_14_90plus["Factor for CI of RR"]).loc[pos_d2])
RESULTS_CI["D2(CI)"]["90+->14d+"] = calc_ci(RESULTS["D2"]["90+->14d+"], CI_AUX)

#ax = pointplot_VE(RESULTS, RESULTS_CI)
