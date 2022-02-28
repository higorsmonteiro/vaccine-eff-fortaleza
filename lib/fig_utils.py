import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from src.MatchedCount import MatchedCount

def included_per_day(general_pop, pop_corona, init_cohort, final_cohort,
                     vaccine_name="CoronaVac"):
    '''
    
    '''
    # dummy0 -> Total population
    # dummy1 -> Included in the cohort
    # dummy2 -> Included & matched
    cond1 = general_pop["VACINA(VACINADOS)"]=="CORONAVAC"
    cond2 = pd.isna(general_pop["VACINA(VACINADOS)"])
    dummy0 = general_pop[cond1 | cond2]["DATA D1(VACINADOS)"].value_counts().reset_index().sort_values(by="index")
    dummy1 = pop_corona["DATA D1(VACINADOS)"].value_counts().reset_index().sort_values(by="index")
    dummy2 = pop_corona[pop_corona["PAREADO"]=="SIM"]["DATA D1(VACINADOS)"].value_counts().reset_index().sort_values(by="index")

    final = pd.concat([dummy0.set_index(keys=["index"]), dummy1.set_index(keys=["index"]), 
                       dummy2.set_index(keys=["index"])], axis=1, join="outer")
    final.columns = ["Total population", "Included in the cohort", "Included and matched"]
    final = final.reset_index()
    final["date"] = pd.to_datetime(final["index"], format="%Y-%m-%d", errors="coerce")
    final["date_name"] = final["date"].apply(lambda x: f"{x.strftime('%b')} {x.day}, {x.year}")
    final = final.drop("index", axis=1)
    final = final[(final["date"]>=init_cohort) & (final["date"]<=final_cohort)]

    fig, ax = plt.subplots(1, figsize=(13,5))
    sns.barplot(x="date_name", y="Total population", data=final, ax=ax, color="tab:green", label=f"{vaccine_name} population")
    sns.barplot(x="date_name", y="Included in the cohort", data=final, ax=ax, color="tab:blue", label="Included")
    sns.barplot(x="date_name", y="Included and matched", data=final, ax=ax, color="tab:red", label="Included and matched")

    ax.grid(alpha=0.2)
    ax.legend(loc=1, prop={"size": 14})
    ax.locator_params(axis='x', nbins=14)
    ax.tick_params(rotation=25, direction="out")
    ax.set_xlabel("")
    ax.set_ylabel("Patients", fontsize=14)
    return fig, ax

def demogr_table(general_pop, pop_vaccine, vaccine_name="CORONAVAC"):
    '''
    
    '''
    # Define the shape of the table
    frame = pd.DataFrame(index=[["AGE SECTOR", "AGE SECTOR", "AGE SECTOR", "AGE SECTOR", 
                                "SEX", "SEX", "OUTCOME", "OUTCOME", "OUTCOME"], 
                                ["50-59 YO", "60-69 YO", "70-79 YO", "80+ YO", "M", "F", 
                                "DEATHS", "HOSPITALIZATION", "TESTS"]], 
                                columns=["FULL", "ELIGIBLE", "MATCHED"])

    
    cond1 = general_pop["VACINA(VACINADOS)"]==vaccine_name
    cond2 = pd.isna(general_pop["VACINA(VACINADOS)"])
    # FULL TOTAL
    dummy0 = general_pop[cond1 | cond2]
    dummy0_0 = dummy0.shape[0]
    # FULL age sectors
    dummy0_1 = dummy0[(dummy0["IDADE"]>=50) & (dummy0["IDADE"]<=59)].shape[0]
    dummy0_2 = dummy0[(dummy0["IDADE"]>=60) & (dummy0["IDADE"]<=69)].shape[0]
    dummy0_3 = dummy0[(dummy0["IDADE"]>=70) & (dummy0["IDADE"]<=79)].shape[0]
    dummy0_4 = dummy0[dummy0["IDADE"]>=80].shape[0]

    # ELIGIBLE
    dummy1 = pop_vaccine
    dummy1_1 = dummy1[(dummy1["IDADE"]>=50) & (dummy1["IDADE"]<=59)].shape[0]
    dummy1_2 = dummy1[(dummy1["IDADE"]>=60) & (dummy1["IDADE"]<=69)].shape[0]
    dummy1_3 = dummy1[(dummy1["IDADE"]>=70) & (dummy1["IDADE"]<=79)].shape[0]
    dummy1_4 = dummy1[dummy1["IDADE"]>=80].shape[0]

    # INCLUDED AND MATCHED
    dummy2 = pop_vaccine[pop_vaccine["PAREADO"]=="SIM"]
    dummy2_1 = dummy2[(dummy2["IDADE"]>=50) & (dummy2["IDADE"]<=59)].shape[0]
    dummy2_2 = dummy2[(dummy2["IDADE"]>=60) & (dummy2["IDADE"]<=69)].shape[0]
    dummy2_3 = dummy2[(dummy2["IDADE"]>=70) & (dummy2["IDADE"]<=79)].shape[0]
    dummy2_4 = dummy2[dummy2["IDADE"]>=80].shape[0]

    # OUTCOMES
    death_full = dummy0[pd.notna(dummy0["DATA OBITO"])].shape[0]
    death_eligible = dummy1[pd.notna(dummy1["DATA OBITO"])].shape[0]
    death_matched = dummy1[pd.notna(dummy1["DATA OBITO"])].shape[0]

    hospt_full = dummy0[pd.notna(dummy0["DATA HOSPITALIZACAO"])].shape[0]
    hospt_eligible = dummy1[pd.notna(dummy1["DATA HOSPITALIZACAO"])].shape[0]
    hospt_matched = dummy2[pd.notna(dummy2["DATA HOSPITALIZACAO"])].shape[0]

    tests_full = dummy0[pd.notna(dummy0["DATA SOLICITACAO(TESTES)"])].shape[0]
    tests_eligible = dummy1[pd.notna(dummy1["DATA SOLICITACAO(TESTES)"])].shape[0]
    tests_matched = dummy2[pd.notna(dummy2["DATA SOLICITACAO(TESTES)"])].shape[0]

    # SEX
    pop_vaccine
    m_full = dummy0[dummy0["SEXO(VACINEJA)"]=="M"].shape[0]
    f_full = dummy0[dummy0["SEXO(VACINEJA)"]=="F"].shape[0]
    m_eligible = dummy1[dummy1["SEXO(VACINEJA)"]=="M"].shape[0]
    f_eligible = dummy1[dummy1["SEXO(VACINEJA)"]=="F"].shape[0]
    m_matched = dummy2[dummy2["SEXO(VACINEJA)"]=="M"].shape[0]
    f_matched = dummy2[dummy2["SEXO(VACINEJA)"]=="F"].shape[0]

    frame["FULL"].loc["AGE SECTOR"] = [dummy0_1, dummy0_2, dummy0_3, dummy0_4]
    frame["ELIGIBLE"].loc["AGE SECTOR"] = [dummy1_1, dummy1_2, dummy1_3, dummy1_4]
    frame["MATCHED"].loc["AGE SECTOR"] = [dummy2_1, dummy2_2, dummy2_3, dummy2_4]
    frame["FULL"].loc["SEX"] = [m_full, f_full]
    frame["ELIGIBLE"].loc["SEX"] = [m_eligible, f_eligible]
    frame["MATCHED"].loc["SEX"] = [m_matched, f_matched]
    frame["FULL"].loc["OUTCOME"] = [death_full, hospt_full, tests_full]
    frame["ELIGIBLE"].loc["OUTCOME"] = [death_eligible, hospt_eligible, tests_eligible]
    frame["MATCHED"].loc["OUTCOME"] = [death_matched, hospt_matched, tests_matched]
    return frame

def create_lifetables(survival_fname, survival_corona, init_cohort, final_cohort, outcome_str="OBITO"):
    '''
        Description.

        Args:
            survival_corona:
            init_cohort:
            final_cohort:
        Return:
            lifetb:
    '''
    total_days = (final_cohort - init_cohort).days

    higher_col = [f"{outcome_str}" for i in range(8) ]
    higher_col = [""] + higher_col
    subcol = ["Unvaccinated" for i in range(4)]
    subcol += ["Vaccinated" for i in range(4)]
    subcol = [""] + subcol

    lifetb = pd.DataFrame(index=np.arange(1,222+1, 1), 
                          columns=[higher_col, subcol,
                          ["Time(days)", "Number at risk", "Number of Events", "Number Censored", 
                          "Cumulative Incidence", "Number at risk", "Number of Events",
                          "Number Censored", "Cumulative Incidence"]])

    obj_deaths = MatchedCount(survival_fname, "OBITO")
    obj_hospt = MatchedCount(survival_fname, "HOSPITALIZACAO")
    obj_tests = MatchedCount(survival_fname, "TESTE")

    KM_CASO_DEATH, KM_CONTROLE_DEATH = obj_deaths.fit_km(negative_intervals=False)
    KM_CASO_HOSPT, KM_CONTROLE_HOSPT = obj_hospt.fit_km(negative_intervals=False)
    KM_CASO_TESTS, KM_CONTROLE_TESTS = obj_tests.fit_km(negative_intervals=False)

    if outcome_str=="OBITO":
        km_caso = KM_CASO_DEATH
        km_controle = KM_CONTROLE_DEATH
    elif outcome_str=="HOSPITALIZACAO":
        km_caso = KM_CASO_HOSPT
        km_controle = KM_CONTROLE_HOSPT
    elif outcome_str=="TESTE":
        km_caso = KM_CASO_TESTS
        km_controle = KM_CONTROLE_TESTS

    nrisk_vac = []
    nrisk_unvac = []
    nevents_vac = []
    nevents_unvac = []
    ncensored_vac = []
    ncensored_unvac = []
    for t in np.arange(0, 221+1, 1):
        # Vaccinated
        cond_1 = survival_corona["TIPO"]=="CASO"
        # Unvaccinated
        cond_2 = survival_corona["TIPO"]=="CONTROLE"
        # Event or Censored at the current time
        cond_3 = survival_corona[f"{outcome_str} DURACAO"]==t
        # In risk
        cond_4 = survival_corona[f"{outcome_str} DURACAO"]>t
        # Censored
        cond_5 = survival_corona[f"COM DESFECHO - {outcome_str}"]==False
        # Event
        cond_6 = survival_corona[f"COM DESFECHO - {outcome_str}"]==True
        # vaccinated in risk
        n_risk_vac = survival_corona[cond_1 & cond_4].shape[0]
        # unvaccinated in risk
        n_risk_unvac = survival_corona[cond_2 & cond_4].shape[0]
        # Number of events - vaccinated
        n_events_vac = survival_corona[cond_1 & cond_3 & cond_6].shape[0]
        # Number of events - unvaccinated
        n_events_unvac = survival_corona[cond_2 & cond_3 & cond_6].shape[0]
        # Number of censored - vaccinated
        n_censored_vac = survival_corona[cond_1 & cond_3 & cond_5].shape[0]
        # Number of censored - unvaccinated
        n_censored_unvac = survival_corona[cond_2 & cond_3 & cond_5].shape[0]

        # Fill
        nrisk_vac.append(n_risk_vac)
        nrisk_unvac.append(n_risk_unvac)
        nevents_vac.append(n_events_vac)
        nevents_unvac.append(n_events_unvac)
        ncensored_vac.append(n_censored_vac)
        ncensored_unvac.append(n_censored_unvac)

    lifetb["", "", "Time(days)"] = list(np.arange(0,221+1,1))
    lifetb[f"{outcome_str}", "Unvaccinated", "Number at risk"] = nrisk_unvac
    lifetb[f"{outcome_str}", "Vaccinated", "Number at risk"] = nrisk_vac
    lifetb[f"{outcome_str}", "Unvaccinated", "Number of Events"] = nevents_unvac
    lifetb[f"{outcome_str}", "Vaccinated", "Number of Events"] = nevents_vac
    lifetb[f"{outcome_str}", "Unvaccinated", "Number Censored"] = ncensored_unvac
    lifetb[f"{outcome_str}", "Vaccinated", "Number Censored"] = ncensored_vac
    lifetb[f"{outcome_str}", "Unvaccinated", "Cumulative Incidence"] = km_controle["KM_estimate"]
    lifetb[f"{outcome_str}", "Vaccinated", "Cumulative Incidence"] = km_caso["KM_estimate"]
    return lifetb

def time_window_plot(pop_vaccine, title_str, vac_str="CoronaVac", mode="OBITO"):
    '''
        Description.


    '''
    cols = ["DATA OBITO", "DATA SINTOMAS", "DATA HOSPITALIZACAO", "DATA SINTOMAS (HOSPITALIZACAO)"]
    for j in cols:
        pop_vaccine[j] = pd.to_datetime(pop_vaccine[j], format="%Y-%m-%d", errors="coerce")

    sbst = ["DATA OBITO", "DATA SINTOMAS"]
    pop_vaccine["TW DEATH"] = pop_vaccine[sbst].apply(lambda x: AUX_tw_death(x), axis=1)
    sbst = ["DATA HOSPITALIZACAO", "DATA SINTOMAS (HOSPITALIZACAO)"]
    pop_vaccine["TW HOSPITALIZATION"] = pop_vaccine[sbst].apply(lambda x: AUX_tw_hospt(x), axis=1)

    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(10,4), sharey=True)

    # TW dist for ALL ELIGIBLE
    twdeath_dist_eligible = pop_vaccine["TW DEATH"][pd.notna(pop_vaccine["TW DEATH"]) & (pop_vaccine["TW DEATH"]>=0)]
    # TW dist for MATCHED
    twdeath_dist_matched = pop_vaccine["TW DEATH"][(pd.notna(pop_vaccine["TW DEATH"])) & (pop_vaccine["PAREADO"]=="SIM") & (pop_vaccine["TW DEATH"]>=0)]

    # TW dist for ALL ELIGIBLE
    twhospt_dist_eligible = pop_vaccine["TW HOSPITALIZATION"][pd.notna(pop_vaccine["TW HOSPITALIZATION"]) & (pop_vaccine["TW HOSPITALIZATION"]>=0)]
    twhospt_dist_eligible = twhospt_dist_eligible[twhospt_dist_eligible<100]
    # TW dist for MATCHED
    twhospt_dist_matched = pop_vaccine["TW HOSPITALIZATION"][(pd.notna(pop_vaccine["TW HOSPITALIZATION"])) & (pop_vaccine["PAREADO"]=="SIM") & (pop_vaccine["TW HOSPITALIZATION"]>=0)]
    twhospt_dist_matched = twhospt_dist_matched[twhospt_dist_matched<100]

    if mode=="OBITO":
        df_eligible = twdeath_dist_eligible
        df_included = twdeath_dist_matched
        nbins="auto"
    else:
        df_eligible = twhospt_dist_eligible
        df_included = twhospt_dist_matched
        nbins=40
    median1 = df_eligible.median()
    median2 = df_included.median()
    
    sns.histplot(df_eligible, ax=ax1, bins=nbins)
    sns.histplot(df_included, ax=ax2, bins=nbins)

    ax1.grid(alpha=0.2)
    ax2.grid(alpha=0.2)
    ax1.axvline(median1, color="red", ls="--")
    ax2.axvline(median2, color="red", ls="--")

    ax1.set_xlabel("")
    ax2.set_xlabel("")
    ax1.set_ylabel("Frequency", fontsize=13)
    fig.supxlabel("Time window (days) from first symptoms to death by Covid-19")
    #ax2.set_xlabel("Time window (days) from first symptoms to death by Covid-19")
    ax1.set_title(f"ALL ELIGIBLE - {vac_str}")
    ax2.set_title(f"MATCHED - {vac_str}")
    fig.suptitle(f"{title_str}")
    return fig, (ax1, ax2)

def AUX_tw_death(x):
    dt_death = x["DATA OBITO"]
    dt_prisint = x["DATA SINTOMAS"]
    if pd.isna(dt_death) or pd.isna(dt_prisint):
        return np.nan
    else:
        return (dt_death.date() - dt_prisint.date()).days

def AUX_tw_hospt(x):
    dt_death = x["DATA HOSPITALIZACAO"]
    dt_prisint = x["DATA SINTOMAS (HOSPITALIZACAO)"]
    if pd.isna(dt_death) or pd.isna(dt_prisint):
        return np.nan
    else:
        return (dt_death.date() - dt_prisint.date()).days

def survival_effectiveness_period(file_death, file_hospt):
    '''
    
    '''
    df = pd.DataFrame(index=["D1:0-20d+", "D1:0-27d+", "D1:14-20d+", "D1:14d+", 
                            "D1:20-27d+", "D1:20d+","D2: 0-14d+", "D2: 14-20d+", "D2: 14d+"], 
                            columns=[["VE = 1-RR", "VE = 1-RR", "VE = 1-RR", "VE = 1-RR"],
                                      ["Death by Covid-19", "0.95-CI(+-) Death", 
                                      "Hospitalization due to Covid-19", "0.95-CI(+-) Hospitalization"]])

    obj_deaths = MatchedCount(file_death, "OBITO")
    obj_hospt = MatchedCount(file_hospt, "HOSPITALIZACAO")

    # --> Prepare all the cumulative incidence curves
    KM_CASO_DEATH_0, KM_CONTROLE_DEATH_0 = obj_deaths.fit_km_period(init_period=0, negative_intervals=False)
    KM_CASO_HOSPT_0, KM_CONTROLE_HOSPT_0 = obj_hospt.fit_km_period(init_period=0, negative_intervals=False)
    KM_CASO_DEATH_14, KM_CONTROLE_DEATH_14 = obj_deaths.fit_km_period(init_period=14, negative_intervals=False)
    KM_CASO_HOSPT_14, KM_CONTROLE_HOSPT_14 = obj_hospt.fit_km_period(init_period=14, negative_intervals=False)
    KM_CASO_DEATH_20, KM_CONTROLE_DEATH_20 = obj_deaths.fit_km_period(init_period=20, negative_intervals=False)
    KM_CASO_HOSPT_20, KM_CONTROLE_HOSPT_20 = obj_hospt.fit_km_period(init_period=20, negative_intervals=False)
    KM_CASO_DEATH2_0, KM_CONTROLE_DEATH2_0 = obj_deaths.second_dose_km(init_period=0, negative_intervals=False)
    KM_CASO_HOSPT2_0, KM_CONTROLE_HOSPT2_0 = obj_hospt.second_dose_km(init_period=0, negative_intervals=False)
    KM_CASO_DEATH2_14, KM_CONTROLE_DEATH2_14 = obj_deaths.second_dose_km(init_period=14, negative_intervals=False)
    KM_CASO_HOSPT2_14, KM_CONTROLE_HOSPT2_14 = obj_hospt.second_dose_km(init_period=14, negative_intervals=False)

    # DEATH
    ve_0_20 = 1 - (KM_CASO_DEATH_0["KM_estimate"].loc[20]/KM_CONTROLE_DEATH_0["KM_estimate"].loc[20])
    ve_0_27 = 1 - (KM_CASO_DEATH_0["KM_estimate"].loc[27]/KM_CONTROLE_DEATH_0["KM_estimate"].loc[27])
    ve_14_20 = 1 - (KM_CASO_DEATH_14["KM_estimate"].loc[20]/KM_CONTROLE_DEATH_14["KM_estimate"].loc[20])
    ve_14_plus = 1 - (KM_CASO_DEATH_14["KM_estimate"].loc[60]/KM_CONTROLE_DEATH_14["KM_estimate"].loc[60])
    ve_20_27 = 1 - (KM_CASO_DEATH_20["KM_estimate"].loc[27]/KM_CONTROLE_DEATH_20["KM_estimate"].loc[27])
    ve_20_plus = 1 - (KM_CASO_DEATH_20["KM_estimate"].loc[60]/KM_CONTROLE_DEATH_20["KM_estimate"].loc[60])
    ve2_0_14 = 1 - (KM_CASO_DEATH2_0["KM_estimate"].loc[14]/KM_CONTROLE_DEATH2_0["KM_estimate"].loc[14])
    ve2_14_20 = 1 - (KM_CASO_DEATH2_14["KM_estimate"].loc[20]/KM_CONTROLE_DEATH2_14["KM_estimate"].loc[20])
    ve2_14_plus = 1 - (KM_CASO_DEATH2_14["KM_estimate"].loc[60]/KM_CONTROLE_DEATH2_14["KM_estimate"].loc[60])
    # 95% CI
    ci_0_20 = 1.96*np.sqrt(KM_CASO_DEATH_0["Factor for CI of RR"].loc[20] + KM_CONTROLE_DEATH_0["Factor for CI of RR"].loc[20])
    ci_0_27 = 1.96*np.sqrt(KM_CASO_DEATH_0["Factor for CI of RR"].loc[27] + KM_CONTROLE_DEATH_0["Factor for CI of RR"].loc[27])
    ci_14_20 = 1.96*np.sqrt(KM_CASO_DEATH_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_DEATH_14["Factor for CI of RR"].loc[20])
    ci_14_plus = 1.96*np.sqrt(KM_CASO_DEATH_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_DEATH_14["Factor for CI of RR"].loc[60])
    ci_20_27 = 1.96*np.sqrt(KM_CASO_DEATH_20["Factor for CI of RR"].loc[27] + KM_CONTROLE_DEATH_20["Factor for CI of RR"].loc[27])
    ci_20_plus = 1.96*np.sqrt(KM_CASO_DEATH_20["Factor for CI of RR"].loc[60] + KM_CONTROLE_DEATH_20["Factor for CI of RR"].loc[60])
    ci2_0_14 = 1.96*np.sqrt(KM_CASO_DEATH2_0["Factor for CI of RR"].loc[14] + KM_CONTROLE_DEATH2_0["Factor for CI of RR"].loc[14])
    ci2_14_20 = 1.96*np.sqrt(KM_CASO_DEATH2_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_DEATH2_14["Factor for CI of RR"].loc[20])
    ci2_14_plus = 1.96*np.sqrt(KM_CASO_DEATH2_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_DEATH2_14["Factor for CI of RR"].loc[60])
    df["VE = 1-RR", "Death by Covid-19"] = [ve_0_20, ve_0_27, ve_14_20, ve_14_plus, ve_20_27, ve_20_plus, ve2_0_14, ve2_14_20, ve2_14_plus]
    df["VE = 1-RR", "0.95-CI(+-) Death"] = [ci_0_20, ci_0_27, ci_14_20, ci_14_plus, ci_20_27, ci_20_plus, ci2_0_14, ci2_14_20, ci2_14_plus]
    # HOSPITALIZATION
    ve_0_20 = 1 - (KM_CASO_HOSPT_0["KM_estimate"].loc[20]/KM_CONTROLE_HOSPT_0["KM_estimate"].loc[20])
    ve_0_27 = 1 - (KM_CASO_HOSPT_0["KM_estimate"].loc[27]/KM_CONTROLE_HOSPT_0["KM_estimate"].loc[27])
    ve_14_20 = 1 - (KM_CASO_HOSPT_14["KM_estimate"].loc[20]/KM_CONTROLE_HOSPT_14["KM_estimate"].loc[20])
    ve_14_plus = 1 - (KM_CASO_HOSPT_14["KM_estimate"].loc[60]/KM_CONTROLE_HOSPT_14["KM_estimate"].loc[60])
    ve_20_27 = 1 - (KM_CASO_HOSPT_20["KM_estimate"].loc[27]/KM_CONTROLE_HOSPT_20["KM_estimate"].loc[27])
    ve_20_plus = 1 - (KM_CASO_HOSPT_20["KM_estimate"].loc[60]/KM_CONTROLE_HOSPT_20["KM_estimate"].loc[60])
    ve2_0_14 = 1 - (KM_CASO_HOSPT2_0["KM_estimate"].loc[14]/KM_CONTROLE_HOSPT2_0["KM_estimate"].loc[14])
    ve2_14_20 = 1 - (KM_CASO_HOSPT2_14["KM_estimate"].loc[20]/KM_CONTROLE_HOSPT2_14["KM_estimate"].loc[20])
    ve2_14_plus = 1 - (KM_CASO_HOSPT2_14["KM_estimate"].loc[60]/KM_CONTROLE_HOSPT2_14["KM_estimate"].loc[60])
    # 95% CI
    ci_0_20 = 1.96*np.sqrt(KM_CASO_HOSPT_0["Factor for CI of RR"].loc[20] + KM_CONTROLE_HOSPT_0["Factor for CI of RR"].loc[20])
    ci_0_27 = 1.96*np.sqrt(KM_CASO_HOSPT_0["Factor for CI of RR"].loc[27] + KM_CONTROLE_HOSPT_0["Factor for CI of RR"].loc[27])
    ci_14_20 = 1.96*np.sqrt(KM_CASO_HOSPT_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_HOSPT_14["Factor for CI of RR"].loc[20])
    ci_14_plus = 1.96*np.sqrt(KM_CASO_HOSPT_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_HOSPT_14["Factor for CI of RR"].loc[60])
    ci_20_27 = 1.96*np.sqrt(KM_CASO_HOSPT_20["Factor for CI of RR"].loc[27] + KM_CONTROLE_HOSPT_20["Factor for CI of RR"].loc[27])
    ci_20_plus = 1.96*np.sqrt(KM_CASO_HOSPT_20["Factor for CI of RR"].loc[60] + KM_CONTROLE_HOSPT_20["Factor for CI of RR"].loc[60])
    ci2_0_14 = 1.96*np.sqrt(KM_CASO_HOSPT2_0["Factor for CI of RR"].loc[14] + KM_CONTROLE_HOSPT2_0["Factor for CI of RR"].loc[14])
    ci2_14_20 = 1.96*np.sqrt(KM_CASO_HOSPT2_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_HOSPT2_14["Factor for CI of RR"].loc[20])
    ci2_14_plus = 1.96*np.sqrt(KM_CASO_HOSPT2_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_HOSPT2_14["Factor for CI of RR"].loc[60])
    df["VE = 1-RR", "Hospitalization due to Covid-19"] = [ve_0_20, ve_0_27, ve_14_20, ve_14_plus, ve_20_27, ve_20_plus, ve2_0_14, ve2_14_20, ve2_14_plus]
    df["VE = 1-RR", "0.95-CI(+-) Hospitalization"] = [ci_0_20, ci_0_27, ci_14_20, ci_14_plus, ci_20_27, ci_20_plus, ci2_0_14, ci2_14_20, ci2_14_plus]
    
    sbst = ["Death by Covid-19", "0.95-CI(+-) Death"]
    df["VE = 1-RR", "CI(upper, lower) - Death"] = df["VE = 1-RR"][sbst].apply(lambda x: calc_ci(x[sbst[0]], x[sbst[1]]), axis=1)
    sbst = ["Hospitalization due to Covid-19", "0.95-CI(+-) Hospitalization"]
    df["VE = 1-RR", "CI(upper, lower) - Hospitalization"] = df["VE = 1-RR"][sbst].apply(lambda x: calc_ci(x[sbst[0]], x[sbst[1]]), axis=1)
    # remove aux columns of 'df'
    df = df.drop("0.95-CI(+-) Death", level=1, axis=1)
    df = df.drop("0.95-CI(+-) Hospitalization", level=1, axis=1)
    return df

def survival_effectiveness_strat(pop_vac, file_death, file_hospt):
    '''
    
    '''
    df = pd.DataFrame(index=["D1: Age 50-59", "D1: Age 60-69", "D1: Age 70-79", "D1: Age 80+", 
                            "D2: Age 50-59", "D2: Age 60-69","D2: Age 70-79", "D2: Age 80+",
                            "D1: Only Males", "D2: Only Males", "D1: Only Females", "D2: Only Females"], 
                      columns=[["VE = 1-RR", "VE = 1-RR", "VE = 1-RR", "VE = 1-RR"],
                               ["Death by Covid-19", "0.95-CI(+-) Death", 
                                "Hospitalization due to Covid-19", "0.95-CI(+-) Hospitalization"]])

    obj_deaths = MatchedCount(file_death, "OBITO")
    obj_hospt = MatchedCount(file_hospt, "HOSPITALIZACAO")

    # --> Prepare all the cumulative incidence curves
    res_death = obj_deaths.sex_km(pop_vac, negative_intervals=False)
    res_hospt = obj_hospt.sex_km(pop_vac, negative_intervals=False)
    res_death_2nd = obj_deaths.sex_km(pop_vac, second_dose_filter=True, negative_intervals=False)
    res_hospt_2nd = obj_hospt.sex_km(pop_vac, second_dose_filter=True, negative_intervals=False)

    # Only males and only females - D1(t=0) and D2(t=0)
    KM_CASO_DEATH_M, KM_CONTROLE_DEATH_M = res_death["M"]
    KM_CASO_DEATH_F, KM_CONTROLE_DEATH_F = res_death["F"]
    KM_CASO_HOSPT_M, KM_CONTROLE_HOSPT_M = res_hospt["M"]
    KM_CASO_HOSPT_F, KM_CONTROLE_HOSPT_F = res_hospt["F"]
    KM_CASO_DEATH_M2ND, KM_CONTROLE_DEATH_M2ND = res_death_2nd["M"]
    KM_CASO_DEATH_F2ND, KM_CONTROLE_DEATH_F2ND = res_death_2nd["F"]
    KM_CASO_HOSPT_M2ND, KM_CONTROLE_HOSPT_M2ND = res_hospt_2nd["M"]
    KM_CASO_HOSPT_F2ND, KM_CONTROLE_HOSPT_F2ND = res_hospt_2nd["F"]
    # Stratified by AGE
    obj_deaths = MatchedCount(file_death, "OBITO")
    obj_hospt = MatchedCount(file_hospt, "HOSPITALIZACAO")
    # D1 as t0
    KM_CASO_DEATH_5059, KM_CONTROLE_DEATH_5059 = obj_deaths.age_km(pop_vac, [50,59], negative_intervals=False)
    KM_CASO_DEATH_6069, KM_CONTROLE_DEATH_6069 = obj_deaths.age_km(pop_vac, [60,69], negative_intervals=False)
    KM_CASO_DEATH_7079, KM_CONTROLE_DEATH_7079 = obj_deaths.age_km(pop_vac, [70,79], negative_intervals=False)
    KM_CASO_DEATH_80, KM_CONTROLE_DEATH_80 = obj_deaths.age_km(pop_vac, [80,200],  negative_intervals=False)
    KM_CASO_HOSPT_5059, KM_CONTROLE_HOSPT_5059 = obj_hospt.age_km(pop_vac, [50,59], negative_intervals=False)
    KM_CASO_HOSPT_6069, KM_CONTROLE_HOSPT_6069 = obj_hospt.age_km(pop_vac, [60,69], negative_intervals=False)
    KM_CASO_HOSPT_7079, KM_CONTROLE_HOSPT_7079 = obj_hospt.age_km(pop_vac, [70,79],  negative_intervals=False)
    KM_CASO_HOSPT_80, KM_CONTROLE_HOSPT_80 = obj_hospt.age_km(pop_vac, [80,200],  negative_intervals=False)
    # D2 as t0
    KM_CASO_DEATH_50592ND, KM_CONTROLE_DEATH_50592ND = obj_deaths.age_km(pop_vac, [50,59], second_dose_filter=True, negative_intervals=False)
    KM_CASO_DEATH_60692ND, KM_CONTROLE_DEATH_60692ND = obj_deaths.age_km(pop_vac, [60,69], second_dose_filter=True, negative_intervals=False)
    KM_CASO_DEATH_70792ND, KM_CONTROLE_DEATH_70792ND = obj_deaths.age_km(pop_vac, [70,79],  second_dose_filter=True, negative_intervals=False)
    KM_CASO_DEATH_802ND, KM_CONTROLE_DEATH_802ND = obj_deaths.age_km(pop_vac, [80,200],  second_dose_filter=True, negative_intervals=False)
    KM_CASO_HOSPT_50592ND, KM_CONTROLE_HOSPT_50592ND = obj_hospt.age_km(pop_vac, [50,59], second_dose_filter=True, negative_intervals=False)
    KM_CASO_HOSPT_60692ND, KM_CONTROLE_HOSPT_60692ND = obj_hospt.age_km(pop_vac, [60,69], second_dose_filter=True, negative_intervals=False)
    KM_CASO_HOSPT_70792ND, KM_CONTROLE_HOSPT_70792ND = obj_hospt.age_km(pop_vac, [70,79],  second_dose_filter=True, negative_intervals=False)
    KM_CASO_HOSPT_802ND, KM_CONTROLE_HOSPT_802ND = obj_hospt.age_km(pop_vac, [80,200],  second_dose_filter=True, negative_intervals=False)
    

def ve_periods(pop_vac, file_death, file_hospt, option="50-59"):
    '''
    
    '''
    df = pd.DataFrame(index=["D1:0-20d+", "D1:0-27d+", "D1:14-20d+", "D1:14d+", 
                            "D1:20-27d+", "D1:20d+","D2: 0-14d+", "D2: 14-20d+", "D2: 14d+"], 
                      columns=[["VE = 1-RR", "VE = 1-RR", "VE = 1-RR", "VE = 1-RR"],
                              ["Death by Covid-19", "0.95-CI(+-) Death", 
                               "Hospitalization due to Covid-19", "0.95-CI(+-) Hospitalization"]])

    age_range = None
    if option not in ["MALE", "FEMALE"]:
        age_range = [int(option.split("-")[0]), int(option.split("-")[1])]
        print(age_range)
    
    obj_deaths = MatchedCount(file_death, "OBITO")
    obj_hospt = MatchedCount(file_hospt, "HOSPITALIZACAO")
    if age_range is not None:
        KM_CASO_DEATH_AGE_0, KM_CONTROLE_DEATH_AGE_0 = obj_deaths.age_km(pop_vac, age_interval=age_range, init_period=0, negative_intervals=False)
        KM_CASO_HOSPT_AGE_0, KM_CONTROLE_HOSPT_AGE_0 = obj_hospt.age_km(pop_vac, age_interval=age_range, init_period=0, negative_intervals=False)
        KM_CASO_DEATH_AGE_14, KM_CONTROLE_DEATH_AGE_14 = obj_deaths.age_km(pop_vac, age_interval=age_range, init_period=14, negative_intervals=False)
        KM_CASO_HOSPT_AGE_14, KM_CONTROLE_HOSPT_AGE_14 = obj_hospt.age_km(pop_vac, age_interval=age_range, init_period=14, negative_intervals=False)
        KM_CASO_DEATH_AGE_20, KM_CONTROLE_DEATH_AGE_20 = obj_deaths.age_km(pop_vac, age_interval=age_range, init_period=20, negative_intervals=False)
        KM_CASO_HOSPT_AGE_20, KM_CONTROLE_HOSPT_AGE_20 = obj_hospt.age_km(pop_vac, age_interval=age_range, init_period=20, negative_intervals=False)
        KM_CASO_DEATH2_AGE_0, KM_CONTROLE_DEATH2_AGE_0 = obj_deaths.age_km(pop_vac, age_interval=age_range, init_period=0, negative_intervals=False, second_dose_filter=True)
        KM_CASO_HOSPT2_AGE_0, KM_CONTROLE_HOSPT2_AGE_0 = obj_hospt.age_km(pop_vac, age_interval=age_range, init_period=0, negative_intervals=False, second_dose_filter=True)
        KM_CASO_DEATH2_AGE_14, KM_CONTROLE_DEATH2_AGE_14 = obj_deaths.age_km(pop_vac, age_interval=age_range, init_period=14, negative_intervals=False, second_dose_filter=True)
        KM_CASO_HOSPT2_AGE_14, KM_CONTROLE_HOSPT2_AGE_14 = obj_hospt.age_km(pop_vac, age_interval=age_range, init_period=14, negative_intervals=False, second_dose_filter=True)
    else:
        res0_death = obj_deaths.sex_km(pop_vac, init_period=0, negative_intervals=False)
        res0_hospt = obj_hospt.sex_km(pop_vac, init_period=0, negative_intervals=False)
        res14_death = obj_deaths.sex_km(pop_vac, init_period=14, negative_intervals=False)
        res14_hospt = obj_hospt.sex_km(pop_vac, init_period=14, negative_intervals=False)
        res20_death = obj_deaths.sex_km(pop_vac, init_period=20, negative_intervals=False)
        res20_hospt = obj_hospt.sex_km(pop_vac, init_period=20, negative_intervals=False)
        res0_death2 = obj_deaths.sex_km(pop_vac, init_period=0, negative_intervals=False, second_dose_filter=True)
        res0_hospt2 = obj_hospt.sex_km(pop_vac, init_period=0, negative_intervals=False, second_dose_filter=True)
        res14_death2 = obj_deaths.sex_km(pop_vac, init_period=14, negative_intervals=False, second_dose_filter=True)
        res14_hospt2 = obj_hospt.sex_km(pop_vac, init_period=14, negative_intervals=False, second_dose_filter=True)
        if option=="MALE":
            s = "M"
            KM_CASO_DEATH_AGE_0, KM_CONTROLE_DEATH_AGE_0 = res0_death[s]
            KM_CASO_HOSPT_AGE_0, KM_CONTROLE_HOSPT_AGE_0 = res0_hospt[s]
            KM_CASO_DEATH_AGE_14, KM_CONTROLE_DEATH_AGE_14 = res14_death[s]
            KM_CASO_HOSPT_AGE_14, KM_CONTROLE_HOSPT_AGE_14 = res14_hospt[s]
            KM_CASO_DEATH_AGE_20, KM_CONTROLE_DEATH_AGE_20 = res20_death[s]
            KM_CASO_HOSPT_AGE_20, KM_CONTROLE_HOSPT_AGE_20 = res20_hospt[s]
            KM_CASO_DEATH2_AGE_0, KM_CONTROLE_DEATH2_AGE_0 = res0_death2[s]
            KM_CASO_HOSPT2_AGE_0, KM_CONTROLE_HOSPT2_AGE_0 = res0_hospt2[s]
            KM_CASO_DEATH2_AGE_14, KM_CONTROLE_DEATH2_AGE_14 = res14_death2[s]
            KM_CASO_HOSPT2_AGE_14, KM_CONTROLE_HOSPT2_AGE_14 = res14_hospt2[s]
        elif option=="FEMALE":
            s = "F"
            KM_CASO_DEATH_AGE_0, KM_CONTROLE_DEATH_AGE_0 = res0_death[s]
            KM_CASO_HOSPT_AGE_0, KM_CONTROLE_HOSPT_AGE_0 = res0_hospt[s]
            KM_CASO_DEATH_AGE_14, KM_CONTROLE_DEATH_AGE_14 = res14_death[s]
            KM_CASO_HOSPT_AGE_14, KM_CONTROLE_HOSPT_AGE_14 = res14_hospt[s]
            KM_CASO_DEATH_AGE_20, KM_CONTROLE_DEATH_AGE_20 = res20_death[s]
            KM_CASO_HOSPT_AGE_20, KM_CONTROLE_HOSPT_AGE_20 = res20_hospt[s]
            KM_CASO_DEATH2_AGE_0, KM_CONTROLE_DEATH2_AGE_0 = res0_death2[s]
            KM_CASO_HOSPT2_AGE_0, KM_CONTROLE_HOSPT2_AGE_0 = res0_hospt2[s]
            KM_CASO_DEATH2_AGE_14, KM_CONTROLE_DEATH2_AGE_14 = res14_death2[s]
            KM_CASO_HOSPT2_AGE_14, KM_CONTROLE_HOSPT2_AGE_14 = res14_hospt2[s]
        else:
            return -1
    # DEATH
    try: ve_0_20 = 1 - (KM_CASO_DEATH_AGE_0["KM_estimate"].loc[20]/KM_CONTROLE_DEATH_AGE_0["KM_estimate"].loc[20]) 
    except: ve_0_20 = np.nan
    try: ve_0_27 = 1 - (KM_CASO_DEATH_AGE_0["KM_estimate"].loc[27]/KM_CONTROLE_DEATH_AGE_0["KM_estimate"].loc[27]) 
    except: ve_0_27 = np.nan
    try: ve_14_20 = 1 - (KM_CASO_DEATH_AGE_14["KM_estimate"].loc[20]/KM_CONTROLE_DEATH_AGE_14["KM_estimate"].loc[20]) 
    except: ve_14_20 = np.nan
    try: ve_14_plus = 1 - (KM_CASO_DEATH_AGE_14["KM_estimate"].loc[60]/KM_CONTROLE_DEATH_AGE_14["KM_estimate"].loc[60]) 
    except: ve_14_plus = np.nan
    try: ve_20_27 = 1 - (KM_CASO_DEATH_AGE_20["KM_estimate"].loc[27]/KM_CONTROLE_DEATH_AGE_20["KM_estimate"].loc[27]) 
    except: ve_20_27 = np.nan
    try: ve_20_plus = 1 - (KM_CASO_DEATH_AGE_20["KM_estimate"].loc[60]/KM_CONTROLE_DEATH_AGE_20["KM_estimate"].loc[60]) 
    except: ve_20_plus = np.nan
    try: ve2_0_14 = 1 - (KM_CASO_DEATH2_AGE_0["KM_estimate"].loc[14]/KM_CONTROLE_DEATH2_AGE_0["KM_estimate"].loc[14]) 
    except: ve2_0_14 = np.nan
    try: ve2_14_20 = 1 - (KM_CASO_DEATH2_AGE_14["KM_estimate"].loc[20]/KM_CONTROLE_DEATH2_AGE_14["KM_estimate"].loc[20]) 
    except: ve2_14_20 = np.nan
    try: ve2_14_plus = 1 - (KM_CASO_DEATH2_AGE_14["KM_estimate"].loc[60]/KM_CONTROLE_DEATH2_AGE_14["KM_estimate"].loc[60]) 
    except: ve2_14_plus = np.nan
    # 95% CI
    try: ci_0_20 = 1.96*np.sqrt(KM_CASO_DEATH_AGE_0["Factor for CI of RR"].loc[20] + KM_CONTROLE_DEATH_AGE_0["Factor for CI of RR"].loc[20]) 
    except: ci_0_20 = np.nan
    try: ci_0_27 = 1.96*np.sqrt(KM_CASO_DEATH_AGE_0["Factor for CI of RR"].loc[27] + KM_CONTROLE_DEATH_AGE_0["Factor for CI of RR"].loc[27]) 
    except: ci_0_27 = np.nan
    try: ci_14_20 = 1.96*np.sqrt(KM_CASO_DEATH_AGE_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_DEATH_AGE_14["Factor for CI of RR"].loc[20]) 
    except: ci_14_20 = np.nan
    try: ci_14_plus = 1.96*np.sqrt(KM_CASO_DEATH_AGE_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_DEATH_AGE_14["Factor for CI of RR"].loc[60]) 
    except: ci_14_plus = np.nan
    try: ci_20_27 = 1.96*np.sqrt(KM_CASO_DEATH_AGE_20["Factor for CI of RR"].loc[27] + KM_CONTROLE_DEATH_AGE_20["Factor for CI of RR"].loc[27]) 
    except: ci_20_27 = np.nan
    try: ci_20_plus = 1.96*np.sqrt(KM_CASO_DEATH_AGE_20["Factor for CI of RR"].loc[60] + KM_CONTROLE_DEATH_AGE_20["Factor for CI of RR"].loc[60]) 
    except: ci_20_plus = np.nan
    try: ci2_0_14 = 1.96*np.sqrt(KM_CASO_DEATH2_AGE_0["Factor for CI of RR"].loc[14] + KM_CONTROLE_DEATH2_AGE_0["Factor for CI of RR"].loc[14]) 
    except: ci2_0_14 = np.nan
    try: ci2_14_20 = 1.96*np.sqrt(KM_CASO_DEATH2_AGE_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_DEATH2_AGE_14["Factor for CI of RR"].loc[20]) 
    except: ci2_14_20 = np.nan
    try: ci2_14_plus = 1.96*np.sqrt(KM_CASO_DEATH2_AGE_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_DEATH2_AGE_14["Factor for CI of RR"].loc[60]) 
    except: ci2_14_plus = np.nan
    df["VE = 1-RR", "Death by Covid-19"] = [ve_0_20, ve_0_27, ve_14_20, ve_14_plus, ve_20_27, ve_20_plus, ve2_0_14, ve2_14_20, ve2_14_plus]
    df["VE = 1-RR", "0.95-CI(+-) Death"] = [ci_0_20, ci_0_27, ci_14_20, ci_14_plus, ci_20_27, ci_20_plus, ci2_0_14, ci2_14_20, ci2_14_plus]
    # HOSPITALIZATION
    try: ve_0_20 = 1 - (KM_CASO_HOSPT_AGE_0["KM_estimate"].loc[20]/KM_CONTROLE_HOSPT_AGE_0["KM_estimate"].loc[20]) 
    except: ve_0_20 = np.nan
    try: ve_0_27 = 1 - (KM_CASO_HOSPT_AGE_0["KM_estimate"].loc[27]/KM_CONTROLE_HOSPT_AGE_0["KM_estimate"].loc[27]) 
    except: ve_0_27 = np.nan
    try: ve_14_20 = 1 - (KM_CASO_HOSPT_AGE_14["KM_estimate"].loc[20]/KM_CONTROLE_HOSPT_AGE_14["KM_estimate"].loc[20]) 
    except: ve_14_20 = np.nan
    try: ve_14_plus = 1 - (KM_CASO_HOSPT_AGE_14["KM_estimate"].loc[60]/KM_CONTROLE_HOSPT_AGE_14["KM_estimate"].loc[60]) 
    except: ve_14_plus = np.nan
    try: ve_20_27 = 1 - (KM_CASO_HOSPT_AGE_20["KM_estimate"].loc[27]/KM_CONTROLE_HOSPT_AGE_20["KM_estimate"].loc[27]) 
    except: ve_20_27 = np.nan
    try: ve_20_plus = 1 - (KM_CASO_HOSPT_AGE_20["KM_estimate"].loc[60]/KM_CONTROLE_HOSPT_AGE_20["KM_estimate"].loc[60]) 
    except: ve_20_plus = np.nan
    try: ve2_0_14 = 1 - (KM_CASO_HOSPT2_AGE_0["KM_estimate"].loc[14]/KM_CONTROLE_HOSPT2_AGE_0["KM_estimate"].loc[14]) 
    except: ve2_0_14 = np.nan
    try: ve2_14_20 = 1 - (KM_CASO_HOSPT2_AGE_14["KM_estimate"].loc[20]/KM_CONTROLE_HOSPT2_AGE_14["KM_estimate"].loc[20]) 
    except: ve2_14_20 = np.nan
    try: ve2_14_plus = 1 - (KM_CASO_HOSPT2_AGE_14["KM_estimate"].loc[60]/KM_CONTROLE_HOSPT2_AGE_14["KM_estimate"].loc[60]) 
    except: ve2_14_plus = np.nan
    # 95% CI
    try: ci_0_20 = 1.96*np.sqrt(KM_CASO_HOSPT_AGE_0["Factor for CI of RR"].loc[20] + KM_CONTROLE_HOSPT_AGE_0["Factor for CI of RR"].loc[20])
    except: ci_0_20 = np.nan
    try: ci_0_27 = 1.96*np.sqrt(KM_CASO_HOSPT_AGE_0["Factor for CI of RR"].loc[27] + KM_CONTROLE_HOSPT_AGE_0["Factor for CI of RR"].loc[27])
    except: ci_0_27 = np.nan
    try: ci_14_20 = 1.96*np.sqrt(KM_CASO_HOSPT_AGE_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_HOSPT_AGE_14["Factor for CI of RR"].loc[20])
    except: ci_14_20 = np.nan
    try: ci_14_plus = 1.96*np.sqrt(KM_CASO_HOSPT_AGE_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_HOSPT_AGE_14["Factor for CI of RR"].loc[60])
    except: ci_14_plus = np.nan
    try: ci_20_27 = 1.96*np.sqrt(KM_CASO_HOSPT_AGE_20["Factor for CI of RR"].loc[27] + KM_CONTROLE_HOSPT_AGE_20["Factor for CI of RR"].loc[27])
    except: ci_20_27 = np.nan
    try: ci_20_plus = 1.96*np.sqrt(KM_CASO_HOSPT_AGE_20["Factor for CI of RR"].loc[60] + KM_CONTROLE_HOSPT_AGE_20["Factor for CI of RR"].loc[60])
    except: ci_20_plus = np.nan
    try: ci2_0_14 = 1.96*np.sqrt(KM_CASO_HOSPT2_AGE_0["Factor for CI of RR"].loc[14] + KM_CONTROLE_HOSPT2_AGE_0["Factor for CI of RR"].loc[14])
    except: ci2_0_14 = np.nan
    try: ci2_14_20 = 1.96*np.sqrt(KM_CASO_HOSPT2_AGE_14["Factor for CI of RR"].loc[20] + KM_CONTROLE_HOSPT2_AGE_14["Factor for CI of RR"].loc[20])
    except: ci2_14_20 = np.nan
    try: ci2_14_plus = 1.96*np.sqrt(KM_CASO_HOSPT2_AGE_14["Factor for CI of RR"].loc[60] + KM_CONTROLE_HOSPT2_AGE_14["Factor for CI of RR"].loc[60])
    except: ci2_14_plus = np.nan
    df["VE = 1-RR", "Hospitalization due to Covid-19"] = [ve_0_20, ve_0_27, ve_14_20, ve_14_plus, ve_20_27, ve_20_plus, ve2_0_14, ve2_14_20, ve2_14_plus]
    df["VE = 1-RR", "0.95-CI(+-) Hospitalization"] = [ci_0_20, ci_0_27, ci_14_20, ci_14_plus, ci_20_27, ci_20_plus, ci2_0_14, ci2_14_20, ci2_14_plus]

    sbst = ["Death by Covid-19", "0.95-CI(+-) Death"]
    df["VE = 1-RR", "CI(upper, lower) - Death"] = df["VE = 1-RR"][sbst].apply(lambda x: calc_ci(x[sbst[0]], x[sbst[1]]), axis=1)
    sbst = ["Hospitalization due to Covid-19", "0.95-CI(+-) Hospitalization"]
    df["VE = 1-RR", "CI(upper, lower) - Hospitalization"] = df["VE = 1-RR"][sbst].apply(lambda x: calc_ci(x[sbst[0]], x[sbst[1]]), axis=1)
    # remove aux columns of 'df'
    df = df.drop("0.95-CI(+-) Death", level=1, axis=1)
    df = df.drop("0.95-CI(+-) Hospitalization", level=1, axis=1)
    return df

def calc_ci(ve, ci_value):
    rr = 1 - ve
    lower = np.exp(np.log(rr)-ci_value)
    upper = np.exp(np.log(rr)+ci_value)
    return (1-lower, 1-upper)

def KM_plotting(km_case_death, km_control_death, km_case_hospt, km_control_hospt):
    fig, AX = plt.subplots(2,2, figsize=(8,8))

    sns.lineplot(x="day", y="KM_estimate_porc", data=km_case_death, ax=AX[0,0])
    sns.lineplot(x="day", y="KM_estimate_porc", data=km_control_death, ax=AX[0,0])
    AX[0,0].fill_between(km_case_death["day"], km_case_death["KM_estimate_CI_lower_porc"], km_case_death["KM_estimate_CI_upper_porc"], alpha=0.2)
    AX[0,0].fill_between(km_control_death["day"], km_control_death["KM_estimate_CI_lower_porc"], km_control_death["KM_estimate_CI_upper_porc"], alpha=0.2)

    sns.lineplot(x="day", y="KM_estimate_porc", data=km_case_hospt, ax=AX[0,1], label="Vaccinated")
    sns.lineplot(x="day", y="KM_estimate_porc", data=km_control_hospt, ax=AX[0,1], label="Unvaccinated")
    AX[0,1].fill_between(km_case_hospt["day"], km_case_hospt["KM_estimate_CI_lower_porc"], km_case_hospt["KM_estimate_CI_upper_porc"], alpha=0.2)
    AX[0,1].fill_between(km_control_hospt["day"], km_control_hospt["KM_estimate_CI_lower_porc"], km_control_hospt["KM_estimate_CI_upper_porc"], alpha=0.2)

    sns.lineplot(x="day", y="KM_survival", data=km_case_death, ax=AX[1,0])
    sns.lineplot(x="day", y="KM_survival", data=km_control_death, ax=AX[1,0])
    AX[1,0].fill_between(km_case_death["day"], km_case_death["KM_survival_CI_lower"], km_case_death["KM_survival_CI_upper"], alpha=0.2)
    AX[1,0].fill_between(km_control_death["day"], km_control_death["KM_survival_CI_lower"], km_control_death["KM_survival_CI_upper"], alpha=0.2)

    sns.lineplot(x="day", y="KM_survival", data=km_case_hospt, ax=AX[1,1])
    sns.lineplot(x="day", y="KM_survival", data=km_control_hospt, ax=AX[1,1])
    AX[1,1].fill_between(km_case_hospt["day"], km_case_hospt["KM_survival_CI_lower"], km_case_hospt["KM_survival_CI_upper"], alpha=0.2)
    AX[1,1].fill_between(km_control_hospt["day"], km_control_hospt["KM_survival_CI_lower"], km_control_hospt["KM_survival_CI_upper"], alpha=0.2)

    for axis in [AX[0,0], AX[0,1], AX[1,0], AX[1,1]]:
        axis.tick_params(labelsize=13)
        axis.grid(alpha=0.2)
        axis.set_xlim([0,80])
        axis.set_xlabel("days", fontsize=15)
        axis.set_ylabel("")
        axis.set_xticks(np.arange(km_case_death["day"].min(), km_case_death["day"].max()+1, 14))
        axis.set_xlim([0,80])
    AX[0,1].legend(loc=2, prop={'size': 12}, bbox_to_anchor=(0.39, 1.35))    
    AX[0,0].set_ylabel("Cumulative incidence (%)", fontsize=14)
    AX[0,0].set_xlim([0,80])
    AX[0,0].set_xlabel("")
    AX[0,1].set_xlabel("")
    AX[0,0].set_title("Death by Covid-19", fontsize=13)
    AX[0,1].set_title("Hospitalization due to Covid-19", fontsize=13)
    AX[1,0].set_ylabel("Kaplan-Meier survival curve", fontsize=14)

    fig.suptitle("CORONAVAC", fontsize=14, y=0.92)
    plt.tight_layout()
    return fig, AX

def daily_hazard(KM_CASO_DEATH, KM_CONTROLE_DEATH, KM_CASO_HOSPT, KM_CONTROLE_HOSPT):
    '''
    
    '''
    fig, ax = plt.subplots(1, figsize=(8,6))
    ve_death = 1 - (KM_CASO_DEATH["KM_estimate"]/KM_CONTROLE_DEATH["KM_estimate"])
    ve_hospt = 1 - (KM_CASO_HOSPT["KM_estimate"]/KM_CONTROLE_HOSPT["KM_estimate"])
    ve_death.name = "VE_DEATH"
    ve_hospt.name = "VE_HOSPT"
    df = pd.concat([ve_death, ve_hospt], axis=1)
    df = df.dropna(subset=["VE_DEATH", "VE_HOSPT"], how="any", axis=0)
    df = df.reset_index()

    sns.lineplot(x="index", y="VE_DEATH", data=df, ax=ax, label="Death by Covid-19", lw=2)
    sns.lineplot(x="index", y="VE_HOSPT", data=df, ax=ax, label="Hospitalization due to Covid-19", lw=2)

    #ax.plot(day_death, ve_death, label="Death by Covid-19", lw=2)
    #ax.plot(day_hospt, ve_hospt, label="Hospitalization due to Covid-19", lw=2)
    ax.set_xlabel("days", fontsize=13)
    ax.set_ylabel("1 - Hazard Ratio", fontsize=13)
    ax.set_xlim([7,80])
    ax.set_ylim([0.0,1.0])
    ax.set_xticks(np.arange(7, 90+1, 7))
    ax.legend(loc=3, prop={"size":13})
    ax.grid(alpha=0.2)
    return fig, ax

def pointplot_VE(RESULTS, RESULTS_CI):
    '''
        Plot point estimates and confidence intervals for the Vaccine Effectiveness.

        Args:
            RESULTS:
                Dictionary.
            RESULTS_CI:
                Dictionary.
        Return:
            fig:
                plt.figure().
            ax:
                plt.axis. 
    '''
    fig1, ax1 = plt.subplots(1, figsize=(8,6))
    fig2, ax2 = plt.subplots(1, figsize=(8,6))
    fig3, ax3 = plt.subplots(1, figsize=(8,6)) 

    keys = ["GERAL->14d+", "MALE->14d+", "FEMALE->14d+",
              "60-69->14d+", "70-79->14d+", "80+->14d+",
              "90+->14d+"]
    x_labels_d1 = ["D1", "D1(MALE)", "D1(FEMALE)", "D1(60-69YO)", 
                "D1(70-79)", "D1(80-89)", "D1(90+)"]
    x_labels_d2 = ["D2", "D2(MALE)", "D2(FEMALE)", "D2(60-69YO)", 
                "D2(70-79YO)", "D2(80-89YO)", "D2(90+)"]

    y_axis_d1 = [ RESULTS["D1"][name] for name in keys ]
    y_axis_d2 = [ RESULTS["D2"][name] for name in keys ]
    bar_axis_d1 = [ [RESULTS_CI["D1(CI)"][name][0]-RESULTS["D1"][name],
                    RESULTS["D1"][name]-RESULTS_CI["D1(CI)"][name][1]] for name in keys ]
    bar_axis_d2 = [ [RESULTS_CI["D2(CI)"][name][0]-RESULTS["D2"][name],
                    RESULTS["D2"][name]-RESULTS_CI["D2(CI)"][name][1]] for name in keys ]

    ax1.errorbar(x_labels_d1+x_labels_d2, y_axis_d1+y_axis_d2, yerr=np.array(bar_axis_d1+bar_axis_d2).T, ls="",
                marker="o", ms=8)
    
    join_lst = y_axis_d1+y_axis_d2
    join_lst_x = x_labels_d1+x_labels_d2
    for i, value in enumerate(join_lst):
        ax1.annotate(f"{value:0.3f}", (join_lst_x[i], join_lst[i]))
    ax2.errorbar(x_labels_d1, y_axis_d1, yerr=np.array(bar_axis_d1).T, ls="",
                marker="o", ms=8)
    for i, value in enumerate(y_axis_d1):
        ax2.annotate(f"{value:0.3f}", (x_labels_d1[i], y_axis_d1[i]))
    ax3.errorbar(x_labels_d2, y_axis_d2, yerr=np.array(bar_axis_d2).T, ls="",
                marker="o", ms=8)
    for i, value in enumerate(y_axis_d2):
        ax3.annotate(f"{value:0.3f}", (x_labels_d2[i], y_axis_d2[i]))

    for axis in [ax1, ax2, ax3]:
        axis.grid(alpha=0.25)
        axis.set_ylim([-0.001, 1.1])
        axis.tick_params(rotation=25)

    return (fig1, fig2, fig3), (ax1, ax2, ax3)



