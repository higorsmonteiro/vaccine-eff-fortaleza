import pandas as pd
from lifelines import KaplanMeierFitter

def fit_dose_v2(df_survival, km_objects, sub_data, event):
    '''
    
    '''
    dff = df_survival[ (df_survival["TIPO"]=="CASO")  &  (df_survival[f"t - {event}"]>=0) ]
    km_objects["DOSE"]["CASO"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CASO")
    sub_data["DOSE"]["CASO"] = dff[["CPF"]].copy()

    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE")  &  (df_survival[f"t - {event}"]>=0) ]
    km_objects["DOSE"]["CONTROLE"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CONTROLE")
    sub_data["DOSE"]["CONTROLE"] = dff[["CPF"]].copy()

def fit_sex_v2(df_survival, km_objects, sub_data, sex, event):
    '''
    
    '''
    if sex=="M":
        fstring = "MALE"
    else:
        fstring = "FEMALE"
    
    dff = df_survival[ (df_survival["TIPO"]=="CASO")  &  (df_survival[f"t - {event}"]>=0) & (df_survival["SEXO"]==sex) ]
    km_objects[fstring]["CASO"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CASO")
    sub_data[fstring]["CASO"] = dff[["CPF"]].copy()

    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE")  &  (df_survival[f"t - {event}"]>=0) & (df_survival["SEXO"]==sex) ]
    km_objects[fstring]["CONTROLE"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CONTROLE")
    sub_data[fstring]["CONTROLE"] = dff[["CPF"]].copy()

def fit_age_v2(df_survival, pairs_hash, km_objects, sub_data, age, event):
    '''
    
    '''
    # Select subset of individuals for the given age range
    casos_cpf = df_survival[(df_survival["TIPO"]=="CASO") & (df_survival["IDADE"]>=age[0]) & (df_survival["IDADE"]<=age[1])]["CPF"].tolist()
    controles_cpf = [ pairs_hash[cpf] for cpf in casos_cpf ]
    df_survival = df_survival[df_survival["CPF"].isin(casos_cpf+controles_cpf)]

    # Perform fitting
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - {event}"]>=0) ]
    string = f"{age[0]}{age[1]}"
    if age[0]==80:
        string = "80+"
    # ----> D1 CASO
    km_objects[string]["CASO"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CASO")
    sub_data[string]["CASO"] = dff[["CPF"]].copy()
    
    # ----> D1 CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - {event}"]>=0) ]
    km_objects[string]["CONTROLE"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CONTROLE")
    sub_data[string]["CONTROLE"] = dff[["CPF"]].copy()

def fit_hdi_v2(df_survival, km_objects, sub_data, event, hdi_strat=[0.000,0.500,1.000]):
    '''

    '''
    hdi_condition_1 = (df_survival[f"IDH2010"]>=hdi_strat[0]) & (df_survival[f"IDH2010"]<=hdi_strat[1])
    hdi_condition_2 = (df_survival[f"IDH2010"]>hdi_strat[1]) & (df_survival[f"IDH2010"]<=hdi_strat[2])

    str_lower = "HDI_000_050"
    str_upper = "HDI_050_1"
    if hdi_strat[1]==0.59:
        str_lower = "HDI_000_059"
        str_upper = "HDI_060_1"

    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - {event}"]>=0) & hdi_condition_1 ]
    if dff.shape[0]!=0:
        km_objects[str_lower]["CASO"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CASO")
        sub_data[str_lower]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - {event}"]>=0) & hdi_condition_1 ]
    if dff.shape[0]!=0:
        km_objects[str_lower]["CONTROLE"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CONTROLE")
        sub_data[str_lower]["CONTROLE"] = dff[["CPF"]].copy()

    
    # ----> CASO UPPER
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - {event}"]>=0) & hdi_condition_2 ]
    if dff.shape[0]!=0:
        km_objects[str_upper]["CASO"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CASO")
        sub_data[str_upper]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - {event}"]>=0) & hdi_condition_2 ]
    if dff.shape[0]!=0:
        km_objects[str_upper]["CONTROLE"].fit(dff[f"t - {event}"], event_observed=dff[f"E - {event}"], label="CONTROLE")
        sub_data[str_upper]["CONTROLE"] = dff[["CPF"]].copy()
    
def create_km_objects():
    '''
    
    '''
    template = {
        "CASO": None,
        "CONTROLE": None
    }
    # Store the KaplanMeier objects fitted to each case data.
    km_objects = {
        "DOSE": dict(template), "MALE": dict(template), "FEMALE": dict(template), 
        "6069": dict(template), "7079": dict(template), "80+": dict(template), 
        "HDI_000_059": dict(template), "HDI_060_1": dict(template),
        "HDI_000_050": dict(template), "HDI_050_1": dict(template),
    }
    for key in km_objects.keys():
        km_objects[key]["CASO"] = KaplanMeierFitter()
        km_objects[key]["CONTROLE"] = KaplanMeierFitter()
    
    sub_data = {
        "DOSE": dict(template), "MALE": dict(template), "FEMALE": dict(template), 
        "6069": dict(template), "7079": dict(template), "80+": dict(template), 
        "HDI_000_059": dict(template), "HDI_060_1": dict(template),
        "HDI_000_050": dict(template), "HDI_050_1": dict(template),
    }
    return km_objects, sub_data

def generate_table(km_objects):
    '''
    
    '''
    # Store final tables regarding Kaplan Meier curves.
    etables = {
        "DOSE": None, "MALE": None, "FEMALE": None, 
        "6069": None, "7079": None, "80+": None, 
        "HDI_000_059": None, "HDI_060_1": None,
        "HDI_000_050": None, "HDI_050_1": None,
    }

    for key in etables.keys():
        if km_objects[key]["CASO"] is None:
            continue
        event_caso = km_objects[key]["CASO"].event_table.reset_index().add_suffix("(caso)").rename({"event_at(caso)": "t"}, axis=1)
        event_controle = km_objects[key]["CONTROLE"].event_table.reset_index().add_suffix("(controle)").rename({"event_at(controle)": "t"}, axis=1)
        S_caso = km_objects[key]["CASO"].cumulative_density_.reset_index().rename({'timeline': "t"}, axis=1)
        S_controle = km_objects[key]["CONTROLE"].cumulative_density_.reset_index().rename({'timeline': "t"}, axis=1)
        S_caso_ci = km_objects[key]["CASO"].confidence_interval_cumulative_density_.reset_index().rename({"index": "t"}, axis=1)
        S_controle_ci = km_objects[key]["CONTROLE"].confidence_interval_cumulative_density_.reset_index().rename({"index": "t"}, axis=1)
        final = event_caso.merge(S_caso, on="t", how="left")
        final = final.merge(S_caso_ci, on="t", how="left")
        final = final.merge(event_controle, on="t", how="left")
        final = final.merge(S_controle, on="t", how="left")
        final = final.merge(S_controle_ci, on="t", how="left")
        etables[key] = final.copy()
        etables[key] = etables[key].rename({"CASO": "KM(caso)", "CONTROLE": "KM(controle)",
                                            "CONTROLE_lower_0.95": "KM_lower_0.95(controle)",
                                            "CONTROLE_upper_0.95": "KM_upper_0.95(controle)",
                                            "CASO_lower_0.95": "KM_lower_0.95(caso)",
                                            "CASO_upper_0.95": "KM_upper_0.95(caso)"}, axis=1)
    return etables

def f_hdi_range(x, irange, include_nans=False):
    '''
        Auxiliary function for .apply() to define categorical variables
        for the HDI variable.
    '''
    for k in range(len(irange)-1):
        if include_nans and pd.isna(x):
            return k
        if x>irange[k] and x<=irange[k+1]:
            return k