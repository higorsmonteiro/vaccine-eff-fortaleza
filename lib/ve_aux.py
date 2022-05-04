import pandas as pd
from lifelines import KaplanMeierFitter

def fit_dose(df_survival, km_objects, sub_data, event, t_min=0):
    '''
    
    '''
    # --> D1
    dff = df_survival[ (df_survival["TIPO"]=="CASO")  &  (df_survival[f"t - D1 {event}"]>=t_min) ]
    km_objects["D1"]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO")
    sub_data["D1"]["CASO"] = dff[["CPF"]].copy()

    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE")  &  (df_survival[f"t - D1 {event}"]>=t_min) ]
    km_objects["D1"]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CONTROLE")
    sub_data["D1"]["CONTROLE"] = dff[["CPF"]].copy()
    
    # --> D2
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=t_min) ]
    km_objects["D2"]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO")
    sub_data["D2"]["CASO"] = dff[["CPF"]].copy()
    
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=t_min) ]
    km_objects["D2"]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CONTROLE")
    sub_data["D2"]["CONTROLE"] = dff[["CPF"]].copy()

def fit_sex(df_survival, km_objects, sub_data, sex, event, t_min=0):
    '''
    
    '''
    if sex=="M":
        fstring1 = "D1_MALE"
        fstring2 = "D2_MALE"
    else:
        fstring1 = "D1_FEMALE"
        fstring2 = "D2_FEMALE"
    
    # --> D1 MALE
    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=t_min) & (df_survival["SEXO"]==sex) ]
    km_objects[fstring1]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO")
    sub_data[fstring1]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=t_min) & (df_survival["SEXO"]==sex) ]
    km_objects[fstring1]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CONTROLE")
    sub_data[fstring1]["CONTROLE"] = dff[["CPF"]].copy()
    
    # --> D2 MALE
    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=t_min) & (df_survival["SEXO"]==sex) ]
    km_objects[fstring2]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO")
    sub_data[fstring2]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=t_min) & (df_survival["SEXO"]==sex) ]
    km_objects[fstring2]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CONTROLE")
    sub_data[fstring2]["CONTROLE"] = dff[["CPF"]].copy()

def fit_age(df_survival, pairs_hash, km_objects, sub_data, age, event, t_min):
    '''
    
    '''
    # Select subset of individuals for the given age range
    casos_cpf = df_survival[(df_survival["TIPO"]=="CASO") & (df_survival["IDADE"]>=age[0]) & (df_survival["IDADE"]<=age[1])]["CPF"].tolist()
    controles_cpf = [ pairs_hash[cpf] for cpf in casos_cpf ]
    df_survival = df_survival[df_survival["CPF"].isin(casos_cpf+controles_cpf)]

    # Perform fitting
    # --> D1
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=t_min) ]
    string = f"D1_{age[0]}{age[1]}"
    if age[0]==80:
        string = "D1_80+"
    # ----> D1 CASO
    km_objects[string]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO")
    sub_data[string]["CASO"] = dff[["CPF"]].copy()
    
    # ----> D1 CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=t_min) ]
    km_objects[string]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CONTROLE")
    sub_data[string]["CONTROLE"] = dff[["CPF"]].copy()
    
    # --> D2
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=t_min) ]
    string = f"D2_{age[0]}{age[1]}"
    if age[0]==80:
        string = "D2_80+"
    # ----> D2 CASO
    km_objects[string]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO")
    sub_data[string]["CASO"] = dff[["CPF"]].copy()

    # ----> D2 CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=t_min) ]
    km_objects[string]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CONTROLE")
    sub_data[string]["CONTROLE"] = dff[["CPF"]].copy()

def fit_hdi(df_survival, km_objects, sub_data, event, t_min, hdi_strat=[0.000,0.500,1.000]):
    '''

    '''
    hdi_condition_1 = (df_survival[f"IDH2010"]>=hdi_strat[0]) & (df_survival[f"IDH2010"]<=hdi_strat[1])
    hdi_condition_2 = (df_survival[f"IDH2010"]>hdi_strat[1]) & (df_survival[f"IDH2010"]<=hdi_strat[2])

    d1_str_lower = "D1_HDI_000_050"
    d1_str_upper = "D1_HDI_050_1"
    d2_str_lower = "D2_HDI_000_050"
    d2_str_upper = "D2_HDI_050_1"
    if hdi_strat[1]==0.59:
        d1_str_lower = "D1_HDI_000_059"
        d1_str_upper = "D1_HDI_060_1"
        d2_str_lower = "D2_HDI_000_059"
        d2_str_upper = "D2_HDI_060_1"
    # --> D1 
    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=t_min) & hdi_condition_1 ]
    if dff.shape[0]!=0:
        km_objects[d1_str_lower]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO")
        sub_data[d1_str_lower]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=t_min) & hdi_condition_1 ]
    if dff.shape[0]!=0:
        km_objects[d1_str_lower]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CONTROLE")
        sub_data[d1_str_lower]["CONTROLE"] = dff[["CPF"]].copy()

    # --> D2
    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=t_min) & hdi_condition_1 ]
    if dff.shape[0]!=0:
        km_objects[d2_str_lower]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO")
        sub_data[d2_str_lower]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=t_min) & hdi_condition_1 ]
    if dff.shape[0]!=0:
        km_objects[d2_str_lower]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CONTROLE")
        sub_data[d2_str_lower]["CONTROLE"] = dff[["CPF"]].copy()
    
    # --> D2 MALE
    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=t_min) & hdi_condition_2 ]
    if dff.shape[0]!=0:
        km_objects[d1_str_upper]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO")
        sub_data[d1_str_upper]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=t_min) & hdi_condition_2 ]
    if dff.shape[0]!=0:
        km_objects[d1_str_upper]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CONTROLE")
        sub_data[d1_str_upper]["CONTROLE"] = dff[["CPF"]].copy()
    
    # --> D2 MALE
    # ----> CASO
    dff = df_survival[ (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=t_min) & hdi_condition_2 ]
    if dff.shape[0]!=0:
        km_objects[d2_str_upper]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO")
        sub_data[d2_str_upper]["CASO"] = dff[["CPF"]].copy()

    # ----> CONTROLE
    dff = df_survival[ (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=t_min) & hdi_condition_2 ]
    if dff.shape[0]!=0:
        km_objects[d2_str_upper]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CONTROLE")
        sub_data[d2_str_upper]["CONTROLE"] = dff[["CPF"]].copy()
    
def create_km_objects():
    '''
    
    '''
    template = {
        "CASO": None,
        "CONTROLE": None
    }
    # Store the KaplanMeier objects fitted to each case data.
    km_objects = {
        "D1": dict(template), "D2": dict(template),
        "D1_MALE": dict(template), "D2_MALE": dict(template),
        "D1_FEMALE": dict(template), "D2_FEMALE": dict(template),
        "D1_6069": dict(template), "D2_6069": dict(template),
        "D1_7079": dict(template), "D2_7079": dict(template),
        "D1_80+": dict(template), "D2_80+": dict(template),
        "D1_HDI_000_059": dict(template), "D1_HDI_060_1": dict(template),
        "D2_HDI_000_059": dict(template), "D2_HDI_060_1": dict(template),
        "D1_HDI_000_050": dict(template), "D1_HDI_050_1": dict(template),
        "D2_HDI_000_050": dict(template), "D2_HDI_050_1": dict(template),
    }
    for key in km_objects.keys():
        km_objects[key]["CASO"] = KaplanMeierFitter()
        km_objects[key]["CONTROLE"] = KaplanMeierFitter()
    
    sub_data = {
        "D1": dict(template), "D2": dict(template), "D1_MALE": dict(template), "D2_MALE": dict(template),
        "D1_FEMALE": dict(template), "D2_FEMALE": dict(template), "D1_6069": dict(template), "D2_6069": dict(template),
        "D1_7079": dict(template), "D2_7079": dict(template), "D1_80+": dict(template), "D2_80+": dict(template),
        "D1_HDI_000_059": dict(template), "D1_HDI_060_1": dict(template), "D2_HDI_000_059": dict(template), "D2_HDI_060_1": dict(template),
        "D1_HDI_000_050": dict(template), "D1_HDI_050_1": dict(template), "D2_HDI_000_050": dict(template), "D2_HDI_050_1": dict(template),
    }
    return km_objects, sub_data

def generate_table(km_objects):
    '''
    
    '''
    # Store final tables regarding Kaplan Meier curves.
    etables = {
        "D1": None, "D2": None, "D1_MALE": None, "D2_MALE": None, "D1_FEMALE": None, 
        "D2_FEMALE": None, "D1_6069": None, "D2_6069": None, "D1_7079": None, 
        "D2_7079": None, "D1_80+": None, "D2_80+": None, "D1_HDI_000_059": None, 
        "D1_HDI_060_1": None, "D2_HDI_000_059": None, "D2_HDI_060_1": None,
        "D1_HDI_000_050": None, 
        "D1_HDI_050_1": None, "D2_HDI_000_050": None, "D2_HDI_050_1": None,
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