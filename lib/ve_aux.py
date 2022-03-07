
def fit_dose(df_survival, km_objects, event):
    '''
    
    '''
    # --> D1
    condition = (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=0)
    dff = df_survival[condition]
    km_objects["D1"]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO", timeline=range(0,90,1))
    condition = (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=0)
    dff = df_survival[condition]
    km_objects["D1"]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=df_survival[f"E - D1 {event}"], label="CONTROLE", timeline=range(0,90,1))
    # --> D2
    condition = (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=0)
    dff = df_survival[condition]
    km_objects["D2"]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO", timeline=range(0,150,1))
    condition = (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=0)
    dff = df_survival[condition]
    km_objects["D2"]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=df_survival[f"E - D2 {event}"], label="CONTROLE", timeline=range(0,150,1))

def fit_sex(df_survival, km_objects, sex, event):
    '''
    
    '''
    if sex=="M":
        fstring1 = "D1_MALE"
        fstring2 = "D2_MALE"
    else:
        fstring1 = "D1_FEMALE"
        fstring2 = "D2_FEMALE"
    # --> D1 MALE
    condition = (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=0) & (df_survival["SEXO"]==sex)
    dff = df_survival[condition]
    km_objects[fstring1]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO", timeline=range(0,90,1))
    condition = (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=0) & (df_survival["SEXO"]==sex)
    dff = df_survival[condition]
    km_objects[fstring1]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=df_survival[f"E - D1 {event}"], label="CONTROLE", timeline=range(0,90,1))
    # --> D2 MALE
    condition = (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=0) & (df_survival["SEXO"]==sex)
    dff = df_survival[condition]
    km_objects[fstring2]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO", timeline=range(0,150,1))
    condition = (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D2 {event}"]>=0) & (df_survival["SEXO"]==sex)
    dff = df_survival[condition]
    km_objects[fstring2]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=df_survival[f"E - D2 {event}"], label="CONTROLE", timeline=range(0,150,1))

def fit_age(df_survival, km_objects, age, event):
    '''
    
    '''
    df_survival["AGE ELIGIBLE"] = df_survival["IDADE"].apply(lambda x: True if x>=age[0] and x<=age[1] else False)

    controles = []
    caso_eli = df_survival["AGE ELIGIBLE"].tolist()
    cpfl = df_survival["CPF"].tolist()
    for j in range(0, df_survival.shape[0], 2):
        if caso_eli[j]==True:
            controles.append(cpfl[j+1])

    # D1
    condition = (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D1 {event}"]>=0) & (df_survival["IDADE"]>=age[0]) & (df_survival["IDADE"]<=age[1])
    dff = df_survival[condition]
    string = f"D1_{age[0]}{age[1]}"
    if age[0]==80:
        string = "D1_80+"
    km_objects[string]["CASO"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CASO", timeline=range(0,90,1))
    condition = (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=0) & (df_survival["CPF"].isin(controles))
    dff = df_survival[condition]
    km_objects[string]["CONTROLE"].fit(dff[f"t - D1 {event}"], event_observed=dff[f"E - D1 {event}"], label="CONTROLE", timeline=range(0,90,1))
    
    # D2
    condition = (df_survival["TIPO"]=="CASO") & (df_survival[f"t - D2 {event}"]>=0) & (df_survival["IDADE"]>=age[0]) & (df_survival["IDADE"]<=age[1])
    dff = df_survival[condition]
    string = f"D2_{age[0]}{age[1]}"
    if age[0]==80:
        string = "D2_80+"
    km_objects[string]["CASO"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CASO", timeline=range(0,150,1))
    condition = (df_survival["TIPO"]=="CONTROLE") & (df_survival[f"t - D1 {event}"]>=0) & (df_survival["CPF"].isin(controles))
    dff = df_survival[condition]
    km_objects[string]["CONTROLE"].fit(dff[f"t - D2 {event}"], event_observed=dff[f"E - D2 {event}"], label="CONTROLE", timeline=range(0,150,1))
    