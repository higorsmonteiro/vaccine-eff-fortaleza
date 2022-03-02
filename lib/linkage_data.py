import numpy as np
from collections import defaultdict

def linkage_vacinados(vacineja_df, vacinados_df):
    '''
    
    '''
    col_linkage1 = ["cpf(VACINADOS)"]
    col_linkage2 = ["cpf(VACINADOS)", "NOMENASCIMENTOCHAVE"]
    col_linkage3 = ["cpf(VACINADOS)", "NOMENOMEMAECHAVE"]
    col_linkage4 = ["cpf(VACINADOS)", "NOMEMAENASCIMENTOCHAVE"]
    col_linkage5 = ["cpf(VACINADOS)", "NOMEHASHNASCIMENTOCHAVE"]
    col_linkage6 = ["cpf(VACINADOS)", "NOMEMAEHASHNASCIMENTOCHAVE"]

    linkage1 = vacinados_df[col_linkage1].dropna(subset=["cpf(VACINADOS)"], axis=0).merge(vacineja_df.add_suffix("(VACINEJA)")[["cpf(VACINEJA)"]], left_on="cpf(VACINADOS)", right_on="cpf(VACINEJA)", how="left").dropna(subset=["cpf(VACINEJA)"], axis=0)
    linkage2 = vacinados_df[col_linkage2].merge(vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]], on="NOMENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0).dropna(subset=["cpf"], axis=0)
    #linkage3 = vacinados_df[col_linkage3].dropna(subset=["NOMENOMEMAECHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMENOMEMAECHAVE"]], on="NOMENOMEMAECHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage4 = vacinados_df[col_linkage4].dropna(subset=["NOMEMAENASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]], on="NOMEMAENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage5 = vacinados_df[col_linkage5].dropna(subset=["NOMEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]], on="NOMEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage6 = vacinados_df[col_linkage6].dropna(subset=["NOMEMAEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)

    vacinados_to_vacinejacpf = defaultdict(lambda: np.nan)
    vacinados_to_vacinejacpf.update(zip(linkage1["cpf(VACINADOS)"], linkage1["cpf(VACINEJA)"]))
    vacinados_to_vacinejacpf.update(zip(linkage2["cpf(VACINADOS)"], linkage2["cpf"]))
    #vacinados_to_vacinejacpf.update(zip(linkage3["cpf(VACINADOS)"], linkage3["cpf"]))
    vacinados_to_vacinejacpf.update(zip(linkage4["cpf(VACINADOS)"], linkage4["cpf"]))
    vacinados_to_vacinejacpf.update(zip(linkage5["cpf(VACINADOS)"], linkage5["cpf"]))
    vacinados_to_vacinejacpf.update(zip(linkage6["cpf(VACINADOS)"], linkage6["cpf"]))

    vacinados_df["cpf(VACINEJA)"] = vacinados_df["cpf(VACINADOS)"].apply(lambda x: vacinados_to_vacinejacpf[x])
    vacinados_df = vacinados_df[["cpf(VACINADOS)", "cpf(VACINEJA)"]]

    # --> UNIFORMIZATION OF VACINEJA + VACINADOS
    temp = vacinados_df
    col_vacinados = defaultdict(lambda: np.nan)
    tests_group = vacinados_df.groupby("cpf(VACINEJA)")
    count_group = tests_group.count()
    single_link = count_group[count_group["cpf(VACINADOS)"]==1]
    multiple_link = count_group[count_group["cpf(VACINADOS)"]>1]

    temp = vacinados_df[vacinados_df["cpf(VACINEJA)"].isin(single_link.index)]
    col_vacinados.update(zip(temp["cpf(VACINEJA)"], temp["cpf(VACINADOS)"]))
    temp = vacinados_df[vacinados_df["cpf(VACINEJA)"].isin(multiple_link.index)]
    col_vacinados.update([x for x in temp.values if np.all(x==x[0])])

    vacinados_df["cpf(VACINEJA)"] = vacinados_df.apply(lambda x: x["cpf(VACINEJA)"] if col_vacinados[x["cpf(VACINEJA)"]]==x["cpf(VACINADOS)"] else np.nan, axis=1)
    return vacinados_df

def linkage_integrasus(vacineja_df, integrasus_df):
    '''
    
    '''
    col_linkage1 = ["id", "cpf"]
    col_linkage2 = ["id", "NOMENASCIMENTOCHAVE"]
    col_linkage3 = ["id", "NOMENOMEMAECHAVE"]
    col_linkage4 = ["id", "NOMEMAENASCIMENTOCHAVE"]
    col_linkage5 = ["id", "NOMEHASHNASCIMENTOCHAVE"]
    col_linkage6 = ["id", "NOMEMAEHASHNASCIMENTOCHAVE"]

    linkage1 = integrasus_df[col_linkage1].dropna(subset=["cpf"], axis=0).merge(vacineja_df.add_suffix("(vj)")["cpf(vj)"], left_on="cpf", right_on="cpf(vj)", how="left").dropna(subset=["cpf(vj)"], axis=0)
    linkage2 = integrasus_df[col_linkage2].merge(vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]], on="NOMENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage3 = integrasus_df[col_linkage3].dropna(subset=["NOMENOMEMAECHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMENOMEMAECHAVE"]], on="NOMENOMEMAECHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage4 = integrasus_df[col_linkage4].dropna(subset=["NOMEMAENASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]], on="NOMEMAENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage5 = integrasus_df[col_linkage5].dropna(subset=["NOMEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]], on="NOMEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage6 = integrasus_df[col_linkage6].dropna(subset=["NOMEMAEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)

    id_to_vacinejacpf = defaultdict(lambda: np.nan)
    id_to_vacinejacpf.update(zip(linkage1["id"], linkage1["cpf(vj)"]))
    id_to_vacinejacpf.update(zip(linkage2["id"], linkage2["cpf"]))
    id_to_vacinejacpf.update(zip(linkage3["id"], linkage3["cpf"]))
    id_to_vacinejacpf.update(zip(linkage4["id"], linkage4["cpf"]))
    id_to_vacinejacpf.update(zip(linkage5["id"], linkage5["cpf"]))
    id_to_vacinejacpf.update(zip(linkage6["id"], linkage6["cpf"]))

    integrasus_df["cpf(VACINEJA)"] = integrasus_df["id"].apply(lambda x: id_to_vacinejacpf[x])
    integrasus_df = integrasus_df[["id", "PRIMARY_KEY_PERSON", "cpf(VACINEJA)"]]

    # --> UNIFORMIZATION OF VACINEJA + INTEGRASUS -> cpf to list of IDs on IntegraSUS
    tests_group = integrasus_df.groupby("cpf(VACINEJA)")
    count_group = tests_group.count()
    has_id = count_group[count_group["id"]>=1]

    temp = integrasus_df[integrasus_df["cpf(VACINEJA)"].isin(has_id.index)]
    integrasus_df = temp.groupby("cpf(VACINEJA)").agg({'id': list}).reset_index()
    return integrasus_df

def linkage_obitos_covid(vacineja_df, obitos_covid_df, obitos_cartorios_df):
    '''
        Description.

        Args:
            vacineja_df:
                pandas.DataFrame.
            obitos_covid_df:
                pandas.DataFrame.
            obitos_cartorios_df:
                pandas.DataFrame.
        Return:
            linkage:
                pandas.DataFrame.
    '''
    # --> Linkage to found deaths from Covid-19 in 'Vacine Já'
    col_include = ["cpf", "do_8"]
    # Get the CPF for Covid-19 deaths from cartórios.
    obitos_covid_df = obitos_covid_df.dropna(subset=["numerodo"], axis=0).merge(obitos_cartorios_df[col_include], left_on="numerodo", right_on="do_8", how="left")
    obitos_covid_df = obitos_covid_df.rename({'cpf': 'cpf(COVID)'}, axis=1)

    # ----> Columns for each linkage.
    col_linkage1 = ["cpf(COVID)", "numerodo", "ORDEM(OBITO COVID)"]
    col_linkage2 = ["cpf(COVID)", "NOMENASCIMENTOCHAVE", "numerodo", "ORDEM(OBITO COVID)"]
    col_linkage3 = ["NOMENOMEMAECHAVE", "numerodo", "ORDEM(OBITO COVID)"]
    #col_linkage4 = ["cpf(COVID)", "NOMEMAENASCIMENTOCHAVE", "numerodo", "ORDEM(OBITO COVID)"]
    col_linkage5 = ["cpf(COVID)", "NOMEHASHNASCIMENTOCHAVE", "numerodo", "ORDEM(OBITO COVID)"]
    #col_linkage6 = ["cpf(COVID)", "NOMEMAEHASHNASCIMENTOCHAVE", "numerodo", "ORDEM(OBITO COVID)"]

    linkage1 = obitos_covid_df[col_linkage1].dropna(subset=["cpf(COVID)"], axis=0).merge(vacineja_df.add_suffix("(VACINEJA)")["cpf(VACINEJA)"], left_on="cpf(COVID)", right_on="cpf(VACINEJA)", how="left").dropna(subset=["cpf(VACINEJA)"], axis=0)
    linkage2 = obitos_covid_df[col_linkage2].dropna(subset=["NOMENASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]], on="NOMENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage3 = obitos_covid_df[col_linkage3].dropna(subset=["NOMENOMEMAECHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMENOMEMAECHAVE"]], on="NOMENOMEMAECHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #linkage4 = obitos_covid_df[col_linkage4].dropna(subset=["NOMEMAENASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]], on="NOMEMAENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage5 = obitos_covid_df[col_linkage5].dropna(subset=["NOMEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]], on="NOMEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #linkage6 = obitos_covid_df[col_linkage6].dropna(subset=["NOMEMAEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)

    do_to_vacinejacpf = defaultdict(lambda: np.nan)
    do_to_vacinejacpf.update(zip(linkage1["ORDEM(OBITO COVID)"], linkage1["cpf(VACINEJA)"]))
    do_to_vacinejacpf.update(zip(linkage2["ORDEM(OBITO COVID)"], linkage2["cpf"]))
    do_to_vacinejacpf.update(zip(linkage3["ORDEM(OBITO COVID)"], linkage3["cpf"]))
    #do_to_vacinejacpf.update(zip(linkage4["ORDEM(OBITO COVID)"], linkage4["cpf"]))
    do_to_vacinejacpf.update(zip(linkage5["ORDEM(OBITO COVID)"], linkage5["cpf"]))
    #do_to_vacinejacpf.update(zip(linkage6["ORDEM(OBITO COVID)"], linkage6["cpf"]))
    obitos_covid_df["cpf(VACINEJA)"] = obitos_covid_df["ORDEM(OBITO COVID)"].apply(lambda x: do_to_vacinejacpf[x])
    obitos_covid_df = obitos_covid_df[["cpf(COVID)", "numerodo", "ORDEM(OBITO COVID)", "cpf(VACINEJA)"]]
    return obitos_covid_df

def linkage_obito_cartorios(vacineja_df, obitos_covid_df, obitos_cartorios_df):
    '''
        Description.

        Args:
            vacineja_df:
                pandas.DataFrame.
            obitos_covid_df:
                pandas.DataFrame.
            obitos_cartorios_df:
                pandas.DataFrame.
        Return:
            linkage:
                pandas.DataFrame.
    '''
    # ----> Columns for each linkage.
    col_linkage1 = ["cpf(CARTORIOS)"]
    col_linkage2 = ["NOMENASCIMENTOCHAVE", "cpf(CARTORIOS)"]
    col_linkage3 = ["NOMENOMEMAECHAVE", "cpf(CARTORIOS)"]
    #col_linkage4 = ["NOMEMAENASCIMENTOCHAVE", "cpf(CARTORIOS)"]
    col_linkage5 = ["NOMEHASHNASCIMENTOCHAVE", "cpf(CARTORIOS)"]
    #col_linkage6 = ["NOMEMAEHASHNASCIMENTOCHAVE", "cpf(CARTORIOS)"]

    linkage1 = obitos_cartorios_df[col_linkage1].dropna(subset=["cpf(CARTORIOS)"], axis=0).merge(vacineja_df.add_suffix("(VACINEJA)")["cpf(VACINEJA)"], left_on="cpf(CARTORIOS)", right_on="cpf(VACINEJA)", how="left").dropna(subset=["cpf(VACINEJA)"], axis=0)
    linkage2 = obitos_cartorios_df[col_linkage2].dropna(subset=["NOMENASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMENASCIMENTOCHAVE"]], on="NOMENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage3 = obitos_cartorios_df[col_linkage3].dropna(subset=["NOMENOMEMAECHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMENOMEMAECHAVE"]], on="NOMENOMEMAECHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #linkage4 = obitos_cartorios_df[col_linkage4].dropna(subset=["NOMEMAENASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAENASCIMENTOCHAVE"]], on="NOMEMAENASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    linkage5 = obitos_cartorios_df[col_linkage5].dropna(subset=["NOMEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEHASHNASCIMENTOCHAVE"]], on="NOMEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)
    #linkage6 = obitos_cartorios_df[col_linkage6].dropna(subset=["NOMEMAEHASHNASCIMENTOCHAVE"], axis=0).merge(vacineja_df[["cpf", "NOMEMAEHASHNASCIMENTOCHAVE"]], on="NOMEMAEHASHNASCIMENTOCHAVE", how="left").dropna(subset=["cpf"], axis=0)

    cpf_to_vacinejacpf = defaultdict(lambda: np.nan)
    cpf_to_vacinejacpf.update(zip(linkage1["cpf(CARTORIOS)"], linkage1["cpf(VACINEJA)"]))
    cpf_to_vacinejacpf.update(zip(linkage2["cpf(CARTORIOS)"], linkage2["cpf"]))
    cpf_to_vacinejacpf.update(zip(linkage3["cpf(CARTORIOS)"], linkage3["cpf"]))
    #cpf_to_vacinejacpf.update(zip(linkage4["cpf(CARTORIOS)"], linkage4["cpf"]))
    cpf_to_vacinejacpf.update(zip(linkage5["cpf(CARTORIOS)"], linkage5["cpf"]))
    #cpf_to_vacinejacpf.update(zip(linkage6["cpf(CARTORIOS)"], linkage6["cpf"]))
    obitos_cartorios_df["cpf(VACINEJA)"] = obitos_cartorios_df["cpf(CARTORIOS)"].apply(lambda x: cpf_to_vacinejacpf[x])
    obitos_cartorios_df = obitos_cartorios_df[["cpf(CARTORIOS)", "cpf(VACINEJA)"]]

    # --> UNIFORMIZATION OF VACINEJA + DEATHS NOT COVID 
    col_cart = defaultdict(lambda: np.nan)
    tests_group = obitos_cartorios_df.groupby("cpf(VACINEJA)")
    count_group = tests_group.count()
    single_link = count_group[count_group["cpf(CARTORIOS)"]==1]
    multiple_link = count_group[count_group["cpf(CARTORIOS)"]>1]

    temp = obitos_cartorios_df[obitos_cartorios_df["cpf(VACINEJA)"].isin(single_link.index)]
    col_cart.update(zip(temp["cpf(VACINEJA)"], temp["cpf(CARTORIOS)"]))
    temp = obitos_cartorios_df[obitos_cartorios_df["cpf(VACINEJA)"].isin(multiple_link.index)]
    col_cart.update([x for x in temp.values if np.all(x==x[0])])

    obitos_cartorios_df["cpf(VACINEJA)"] = obitos_cartorios_df.apply(lambda x: x["cpf(VACINEJA)"] if col_cart[x["cpf(VACINEJA)"]]==x["cpf(CARTORIOS)"] else np.nan, axis=1)
    return obitos_cartorios_df
