'''
    MAIN FUNCTIONS REGARDING THE SURVIVAL ANALYSIS USING KAPLAN-MEIER ESTIMATOR.
'''

import os
import numpy as np
import pandas as pd

def determine_event(df, dose="D1"):
    '''
        Given a dataframe containing the calculated intervals between starting date (D1 or D2,
        select the event representing the earliest event and determine if it is censored or not.

        Args:
            df:
                pandas.DataFrame.
            cols_caso:
                List of Strings.
            cols_controle:
                List of Strings.
        Return:
            df:
                pandas.DataFrame.
    '''
    cols_caso = [f"INTV OBITO COVID CASO({dose})", f"INTV OBITO GERAL CASO({dose})",
                 f"INTV D1 CASO CONTROLE({dose})", f"INTV FIM COORTE({dose})"]
    cols_controle = [f"INTV OBITO COVID CONTROLE({dose})", f"INTV OBITO GERAL CONTROLE({dose})",
                 f"INTV D1 CASO CONTROLE({dose})", f"INTV FIM COORTE({dose})"]
    if dose=="D1":
        cols_caso += ["INTV D2-D1 CASO"]
        cols_controle += ["INTV D2-D1 CONTROLE"]
    if dose=="D2":
        df = df[(pd.notna(df["DATA D2 CASO"]))]
    #elif dose=="D1":
    #    df = df[(pd.notna(df["DATA D1 CASO"]) & (pd.isna(df["DATA D2 CASO"])))]
    
    interval_caso, interval_control = [], []
    event_var_caso, event_var_controle = [], []
    censored_caso, censored_controle = [], []
    for j in range(df.shape[0]):
        # get interval for each event
        caso_t = [ df[nm].iat[j] for nm in cols_caso ]
        controle_t = [ df[nm].iat[j] for nm in cols_controle ]
        # sort the interval to get the earliest event.
        order_caso = np.argsort(caso_t)
        order_controle = np.argsort(controle_t)
        # get values and verify censoring
        interval_caso.append(caso_t[order_caso[0]])
        interval_control.append(controle_t[order_controle[0]])
        event_var_caso.append(cols_caso[order_caso[0]])
        event_var_controle.append(cols_controle[order_controle[0]])
        censored_caso.append(True)
        censored_controle.append(True)
        if event_var_caso[-1]==f"INTV OBITO COVID CASO({dose})":
            censored_caso[-1] = False
        if event_var_controle[-1]==f"INTV OBITO COVID CONTROLE({dose})":
            censored_controle[-1] = False
    # --> Define variables
    df["TEMPO CASO"] = interval_caso
    df["TEMPO CONTROLE"] = interval_control
    df["EVENTO CASO"] = event_var_caso
    df["EVENTO CONTROLE"] = event_var_controle
    df["CENSORED CASO"] = censored_caso
    df["CENSORED CONTROLE"] = censored_controle
    #col_order = ["CPF CASO", "DATA D1 CASO", "DATA D2 CASO", "CPF CONTROLE", "DATA D1 CONTROLE",
    #             "DATA D2 CONTROLE", "TEMPO CASO", "TEMPO CONTROLE", "EVENTO CASO", "CENSORED CASO", 
    #             "EVENTO CONTROLE", "CENSORED CONTROLE"]
    return df
    #return df[col_order]

def push_info(df, df_pop):
    '''

    '''
    col_info = ["cpf", "data_nascimento", "bairro", "sexo", "idade", "faixa etaria(VACINADOS)", "bairro id(VACINADOS)"]
    df_pop_caso = df_pop[col_info].add_suffix("(CASO)")
    df_pop_controle = df_pop[col_info].add_suffix("(CONTROLE)")

    df = df.merge(df_pop_caso, left_on="CPF CASO", right_on="cpf(CASO)", how="left")
    df = df.merge(df_pop_controle, left_on="CPF CONTROLE", right_on="cpf(CONTROLE)", how="left")
    return df

def generate_survival_table(df, group="CASO", dose="D1"):
    '''
        From the DataFrame containing the information on the earliest events for
        each individual, create a survival table for application of the Kaplan-Meier
        estimate.

        Args:
            df:
                pandas.DataFrame.
            group:
                String. {"CASO", "CONTROLE"}.
            age:
                Tuple.
            sex:
                String.
        Return:
            tb:
    '''
    tb = {'t_f': [], 'n_f': [], "m_f": [], "q_f": []}
    time_line = np.arange(0, 201, 1)
    for t in time_line:
        if df[df[f"{group} {dose} INTERVALO"]>=t].shape[0]==0:
            break
        tb['t_f'].append(t)
        tb['n_f'].append(df[df[f"{group} {dose} INTERVALO"]>=t].shape[0])
        if t!=0:
            tb['m_f'].append(df[(df[f"{group} {dose} INTERVALO"]==t) & (df[f"{group} {dose} CENSURADO"]==False)].shape[0])
            val = df[(df[f"{group} {dose} INTERVALO"]==t) & (df[f"{group} {dose} CENSURADO"]==True)].shape[0]
            tb['q_f'].append(val)
        else:
            tb['m_f'].append(0)
            tb['q_f'].append(df[df[f"{group} {dose} INTERVALO"]<=0].shape[0])
    return pd.DataFrame(tb)

def generate_survival_table_old(df, group="CASO", age=None, sex=None):
    '''
        From the DataFrame containing the information on the earliest events for
        each individual, create a survival table for application of the Kaplan-Meier
        estimate.

        Args:
            df:
                pandas.DataFrame.
            group:
                String. {"CASO", "CONTROLE"}.
            age:
                Tuple.
            sex:
                String.
        Return:
            tb:
    '''
    if age is not None:
        df = df[(df[f"idade({group})"]>=age[0]) & (df[f"idade({group})"]<=age[1])]
    if sex is not None:
        df = df[df[f"sexo({group})"]==sex]
    
    tb = {'t_f': [], 'n_f': [], "m_f": [], "q_f": []}
    time_line = np.arange(0, 201, 1)
    for t in time_line:
        if df[df[f"TEMPO {group}"]>=t].shape[0]==0:
            break
        tb['t_f'].append(t)
        tb['n_f'].append(df[df[f"TEMPO {group}"]>=t].shape[0])
        if t!=0:
            tb['m_f'].append(df[(df[f"TEMPO {group}"]==t) & (df[f"CENSORED {group}"]==False)].shape[0])
            val = df[(df[f"TEMPO {group}"]==t) & (df[f"CENSORED {group}"]==True)].shape[0]
            tb['q_f'].append(val)
        else:
            tb['m_f'].append(0)
            tb['q_f'].append(df[df[f"TEMPO {group}"]<=0].shape[0])
    return pd.DataFrame(tb), df

def kaplan_meier_estimate(tb_surv, t_0=0):
    '''
        Calculate the Kaplan-Meier estimate from a survival table. Confidence intervals
        are also calculated.

        Args:
            tb_surv:
                pandas.DataFrame.
            t_0:
                Integer.
        Return:
            tb_surv:
                pandas.DataFrame.
    '''
    St = [1 for n in range(0,t_0+1)]
    St_CI = [0.0 for n in range(0, t_0+1)]
    for j in range(t_0+1, tb_surv.shape[0]):
        cur_St = St[-1]*(tb_surv["n_f"].iat[j]-tb_surv['m_f'].iat[j])/(tb_surv["n_f"].iat[j])
        St.append(cur_St)
        # Calculate the Confidence interval magnitude
        sum_term = (tb_surv[t_0+1:j]["m_f"]/(tb_surv[t_0+1:j]['n_f']*(tb_surv[t_0+1:j]['n_f'] - tb_surv[t_0+1:j]['m_f']))).sum()
        St_CI.append((cur_St**2)*sum_term)
    tb_surv["KM S^(t)"] = St
    tb_surv["1 - KM"] = 1 - tb_surv["KM S^(t)"]
    tb_surv["KM S^(t) - Confidence Interval"] = 1.96*np.sqrt(np.array(St_CI))
    tb_surv["KM S^(t) lower"] = tb_surv["KM S^(t)"] - tb_surv["KM S^(t) - Confidence Interval"]
    tb_surv["KM S^(t) upper"] = tb_surv["KM S^(t)"] + tb_surv["KM S^(t) - Confidence Interval"]
    return tb_surv

def get_casocontrole_survival(surv_caso, surv_controle, t_0=0):
    '''
        Generate survival table for cases and controls together with the risk ratios, 
        effectiveness and confidence intervals.

        Args:
            df1:
                pandas.DataFrame.
            age:
                tuple of integers.
            sex:
                String. 
            t_0:
                Integer.
        Return:
            final:
                pandas.DataFrame.
    '''
    surv_caso1 = kaplan_meier_estimate(surv_caso, t_0=t_0)
    surv_controle1 = kaplan_meier_estimate(surv_controle, t_0=t_0)

    surv_caso1 = surv_caso1.add_suffix("(caso)")
    surv_controle1 = surv_controle1.add_suffix("(controle)")

    final = surv_caso1.merge(surv_controle1, left_on="t_f(caso)", right_on="t_f(controle)", how="left")
    final["1 - Risk Ratio"] = 1 - final["1 - KM(caso)"]/final["1 - KM(controle)"]
    # --> Confidence interval
    VE_CI = []
    n1, n2 = final["n_f(caso)"].iat[1], final["n_f(controle)"].iat[1]
    for j in range(final.shape[0]):
        x1 = final["m_f(caso)"].iloc[:j].sum()
        x2 = final["m_f(controle)"].iloc[:j].sum()
        ci_upper = np.log(1 - 1.0*final["1 - Risk Ratio"].iat[j]) + 1.96*np.sqrt((n1-x1)/(n1*x1) + (n2-x2)/(n2*x2))
        ci_lower = np.log(1 - 1.0*final["1 - Risk Ratio"].iat[j]) - 1.96*np.sqrt((n1-x1)/(n1*x1) + (n2-x2)/(n2*x2))
        ci_upper = np.exp(ci_upper)
        ci_lower = np.exp(ci_lower)
        VE_CI.append((1-ci_upper, 1-ci_lower))
    final["1 - Risk Ratio (CI_lower)"] = [ ve[0] for ve in VE_CI ]
    final["1 - Risk Ratio (CI_upper)"] = [ ve[1] for ve in VE_CI ]
    return final

def transform_observations(df, df_pop):
    '''
        From the table of intervals for each individuals in the cohort, increment
        the data with general info on sex, age, date of birth and others. 

        Args:
            df:
                pandas.DataFrame.
            df_pop:
                pandas.DataFrame.
        Return:
            new_df:
                pandas.DataFrame.
    '''
    new_df = {"CPF": [], "TEMPO PARA EVENTO D1": [], "EVENTO D1": [],
              "TEMPO PARA EVENTO D2": [], "EVENTO D2": [], "VACINADO": [] }
    for j in range(0, df.shape[0]):
        cpf_caso, cpf_controle = df["CPF CASO"].iat[j], df["CPF CONTROLE"].iat[j]
        tempo_caso_d1, tempo_controle_d1 = df["CASO D1 INTERVALO"].iat[j], df["CONTROLE D1 INTERVALO"].iat[j]
        tempo_caso_d2, tempo_controle_d2 = df["CASO D2 INTERVALO"].iat[j], df["CONTROLE D2 INTERVALO"].iat[j]
        evento_caso_d1, evento_controle_d1 = df["CASO D1 CENSURADO"].iat[j], df["CONTROLE D1 CENSURADO"].iat[j]
        evento_caso_d2, evento_controle_d2 = df["CASO D2 CENSURADO"].iat[j], df["CONTROLE D2 CENSURADO"].iat[j]

        new_df["CPF"] += [cpf_caso, cpf_controle]
        new_df["TEMPO PARA EVENTO D1"] += [tempo_caso_d1, tempo_controle_d1]
        new_df["TEMPO PARA EVENTO D2"] += [tempo_caso_d2, tempo_controle_d2]
        new_df["EVENTO D1"] += [evento_caso_d1, evento_controle_d1]
        new_df["EVENTO D2"] += [evento_caso_d2, evento_controle_d2]
        new_df["VACINADO"] += [1, 0]
    new_df = pd.DataFrame(new_df)
    new_df["EVENTO D1"] = new_df["EVENTO D1"].apply(lambda x: 1 if not x else (0 if not pd.isna(x) else np.nan))
    new_df["EVENTO D2"] = new_df["EVENTO D2"].apply(lambda x: 1 if not x else (0 if not pd.isna(x) else np.nan))

    col_info = ["cpf", "data_nascimento", "bairro", "sexo", "idade", "faixa etaria(VACINADOS)", "bairro id(VACINADOS)"]
    df_pop = df_pop[col_info]

    new_df = new_df.merge(df_pop, left_on="CPF", right_on="cpf", how='left')
    return new_df

# --> AUX
def calc_interval(x, sbst):
    '''
    
    '''
    if pd.isna(x[sbst[0]]) or pd.isna(x[sbst[1]]):
        return np.nan
    else:
        return (x[sbst[0]].date() - x[sbst[1]].date()).days