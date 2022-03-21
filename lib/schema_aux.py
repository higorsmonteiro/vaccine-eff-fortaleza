import numpy as np
import pandas as pd

def select_indexes(integra_df, indexes):
    '''
        Auxiliary function to verify whether a given individual has a positive COVID-19 test
        before the beginning of the cohort. 

        The dates of the cohort are already included within 'integra_df' columns: 'INFO COORTE
        SINTOMAS', 'INFO COORTE SOLICITACAO' and 'INFO COORTE COLETA'. These columns contain 
        "SIM" if the individual has at least one positive test before the start of the cohort. 

        Args:
            integra_df:
                pandas.DataFrame.
            indexes:
                list of integers.
        Return:
            String.
    '''
    selected = integra_df.loc[indexes]
    if np.isin(["SIM"], selected["INFO COORTE SINTOMAS"].values)[0]:
        return True
    if np.isin(["SIM"], selected["INFO COORTE SOLICITACAO"].values)[0]:
        return True
    if np.isin(["SIM"], selected["INFO COORTE COLETA"].values)[0]:
        return True
    return False

def compare_vaccine_death(x, 
                          cols = {
                              "OBITO": "DATA OBITO",
                              "D1": "DATA D1",
                              "D2": "DATA D2",
                              "D3": "DATA D3",
                              "D4": "DATA D4",
                          }):
    '''
        Compare the date of death (if any) and compare with the dates of the vaccines
        to find possible inconsistencies.
    '''
    obito = x[cols["OBITO"]]
    d1 = x[cols["D1"]]
    d2 = x[cols["D2"]]
    d3 = x[cols["D3"]]
    d4 = x[cols["D4"]]
    if pd.isna(obito):
        return False
    else:
        if pd.notna(d4):
            if d4>obito:
                return True
            elif pd.notna(d3) and d3>obito:
                return True
            elif pd.notna(d2) and d2>obito:
                return True
            elif pd.notna(d1) and d1>obito:
                return True
            return False
        elif pd.notna(d3) and d3>obito:
            return True
        elif pd.notna(d2) and d2>obito:
            return True
        elif pd.notna(d1) and d1>obito:
            return True
        else:
            return False

def vaccination_during_cohort(x, init_cohort, final_cohort):
    '''
        Show vaccination status of each individual during cohort period.
    '''
    d1 = x['DATA D1']
    d2 = x['DATA D2']
    d3 = x['DATA D3']
    d4 = x['DATA D4']
    
    status = ''
    if pd.notna(d1) and (d1>=init_cohort and d1<=final_cohort):
        status+='(D1)'
    if pd.notna(d2) and (d2>=init_cohort and d2<=final_cohort):
        status+='(D2)'
    if pd.notna(d3) and (d3>=init_cohort and d3<=final_cohort):
        status+='(D3)'
    if pd.notna(d4) and (d4>=init_cohort and d4<=final_cohort):
        status+='(D4)'
    return status

def vaccination_status(x):
    '''
    
    '''
    d1 = x['DATA D1']
    d2 = x['DATA D2']
    d3 = x['DATA D3']
    d4 = x['DATA D4']
    
    status = ''
    if pd.notna(d1):
        status+='(D1)'
    if pd.notna(d2):
        status+='(D2)'
    if pd.notna(d3):
        status+='(D3)'
    if pd.notna(d4):
        status+='(D4)'
    return status

def new_uti_date(x, cohort):
    '''
    
    '''
    if not np.any(pd.notna(x)):
        return np.nan
    x = np.sort([xx for xx in x if pd.notna(xx)]) 
    condition = (x>=cohort[0]) & (x<=cohort[1])
    if x[condition].shape[0]>0:
        return x[condition][0]
    else:
        return np.nan