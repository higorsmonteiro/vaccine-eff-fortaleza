'''
    AUXILIAR FUNCTIONS FOR THE MAIN CLASSES.
'''
from importlib_metadata import re
import numpy as np
import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def f_testes(x1, x2):
    '''
        Description.

        Args:
            x1:
            x2:
        Return:
            x:
    '''
    if not pd.isna(x1):
        return x1
    elif not pd.isna(x2):
        return x2
    else:
        return np.nan

def f_d1d2(x1, x2):
    '''
        Compare D1 and D2 vaccination dates to find inconsistencies between them.

        Args:
            x1:
                dt.datetime.date. D1 vaccination date.
            x2:
                dt.datetime.date. D2 vaccination date.
        Return:
            String.
    '''
    d1 = x1
    d2 = x2
    if pd.isna(d1) and not pd.isna(d2):
        return "N"
    elif not pd.isna(d1) and not pd.isna(d2):
        d1 = d1.date()
        d2 = d2.date()
        if d1>d2:
            return "N"
        else:
            return "S"
    else:
        return "S"

def f_resultado(x):
    '''
        Format GAL test result column.
    '''
    res_str = ["Resultado: Não Detectável", "Resultado: Detectável"]
    if pd.isna(x): return "OUTROS"
    if x==res_str[0]:
        return "NEGATIVO"
    elif x==res_str[1]:
        return "POSITIVO"
    else:
        return "OUTROS"

def f_resultado_integra(x):
    '''
        Format IntegraSUS test result column.
    '''
    if pd.isna(x): return "OUTROS"
    if x=="POSITIVO":
        return "POSITIVO"
    else:
        return "OUTROS"

def f_sexo_gal(x):
    '''
        Format sex column from GAL database.
    '''
    if pd.isna(x): return np.nan
    if x=="MASCULINO":
        return "M"
    elif x=="FEMININO":
        return "F"
    else:
        return np.nan

def f_sexo(x):
    if pd.isna(x): return np.nan
    if x=="MASCULINO":
        return "M"
    elif x=="FEMININO":
        return "F"
    else:
        return np.nan

def f_sexo_integra(x):
    if pd.isna(x): return np.nan
    if x=="MASC":
        return "M"
    elif x=="FEM":
        return "F"
    else:
        return np.nan

def f_idade(x, cur_date):
    curd = cur_date
    nasc = x["data_nascimento"]
    cart = x["data falecimento(CARTORIOS)"]
    covid = x["data_obito(OBITO COVID)"]
    if not pd.isna(cur_date):
        if not pd.isna(cart) and cart.date()<curd:
            curd = cart.date()
        elif not pd.isna(covid) and covid.date()<curd:
            curd = covid.date()
    else:
        return np.nan
    return relativedelta(curd, nasc.date()).years

def f_vaccination_death(death_date, d1_date, d2_date, d3_date, d4_date):
    '''
    
    '''
    if pd.isna(death_date):
        return "N"
    else:
        death_date = death_date.date()
        if not pd.isna(d4_date):
            if d4_date.date()>death_date:
                return "S"
            elif not pd.isna(d3_date) and d3_date.date()>death_date:
                return "S"
            elif not pd.isna(d2_date) and d2_date.date()>death_date:
                return "S"
            elif not pd.isna(d1_date) and d1_date.date()>death_date:
                return "S"
            return "N"
        elif not pd.isna(d3_date) and d3_date.date()>death_date:
            return "S"
        elif not pd.isna(d2_date) and d2_date.date()>death_date:
            return "S"
        elif not pd.isna(d1_date) and d1_date.date()>death_date:
            return "S"
        else:
            return "N"
    
def f_eligible_test(x, init_cohort, final_cohort):
    '''
    
    '''
    result = x["RESULTADO FINAL GAL-INTEGRASUS"]
    coleta = x["Data da Coleta(GAL)"]
    solicit = x["Data da Solicitação(GAL)"]
    if pd.isna(result):
        return "APTO"
    elif result=="POSITIVO":
        if not pd.isna(solicit):
            solicit = solicit.date()
            if solicit<init_cohort:
                return "NAO APTO"
            else:
                return "APTO"
        elif not pd.isna(coleta):
            coleta = coleta.date()
            if coleta<init_cohort:
                return "NAO APTO"
            else:
                return "APTO"
        else:
            return "NAO APTO"
    else:
        return "APTO"

def f_when_vaccine(x, init_cohort, final_cohort):
    '''
        
    '''
    d1 = x["data D1(VACINADOS)"]
    d2 = x["data D2(VACINADOS)"]

    if pd.isna(d1): d1 = -1
    else: d1 = d1.date()
    if pd.isna(d2): d2 = -1
    else: d2 = d2.date()

    if d1!=-1 and d1<init_cohort:
        return "D1 ANTES DA COORTE"
    elif d1!=-1 and d1>=init_cohort and d1<=final_cohort:
        return "D1 DURANTE COORTE"
    elif d1!=-1 and d1>final_cohort:
        return "D1 APOS COORTE"
    elif d2!=-1:
        return "INVALIDO - D2 ANTES DA D1"
    else:
        return "NAO VACINADO"

def f_immunization(x, init_cohort, final_cohort, partial=14, fully=14):
    '''
        Classify the maximum immunization status during the cohort. 
    '''
    d1 = x["data D1(VACINADOS)"]
    d2 = x["data D2(VACINADOS)"]

    if pd.isna(d1): d1 = -1
    else: d1 = d1.date()
    if pd.isna(d2): d2 = -1
    else: d2 = d2.date()

    if d1!=-1 and (d1<init_cohort or d1>final_cohort):
        return "D1 FORA DA COORTE"
    if d1==-1 and d2!=-1:
        return "INVALIDO - D2 ANTES DA D1"

    if d2!=-1 and d2+timedelta(days=fully)<=final_cohort:
        return "TOTALMENTE IMUNIZADO"
    if d2!=-1 and d2+timedelta(days=partial)>final_cohort:
        return "PARCIALMENTE IMUNIZADO"
    if d1!=-1 and d1>=init_cohort and d1+timedelta(days=partial)<=final_cohort:
        return "PARCIALMENTE IMUNIZADO"
    if d1!=-1 and d1<=final_cohort and d1+timedelta(days=partial)>final_cohort:
        return "VACINADO SEM IMUNIZACAO"
    else:
        return "NAO VACINADO"