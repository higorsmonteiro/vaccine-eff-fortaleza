'''

'''
import os
import pandas as pd
import datetime as dt
import lib.utils as utils 
import lib.db_utils as dutils
import lib.outcomes_utils as oututils
from collections import defaultdict

from src.join_tests import JOIN_TESTER
# Vacine Já and Vacinados databases.

# --> LOAD ALL DATA
paths_data, names_data = dutils.data_hash()

vacinejaplus_path = os.path.join("..", "..", "data", "vacinado_obito", "VACINEJAPLUS.csv")
vacinejaplus_df = pd.read_csv(vacinejaplus_path)
vacinejaplus_df["data_nascimento"] = pd.to_datetime(vacinejaplus_df["data_nascimento"], format="%Y-%m-%d", errors="coerce") 

# TESTS (GAL & IntegraSUS)
gal_integra_path = os.path.join("..", "..", "data", "testes_covid19", "GAL_INTEGRA_JOINED.csv")
gal_integra_df = pd.read_csv(gal_integra_path)
# DEATHS BY COVID-19
obitos_path = os.path.join(paths_data["OBITOS COVID-19"], names_data["OBITOS COVID-19"])
obitos_df = dutils.open_obitos(obitos_path)
# HOSPITALIZATION DUE COVID-19
hospt_path = os.path.join(paths_data["HOSPITALIZACAO COVID-19"], names_data["HOSPITALIZACAO COVID-19"])
hospt_df = dutils.open_hospitalizacao(hospt_path)

# ---> DEFINE ALL THE TESTS
# --> DEATHS
### 1
test1 = {
    "NOME TRATADO_vacineja": "nome_tratado"
}
left1_date = defaultdict(lambda: False)
right1_date = defaultdict(lambda: False)
### 2
test2 = {
    "NOME TRATADO_vacineja": "nome_tratado",
    "data_nascimento": "data_nascimento"
}
left2_date = defaultdict(lambda: False)
right2_date = defaultdict(lambda: False)
left2_date.update({
    "data_nascimento": True
})
right2_date.update({
    "data_nascimento": True
})
### 3
test3 = {
    "NOME TRATADO_vacineja": "nome_tratado",
    "NOME MAE TRATADO": "nome_mae_tratado"
}
left3_date = defaultdict(lambda: False)
right3_date = defaultdict(lambda: False)
### 4
test4 = {
    "NOME TRATADO_vacineja": "nome_tratado",
    "bairro_vacineja": "bairro_ajustado"
}
left4_date = defaultdict(lambda: False)
right4_date = defaultdict(lambda: False)

obj1 = JOIN_TESTER()
test1_df = obj1.vacinejaplus_obitos(vacinejaplus_df, obitos_df, test1, left1_date, right1_date)
test2_df = obj1.vacinejaplus_obitos(vacinejaplus_df, obitos_df, test2, left2_date, right2_date)
test3_df = obj1.vacinejaplus_obitos(vacinejaplus_df, obitos_df, test3, left3_date, right3_date)
test4_df = obj1.vacinejaplus_obitos(vacinejaplus_df, obitos_df, test4, left4_date, right4_date)

print(test1_df.shape, test2_df.shape, test3_df.shape, test4_df.shape)