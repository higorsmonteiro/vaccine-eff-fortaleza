import os
import pandas as pd
import lib.utils as utils
import lib.matching_aux as aux
from collections import defaultdict

class PerformMatching:
    def __init__(self, final_schema, cohort=None, colnames=None):
        '''
            Description.

            Args:
                final_schema:
                    pandas.DataFrame.
                cohort:
                    2-Tuple of datetime.datetime. Define beginning and ending of 
                    the cohort.
                colnames:
                    Dictionary. 
        '''
        self.fschema = final_schema.copy()
        self.cohort = cohort
        self.init_cohort = cohort[0]
        self.final_cohort = cohort[1]
        self.pareados = None
        self.events_df = None

        self.colnames = {
            "D1": "DATA D1",
            "D2": "DATA D2",
            "OBITO COVID": "DATA OBITO",
            "OBITO GERAL": "DATA FALECIMENTO(CARTORIOS)",
            "HOSPITALIZACAO COVID": "DATA HOSPITALIZACAO",
        }

    def perform_matching(self, output_folder, vaccine="CORONAVAC", age_range=(18,200), seed=0):
        '''
            Perform the matching mechanism to find the case-control pairs.

            After selecting all individuals who took the specified vaccine at a day
            during cohort, we find all possible controls for the cases each day. 
            Matching is performed using sex and age variables.

            Args:  
                vaccine:
                    String. {"CORONAVAC", "ASTRAZENECA", "PFIZER"}. Vaccine to consider
                    during matching for the cases.
                age_range:
                    2-Tuple of integers. Range of age to consider during matching process.
            Return:
                ...
        '''

        # Perform essential filtering for eligibility criteria.
        df, df_vac, df_nonvac = aux.initial_filtering(self.fschema, vaccine, age_range, self.cohort)
        datelst = utils.generate_date_list(self.cohort[0], self.cohort[1], interval=1)

        df_pop = pd.concat([df_vac, df_nonvac])
        '''
            Create hash structures for: 
                control_used: To signal used controls in the matching process.
                control_reservoir: To withdraw eligible controls for a given case.
                control_dates: Quick access to an individual dates based on its cpf.
        '''
        control_used = defaultdict(lambda: False)
        control_reservoir = defaultdict(lambda: [])
        control_dates = {
            "D1": defaultdict(lambda: -1),
            "D2": defaultdict(lambda: -1),
            "DEATH COVID": defaultdict(lambda: -1),
            "DEATH GENERAL": defaultdict(lambda: -1),
            "HOSPITALIZATION COVID": defaultdict(lambda: -1),
        }
        # --> Coletando datas para toda população considerada
        aux.collect_dates_for_cohort(df_pop, control_reservoir, control_dates, self.colnames)
        aux.rearrange_controls(control_reservoir, seed)
        self.pareados, self.matched = aux.perform_matching(datelst, df_vac, control_reservoir, control_used, control_dates, self.colnames)
    
        self.events_df = aux.get_events(df_pop, self.pareados, self.matched, self.colnames)
        df_pop["PAREADO"] = df_pop["CPF"].apply(lambda x: True if self.matched[x] else False)
        
        self.pareados.to_parquet(os.path.join(output_folder, f"PAREADOS_CPF_{seed}.parquet"))
        self.events_df.to_parquet(os.path.join(output_folder, f"EVENTOS_PAREADOS_{seed}.parquet"))

    def generate_survival_info(self, output_folder, seed=0):
        '''
            Description.
        '''
        if self.pareados is None:
            self.pareados = pd.read_parquet(os.path.join(output_folder, f"PAREADOS_CPF_{seed}.parquet"))
            self.events_df = pd.read_parquet(os.path.join(output_folder, f"EVENTOS_PAREADOS_{seed}.parquet"))

        events_col = {
            "D1": "DATA D1",
            "D2": "DATA D2",
            "OBITO COVID": "DATA OBITO COVID",
            "OBITO GERAL": "DATA OBITO GERAL",
        }
        
        events = self.events_df[pd.notna(self.events_df["PAR"])]
        events = events[events["PAREADO"]==True]
        f = lambda x: {"CPF": x["CPF"], "DATA D1": x["DATA D1"], "DATA D2": x["DATA D2"], "DATA OBITO COVID": x["DATA OBITO COVID"],
                       "DATA OBITO GERAL": x["DATA OBITO GERAL"], "DATA HOSPITALIZACAO": x["DATA HOSPITALIZACAO"], "TIPO": x["TIPO"]}
        events["DICT_INFO"] = events.apply(f, axis=1)
        events["KEY_DICT"] = events["CPF"]+events["TIPO"]
        hashdict = dict(zip(events["KEY_DICT"], events["DICT_INFO"]))
        self.events_caso = events[events["TIPO"]=="CASO"]
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type(x))
        self.events_caso = self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO"], axis=1)
        self.survival_table = aux.organize_table_for_survival(self.events_caso)

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_{seed}.parquet"))        
        self.events_caso.to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_{seed}.parquet"))
        
        