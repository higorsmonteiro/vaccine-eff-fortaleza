'''
    Class defined to execute the main functions regarding the matching algorithm.
'''
import os
import pandas as pd
import lib.utils as utils
from datetime import timedelta
import lib.matching_aux as aux
from collections import defaultdict

class PerformMatching:
    def __init__(self, final_schema, vaccine, hdi_index=0, days_after=7, cohort=None, colnames=None):
        '''
            Set the parameters to perform matching over the population given by 'final_schema'
            regarding a cohort defined by the tuple 'cohort'.

            Args:
                final_schema:
                    pandas.DataFrame. Data containing all available population together with 
                    the main information on vaccination dates, events and demographic information. 
                vaccine:
                    String. {"CORONAVAC", "ASTRAZENECA", "PFIZER"}. Vaccine to consider
                    during matching for the vaccinated. 
                hdi_index:
                    Integer.
                days_after:
                    Integer. 
                cohort:
                    2-Tuple of datetime.datetime. Define beginning and ending of 
                    the cohort.
                colnames:
                    Dictionary. Hash to signal the names of the columns used during matching.
        '''
        self.fschema = final_schema.set_index("CPF").copy()
        self.vaccine = vaccine
        self.hdi_index = hdi_index
        self.days_after = days_after
        self.cohort = cohort
        self.init_cohort = cohort[0]
        self.final_cohort = cohort[1]
        self.pareados = None
        self.events_df = None

        # Define HDI variables (Include NaN for IDH 0):
        self.fschema["IDH 0"] = pd.cut(self.fschema["IDH2010"], [0.0, 1.0]).astype(str).map({"(0.0, 1.0]": 0, "nan": 0})
        self.fschema["IDH 1"] = pd.cut(self.fschema["IDH2010"], [0.0, 0.250, 0.350, 0.500, 1.0]).astype(str).map(
            {
                "(0.0, 0.25]": 0, "(0.25, 0.35]": 1, "(0.35, 0.5]": 2, "(0.5, 1.0]": 3,
            })
        self.fschema["IDH 2"] = pd.cut(self.fschema["IDH2010"], [0.0, 0.499, 0.599, 0.699, 0.799, 1.0]).astype(str).map(
            {
                "(0.0, 0.499]": 0, "(0.499, 0.599]": 1, "(0.599, 0.699]": 2, "(0.699, 0.799]": 3, "(0.799, 1.0]": 4
            })
        # Join matching variables:
        self.fschema["MATCHING 0"] = list(zip(self.fschema["IDADE"].tolist(), self.fschema["SEXO"].tolist(), self.fschema["IDH 0"]))
        self.fschema["MATCHING 1"] = list(zip(self.fschema["IDADE"].tolist(), self.fschema["SEXO"].tolist(), self.fschema["IDH 1"]))
        self.fschema["MATCHING 2"] = list(zip(self.fschema["IDADE"].tolist(), self.fschema["SEXO"].tolist(), self.fschema["IDH 2"]))

        if colnames is None:
            self.colnames = {
                "D1": "DATA D1", "D2": "DATA D2", "OBITO GERAL": "DATA FALECIMENTO(CARTORIOS)",
                "PRI SINTOMAS": "INTEGRA PRI SINTOMAS DATA", "OBITO COVID": "DATA OBITO",
                "HOSPITALIZACAO COVID": "DATA HOSPITALIZACAO", "UTI COVID": "DATA UTI",
            }
        else:
            self.colnames = colnames
        
        # Subset of variables
        vars_ = list(self.colnames.values())
        # --> Data structure for quick access to event dates of individuals
        self.person_dates = self.fschema[vars_].to_dict(orient='index')
        # --> Data structures to signal info for potential control individuals.
        self.control_used = defaultdict(lambda: False)
        self.control_reservoir = defaultdict(lambda: [])
    
    def perform_matching(self, output_folder, date_str="DATA D1", age_range=(18,200), seed=1, pop_test="ALL"):
        '''
            Perform the matching algorithm to find the vaccinated-unvaccinated pairs.

            After selecting all individuals who took the specified vaccine at the current
            day during cohort which does not have a positive test (or either death, 
            hospitalization or ICU) before the vaccination day, we find all possible
            controls/unvaccinated for the eligible vaccinated each day. Matching is performed 
            using info on sex, age and HDI. Pairs are defined as two healthy individuals 
            without preexisting documented infection or Covid-19-related event.

            Args:
                output_folder:
                    String. Destiny folder for the matched pairs file and for the events file. 
                age_range:
                    2-Tuple of integers. Range of age to consider during matching process.
                seed:
                    Integer. Seed to feed the pseudorandom shuffling of potential controls/unvaccinated.
                pop_test:
                    String. {"ALL", "VACCINE"}. Test variable for obtaining pairs considering either 
                    the whole population or only population within a given vaccine:
                        "ALL" -> Controls are selected no matter whether individuals takes a different
                                 vaccine in future dates.
                        "VACCINE" -> Controls are selected within nonvaccinated people (never took a 
                                     vaccine) or people who took the selected vaccine in future dates.
            Return:
                None.
        '''
        # --> Perform essential filtering for eligibility criteria.
        df_vac, df_nonvac = aux.initial_filtering(self.fschema, self.vaccine, age_range, hdi_index=self.hdi_index, pop_test=pop_test)
        df_pop = pd.concat([df_vac, df_nonvac])
        # --> Set reservoir of controls
        matching_vars = df_pop.groupby(f'MATCHING {self.hdi_index}')
        self.control_reservoir = defaultdict(lambda: [], matching_vars.groups) # EX: x[(67,'M',3)] = [ list of cpfs having these variables ]

        # --> Shuffle the potential controls according to the given seed .
        aux.rearrange_controls(self.control_reservoir, seed)
        # --> Perform matching based on the variables defined for matching (age, sex, HDI) 
        self.pareados, self.matched = aux.perform_matching1(self.cohort, df_vac, self.control_reservoir, self.control_used, self.person_dates, self.hdi_index, date_str, self.days_after)
    
        df_pop = df_pop.reset_index().copy()
        self.events_df = aux.get_events(df_pop, self.pareados, self.matched, self.colnames)
        df_pop["PAREADO"] = df_pop["CPF"].apply(lambda x: True if self.matched[x] else False)

        # --> Output files
        fname_pairs = f"PAREADOS_CPF_D1_DAY{self.days_after}_{seed}.parquet"
        fname_events = f"EVENTOS_PAREADOS_D1_DAY{self.days_after}_{seed}.parquet"
        if date_str=="DATA D2":
            fname_pairs = f"PAREADOS_CPF_D2_DAY{self.days_after}_{seed}.parquet"
            fname_events = f"EVENTOS_PAREADOS_D2_DAY{self.days_after}_{seed}.parquet"
        
        self.pareados.to_parquet(os.path.join(output_folder, fname_pairs))
        self.events_df.to_parquet(os.path.join(output_folder, fname_events))

    def generate_survival_info(self, output_folder, date_str="DATA D1", seed=1):
        '''
            After the case-control pairs are defined and their event dates are obtained, 
            calculate the size of the survival intervals for each pair. The calculation is
            vectorized for performance.

            Args:
                output_folder:
                    String. 
                seed:
                    Integer.
            Return:
                None.
        '''
        if self.pareados is None:
            self.pareados = pd.read_parquet(os.path.join(output_folder, f"PAREADOS_CPF_{seed}.parquet"))
            self.events_df = pd.read_parquet(os.path.join(output_folder, f"EVENTOS_PAREADOS_{seed}.parquet"))

        # --> Reduce hospitalization and ICU dates to only one (the first date in the cohort period)
        self.events_df["DATA HOSPITALIZACAO"] = self.events_df["DATA HOSPITALIZACAO"].apply(lambda x: aux.new_hospitalization_date(x, self.cohort))
        self.events_df["DATA UTI"] = self.events_df["DATA UTI"].apply(lambda x: aux.new_hospitalization_date(x, self.cohort))
        
        events_col = {
            "D1": "DATA D1",
            "D2": "DATA D2",
            "OBITO COVID": "DATA OBITO COVID",
            "OBITO GERAL": "DATA OBITO GERAL",
            "HOSPITALIZACAO COVID": "DATA HOSPITALIZACAO",
            "UTI COVID": "DATA UTI",
        }
        
        events = self.events_df[pd.notna(self.events_df["PAR"])]
        events = events[events["PAREADO"]==True]
        # --> Vectorized calculation of intervals for cases and controls.
        f = lambda x: {"CPF": x["CPF"], "DATA D1": x["DATA D1"], "DATA D2": x["DATA D2"], "DATA OBITO COVID": x["DATA OBITO COVID"],
                       "DATA OBITO GERAL": x["DATA OBITO GERAL"], "DATA HOSPITALIZACAO": x["DATA HOSPITALIZACAO"], "DATA UTI": x["DATA UTI"], 
                       "TIPO": x["TIPO"]}
        events["DICT_INFO"] = events.apply(f, axis=1)
        events["KEY_DICT"] = events["CPF"]+events["TIPO"]
        hashdict = dict(zip(events["KEY_DICT"], events["DICT_INFO"]))
        self.events_caso = events[events["TIPO"]=="CASO"]
        # --> Perform comparison for Death by Covid-19 as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival_v2(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, dose=date_str, col_event="OBITO COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type_v2(x, date_str))

        #self.events_caso["CASO D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 INTERVALO"])
        #self.events_caso["CASO D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 CENSURADO"])
        #self.events_caso["CASO D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 INTERVALO"])
        #self.events_caso["CASO D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 CENSURADO"])
        #self.events_caso["CONTROLE D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 INTERVALO"])
        #self.events_caso["CONTROLE D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 CENSURADO"])
        #self.events_caso["CONTROLE D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 INTERVALO"])
        #self.events_caso["CONTROLE D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 CENSURADO"])
        self.events_caso["CASO INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO INTERVALO"])
        self.events_caso["CASO CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO CENSURADO"])
        self.events_caso["CONTROLE INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE INTERVALO"])
        self.events_caso["CONTROLE CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE CENSURADO"])
        self.survival_table = aux.organize_table_for_survival(self.events_caso, event_string="OBITO")

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_OBITO_DAY{date_str}_{seed}.parquet"))        
        self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1).to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_OBITO_DAY{date_str}_{seed}.parquet"))

        # --> Perform comparison for Covid19 hospitalization as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, col_event="HOSPITALIZACAO COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type(x))
        self.events_caso["CASO D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 INTERVALO"])
        self.events_caso["CASO D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 CENSURADO"])
        self.events_caso["CASO D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 INTERVALO"])
        self.events_caso["CASO D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 CENSURADO"])
        self.events_caso["CONTROLE D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 INTERVALO"])
        self.events_caso["CONTROLE D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 CENSURADO"])
        self.events_caso["CONTROLE D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 INTERVALO"])
        self.events_caso["CONTROLE D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 CENSURADO"])
        self.survival_table = aux.organize_table_for_survival(self.events_caso, event_string="HOSPITAL")

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_HOSPITAL_DAY{date_str}_{seed}.parquet"))        
        self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1).to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_HOSPITAL_DAY{date_str}_{seed}.parquet"))

        # --> Perform comparison for Covid-19 ICU admission as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, col_event="UTI COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type(x))
        self.events_caso["CASO D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 INTERVALO"])
        self.events_caso["CASO D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 CENSURADO"])
        self.events_caso["CASO D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 INTERVALO"])
        self.events_caso["CASO D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 CENSURADO"])
        self.events_caso["CONTROLE D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 INTERVALO"])
        self.events_caso["CONTROLE D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 CENSURADO"])
        self.events_caso["CONTROLE D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 INTERVALO"])
        self.events_caso["CONTROLE D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 CENSURADO"])
        self.survival_table = aux.organize_table_for_survival(self.events_caso, event_string="UTI")
        self.events_caso = self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1)

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_UTI_DAY{date_str}_{seed}.parquet"))        
        self.events_caso.to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_UTI_DAY{date_str}_{seed}.parquet"))

        self.pareados = None
        self.events_df = None

    def generate_survival_info_v2(self, output_folder, date_str="DATA D1", seed=1, delay_vaccine=0):
        '''
            After the case-control pairs are defined and their event dates are obtained, 
            calculate the size of the survival intervals for each pair. The calculation is
            vectorized for performance.

            Args:
                output_folder:
                    String. 
                delay_vaccine:
                    Integer. {default: None}. Parameter for sensitivy analysis. It increases the
                    date of receipt of the first dose of all controls individuals by the number 
                    of days given by this parameter.
                seed:
                    Integer.
            Return:
                None.
        '''
        survival_death_f = f"SURVIVAL_{date_str.split(' ')[1]}_OBITO_DAY{self.days_after}_{seed}.parquet"
        survival_hosp_f = f"SURVIVAL_{date_str.split(' ')[1]}_HOSPITAL_DAY{self.days_after}_{seed}.parquet"
        survival_icu_f = f"SURVIVAL_{date_str.split(' ')[1]}_UTI_DAY{self.days_after}_{seed}.parquet"
        pairs_death_f = f"PAREADOS_COM_INTERVALOS_{date_str.split(' ')[1]}_OBITO_DAY{self.days_after}_{seed}.parquet"
        pairs_hosp_f = f"PAREADOS_COM_INTERVALOS_{date_str.split(' ')[1]}_HOSPITAL_DAY{self.days_after}_{seed}.parquet"
        pairs_icu_f = f"PAREADOS_COM_INTERVALOS_{date_str.split(' ')[1]}_UTI_DAY{self.days_after}_{seed}.parquet" 
        if self.pareados is None:
            self.pareados = pd.read_parquet(os.path.join(output_folder, f"PAREADOS_CPF_{date_str.split(' ')[1]}_DAY{self.days_after}_{seed}.parquet"))
            self.events_df = pd.read_parquet(os.path.join(output_folder, f"EVENTOS_PAREADOS_{date_str.split(' ')[1]}_DAY{self.days_after}_{seed}.parquet"))

        # --> Reduce hospitalization and ICU dates to only one (the first date in the cohort period)
        self.events_df["DATA HOSPITALIZACAO"] = self.events_df["DATA HOSPITALIZACAO"].apply(lambda x: aux.new_hospitalization_date(x, self.cohort))
        self.events_df["DATA UTI"] = self.events_df["DATA UTI"].apply(lambda x: aux.new_hospitalization_date(x, self.cohort))

        if delay_vaccine>0:
            temp_f = lambda x: x["DATA D1"]+timedelta(days=delay_vaccine) if pd.notna(x["DATA D1"]) and x["TIPO"]=="CONTROLE" else x["DATA D1"]
            self.events_df["DATA D1"] = self.events_df[["DATA D1", "TIPO"]].apply(temp_f, axis=1)

        
        events_col = {
            "D1": "DATA D1",
            "D2": "DATA D2",
            "OBITO COVID": "DATA OBITO COVID",
            "OBITO GERAL": "DATA OBITO GERAL",
            "HOSPITALIZACAO COVID": "DATA HOSPITALIZACAO",
            "UTI COVID": "DATA UTI",
        }
        
        events = self.events_df[pd.notna(self.events_df["PAR"])]
        events = events[events["PAREADO"]==True]
        # --> Vectorized calculation of intervals for cases and controls.
        f = lambda x: {"CPF": x["CPF"], "DATA D1": x["DATA D1"], "DATA D2": x["DATA D2"], "DATA OBITO COVID": x["DATA OBITO COVID"],
                       "DATA OBITO GERAL": x["DATA OBITO GERAL"], "DATA HOSPITALIZACAO": x["DATA HOSPITALIZACAO"], "DATA UTI": x["DATA UTI"], 
                       "TIPO": x["TIPO"]}
        events["DICT_INFO"] = events.apply(f, axis=1)
        events["KEY_DICT"] = events["CPF"]+events["TIPO"]
        hashdict = dict(zip(events["KEY_DICT"], events["DICT_INFO"]))
        self.events_caso = events[events["TIPO"]=="CASO"]
        # --> Perform comparison for Death by Covid-19 as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival_v2(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, dose=date_str, col_event="OBITO COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type_v2(x, date_str))
        self.events_caso["CASO INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO INTERVALO"])
        self.events_caso["CASO CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO CENSURADO"])
        self.events_caso["CONTROLE INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE INTERVALO"])
        self.events_caso["CONTROLE CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE CENSURADO"])
        self.survival_table = aux.organize_table_for_survival_v2(self.events_caso, event_string="OBITO")

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", survival_death_f))        
        self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1).to_parquet(os.path.join(output_folder, pairs_death_f))

        # --> Perform comparison for Covid19 hospitalization as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival_v2(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, dose=date_str, col_event="HOSPITALIZACAO COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type_v2(x, date_str))
        self.events_caso["CASO INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO INTERVALO"])
        self.events_caso["CASO CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO CENSURADO"])
        self.events_caso["CONTROLE INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE INTERVALO"])
        self.events_caso["CONTROLE CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE CENSURADO"])
        self.survival_table = aux.organize_table_for_survival_v2(self.events_caso, event_string="HOSPITAL")

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", survival_hosp_f))        
        self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1).to_parquet(os.path.join(output_folder, pairs_hosp_f))

        # --> Perform comparison for Covid-19 ICU admission as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival_v2(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, dose=date_str, col_event="UTI COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type_v2(x, date_str))
        self.events_caso["CASO INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO INTERVALO"])
        self.events_caso["CASO CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO CENSURADO"])
        self.events_caso["CONTROLE INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE INTERVALO"])
        self.events_caso["CONTROLE CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE CENSURADO"])
        self.survival_table = aux.organize_table_for_survival_v2(self.events_caso, event_string="UTI")
        self.events_caso = self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1)

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", survival_icu_f))        
        self.events_caso.to_parquet(os.path.join(output_folder, pairs_icu_f))

        self.pareados = None
        self.events_df = None
        
        