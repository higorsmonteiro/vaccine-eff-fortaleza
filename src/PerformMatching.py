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
                suffix:
                    String. String to append to the filenames. 
        '''
        self.fschema = final_schema.copy()
        self.cohort = cohort
        self.init_cohort = cohort[0]
        self.final_cohort = cohort[1]
        self.pareados = None
        self.events_df = None

        # Define HDI variables:
        self.fschema["IDH 0"] = self.fschema["IDH2010"].apply(lambda x: aux.f_hdi_range(x, [0.0, 1.0], include_nans=True))
        self.fschema["IDH 1"] = self.fschema["IDH2010"].apply(lambda x: aux.f_hdi_range(x, [0.0, 0.550, 0.70, 0.80, 1.00]))
        self.fschema["IDH 2"] = self.fschema["IDH2010"].apply(lambda x: aux.f_hdi_range(x, [0.0, 0.499, 0.599, 0.699, 0.799, 1.0]))

        if colnames is None:
            self.colnames = {
                "D1": "DATA D1",
                "D2": "DATA D2",
                "OBITO GERAL": "DATA FALECIMENTO(CARTORIOS)",
                "PRI SINTOMAS": "INTEGRA PRI SINTOMAS DATA",
                "OBITO COVID": "DATA OBITO",
                "HOSPITALIZACAO COVID": "DATA HOSPITALIZACAO",
                "UTI COVID": "DATA UTI",
            }
        else:
            self.colnames = colnames
    
    def perform_matching(self, output_folder, vaccine="CORONAVAC", age_range=(18,200), HDI_index=0, seed=1, pop_test="ALL"):
        '''
            Perform the matching mechanism to find the case-control pairs.

            After selecting all individuals who took the specified vaccine at the current
            day during cohort which does not have a positive test (or either 
            death or hospitalization) before the vaccination day, we find all possible
            controls for the eligible cases each day. Matching is performed using sex, age
            and HDI. Pairs are between two health individuals without preexisting infection.

            Args:
                output_folder:
                    String. Destiny folder for the matched pairs file and for the events file. 
                vaccine:
                    String. {"CORONAVAC", "ASTRAZENECA", "PFIZER"}. Vaccine to consider
                    during matching for the cases.
                age_range:
                    2-Tuple of integers. Range of age to consider during matching process.
                seed:
                    Integer. Seed to feed the pseudorandom shuffling of potential controls.
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
        # Perform essential filtering for eligibility criteria.
        df, df_vac, df_nonvac = aux.initial_filtering(self.fschema, vaccine, age_range, HDI_index=HDI_index, pop_test=pop_test)
        # List of dates comprising the cohort period.
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
            "D1": defaultdict(lambda: -1), "D2": defaultdict(lambda: -1), "PRI SINTOMAS": defaultdict(lambda: -1),
            "DEATH COVID": defaultdict(lambda: -1), "DEATH GENERAL": defaultdict(lambda: -1),
            "HOSPITALIZATION COVID": defaultdict(lambda: -1), "UTI COVID": defaultdict(lambda: -1),
        }
        # Collect dates for the whole population considered (fill 'control_reservoir' and 'control_dates')
        aux.collect_dates_for_cohort(df_pop, control_reservoir, control_dates, HDI_index=HDI_index, col_names=self.colnames)
        # According to the given seed shuffle the potential controls.
        aux.rearrange_controls(control_reservoir, seed)
        # Perform matching based on the variables defined for matching (age, sex) -> maybe include the 'IDH'.
        self.pareados, self.matched = aux.perform_matching(datelst, df_vac, control_reservoir, control_used, control_dates, HDI_index, self.cohort, self.colnames)
    
        self.events_df = aux.get_events(df_pop, self.pareados, self.matched, self.colnames)
        df_pop["PAREADO"] = df_pop["CPF"].apply(lambda x: True if self.matched[x] else False)
        
        self.pareados.to_parquet(os.path.join(output_folder, f"PAREADOS_CPF_{seed}.parquet"))
        self.events_df.to_parquet(os.path.join(output_folder, f"EVENTOS_PAREADOS_{seed}.parquet"))

    def generate_survival_info(self, output_folder, seed=1):
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

        # --> Reduce hospitalization dates to only one (the first date in the cohort period)
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
        # --> Perform comparison for Covid19 death as event.
        self.events_caso["RESULT"] = self.events_caso.apply(lambda x: aux.compare_pair_survival(x["DICT_INFO"], hashdict[x["PAR"]+"CONTROLE"], events_col, self.final_cohort, col_event="OBITO COVID"), axis=1)
        self.events_caso["FINAL SURVIVAL"] = self.events_caso["RESULT"].apply(lambda x: aux.define_interval_type(x))
        self.events_caso["CASO D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 INTERVALO"])
        self.events_caso["CASO D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D1 CENSURADO"])
        self.events_caso["CASO D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 INTERVALO"])
        self.events_caso["CASO D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CASO D2 CENSURADO"])
        self.events_caso["CONTROLE D1 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 INTERVALO"])
        self.events_caso["CONTROLE D1 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D1 CENSURADO"])
        self.events_caso["CONTROLE D2 INTERVALO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 INTERVALO"])
        self.events_caso["CONTROLE D2 CENSURADO"] = self.events_caso["FINAL SURVIVAL"].apply(lambda x: x["CONTROLE D2 CENSURADO"])
        self.survival_table = aux.organize_table_for_survival(self.events_caso, event_string="OBITO")

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_OBITO_{seed}.parquet"))        
        self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1).to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_OBITO_{seed}.parquet"))

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

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_HOSPITAL_{seed}.parquet"))        
        self.events_caso.drop(["DICT_INFO", "KEY_DICT", "PAREADO", "RESULT", "FINAL SURVIVAL"], axis=1).to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_HOSPITAL_{seed}.parquet"))

        # --> Perform comparison for Covid19 UTI as event.
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

        self.survival_table.to_parquet(os.path.join(output_folder, "SURVIVAL", f"SURVIVAL_CORONAVAC_D1D2_UTI_{seed}.parquet"))        
        self.events_caso.to_parquet(os.path.join(output_folder, f"PAREADOS_COM_INTERVALOS_UTI_{seed}.parquet"))

        self.pareados = None
        self.events_df = None

    def random_matching(self):
        '''
        
        '''
        pass
        
        