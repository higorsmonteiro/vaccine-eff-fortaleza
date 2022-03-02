import os
import lib.utils as utils
import lib.matching_aux as aux

class PerformMatching:
    def __init__(self, final_schema, cohort=None):
        '''
            Description.

            Args:
                final_schema:
                    pandas.DataFrame.
                cohort:
                    2-Tuple of datetime.datetime. Define beginning and ending of 
                    the cohort.
        '''
        self.fschema = final_schema.copy()
        self.cohort = cohort
        self.init_cohort = cohort[0]
        self.final_cohort = cohort[1]
        self.fschema["VACINA APLICADA"] = self.fschema["VACINA APLICADA"].fillna("NAO VACINADO")

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
        
        df, df_vac, df_nonvac = aux.initial_filtering(self.fschema, vaccine, age_range, self.cohort)
        datelst = utils.generate_date_list(self.cohort[0], self.cohort[1], interval=1)
        self.pareados, self.events_df, self.matched = aux.setup_scheme(df_vac, df_nonvac, datelst, seed)

        self.pareados.to_parquet(os.path.join(output_folder, f"PAREADOS_CPF_{seed}.parquet"))
        self.events_df.to_parquet(os.path.join(output_folder, f"EVENTOS_PAREADOS_{seed}.parquet"))
        