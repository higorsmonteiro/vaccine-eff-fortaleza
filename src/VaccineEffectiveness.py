'''

'''
import os
import pandas as pd
import numpy as np
import lib.ve_aux as aux
from lifelines import KaplanMeierFitter

class VaccineEffectiveness:
    def __init__(self, final_schema, cohort, event="OBITO"):
        '''
        
        '''
        self.fschema = final_schema
        self.cohort = cohort
        self.event = event

    def initialize_objects(self):
        '''
        
        '''
        template = {
            "CASO": KaplanMeierFitter(),
            "CONTROLE": KaplanMeierFitter()
        }
        self.km_objects = {
            "D1": dict(template),
            "D2": dict(template),
            "D1_MALE": dict(template),
            "D2_MALE": dict(template),
            "D1_FEMALE": dict(template),
            "D2_FEMALE": dict(template),
            "D1_6069": dict(template),
            "D2_6069": dict(template),
            "D1_7079": dict(template),
            "D2_7079": dict(template),
            "D1_80+": dict(template),
            "D2_80+": dict(template)
        }

    def load_survival_data(self, survival_folder, seed=1):
        '''
        
        '''
        fname = os.path.join(survival_folder, f"SURVIVAL_CORONAVAC_D1D2_{self.event}_{self.seed}.parquet")
        self.fsurvival = pd.read_parquet(fname)
        # Obtain demographic data
        self.fsurvival = self.fsurvival.merge(self.fschema[["CPF", "BAIRRO", "IDADE", "SEXO"]], on="CPF", how="left")

    def fit_data(self):
        '''
        
        '''
        aux.fit_dose(self.fsurvival, self.km_objects, self.event)
        aux.fit_sex(self.fsurvival, self.km_objects, "M", self.event)
        aux.fit_sex(self.fsurvival, self.km_objects, "F", self.event)
        aux.fit_age(self.fsurvival, self.km_objects, (60,69), self.event)
        aux.fit_age(self.fsurvival, self.km_objects, (70,79), self.event)
        aux.fit_age(self.fsurvival, self.km_objects, (80,200), self.event)

        