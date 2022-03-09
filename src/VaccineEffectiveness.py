'''

'''
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import lib.ve_aux as aux
from lifelines import KaplanMeierFitter
from lifelines.plotting import add_at_risk_counts
import matplotlib.pyplot as plt

class VaccineEffectiveness:
    def __init__(self, final_schema, pairs_df, cohort, event="OBITO"):
        '''
            Description.
        '''
        self.fschema = final_schema.copy()
        self.pairs_hash = dict(zip(pairs_df["CPF CASO"], pairs_df["CPF CONTROLE"]))
        self.cohort = cohort
        self.event = event

    def initialize_objects(self):
        '''
            Description.
        '''
        template = {
            "CASO": None,
            "CONTROLE": None
        }
        # Store the KaplanMeier objects fitted to each case data.
        self.km_objects = {
            "D1": dict(template), "D2": dict(template),
            "D1_MALE": dict(template), "D2_MALE": dict(template),
            "D1_FEMALE": dict(template), "D2_FEMALE": dict(template),
            "D1_6069": dict(template), "D2_6069": dict(template),
            "D1_7079": dict(template), "D2_7079": dict(template),
            "D1_80+": dict(template), "D2_80+": dict(template)
        }
        for key in self.km_objects.keys():
            self.km_objects[key]["CASO"] = KaplanMeierFitter()
            self.km_objects[key]["CONTROLE"] = KaplanMeierFitter()
        
        # Store subsets of data regarding stratifications.
        self.sub_data = {
            "D1": dict(template), "D2": dict(template), "D1_MALE": dict(template), "D2_MALE": dict(template),
            "D1_FEMALE": dict(template), "D2_FEMALE": dict(template), "D1_6069": dict(template), "D2_6069": dict(template),
            "D1_7079": dict(template), "D2_7079": dict(template), "D1_80+": dict(template), "D2_80+": dict(template)
        }

        # Store final tables regarding Kaplan Meier curves.
        self.etables = {
            "D1": None, "D2": None, "D1_MALE": None, "D2_MALE": None,
            "D1_FEMALE": None, "D2_FEMALE": None, "D1_6069": None, "D2_6069": None,
            "D1_7079": None, "D2_7079": None, "D1_80+": None, "D2_80+": None
        }

        # Store figure/axis objects regarding Kaplan Meier curves.
        self.cumul_figures = {
            "D1": None, "D2": None, "D1_MALE": None, "D2_MALE": None,
            "D1_FEMALE": None, "D2_FEMALE": None, "D1_6069": None, "D2_6069": None,
            "D1_7079": None, "D2_7079": None, "D1_80+": None, "D2_80+": None
        }

    def load_survival_data(self, survival_folder, vaccine="CORONAVAC", seed=1):
        '''
            Description.
        '''
        fname = os.path.join(survival_folder, f"SURVIVAL_{vaccine}_D1D2_{self.event}_{seed}.parquet")
        self.fsurvival = pd.read_parquet(fname)
        # Obtain demographic data
        self.fsurvival = self.fsurvival.merge(self.fschema[["CPF", "BAIRRO", "IDADE", "SEXO"]], on="CPF", how="left")

    def fit_data(self):
        '''
            Description.
        '''
        aux.fit_dose(self.fsurvival, self.km_objects, self.sub_data, self.event)
        aux.fit_sex(self.fsurvival, self.km_objects, self.sub_data, "M", self.event)
        aux.fit_sex(self.fsurvival, self.km_objects, self.sub_data, "F", self.event)
        aux.fit_age(self.fsurvival, self.pairs_hash, self.km_objects, self.sub_data, (60,69), self.event)
        aux.fit_age(self.fsurvival, self.pairs_hash, self.km_objects, self.sub_data, (70,79), self.event)
        aux.fit_age(self.fsurvival, self.pairs_hash, self.km_objects, self.sub_data, (80,200), self.event)

    def bootstrap_ve(self, survival_folder, n_resampling, seed):
        '''
            Calculate the vaccine effectiveness using the Kaplan-Meier estimate and the confidence intervals
            using the percentile bootstrap method.

            Args:
                base_folder:
                    String. Folder "PAREAMENTO".
                n_resampling:

                seed:
        '''
        fname_survival = os.path.join(survival_folder, f"SURVIVAL_CORONAVAC_D1D2_{self.event}_{seed}.parquet")
        fsurvival = pd.read_parquet(fname_survival)
        fsurvival = fsurvival.merge(self.fschema[["CPF", "BAIRRO", "IDADE", "SEXO"]], on="CPF", how="left")

        caso = fsurvival[fsurvival["TIPO"]=="CASO"]
        controle = fsurvival[fsurvival["TIPO"]=="CONTROLE"]

        # Perform bootstrapping sampling 'n_resampling' times. 
        # For each bootstrapping sample, calculate 1 - RR for each time step and measure the confidence intervals.
        self.bootstrap_results = {
            "D1": [], "D2": [], "D1_MALE": [], "D2_MALE": [],
            "D1_FEMALE": [], "D2_FEMALE": [], "D1_6069": [], "D2_6069": [],
            "D1_7079": [], "D2_7079": [], "D1_80+": [], "D2_80+": []
        }
        
        for ns in tqdm(range(n_resampling)):
            # Sampling with replacement for the cases
            caso_resamp = caso.sample(n=caso.shape[0], replace=True, random_state=ns)
            cpf_casos = caso_resamp["CPF"].tolist()
            cpf_controles = [ self.pairs_hash[cpf] for cpf in cpf_casos ]
            controle_resamp = controle[controle["CPF"].isin(cpf_controles)]
            r_fsurvival = pd.concat([caso_resamp, controle_resamp])

            km_objects, subdata = aux.create_km_objects()
            # Fit data
            aux.fit_dose(r_fsurvival, km_objects, subdata, self.event)
            aux.fit_sex(r_fsurvival, km_objects, subdata, "M", self.event)
            aux.fit_sex(r_fsurvival, km_objects, subdata, "F", self.event)
            aux.fit_age(r_fsurvival, self.pairs_hash, km_objects, subdata, (60,69), self.event)
            aux.fit_age(r_fsurvival, self.pairs_hash, km_objects, subdata, (70,79), self.event)
            aux.fit_age(r_fsurvival, self.pairs_hash, km_objects, subdata, (80,200), self.event)

            # --> Calculate VE for each case
            ev_table = aux.generate_table(km_objects)
            for key in self.bootstrap_results.keys():
                self.bootstrap_results[key].append((1 - (ev_table[key]["KM(caso)"]/ev_table[key]["KM(controle)"])))
        
        for key in self.bootstrap_results.keys():
            self.bootstrap_results[key] = pd.concat(self.bootstrap_results[key], axis=1)
            self.bootstrap_results[key]["VE_temp"] = self.bootstrap_results[key].apply(lambda x: (np.percentile(x.array, 2.5), np.mean(x.array), np.percentile(x.array, 97.5)), axis=1)
            self.bootstrap_results[key]["VE_lower_0.95"] = self.bootstrap_results[key]["VE_temp"].apply(lambda x: x[0])
            self.bootstrap_results[key]["VE"] = self.bootstrap_results[key]["VE_temp"].apply(lambda x: x[1])
            self.bootstrap_results[key]["VE_upper_0.95"] = self.bootstrap_results[key]["VE_temp"].apply(lambda x: x[2])
            self.bootstrap_results[key] = self.bootstrap_results[key].drop("VE_temp", axis=1)
        
    
    def generate_tables(self):
        '''
        
        '''
        for key in self.etables.keys():
            event_caso = self.km_objects[key]["CASO"].event_table.reset_index().add_suffix("(caso)").rename({"event_at(caso)": "t"}, axis=1)
            event_controle = self.km_objects[key]["CONTROLE"].event_table.reset_index().add_suffix("(controle)").rename({"event_at(controle)": "t"}, axis=1)
            S_caso = self.km_objects[key]["CASO"].cumulative_density_.reset_index().rename({'timeline': "t"}, axis=1)
            S_controle = self.km_objects[key]["CONTROLE"].cumulative_density_.reset_index().rename({'timeline': "t"}, axis=1)
            S_caso_ci = self.km_objects[key]["CASO"].confidence_interval_cumulative_density_.reset_index().rename({"index": "t"}, axis=1)
            S_controle_ci = self.km_objects[key]["CONTROLE"].confidence_interval_cumulative_density_.reset_index().rename({"index": "t"}, axis=1)

            final = event_caso.merge(S_caso, on="t", how="left")
            final = final.merge(S_caso_ci, on="t", how="left")
            final = final.merge(event_controle, on="t", how="left")
            final = final.merge(S_controle, on="t", how="left")
            final = final.merge(S_controle_ci, on="t", how="left")
            self.etables[key] = final.copy()
            self.etables[key] = self.etables[key].rename({"CASO": "KM(caso)", "CONTROLE": "KM(controle)",
                                                          "CONTROLE_lower_0.95": "KM_lower_0.95(controle)",
                                                          "CONTROLE_upper_0.95": "KM_upper_0.95(controle)",
                                                          "CASO_lower_0.95": "KM_lower_0.95(caso)",
                                                          "CASO_upper_0.95": "KM_upper_0.95(caso)"}, axis=1)

    def generate_curves(self):
        '''
            Generate the incidence curves for case and controls.
        '''
        for key in self.km_objects.keys():
            kmf_caso = self.km_objects[key]["CASO"]
            kmf_controle = self.km_objects[key]["CONTROLE"]

            fig, ax = plt.subplots(1, figsize=(8,6))
            ax = kmf_caso.plot_survival_function()
            ax = kmf_controle.plot_survival_function()
            add_at_risk_counts(kmf_caso, kmf_controle, ax=ax)

            self.cumul_figures[key] = (fig,ax)


        