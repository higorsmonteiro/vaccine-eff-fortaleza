'''
    This class represents the endpoint of the pipeline for vaccine effectiveness estimation.
    Using survival analysis we estimate VE as 1 - Risk Ratio, where risk ratio is obtained
    through the Kaplan-Meier curves. 

    The input data for this class are the 'final_schema' DataFrame generated by the 'DefineSchema'
    class and the 'pairs_df' DataFrame generated by the 'PerformMatching' class. Additional input
    concerns the type of event to be considered ('OBITO', 'HOSPITAL', 'UTI') and the suffix for 
    output data.
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
    def __init__(self, final_schema, pairs_df, cohort, dose="D1", days_after=0, event="OBITO"):
        '''
            Description.
        '''
        self.fschema = final_schema.copy()
        self.pairs_df = pairs_df.dropna(axis=0)
        self.pairs_hash = dict(zip(pairs_df["CPF CASO"], pairs_df["CPF CONTROLE"]))
        self.cohort = cohort
        self.dose = dose
        self.days_after = days_after
        self.event = event

        # Define HDI variables:
        #self.fschema["IDH 0"] = self.fschema["IDH2010"].apply(lambda x: aux.f_hdi_range(x, [0.0, 1.0], include_nans=True))
        #self.fschema["IDH 1"] = self.fschema["IDH2010"].apply(lambda x: aux.f_hdi_range(x, [0.0, 0.550, 0.70, 0.80, 1.00]))
        #self.fschema["IDH 2"] = self.fschema["IDH2010"].apply(lambda x: aux.f_hdi_range(x, [0.0, 0.499, 0.599, 0.699, 0.799, 1.0]))

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
            "DOSE": dict(template), "MALE": dict(template), "FEMALE": dict(template), 
            "6069": dict(template), "7079": dict(template), "80+": dict(template), 
            "HDI_000_059": dict(template), "HDI_060_1": dict(template),
            "HDI_000_050": dict(template), "HDI_050_1": dict(template),
        }
        for key in self.km_objects.keys():
            self.km_objects[key]["CASO"] = KaplanMeierFitter()
            self.km_objects[key]["CONTROLE"] = KaplanMeierFitter()
        
        # Store subsets of data regarding stratifications.
        self.sub_data = {
            "DOSE": dict(template), "MALE": dict(template), "FEMALE": dict(template), 
            "6069": dict(template), "7079": dict(template), "80+": dict(template), 
            "HDI_000_059": dict(template), "HDI_060_1": dict(template),
            "HDI_000_050": dict(template), "HDI_050_1": dict(template),
        }

        # Store final tables regarding Kaplan-Meier curves.
        self.etables = {
            "DOSE": None, "MALE": None, "FEMALE": None, 
            "6069": None, "7079": None, "80+": None, 
            "HDI_000_059": None, "HDI_060_1": None,
            "HDI_000_050": None, "HDI_050_1": None,
        }

        # Store figure/axis objects regarding Kaplan-Meier curves.
        self.cumul_figures = {
            "DOSE": None, "MALE": None, "FEMALE": None, 
            "6069": None, "7079": None, "80+": None, 
            "HDI_000_059": None, "HDI_060_1": None,
            "HDI_000_050": None, "HDI_050_1": None,
        }

    def load_survival_data(self, survival_folder, seed=1):
        '''
            Description.
        '''
        fname = os.path.join(survival_folder, f"SURVIVAL_{self.dose}_{self.event}_DAY{self.days_after}_{seed}.parquet")
        self.fsurvival = pd.read_parquet(fname)
        # Obtain demographic data
        self.fsurvival = self.fsurvival.merge(self.fschema[["CPF", "BAIRRO", "IDADE", "SEXO", f"IDH2010"]], on="CPF", how="left")

    def fit_data(self, t_min):
        '''
            Description.
        '''
        aux.fit_dose_v2(self.fsurvival, self.km_objects, self.sub_data, self.event, t_min)
        aux.fit_sex_v2(self.fsurvival, self.km_objects, self.sub_data, "M", self.event, t_min)
        aux.fit_sex_v2(self.fsurvival, self.km_objects, self.sub_data, "F", self.event, t_min)
        aux.fit_age_v2(self.fsurvival, self.pairs_hash, self.km_objects, self.sub_data, (60,69), self.event, t_min)
        aux.fit_age_v2(self.fsurvival, self.pairs_hash, self.km_objects, self.sub_data, (70,79), self.event, t_min)
        aux.fit_age_v2(self.fsurvival, self.pairs_hash, self.km_objects, self.sub_data, (80,200), self.event, t_min)

    def bootstrap_ve(self, survival_folder, n_resampling, seed, hdi=True):
        '''
            Calculate the vaccine effectiveness using the Kaplan-Meier estimate and the confidence intervals
            using the percentile bootstrap method.

            Args:
                base_folder:
                    String. Folder "PAREAMENTO".
                n_resampling:

                seed:
        '''
        fname_survival = os.path.join(survival_folder, f"SURVIVAL_{self.dose}_{self.event}_DAY{self.days_after}_{seed}.parquet")
        fsurvival = pd.read_parquet(fname_survival)
        fsurvival = fsurvival.merge(self.fschema[["CPF", "BAIRRO", "IDADE", "SEXO", f"IDH2010"]], on="CPF", how="left")

        caso = fsurvival[fsurvival["TIPO"]=="CASO"]
        controle = fsurvival[fsurvival["TIPO"]=="CONTROLE"]

        print(caso.shape, fsurvival.shape, self.pairs_df.shape)

        # Perform bootstrapping sampling 'n_resampling' times. 
        # For each bootstrapping sample, calculate 1 - RR for each time step and measure the confidence intervals.
        self.bootstrap_results = {
            "DOSE": [], "MALE": [], "FEMALE": [], 
            "6069": [], "7079": [], "80+": [], 
            "HDI_000_059": [], "HDI_060_1": [],
            "HDI_000_050": [], "HDI_050_1": [],
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
            aux.fit_dose_v2(r_fsurvival, km_objects, subdata, self.event)
            aux.fit_sex_v2(r_fsurvival, km_objects, subdata, "M", self.event)
            aux.fit_sex_v2(r_fsurvival, km_objects, subdata, "F", self.event)
            aux.fit_age_v2(r_fsurvival, self.pairs_hash, km_objects, subdata, (60,69), self.event)
            aux.fit_age_v2(r_fsurvival, self.pairs_hash, km_objects, subdata, (70,79), self.event)
            aux.fit_age_v2(r_fsurvival, self.pairs_hash, km_objects, subdata, (80,200), self.event)
            if hdi:
                aux.fit_hdi_v2(r_fsurvival, km_objects, subdata, self.event, hdi_strat=[0.0,0.5,1.0])
                aux.fit_hdi_v2(r_fsurvival, km_objects, subdata, self.event, hdi_strat=[0.0,0.590,1.0])

            # --> Calculate VE for each case
            ev_table = aux.generate_table(km_objects)
            for key in self.bootstrap_results.keys():
                if ev_table[key] is None:
                    continue
                ev_table[key] = ev_table[key].set_index("t")
                self.bootstrap_results[key].append((1 - (ev_table[key]["KM(caso)"]/ev_table[key]["KM(controle)"])))
                self.etables[key] = ev_table[key]

        # --> Generate the Kaplan-Meier curves
        self.generate_curves(km_objects)
        
        for key in self.bootstrap_results.keys():
            if self.bootstrap_results[key] is None:
                continue
            self.bootstrap_results[key] = pd.concat(self.bootstrap_results[key], axis=1)
            self.bootstrap_results[key]["VE_lower_0.95"] = self.bootstrap_results[key].apply(lambda x: np.percentile(x.to_numpy()[~np.isnan(x.to_numpy())], 2.5) if x.to_numpy()[~np.isnan(x.to_numpy())].shape[0]!=0 else np.nan, axis=1)
            self.bootstrap_results[key]["VE"] = self.bootstrap_results[key].apply(lambda x: np.mean( x.to_numpy()[~np.isnan(x.to_numpy())] ) if x.to_numpy()[~np.isnan(x.to_numpy())].shape[0]!=0 else np.nan, axis=1)
            self.bootstrap_results[key]["VE_upper_0.95"] = self.bootstrap_results[key].apply(lambda x: np.percentile(x.to_numpy()[~np.isnan(x.to_numpy())], 97.5) if x.to_numpy()[~np.isnan(x.to_numpy())].shape[0]!=0 else np.nan, axis=1)
            self.bootstrap_results[key] = self.bootstrap_results[key][["VE_lower_0.95", "VE", "VE_upper_0.95"]]
        
    
    def generate_tables(self):
        '''
        
        '''
        for key in self.etables.keys():
            if self.etables[key] is None:
                continue
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

    def generate_curves(self, km_objects=None):
        '''
            Generate the incidence curves for case and controls.
        '''
        # --> If 'km_objects' is not provided, generate the curves with the internal variable. 
        km_obj = km_objects
        if km_objects is None:
            km_obj = self.km_objects

        for key in km_obj.keys():
            if km_obj[key] is None:
                continue
            kmf_caso = km_obj[key]["CASO"]
            kmf_controle = km_obj[key]["CONTROLE"]

            fig, ax = plt.subplots(1, figsize=(8,6))
            ax = kmf_caso.plot_cumulative_density()
            ax = kmf_controle.plot_cumulative_density()
            #if "D1" in key:
            #    add_at_risk_counts(kmf_caso, kmf_controle, ax=ax, xticks=[0,20,40,60])
            #else:
            #    add_at_risk_counts(kmf_caso, kmf_controle, ax=ax, xticks=[0,20,40,60,80,100,120])
            add_at_risk_counts(kmf_caso, kmf_controle, ax=ax, xticks=[0,20,40,60,80,100,120])                
            self.cumul_figures[key] = (fig,ax)
            self.cumul_figures[key][1].set_title(key)
            #self.cumul_figures[key][1].savefig(os.path.join(folder, f"{key}_{t_min}.pdf"), dpi=120)

    def generate_output(self, seed_folder):
        '''
        
        '''
        with pd.ExcelWriter(os.path.join(seed_folder, f"VE_{self.dose}_{self.event}_DAY{self.days_after}.xlsx")) as writer:
            self.bootstrap_results["DOSE"].to_excel(writer, sheet_name=f"DOSE")
            self.bootstrap_results["MALE"].to_excel(writer, sheet_name=f"MALE")
            self.bootstrap_results["FEMALE"].to_excel(writer, sheet_name=f"FEMALE")
            self.bootstrap_results["6069"].to_excel(writer, sheet_name=f"6069")
            self.bootstrap_results["7079"].to_excel(writer, sheet_name=f"7079")
            self.bootstrap_results["80+"].to_excel(writer, sheet_name=f"80+")
            if self.bootstrap_results["HDI_000_059"] is not None:
                self.bootstrap_results["HDI_000_059"].to_excel(writer, sheet_name=f"HDI_000_059")
                self.bootstrap_results["HDI_060_1"].to_excel(writer, sheet_name=f"HDI_060_1")
            if self.bootstrap_results["HDI_000_050"] is not None:
                self.bootstrap_results["HDI_000_050"].to_excel(writer, sheet_name=f"HDI_000_050")
                self.bootstrap_results["HDI_050_1"].to_excel(writer, sheet_name=f"HDI_050_1")
        with pd.ExcelWriter(os.path.join(seed_folder, f"KM_events_{self.dose}_{self.event}_DAY{self.days_after}.xlsx")) as writer:
            self.etables["DOSE"].to_excel(writer, sheet_name=f"KM_DOSE")
            self.etables["MALE"].to_excel(writer, sheet_name=f"KM_MALE")
            self.etables["FEMALE"].to_excel(writer, sheet_name=f"KM_FEMALE")
            self.etables["6069"].to_excel(writer, sheet_name=f"KM_6069")
            self.etables["7079"].to_excel(writer, sheet_name=f"KM_7079")
            self.etables["80+"].to_excel(writer, sheet_name=f"KM_80+")
            if self.etables["HDI_000_059"] is not None:
                self.etables["HDI_000_059"].to_excel(writer, sheet_name=f"HDI_000_059")
                self.etables["HDI_060_1"].to_excel(writer, sheet_name=f"HDI_060_1")
            if self.etables["HDI_000_050"] is not None:
                self.etables["HDI_000_050"].to_excel(writer, sheet_name=f"HDI_000_050")
                self.etables["HDI_050_1"].to_excel(writer, sheet_name=f"HDI_050_1")
        
        # curves
        for key in self.cumul_figures.keys():
            self.cumul_figures[key][0].savefig(os.path.join(seed_folder, "FIGS", f"{key}_DAY{self.days_after}.pdf"))

    def km_curves(self, survival_folder, seed, hdi=True, t_min=0):
        '''
        
        '''
        fname_survival = os.path.join(survival_folder, f"SURVIVAL_{self.dose}_{self.event}_DAY{self.days_after}_{seed}.parquet")
        fsurvival = pd.read_parquet(fname_survival)
        fsurvival = fsurvival.merge(self.fschema[["CPF", "BAIRRO", "IDADE", "SEXO", f"IDH2010"]], on="CPF", how="left")

        caso = fsurvival[fsurvival["TIPO"]=="CASO"]
        controle = fsurvival[fsurvival["TIPO"]=="CONTROLE"]

        km_objects, subdata = aux.create_km_objects()
        # Fit data
        aux.fit_dose_v2(fsurvival, km_objects, subdata, self.event)
        aux.fit_sex_v2(fsurvival, km_objects, subdata, "M", self.event)
        aux.fit_sex_v2(fsurvival, km_objects, subdata, "F", self.event)
        aux.fit_age_v2(fsurvival, self.pairs_hash, km_objects, subdata, (60,69), self.event)
        aux.fit_age_v2(fsurvival, self.pairs_hash, km_objects, subdata, (70,79), self.event)
        aux.fit_age_v2(fsurvival, self.pairs_hash, km_objects, subdata, (80,200), self.event)
        if hdi:
            aux.fit_hdi_v2(fsurvival, km_objects, subdata, self.event, hdi_strat=[0.00,0.50,1.00])
            aux.fit_hdi_v2(fsurvival, km_objects, subdata, self.event, hdi_strat=[0.00,0.59,1.00])

        # --> Calculate VE for each case
        ev_table = aux.generate_table(km_objects)
        for key in ev_table.keys():
            self.etables[key] = ev_table[key]
            self.bootstrap_results[key]["VE_original"] = 1 - (ev_table[key]["KM(caso)"]/ev_table[key]["KM(controle)"])

        # --> Generate the Kaplan-Meier curves
        self.generate_curves(km_objects)
            



        