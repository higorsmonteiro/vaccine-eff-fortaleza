import os
import pandas as pd
import numpy as np
import seaborn as sns
import datetime as dt
import lib.utils as utils
import lib.fig_aux as aux
import lib.matching_aux as aux_mat
import lib.schema_aux as aux_sch
import matplotlib.pyplot as plt

class FigUtils:
    def __init__(self, fschema, vaccine, cohort):
        '''
            Args:
                fschema:
                    pandas.DataFrame.
                vaccine:
                    String.
                cohort:
                    2-tuple of datetime variables.
        '''
        self.fschema = fschema.copy()
        # Define HDI variables:
        self.fschema["IDH 0"] = self.fschema["IDH2010"].apply(lambda x: aux_mat.f_hdi_range(x, [0.0, 1.0], include_nans=True))
        self.fschema["IDH 1"] = self.fschema["IDH2010"].apply(lambda x: aux_mat.f_hdi_range(x, [0.0, 0.550, 0.70, 0.80, 1.00]))
        self.fschema["IDH 2"] = self.fschema["IDH2010"].apply(lambda x: aux_mat.f_hdi_range(x, [0.0, 0.499, 0.599, 0.699, 0.799, 1.0]))
        
        self.vaccine = vaccine
        self.cohort = cohort

        self.obito_df = None
        self.cartorio_df = None
        self.sivep_df = None
        self.integra_df = None

        self.km_hash = None

    def summary_fschema(self):
        '''
            Describe basic summary of the final schema file.
        '''
        summary = aux.summary_fschema(self.fschema)
        return summary
    
    def load_outcomes(self, parquet_folder):
        '''
            Open the outcome files. 
        '''
        fnames = {
            "OBITO": "OBITO_COVID.parquet",
            "CARTORIO": "OBITO_CARTORIO.parquet",
            "SIVEP": "SIVEP_COVID19_2021.parquet",
            "INTEGRASUS": "INTEGRASUS.parquet",
        }
        self.obito_df = pd.read_parquet(os.path.join(parquet_folder, fnames["OBITO"]))
        self.cartorio_df = pd.read_parquet(os.path.join(parquet_folder, fnames["CARTORIO"]))
        self.sivep_df = pd.read_parquet(os.path.join(parquet_folder, "SIVEP-GRIPE", fnames["SIVEP"]))
        self.integra_df = pd.read_parquet(os.path.join(parquet_folder, fnames["INTEGRASUS"]))

        # --> Define hospitalization date
        col = ["DT_NOTIFIC", "DT_INTERNA", "DT_ENTUTI"]
        self.sivep_df["DATA HOSPITALIZACAO"] = self.sivep_df[col].apply(lambda x: x[col[1]] if np.any(pd.notna(x[col[1]])) else (x[col[0]] if np.any(pd.notna(x[col[0]])) else np.nan), axis=1)
        # --> Join SIVEP COLUMNS
        col_sivep = ["PRIMARY_KEY", "DATA HOSPITALIZACAO", "DT_ENTUTI"]
        self.sivep_df = self.sivep_df[col_sivep].astype(str).groupby("PRIMARY_KEY").agg(";".join).reset_index()
        f = lambda x: [pd.to_datetime(xx) for xx in x.split(";")] if pd.notna(x) else np.nan
        self.sivep_df["DATA HOSPITALIZACAO"] = self.sivep_df["DATA HOSPITALIZACAO"].apply(f)
        self.sivep_df["DATA HOSPITALIZACAO"] = self.sivep_df["DATA HOSPITALIZACAO"].apply(lambda x: aux_mat.new_hospitalization_date(x, self.cohort))
        self.sivep_df["DT_ENTUTI"] = self.sivep_df["DT_ENTUTI"].apply(lambda x: [pd.to_datetime(xx) for xx in x.split(";")] if pd.notna(x) else np.nan)
        self.sivep_df["DT_ENTUTI"] = self.sivep_df["DT_ENTUTI"].apply(lambda x: x if not np.all(pd.isna(x)) else np.nan)
        self.sivep_df["DATA UTI"] = self.sivep_df["DT_ENTUTI"].apply(lambda x: aux_sch.new_uti_date(x, self.cohort))

        self.fschema["DATA HOSPITALIZACAO"] = self.fschema["DATA HOSPITALIZACAO"].apply(lambda x: aux_mat.new_hospitalization_date(x, self.cohort))
        self.fschema["DATA UTI"] = self.fschema["DATA UTI"].apply(lambda x: aux_mat.new_hospitalization_date(x, self.cohort))

    def vaccine_coverage(self, return_=False):
        '''
        
        '''
        datelst = utils.generate_date_list(dt.datetime(2021, 1, 1), dt.datetime(2021, 12, 31), interval=1)
        self.vaccine_cov = pd.DataFrame(
            {
                "date": datelst,
            }
        )
        fs_1 = self.fschema[self.fschema["STATUS VACINACAO"].str.contains("(D1)", regex=False)]
        fs_2 = self.fschema[self.fschema["STATUS VACINACAO"].str.contains("(D1)(D2)", regex=False)]
        count_d1 = fs_1["DATA D1"].value_counts().sort_index().reset_index().rename({'index': 'data_d1', 'DATA D1': 'DATA D1 GERAL'}, axis=1)
        count_d2 = fs_2["DATA D2"].value_counts().sort_index().reset_index().rename({'index': 'data_d2', 'DATA D2': 'DATA D2 GERAL'}, axis=1)

        fcoronavac_1 = self.fschema[(self.fschema["VACINA APLICADA"]=="CORONAVAC") & (self.fschema["STATUS VACINACAO"].str.contains("(D1)", regex=False))]
        fastra_1 = self.fschema[(self.fschema["VACINA APLICADA"]=="ASTRAZENECA") & (self.fschema["STATUS VACINACAO"].str.contains("(D1)", regex=False))]
        fpfizer_1 = self.fschema[(self.fschema["VACINA APLICADA"]=="PFIZER") & (self.fschema["STATUS VACINACAO"].str.contains("(D1)", regex=False))]
        fcoronavac_2 = self.fschema[(self.fschema["VACINA APLICADA"]=="CORONAVAC") & (self.fschema["STATUS VACINACAO"].str.contains("(D1)(D2)", regex=False))]
        fastra_2 = self.fschema[(self.fschema["VACINA APLICADA"]=="ASTRAZENECA") & (self.fschema["STATUS VACINACAO"].str.contains("(D1)(D2)", regex=False))]
        fpfizer_2 = self.fschema[(self.fschema["VACINA APLICADA"]=="PFIZER") & (self.fschema["STATUS VACINACAO"].str.contains("(D1)(D2)", regex=False))]
        
        count_d1cor = fcoronavac_1["DATA D1"].value_counts().sort_index().reset_index().rename({'index': 'data_d1_cor', 'DATA D1': 'DATA D1 CORONAVAC'}, axis=1)
        count_d2cor = fcoronavac_2["DATA D2"].value_counts().sort_index().reset_index().rename({'index': 'data_d2_cor', 'DATA D2': 'DATA D2 CORONAVAC'}, axis=1)
        count_d1ast = fastra_1["DATA D1"].value_counts().sort_index().reset_index().rename({'index': 'data_d1_ast', 'DATA D1': 'DATA D1 ASTRAZENECA'}, axis=1)
        count_d2ast = fastra_2["DATA D2"].value_counts().sort_index().reset_index().rename({'index': 'data_d2_ast', 'DATA D2': 'DATA D2 ASTRAZENECA'}, axis=1)
        count_d1pf = fpfizer_1["DATA D1"].value_counts().sort_index().reset_index().rename({'index': 'data_d1_pf', 'DATA D1': 'DATA D1 PFIZER'}, axis=1)
        count_d2pf = fpfizer_2["DATA D2"].value_counts().sort_index().reset_index().rename({'index': 'data_d2_pf', 'DATA D2': 'DATA D2 PFIZER'}, axis=1)

        # --> Merge columns
        self.vaccine_cov = self.vaccine_cov.merge(count_d1, left_on="date", right_on="data_d1", how="left").drop("data_d1", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d2, left_on="date", right_on="data_d2", how="left").drop("data_d2", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d1cor, left_on="date", right_on="data_d1_cor", how="left").drop("data_d1_cor", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d2cor, left_on="date", right_on="data_d2_cor", how="left").drop("data_d2_cor", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d1ast, left_on="date", right_on="data_d1_ast", how="left").drop("data_d1_ast", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d2ast, left_on="date", right_on="data_d2_ast", how="left").drop("data_d2_ast", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d1pf, left_on="date", right_on="data_d1_pf", how="left").drop("data_d1_pf", axis=1)
        self.vaccine_cov = self.vaccine_cov.merge(count_d2pf, left_on="date", right_on="data_d2_pf", how="left").drop("data_d2_pf", axis=1)
        self.vaccine_cov = self.vaccine_cov.fillna(0)

        if return_:
            return self.vaccine_cov
    
    def outcomes_2021(self, return_=False):
        '''
            Generate dataframe holding the number of outcomes along the year of 2021. 
        '''
        datelst = utils.generate_date_list(dt.datetime(2021, 1, 1), dt.datetime(2021, 12, 31), interval=1)
        self.only_outcomes = pd.DataFrame(
            {
                "date": datelst,
            }
        )
        obito_dates = self.obito_df["data_obito(OBITO COVID)"].value_counts().reset_index().rename({"data_obito(OBITO COVID)": "obito total"}, axis=1)
        hosp_dates = self.sivep_df["DATA HOSPITALIZACAO"].value_counts().reset_index().rename({"DATA HOSPITALIZACAO": "hospitalizacao total"}, axis=1)
        uti_dates = self.sivep_df["DATA UTI"].value_counts().reset_index().rename({"DATA UTI": "uti total"}, axis=1)
        # --> Merge columns
        self.only_outcomes = self.only_outcomes.merge(obito_dates, left_on="date", right_on="index", how="left")
        self.only_outcomes = self.only_outcomes.merge(hosp_dates, left_on="date", right_on="index", how="left")
        self.only_outcomes = self.only_outcomes.merge(uti_dates, left_on="date", right_on="index", how="left")
        self.only_outcomes = self.only_outcomes.drop(["index_x", "index_y"], axis=1).fillna(0)
        if return_:
            return self.only_outcomes
    
    def compare_outcomes(self, return_=False):
        '''
            Calculate the number of outcomes occurred in each day found in the 'Vacine Já' and 
            the total occurred in the official databases.
        '''
        datelst = utils.generate_date_list(dt.datetime(2021,1,1), dt.datetime(2021,12,31), interval=1)
        self.outcomes_df = pd.DataFrame({
            "date": datelst,
        })
        obito_dates = self.obito_df["data_obito(OBITO COVID)"].value_counts().reset_index().rename({"data_obito(OBITO COVID)": "obito total"}, axis=1)
        obito_vacineja = self.fschema["DATA OBITO"].value_counts().reset_index().rename({"DATA OBITO": "obito vacineja"}, axis=1)
        hosp_dates = self.sivep_df["DATA HOSPITALIZACAO"].value_counts().reset_index().rename({"DATA HOSPITALIZACAO": "hospitalizacao total"}, axis=1)
        hosp_vacineja = self.fschema["DATA HOSPITALIZACAO"].value_counts().reset_index().rename({"DATA HOSPITALIZACAO": "hospitalizacao vacineja"}, axis=1)
        uti_dates = self.sivep_df["DATA UTI"].value_counts().reset_index().rename({"DATA UTI": "uti total"}, axis=1)
        uti_vacineja = self.fschema["DATA UTI"].value_counts().reset_index().rename({"DATA UTI": "uti vacineja"}, axis=1)
        # --> Merge columns
        self.outcomes_df = self.outcomes_df.merge(obito_dates, left_on="date", right_on="index", how="left")
        self.outcomes_df = self.outcomes_df.merge(obito_vacineja, left_on="date", right_on="index", how="left")
        self.outcomes_df = self.outcomes_df.merge(hosp_dates, left_on="date", right_on="index", how="left")
        self.outcomes_df = self.outcomes_df.merge(hosp_vacineja, left_on="date", right_on="index", how="left")
        self.outcomes_df = self.outcomes_df.merge(uti_dates, left_on="date", right_on="index", how="left")
        self.outcomes_df = self.outcomes_df.merge(uti_vacineja, left_on="date", right_on="index", how="left")
        self.outcomes_df = self.outcomes_df.drop(["index_x", "index_y"], axis=1).fillna(0)

        if return_:
            return self.outcomes_df

    def recruitment_cohort(self, events_df, hdi_index, return_=False, age_range=(60,200)):
        '''
            Temporal series exhibiting the number of matched individuals out of the eligible.
        '''
        self.recruit_df = aux.recruitment_cohort(self.fschema, events_df, self.vaccine, self.cohort, hdi_index, age_range=age_range)
        if return_:
            return self.recruit_df

    def info_for_cohort_diagram(self, pairs_df, include_hdi=True):
        '''
            Method to obtain the info to draw the cohort diagram for each vaccine.

            Args:
                pairs_df:
                    pandas.DataFrame.
                include_hdi:
                    Bool.
            Return:
                List of Strings. 
        '''
        df = self.fschema
        cohort_end = self.cohort[1]
        output_str = aux.cohort_diagram(df, pairs_df, cohort_end, self.vaccine, include_hdi=include_hdi)
        return output_str

    def get_km_curves(self, path, hdi_index, event="OBITO", t_min=0):
        '''
        
        '''
        fig, AX = plt.subplots(2, 3, figsize=(12,8.5))
        fig.suptitle(f"{event} - "+r"$t_{min} = $"+f"{t_min}, HDI index = {hdi_index}")
        for row in [0, 1]:
            AX[row,0].set_ylabel("# events", fontsize=14)
            AX[row,1].set_ylabel("KM estimate", fontsize=14)
            AX[row,2].set_ylabel("1 - Risk Ratio", fontsize=14)
            for col in [0,1,2]:
                if row==0:
                    AX[row, col].set_xlim([0,40])
                else:
                    AX[row, col].set_xlim([0,150])
                if col==2:
                    AX[row,col].set_ylim([0,1])
                AX[row,col].grid(alpha=0.25)

        km_hash = pd.read_excel(os.path.join(path, f"KM_events_{t_min}_{event}.xlsx"), sheet_name=None)
        ve_hash = pd.read_excel(os.path.join(path, f"VE_{t_min}_{event}.xlsx"), sheet_name=None)

        # --> KM CURVES
        kmd1 = km_hash["KM_D1"]
        kmd2 = km_hash["KM_D2"]
        kmd1["observed(caso)-cumm"] = kmd1["observed(caso)"].cumsum()
        kmd1["observed(controle)-cumm"] = kmd1["observed(controle)"].cumsum()
        kmd2["observed(caso)-cumm"] = kmd2["observed(caso)"].cumsum()
        kmd2["observed(controle)-cumm"] = kmd2["observed(controle)"].cumsum()

        sns.lineplot(x='t', y='observed(caso)-cumm', data=kmd1, ax=AX[0,0], color="tab:blue", label="CASO")
        sns.lineplot(x='t', y='observed(controle)-cumm', data=kmd1, ax=AX[0,0], color="tab:orange", label="CONTROLE")

        sns.lineplot(x='t', y='observed(caso)-cumm', data=kmd2, ax=AX[1,0], color="tab:blue")
        sns.lineplot(x='t', y='observed(controle)-cumm', data=kmd2, ax=AX[1,0], color="tab:orange")

        sns.lineplot(x='t', y='KM(caso)', data=kmd1, ax=AX[0,1], color="tab:blue")
        sns.lineplot(x='t', y='KM(controle)', data=kmd1, ax=AX[0,1], color="tab:orange")
        AX[0,1].fill_between(kmd1['t'], kmd1['KM_lower_0.95(caso)'], kmd1['KM_upper_0.95(caso)'], color="tab:blue", alpha=0.2)
        AX[0,1].fill_between(kmd1['t'], kmd1['KM_lower_0.95(controle)'], kmd1['KM_upper_0.95(controle)'], color="tab:orange", alpha=0.2)

        sns.lineplot(x='t', y='KM(caso)', data=kmd2, ax=AX[1,1], color="tab:blue")
        sns.lineplot(x='t', y='KM(controle)', data=kmd2, ax=AX[1,1], color="tab:orange")
        AX[1,1].fill_between(kmd2['t'], kmd2['KM_lower_0.95(caso)'], kmd2['KM_upper_0.95(caso)'], color="tab:blue", alpha=0.2)
        AX[1,1].fill_between(kmd2['t'], kmd2['KM_lower_0.95(controle)'], kmd2['KM_upper_0.95(controle)'], color="tab:orange", alpha=0.2)

        # --> VE CURVES
        ved1 = ve_hash["D1"]
        ved2 = ve_hash["D2"]
        sns.lineplot(x='t', y='VE', data=ved1, ax=AX[0,2], color="tab:blue")
        AX[0,2].fill_between(ved1['t'], ved1['VE_lower_0.95'], ved1['VE_upper_0.95'], color="tab:blue", alpha=0.2)

        sns.lineplot(x='t', y='VE', data=ved2, ax=AX[1,2], color="tab:blue")
        AX[1,2].fill_between(ved2['t'], ved2['VE_lower_0.95'], ved2['VE_upper_0.95'], color="tab:blue", alpha=0.2)

        return (fig, AX)

    def ve_plot(self, path, event="OBITO", suffix=""):
        '''

        '''
        ve_hash_0 = pd.read_excel(os.path.join(path, f"VE_0_{event}.xlsx"), sheet_name=None)
        ve_hash_14 = pd.read_excel(os.path.join(path, f"VE_14_{event}.xlsx"), sheet_name=None)

        ve_d1_0 = ve_hash_0[f"D1{suffix}"].set_index("t")
        ve_d1_14 = ve_hash_14[f"D1{suffix}"].set_index("t")
        ve_d2_14 = ve_hash_14[f"D2{suffix}"].set_index("t")

        labels = ["D1 0-14", "D1 0-21", "D1 14-21", "D1 14-27", "D2 14-45", "D2 14->|"]
        values_m095 = [0.0,0.0,0.0,0.0,0.0,0.0]
        values = [0.0,0.0,0.0,0.0,0.0,0.0]
        values_p095 = [0.0,0.0,0.0,0.0,0.0,0.0]

        values_m095[0], values[0], values_p095[0] = ve_d1_0["VE_lower_0.95"].at[14], ve_d1_0["VE"].at[14], ve_d1_0["VE_upper_0.95"].at[14]
        values_m095[1], values[1], values_p095[1] = ve_d1_0["VE_lower_0.95"].at[21], ve_d1_0["VE"].at[21], ve_d1_0["VE_upper_0.95"].at[21]
        values_m095[2], values[2], values_p095[2] = ve_d1_14["VE_lower_0.95"].at[21], ve_d1_14["VE"].at[21], ve_d1_14["VE_upper_0.95"].at[21]
        values_m095[3], values[3], values_p095[3] = ve_d1_14["VE_lower_0.95"].at[27], ve_d1_14["VE"].at[27], ve_d1_14["VE_upper_0.95"].at[27]
        values_m095[4], values[4], values_p095[4] = ve_d2_14["VE_lower_0.95"].at[45], ve_d2_14["VE"].at[45], ve_d2_14["VE_upper_0.95"].at[45]
        values_m095[5], values[5], values_p095[5] = ve_d2_14["VE_lower_0.95"].iat[-1], ve_d2_14["VE"].iat[-1], ve_d2_14["VE_upper_0.95"].iat[-1]

        return (labels, values_m095, values, values_p095)
        








        





    