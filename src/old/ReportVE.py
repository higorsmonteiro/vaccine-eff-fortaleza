'''
    After we complete the pipeline, from the data preprocessing to the matching 
    and survival analysis, we now report the summaries and results of our output
    data.  
'''
import os
import pandas as pd
from tqdm import tqdm
import lib.utils as utils
from collections import defaultdict

class ReportVE:
    def __init__(self, data_path):
        self.data_path = data_path
    
    def get_recruitment(self, init_cohort, final_cohort, vaccine="corona", seed=1):
        '''
            Show
        '''
        folder = self.data_path["cohort_folder"]
        df = pd.read_csv(os.path.join(folder, self.data_path["pares"](vaccine, seed)))
        df["DATA D1"] = pd.to_datetime(df["DATA D1"], format="%Y-%m-%d", errors="coerce")

        df_eligible = df[(df["TIPO"]=="CASO") | (df["TIPO"]=="NAO PAREADO")]
        lst_date = utils.generate_date_list(init_cohort, final_cohort, interval=1)
        
        recruited = defaultdict(lambda:0)
        total_eligible = defaultdict(lambda:0)

        for cur_date in tqdm(lst_date):
            total_eligible[cur_date] = df_eligible[df_eligible["DATA D1"]==pd.Timestamp(cur_date.year, cur_date.month, cur_date.day)].shape[0]
            recruited[cur_date] = df_eligible[(df_eligible["DATA D1"]==pd.Timestamp(cur_date.year, cur_date.month, cur_date.day)) & (df_eligible["TIPO"]=="CASO")].shape[0]

        recruited_axis = [ recruited[x] for x in lst_date ]
        total_axis = [ total_eligible[x] for x in lst_date ]

        return lst_date, recruited_axis, total_axis

    def count_deaths(self, mode="D1", t_0="t_0", seed=1):
        '''
        
        '''
        folder = self.data_path["seed folder"](seed)
        df = pd.read_csv(os.path.join(folder, t_0, f"survival_tb_{mode}_{t_0}_{seed}.csv"))

        deaths_caso = df.set_index("t_f(caso)")["m_f(caso)"].loc[1:].sum()
        deaths_controle = df.set_index("t_f(controle)")["m_f(controle)"].loc[1:].sum()
        return deaths_caso, deaths_controle

    def get_survival_curves(self, mode="D1", t_0="t_0", seed=1):
        '''
        
        '''
        folder = self.data_path["seed folder"](seed)
        df = pd.read_csv(os.path.join(folder, t_0, f"survival_tb_{mode}_{t_0}_{seed}.csv"))
        
        for_plot = {
            "KM CASO": df["KM S^(t)(caso)"],
            "KM CONTROLE": df["KM S^(t)(controle)"],
            "KM CASO CI-": df["KM S^(t) lower(caso)"],
            "KM CASO CI+": df["KM S^(t) upper(caso)"],
            "KM CONTROLE CI-": df["KM S^(t) lower(controle)"],
            "KM CONTROLE CI+": df["KM S^(t) upper(controle)"],
            "VE": df["1 - Risk Ratio"],
            "VE CI-": df["1 - Risk Ratio (CI_lower)"],
            "VE CI+": df["1 - Risk Ratio (CI_upper)"]
        }
        return for_plot

    def generate_mean_coxhr(self):
        '''
        
        '''
        pass
