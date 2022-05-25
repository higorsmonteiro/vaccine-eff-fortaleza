import os
import numpy as np
from tqdm import tqdm
import pandas as pd
import recordlinkage
from recordlinkage.index import SortedNeighbourhood

import lib.utils as utils


class ApproxLinkage:
    def __init__(self, storage_folder, parquet_folder):
        '''
        
        '''
        self.storage = storage_folder
        self.parquet_folder = parquet_folder
        self.colsforlink = ["primeiro_nome_mae", "segundo_nome_mae", "complemento_nome_mae", 
                            "dia_nascimento", "mes_nascimento", "ano_nascimento", "sexo", "primeiro_nome", 
                            "segundo_nome", "complemento_nome", "bairro", "cpf"]

        self.vacineja_df = None
        self.covid_obito_df = None
        self.cart_obito_df = None

    def load_data(self):
        '''
        
        '''
        self.vacineja_df = pd.read_parquet(os.path.join(self.parquet_folder, "VACINEJA.parquet"))
        self.covid_obito_df = pd.read_parquet(os.path.join(self.parquet_folder, "OBITO_COVID.parquet"))
        self.cart_obito_df = pd.read_parquet(os.path.join(self.parquet_folder, "OBITO_CARTORIO.parquet"))

        self.covid_obito_df = self.covid_obito_df.merge(self.cart_obito_df[["do_8", "cpf"]], left_on="numerodo", right_on="do_8", how="left").drop("do_8", axis=1)

    def format_data(self):
        '''
            pass
        '''
        # --> Vacine Já
        self.vacineja_df["nome"] = self.vacineja_df["nome"].apply(lambda x: utils.replace_string(x, sep=" "))
        self.vacineja_df["nome_mae"] = self.vacineja_df["nome_mae"].apply(lambda x: utils.replace_string(x, sep=" "))

        self.vacineja_df["primeiro_nome"] = self.vacineja_df["nome"].apply(lambda x: x.split(" ")[0] if pd.notna(x) and len(x.split(" "))>0 else np.nan)
        self.vacineja_df["segundo_nome"] = self.vacineja_df["nome"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan)
        self.vacineja_df["complemento_nome"] = self.vacineja_df["nome"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan)
        self.vacineja_df["primeiro_nome_mae"] = self.vacineja_df["nome_mae"].apply(lambda x: x.split(" ")[0] if pd.notna(x) and len(x.split(" "))>0 else np.nan)
        self.vacineja_df["segundo_nome_mae"] = self.vacineja_df["nome_mae"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan)
        self.vacineja_df["complemento_nome_mae"] = self.vacineja_df["nome_mae"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan)
        #self.vacineja_df["timestamp"] = self.vacineja_df["data_nascimento"].apply(lambda x: x.timestamp() if pd.notna(x) else np.nan)
        self.vacineja_df["dia_nascimento"] = self.vacineja_df["data_nascimento"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self.vacineja_df["mes_nascimento"] = self.vacineja_df["data_nascimento"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self.vacineja_df["ano_nascimento"] = self.vacineja_df["data_nascimento"].apply(lambda x: x.year if pd.notna(x) else np.nan)
        self.vacineja_df["cpf"] = self.vacineja_df["cpf"].copy()
        self.vacineja_df["bairro"] = self.vacineja_df["bairro"].copy()
        self.vacineja_df["sexo"] = self.vacineja_df["sexo"].copy()

        # --> Death by Covid-19
        self.covid_obito_df["primeiro_nome"] = self.covid_obito_df["NOME(OBITO COVID)"].apply(lambda x: x.split(" ")[0] if pd.notna(x) and len(x.split(" "))>0 else np.nan)
        self.covid_obito_df["segundo_nome"] = self.covid_obito_df["NOME(OBITO COVID)"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan)
        self.covid_obito_df["complemento_nome"] = self.covid_obito_df["NOME(OBITO COVID)"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan)
        self.covid_obito_df["primeiro_nome_mae"] = self.covid_obito_df["NOME_MAE(OBITO COVID)"].apply(lambda x: x.split(" ")[0] if pd.notna(x) and len(x.split(" "))>0 else np.nan)
        self.covid_obito_df["segundo_nome_mae"] = self.covid_obito_df["NOME_MAE(OBITO COVID)"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan)
        self.covid_obito_df["complemento_nome_mae"] = self.covid_obito_df["NOME_MAE(OBITO COVID)"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan)

        self.covid_obito_df["sexo"] = self.covid_obito_df["SEXO(OBITO COVID)"].map({"MASC": "M", "FEM": "F", "FEM ": "F", "MAS": "M", "MASC ": "M"})
        self.covid_obito_df["bairro"] = self.covid_obito_df["BAIRRO_RESIDENCIA(OBITO COVID)"].apply(lambda x: utils.replace_string(x, sep=" ") if pd.notna(x) else np.nan)
        self.covid_obito_df["data_nascimento"] = self.covid_obito_df["DATA_NASCIMENTO(OBITO COVID)"].copy()
        self.covid_obito_df["dia_nascimento"] = self.covid_obito_df["data_nascimento"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self.covid_obito_df["mes_nascimento"] = self.covid_obito_df["data_nascimento"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self.covid_obito_df["ano_nascimento"] = self.covid_obito_df["data_nascimento"].apply(lambda x: x.year if pd.notna(x) else np.nan)
        self.covid_obito_df["cpf"] = self.covid_obito_df["cpf"].copy()

    def create_total_pairs(self, chunksize=40000):
        '''
            pass
        '''
        vacineja_link = self.vacineja_df[self.colsforlink].reset_index(drop=True)
        covid_link = self.covid_obito_df[self.colsforlink].reset_index(drop=True)

        indexer_local = recordlinkage.Index()
        indexer_local.add(SortedNeighbourhood("primeiro_nome", "primeiro_nome", window=3))

        compare_cl = recordlinkage.Compare()
        compare_cl.string("primeiro_nome_mae", "primeiro_nome_mae", method="jarowinkler", threshold=0.8, label="primeiro_nome_mae")
        compare_cl.string("segundo_nome_mae", "segundo_nome_mae", method="jarowinkler", threshold=0.8, label="segundo_nome_mae")
        compare_cl.string("complemento_nome_mae", "complemento_nome_mae", method="jarowinkler", threshold=0.8, label="complemento_nome_mae")
        #compare_cl.string("primeiro_nome", "primeiro_nome", method="jarowinkler", threshold=0.8, label="primeiro_nome")
        compare_cl.string("segundo_nome", "segundo_nome", method="jarowinkler", threshold=0.8, label="segundo_nome")
        compare_cl.string("complemento_nome", "complemento_nome", method="jarowinkler", threshold=0.8, label="complemento_nome")
        compare_cl.string("bairro", "bairro", method="jarowinkler", threshold=0.70, label="bairro")
        compare_cl.exact("sexo", "sexo", label="sexo")
        compare_cl.exact("cpf", "cpf", label="cpf")
        compare_cl.exact("dia_nascimento", "dia_nascimento", label="dia_nascimento")
        compare_cl.exact("mes_nascimento", "mes_nascimento", label="mes_nascimento")
        compare_cl.exact("ano_nascimento", "ano_nascimento", label="ano_nascimento")
        #compare_cl.date("data_nascimento", "data_nascimento", label="data_nascimento", swap_month_day=0.8)
        
        chunks = np.split(vacineja_link, indices_or_sections=np.arange(chunksize, vacineja_link.shape[0], chunksize))
        for index, chunk in tqdm(enumerate(chunks)):
            candidate_links = indexer_local.index(chunk, covid_link)
            features = compare_cl.compute(candidate_links, chunk, covid_link)
            features["SOMA NASCIMENTO"] = features[["dia_nascimento", "mes_nascimento", "ano_nascimento"]].sum(axis=1)
            features["SOMA NASCIMENTO"] = features["SOMA NASCIMENTO"].apply(lambda x: 1 if x==3 else 0)
            features["SOMA"] = features[["primeiro_nome_mae", "segundo_nome_mae", "complemento_nome_mae",
                                         "segundo_nome", "complemento_nome", "cpf", "sexo", "bairro", "dia_nascimento",
                                         "mes_nascimento", "ano_nascimento"]].sum(axis=1)
            features["SOMA"] = features["SOMA"]+features["SOMA NASCIMENTO"]
            features = features[(features["SOMA"]>=6) | (features["cpf"]==1.0)]
            print(features.shape, candidate_links.shape)
            features.to_csv(os.path.join(self.storage, f"feature_{index}.csv"))
        