import os
import numpy as np
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

config = {
    "fname": "VACINADOS20Jan2022.dta",
    "path_to_data_windows": os.path.join(os.environ["USERPROFILE"], "Documents", "data", "vacinado_obito"),
    "chunksize": 20000,
    'colnames': ["usuario", "cpf_usuario", "data_nascimento", "data_aplicacao_ajustada", "vacina", 
                 "dose", "fornecedor", "idade_anos", "fx_etaria2", "sexo", "grupo_atendimento", 
                 "grupodeatendimento_old", "id_bairro", "bairro_ajustado", "municipio_residencia", 
                 "tipo_atendimento", "tipo_usuario", "grupoprioritario_novo"],
    'output': "vacinas_update_20jan2022.csv"
}
 
data_iterator = pd.read_stata(os.path.join(config['path_to_data_windows'], config['fname']), 
                              iterator=True, chunksize=config['chunksize'], convert_categoricals=False)

info_template = {
    "nome": None, "nome mae": None, "cpf": None, "sexo": None, "data nascimento": None,
    "data D1": np.nan, "data D2": np.nan, "data D3": np.nan, "data D4": np.nan, "vacina": np.nan,
    "vacina(D1)": np.nan, "vacina(D2)": np.nan, "vacina(D3)": np.nan, "vacina(D4)": np.nan,
    "fornecedor": np.nan, "fornecedor(D1)": np.nan, "fornecedor(D2)": np.nan,
    "fornecedor(D3)": np.nan, "fornecedor(D4)": np.nan, "idade anos": None, "faixa etaria": None, 
    "bairro": None, "bairro id": None, "tipo atendimento": "", 
    "tipo usuario": "", "grupo prioritario": "", "grupo atendimento": ""
}

cpf_info = defaultdict(lambda: np.nan)
cpf_dose = defaultdict(lambda: [])

# Loop through the data
for chunk in tqdm(data_iterator):
    chunk["data_aplicacao_ajustada"] = pd.to_datetime(chunk["data_aplicacao_ajustada"], errors="coerce")
    for j in range(chunk.shape[0]):
        cpf = chunk["cpf_usuario"].iat[j]
        dose_str = f'D{chunk["dose"].iat[j]}'
        cpf_dose[cpf].append(dose_str)

        # If there isn't any record for the current CPF.
        if pd.isna(cpf_info[cpf]):
            cpf_info[cpf] = dict(info_template)
            cpf_info[cpf]["cpf"] = cpf
            cpf_info[cpf]["nome"] = chunk["usuario"].iat[j],
            cpf_info[cpf]["nome mae"] = chunk["mae"].iat[j],
            cpf_info[cpf]["data nascimento"] = chunk["data_nascimento"].iat[j]
            cpf_info[cpf]["sexo"] = chunk["sexo"].iat[j]
            cpf_info[cpf]["idade anos"] = chunk["idade_anos"].iat[j]
            cpf_info[cpf]["bairro"] = chunk["bairro_ajustado"].iat[j]
            cpf_info[cpf]["bairro id"] = chunk["id_bairro"].iat[j]
            cpf_info[cpf]["faixa etaria"] = chunk["fx_etaria2"].iat[j]
            cpf_info[cpf]['grupo atendimento'] += chunk["grupo_atendimento"].iat[j]
            cpf_info[cpf]['grupo prioritario'] += chunk["grupoprioritario_novo"].iat[j]
            cpf_info[cpf]['tipo atendimento'] += chunk["tipo_atendimento"].iat[j]
            cpf_info[cpf]["tipo usuario"] += chunk["tipo_usuario"].iat[j]

            cpf_info[cpf][f"data {dose_str}"] = chunk["data_aplicacao_ajustada"].iat[j]
            cpf_info[cpf][f"vacina({dose_str})"] = chunk["vacina"].iat[j]
            cpf_info[cpf][f"fornecedor({dose_str})"] = chunk["fornecedor"].iat[j]
            if dose_str=="D1" or dose_str=="D2":
                    cpf_info[cpf]["vacina"] = chunk["vacina"].iat[j]
                    cpf_info[cpf]["fornecedor"] = chunk["fornecedor"].iat[j]
            elif pd.isna(cpf_info[cpf]["vacina(D1)"]) and pd.isna(cpf_info[cpf]["vacina(D2)"]):
                cpf_info[cpf]["vacina"] = chunk["vacina"].iat[j]
                cpf_info[cpf]["fornecedor"] = chunk["fornecedor"].iat[j]
        else:
            if not pd.isna(cpf_info[cpf][f"data {dose_str}"]) and cpf_info[cpf][f"data {dose_str}"]>=chunk["data_aplicacao_ajustada"].iat[j]:
                continue

            cpf_info[cpf]['grupo atendimento'] += "-"+chunk["grupo_atendimento"].iat[j]
            cpf_info[cpf]['grupo prioritario'] += "-"+chunk["grupoprioritario_novo"].iat[j]
            cpf_info[cpf]['tipo atendimento'] += "-"+chunk["tipo_atendimento"].iat[j]
            cpf_info[cpf]["tipo usuario"] += "-"+chunk["tipo_usuario"].iat[j]
            if pd.isna(cpf_info[cpf][f"data {dose_str}"]):
                cpf_info[cpf][f"data {dose_str}"] = chunk["data_aplicacao_ajustada"].iat[j]
                cpf_info[cpf][f"vacina({dose_str})"] = chunk["vacina"].iat[j]
                cpf_info[cpf][f"fornecedor({dose_str})"] = chunk["fornecedor"].iat[j]
                if dose_str=="D1" or dose_str=="D2":
                    cpf_info[cpf]["vacina"] = chunk["vacina"].iat[j]
                    cpf_info[cpf]["fornecedor"] = chunk["fornecedor"].iat[j]
                elif pd.isna(cpf_info[cpf]["vacina(D1)"]) and pd.isna(cpf_info[cpf]["vacina(D2)"]):
                    cpf_info[cpf]["vacina"] = chunk["vacina"].iat[j]
                    cpf_info[cpf]["fornecedor"] = chunk["fornecedor"].iat[j]
            else:
                cpf_info[cpf][f"data {dose_str}"] = chunk["data_aplicacao_ajustada"].iat[j]
                cpf_info[cpf][f"vacina({dose_str})"] = chunk["vacina"].iat[j]
                cpf_info[cpf][f"fornecedor({dose_str})"] = chunk["fornecedor"].iat[j]
                if dose_str=="D1" or dose_str=="D2":
                    cpf_info[cpf]["vacina"] = chunk["vacina"].iat[j]
                    cpf_info[cpf]["fornecedor"] = chunk["fornecedor"].iat[j]
                elif pd.isna(cpf_info[cpf]["vacina(D1)"]) and pd.isna(cpf_info[cpf]["vacina(D2)"]):
                    cpf_info[cpf]["vacina"] = chunk["vacina"].iat[j]
                    cpf_info[cpf]["fornecedor"] = chunk["fornecedor"].iat[j]

new_df = pd.DataFrame.from_dict(cpf_info, orient="index")
new_df["vacina(original)"] = new_df["vacina"].tolist()
new_df["vacina"] = new_df["vacina"].map({1: "CORONAVAC", 2: "ASTRAZENECA", 3: "PFIZER", 4: "JANSSEN"})
new_df["vacina(D1)"] = new_df["vacina(D1)"].map({1: "CORONAVAC", 2: "ASTRAZENECA", 3: "PFIZER", 4: "JANSSEN"})
new_df["vacina(D2)"] = new_df["vacina(D2)"].map({1: "CORONAVAC", 2: "ASTRAZENECA", 3: "PFIZER", 4: "JANSSEN"})
new_df["vacina(D3)"] = new_df["vacina(D3)"].map({1: "CORONAVAC", 2: "ASTRAZENECA", 3: "PFIZER", 4: "JANSSEN"})
new_df["vacina(D4)"] = new_df["vacina(D4)"].map({1: "CORONAVAC", 2: "ASTRAZENECA", 3: "PFIZER", 4: "JANSSEN"})

new_df["nome"] = new_df["nome"].apply(lambda x: str(x).replace("('", "").replace("',)", ""))
new_df["nome mae"] = new_df["nome mae"].apply(lambda x: str(x).replace("('", "").replace("',)", ""))

fx_etaria_map = {
    1: "<1 ANOS", 2: "1 A 4 ANOS", 3: "5 A 9 ANOS", 4: "10 A 14 ANOS", 5: "15 A 19 ANOS",
    6: "20 A 24 ANOS", 7: "25 A 29 ANOS", 8: "30 A 34 ANOS", 9: "35 A 39 ANOS", 10: "40 A 44 ANOS",
    11: "45 A 49 ANOS", 12: "50 A 54 ANOS", 13: "55 A 59 ANOS", 14: "60 A 64 ANOS", 15: "65 A 69 ANOS",
    16: "70 A 74 ANOS", 17: "75 A 79 ANOS", 18: "80 A 84 ANOS", 19: "85 A 89 ANOS", 20: "90 A 94 ANOS",
    21: "95 A 99 ANOS", 22: ">=100 ANOS"
}
new_df["faixa etaria"] = new_df["faixa etaria"].map(fx_etaria_map)

# --> Save file
#new_df.to_csv(os.path.join(config['path_to_data_windows'], config['output']), index=False)
            


