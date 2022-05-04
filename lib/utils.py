import json
import numpy as np
import pandas as pd
import unidecode
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date, timedelta
from collections import defaultdict

def replace_string(string, sep=''):
    '''
        Return the input string without any special character and numbers.
    '''
    if sep=='':
        new_string = sep.join([char.upper() for char in string if char.isalnum()])
        new_string = sep.join([char.upper() for char in new_string if not char.isdigit()])
    elif sep==' ':
        new_string = []
        string_lst = string.split(sep)
        for s in string_lst:
            new_string.append(''.join([char.upper() for char in s if char.isalnum()]))
        new_string = sep.join(new_string)
    return new_string

#def replace_string(string):
#    '''
#        Return the input string without any special character and numbers.
#    '''
#    new_string = ''.join([char.upper() for char in string if char.isalnum()])
#    new_string = ''.join([char.upper() for char in new_string if not char.isdigit()])
#    return new_string

def replace_string_hash(string):
    '''
        Replace name for a hash code -> Ex: HIGOR DA SILVA MONTEIRO -> HDSM20.
    '''
    names = string.split()
    new_string = ''.join([char.upper() for char in string if char.isalnum()])
    new_string = ''.join([char.upper() for char in new_string if not char.isdigit()])
    hashcode_name = ''.join([x[0] for x in names]) + str(len(new_string))
    return hashcode_name

def return_corrected_strings(str_lst):
    '''
        Correct the list of strings according to the function 'replace_string'.
    '''
    return [ replace_string(string) for string in str_lst ]

def generate_date_list(init_date, final_date, interval=1):
    '''
    
    '''
    datelist = [init_date]
    while datelist[-1]!=final_date:
        datelist.append(datelist[-1] + timedelta(days=interval))
    return datelist

def json_summary_data(data_fname, delimiter=",", encoding="utf-8", onecol=None):
    '''

    '''
    resume_dict = dict()
    sample_df = pd.read_csv(data_fname, delimiter=delimiter, encoding=encoding, nrows=100)
    all_columns = list(sample_df.columns)
    
    j = 0
    df_rows = 0
    interval = 30
    null_tables = []
    while True:
        if j+interval>len(all_columns):
            df_rows = pd.read_csv(data_fname, delimiter=delimiter, encoding=encoding, usecols=all_columns[j:j+2]).shape[0]
            null_tables.append(pd.read_csv(data_fname, delimiter=delimiter, encoding=encoding, usecols=all_columns[j:]).isnull().sum())
            break
        else:
            null_tables.append(pd.read_csv(data_fname, delimiter=delimiter, encoding=encoding, usecols=all_columns[j:j+interval]).isnull().sum())
            j += interval
    null_tables = pd.concat(null_tables)

    resume_dict.update({"Number of rows": df_rows, "Number of columns": sample_df.shape[1]})
    for col in all_columns:
        resume_dict.update({
            col: {
                "Number of nulls": null_tables.loc[col],
                "Sample of values": sample_df[col].sample(n=6, random_state=1).tolist()
            }
        })
    return resume_dict

def generate_vacinados_data(df, cpf_hash):
    '''
        Description.

        Args:
            df:
                Doses dataframe.
            cpf_hash:
                Hash containing the index positions of all rows belonging to a
                given CPF.
        Return:
            vaccinated_df:
                ...
    '''
    new_fields = {"nome": [], "cpf": [], "sexo": [], "data nascimento": [], "data D1": [],
                  "data D2": [], "vacina": [], "fornecedor": [], "idade anos": [], "faixa etaria": [],
                  "bairro": [], "bairro id": [], "tipo atendimento": [], "tipo usuario": [],
                  "grupo prioritario": [], "grupo atendimento": []}
    
    for index, cpf in enumerate(cpf_hash.keys()):
        if index%20000==0:
            print(index)
        locations = cpf_hash[cpf]
        cpf_df = df.iloc[locations]

        cpf_info = {
            "cpf": cpf,
            "nome": cpf_df["usuario"].iat[0],
            "data nascimento": cpf_df["data_nascimento"].iat[0],
            "data aplicacoes": cpf_df["data_aplicacao_ajustada"].tolist(),
            "doses": cpf_df["dose"].tolist(),
            "sexo": cpf_df["sexo"].iat[0],
            "idade anos": cpf_df["idade_anos"].iat[0],
            "faixa etaria": cpf_df["fx_etaria2"].iat[0],
            "fornecedor": cpf_df["fornecedor"].iat[0],
            "grupo atendimento": "-".join(cpf_df["grupo_atendimento"].astype(str).tolist()),
            "tipo usuario": "-".join(cpf_df["tipo_usuario"].astype(str).tolist()),
            "bairro": cpf_df["bairro_ajustado"].iat[0],
            "bairro id": cpf_df["id_bairro"].iat[0],
            "grupo prioritario": "-".join(cpf_df["grupoprioritario_novo"].astype(str).tolist()),
            "vacina": cpf_df["vacina"].iat[0],
            "tipo atendimento": "-".join(cpf_df["tipo_atendimento"].astype(str).tolist())
        }

        # Consider only the latest date for each dose (D1 and D2).
        doses = {
            "D1": [ cpf_info["data aplicacoes"][j] for j in range(0, len(cpf_info["data aplicacoes"])) if cpf_info["doses"][j]=="D1"],
            "D2": [ cpf_info["data aplicacoes"][j] for j in range(0, len(cpf_info["data aplicacoes"])) if cpf_info["doses"][j]=="D2"]
        }

        # -- Fill table --
        new_fields["nome"].append(cpf_info["nome"])
        new_fields["cpf"].append(cpf_info["cpf"])
        new_fields["sexo"].append(cpf_info["sexo"])
        new_fields["data nascimento"].append(cpf_info["data nascimento"])
        new_fields["idade anos"].append(cpf_info["idade anos"])
        new_fields["faixa etaria"].append(cpf_info["faixa etaria"])
        new_fields["vacina"].append(cpf_info["vacina"])
        new_fields["fornecedor"].append(cpf_info["fornecedor"])
        if doses["D1"]:
            new_fields["data D1"].append(max(doses["D1"]))
        else:
            new_fields["data D1"].append(np.nan)
        if doses["D2"]:
            new_fields["data D2"].append(max(doses["D2"]))
        else:
            new_fields["data D2"].append(np.nan)
        new_fields["grupo prioritario"].append(cpf_info["grupo prioritario"])
        new_fields["grupo atendimento"].append(cpf_info["grupo atendimento"])
        new_fields["tipo atendimento"].append(cpf_info["tipo atendimento"])
        new_fields["bairro"].append(cpf_info["bairro"])
        new_fields["bairro id"].append(cpf_info["bairro id"])
        new_fields["tipo usuario"].append(cpf_info["tipo usuario"])

    vaccinated_df = pd.DataFrame(new_fields)
    return vaccinated_df

def KM_plotting(km_case_death, km_control_death, km_case_hospt, km_control_hospt):
    fig, AX = plt.subplots(2,2, figsize=(8,8))

    sns.lineplot(x="day", y="KM_estimate_porc", data=km_case_death, ax=AX[0,0])
    sns.lineplot(x="day", y="KM_estimate_porc", data=km_control_death, ax=AX[0,0])
    AX[0,0].fill_between(km_case_death["day"], km_case_death["KM_estimate_CI_lower_porc"], km_case_death["KM_estimate_CI_upper_porc"], alpha=0.2)
    AX[0,0].fill_between(km_control_death["day"], km_control_death["KM_estimate_CI_lower_porc"], km_control_death["KM_estimate_CI_upper_porc"], alpha=0.2)

    sns.lineplot(x="day", y="KM_estimate_porc", data=km_case_hospt, ax=AX[0,1], label="Vaccinated")
    sns.lineplot(x="day", y="KM_estimate_porc", data=km_control_hospt, ax=AX[0,1], label="Unvaccinated")
    AX[0,1].fill_between(km_case_hospt["day"], km_case_hospt["KM_estimate_CI_lower_porc"], km_case_hospt["KM_estimate_CI_upper_porc"], alpha=0.2)
    AX[0,1].fill_between(km_control_hospt["day"], km_control_hospt["KM_estimate_CI_lower_porc"], km_control_hospt["KM_estimate_CI_upper_porc"], alpha=0.2)

    sns.lineplot(x="day", y="KM_survival", data=km_case_death, ax=AX[1,0])
    sns.lineplot(x="day", y="KM_survival", data=km_control_death, ax=AX[1,0])
    AX[1,0].fill_between(km_case_death["day"], km_case_death["KM_survival_CI_lower"], km_case_death["KM_survival_CI_upper"], alpha=0.2)
    AX[1,0].fill_between(km_control_death["day"], km_control_death["KM_survival_CI_lower"], km_control_death["KM_survival_CI_upper"], alpha=0.2)

    sns.lineplot(x="day", y="KM_survival", data=km_case_hospt, ax=AX[1,1])
    sns.lineplot(x="day", y="KM_survival", data=km_control_hospt, ax=AX[1,1])
    AX[1,1].fill_between(km_case_hospt["day"], km_case_hospt["KM_survival_CI_lower"], km_case_hospt["KM_survival_CI_upper"], alpha=0.2)
    AX[1,1].fill_between(km_control_hospt["day"], km_control_hospt["KM_survival_CI_lower"], km_control_hospt["KM_survival_CI_upper"], alpha=0.2)

    for axis in [AX[0,0], AX[0,1], AX[1,0], AX[1,1]]:
        axis.tick_params(labelsize=13)
        axis.grid(alpha=0.2)
        axis.set_xlim([0,80])
        axis.set_xlabel("days", fontsize=15)
        axis.set_ylabel("")
        axis.set_xticks(np.arange(km_case_death["day"].min(), km_case_death["day"].max()+1, 14))
        axis.set_xlim([0,80])
    AX[0,1].legend(loc=2, prop={'size': 12}, bbox_to_anchor=(0.39, 1.35))    
    AX[0,0].set_ylabel("Cumulative incidence (%)", fontsize=14)
    AX[0,0].set_xlim([0,80])
    AX[0,0].set_xlabel("")
    AX[0,1].set_xlabel("")
    AX[0,0].set_title("Death by Covid-19", fontsize=13)
    AX[0,1].set_title("Hospitalization due to Covid-19", fontsize=13)
    AX[1,0].set_ylabel("Kaplan-Meier survival curve", fontsize=14)

    fig.suptitle("CORONAVAC", fontsize=14, y=0.92)
    plt.tight_layout()
    return fig, AX

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def open_correcao_bairro(fname):
    '''
    
    '''
    lines = []
    with open(fname, "r", encoding="latin") as f:
        for line in f:
            if line!="\n":
                lines.append(line.replace("\n", "").split(" if "))

    hash_name = defaultdict(lambda:np.nan)
    for j in range(len(lines)):
        nome_certo = lines[j][0].replace("replace bairro_ajustado=", "").replace('"',"")
        nome_errado = lines[j][1].replace("bairro_ajustado==", "").replace('"',"")

        if nome_certo=="": continue
        hash_name.update({nome_errado: nome_certo})
    hash_name.update({"IGNORADO": "IGNORADO"})
    hash_name.update({"CONJUNTO CEARA": "CONJUNTO CEARA", "CONJUNTO NOVO MONDUBIM": "NOVO MONDUBIM"})
    hash_name.update({"FÁTIMA": "FATIMA", "JOAQUIM TÁVORA": "JOAQUIM TAVORA", "PASSARÉ": "PASSARE"})
    hash_name.update({"CIDADE DOS FUNCIONÁRIOS": "CIDADE DOS FUNCIONARIOS"})
    hash_name.update({"ANTÔNIO BEZERRA": "ANTONIO BEZERRA", "VILA UNIÃO": "VILA UNIAO"})
    hash_name.update({"JOSE WALTER": "PREFEITO JOSE WALTER", "RODOLFO TEÓFILO": "RODOLFO TEOFILO"})
    hash_name.update({"JARDIM UNIAO PASSARE": "PASSARE", "QUINTO CUNHA": "QUINTINO CUNHA"})
    hash_name.update({"QUIN CUNHA": "QUINTINO CUNHA", "PAUPINA MESSEJANA": "PAUPINA"})
    hash_name.update({"BOA VISTA - CASTELAO": "BOA VISTA", "VILA VELHA 04": "VILA VELHA"})
    hash_name.update({"PERFEITO JOSE WALTER": "PREFEITO JOSE WALTER", "VILLA VELHA": "VILA VELHA"})
    hash_name.update({"PRESENTE KENNEDY": "PRESIDENTE KENNEDY", "HENRI JORGE": "HENRIQUE JORGE"})
    hash_name.update({"VINCENT PINZON": "VICENTE PINZON", "RODOL TEOFILO": "RODOLFO TEOFILO"})
    hash_name.update({"PLANA AYRTON SENNA": "PLANALTO AIRTON SENNA", "HEN JORGE": "HENRIQUE JORGE"})
    hash_name.update({"HEN JORGE":"HENRIQUE JORGE", "GRAN PORTUGAL": "GRANJA PORTUGAL"})
    hash_name.update({"CONJ PREFEITO JOSE WALTER":"PREFEITO JOSE WALTER", "PRESI KENNEDY": "PRESIDENTE KENNEDY"})
    hash_name.update({"PLANALTON AIRTON SENNA":"PLANALTO AIRTON SENNA", "LAGOA REDONDA - CURIO": "LAGOA REDONDA"})
    hash_name.update({"VICE PINZON": "VICENTE PINZON", "PIRABUM": "PIRAMBU", "FATIMA II": "FATIMA"})
    hash_name.update({"GRANJ PORTUGAL": "GRANJA PORTUGAL", "CIDA DOS FUNCIONARIOS": "CIDADE DOS FUNCIONARIOS", "NOVO MUNDOBIM": "NOVO MONDUBIM"})
    hash_name.update({"ANTONIO BEZZERA": "ANTONIO BEZERRA", "NOVA ALDEOTA": "ALDEOTA", "CIDADE DOS FUCIONARIOS": "CIDADE DOS FUNCIONARIOS"})
    hash_name.update({"QUINTINOCUNHA": "QUINTINO CUNHA", "PRESIDE KENNEDY": "PRESIDENTE KENNEDY", "DIONI TORRES": "DIONISIO TORRES"})
    hash_name.update({"NOVO MONDUBIM": "NOVO MONDUBIM", "PARQUE PRESIDENTE VAGAS": "PARQUE PRESIDENTE VARGAS", "PLANALTO GRANJA LISBOA": "GRANJA LISBOA"})
    hash_name.update({"NOVO MODUBIM": "NOVO MONDUBIM", "NOVO MUNDUBIM": "NOVO MONDUBIM", "NOVO MUNDUBIM": "NOVO MONDUBIM", "NOVO MUDUBIM": "NOVO MONDUBIM", "ESPLANADA NOVO MONDUBIM": "NOVO MONDUBIM"})
    hash_name.update({"PARQUE PRESIDENTE VAGAS": "PARQUE PRESIDENTE VARGAS", "M.DIAS BRANCO": "MANUEL DIAS BRANCO", "CONJUNTO PREF. JOSE WALTER": "PREFEITO JOSE WALTER"})
    return hash_name

def format_bairros(vacineja_df, fname, colname="bairro"):
    '''
        Description.
    '''
    new_hash = open_correcao_bairro(fname)
    vacineja_df[colname] = vacineja_df[colname].apply(lambda x: unidecode.unidecode(x) if not pd.isna(x) else x)
    vacineja_df["bairro_ajuste"] = vacineja_df[colname].apply(lambda x: new_hash[x] if not pd.isna(new_hash[x]) else x)
    bairro_df = vacineja_df["bairro_ajuste"].value_counts().reset_index()
    bairro_df.columns = ["BAIRRO", "CONTAGEM"]
    valid_bairros = bairro_df[:122]["BAIRRO"].tolist()
    vacineja_df["bairro_processado"] = vacineja_df["bairro_ajuste"].apply(lambda x: x if x in valid_bairros else np.nan)
    return vacineja_df

