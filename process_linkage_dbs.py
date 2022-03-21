import os
import warnings
import datetime as dt
warnings.filterwarnings("ignore")
from src.ProcessingAndLinkage import ProcessingAndLinkage

init_cohort = dt.datetime(2021, 1, 21)

base_folder = os.path.join(os.environ["USERPROFILE"], "Documents", "data")
fpaths = {
    "VACINACAO POR PESSOA": os.path.join(base_folder, "vacinado_obito"),
    "VACINACAO CADASTRO (VACINE JA)": os.path.join(base_folder, "vacinado_obito"),
    "TESTES COVID-19": os.path.join(base_folder, "testes_covid19"),
    "TESTES COVID-19 INTEGRA": os.path.join(base_folder, "testes_covid19", "FEV42022"),
    #"OBITOS COVID-19": os.path.join(base_folder, "obito_covid19", "READY_TO_LINKAGE"),
    "OBITOS COVID-19": r"C:\\Users\\higor.monteiro\\Documents\\data\\obitos_covid19\\READY_TO_LINKAGE",
    "OBITOS CARTORIOS": os.path.join(base_folder, "obitos_cartorios"),
    "BAIRROS IDH": base_folder,
}

fnames = {
    "VACINACAO CADASTRO (VACINE JA)": "__202202011443_vacineja_pessoas.csv",
    "VACINACAO POR PESSOA": "vacinas_update_20jan2022.csv",
    "TESTES COVID-19 INTEGRA": "base_dados_integrasus_fortaleza_final.dta",
    "OBITOS COVID-19": "POSITIVOS COVID 19 FORTALEZA.xlsx",
    "OBITOS CARTORIOS": "obitos_cartorios_tratado_22_11_2021.csv",
    "BAIRROS IDH": "BAIROS_IDH.xlsx",
}

process = ProcessingAndLinkage(fpaths, fnames)

nrows_dict = {"Vacine Ja": None, "Vacinados": None, "IntegraSUS": None, 
              "Obito Covid": None, "Obito Cartorio": None}
cols_dict = {
    "Vacine Ja": ["nome", "cpf", "data_nascimento", "bairro", "sexo", "created_at", "cns", "cep", "situacao", "nome_mae"], 
    "Vacinados": ["nome", "nome mae", "cpf", "sexo", "bairro id", "vacina", "fornecedor", "data nascimento",
                  "data D1", "data D2", "data D3", "data D4", "idade anos", "faixa etaria", "bairro", 
                  "tipo atendimento", "tipo usuario", "grupo prioritario"], 
    "IntegraSUS": ["id", "nome_paciente", "nome_mae", "cpf", "cns", "data_nascimento", "data_coleta_exame", 
                   "data_inicio_sintomas_nova", "data_internacao_sivep", "data_entrada_uti_sivep", "data_evolucao_caso_sivep",
                   "data_obito", "data_resultado_exame", "data_solicitacao_exame", "data_saida_uti_sivep", "resultado_final_exame"], 
    "Obito Covid": None, 
    "Obito Cartorio": None
}

print("Setting paths ...")
process.set_paths()
print("Loading databases ...")
process.load_dbs(nrows_dict=nrows_dict, cols_dict=cols_dict)
print("Processing databases ...")
process.transform(init_cohort, save=True, fpath=os.path.join(base_folder, "PARQUET_TRANSFORMED"))
print("Linking databases ...")
process.linkage(save=True, fpath=os.path.join("output", "data", "LINKAGE"))

