'''
    COMPLETE PIPELINE FOR VACCINE EFFECTIVENESS IN FORTALEZA.
'''
import os
import datetime as dt
import lib.utils as utils 
import lib.db_utils as dutils
import lib.outcomes_utils as oututils

# - Main sources -
# Vacine Já and Vacinados databases.
from src.VacinadosTransform import VacinadosTransform
from src.VacineJaTransform import VacineJaTransform
# GAL and IntegraSUS Covid-19 tests.
from src.TestesGalTransform import TestesGalTransform
from src.TestesIntegraTransform import TestesIntegraTransform
# Join "Vacine Já +" and "GAL-Integra" database.
from src.JoinVacinadosTestes import JoinVacinadosTestes
# Join "Vacine Já ++" and Outcomes: Deaths and Hospitalizations
from src.JoinOutcomes import JoinOutcomes
# Definition of the cohort
from src.DefineCohortSettings import DefineCohortSettings

to_match = True
verbose = True
seed = 1

# All names and paths to the main databases of the project.
path_to_data, name_of_data = dutils.data_hash()

# 1. Load and transform the data from "Vacine Já"
vacineja_fname = os.path.join(path_to_data["VACINACAO CADASTRO (VACINE JA)"], name_of_data["VACINACAO CADASTRO (VACINE JA)"])
vacineja_obj = VacineJaTransform(vacineja_fname)
vacineja_df = vacineja_obj.load_and_transform(nrows=None)

# 2. Generate databases of vaccinated people (Preferred to be done a priori).
vacinas_fname = os.path.join(path_to_data["VACINAS APLICADAS"], name_of_data["VACINAS APLICADAS"])
vacinados_fname = os.path.join(path_to_data["VACINACAO POR PESSOA"], name_of_data["VACINACAO POR PESSOA"])
vacinacao_hash = {
    "VACINAS APLICADAS": vacinas_fname,
    "VACINADOS": vacinados_fname
}
vacinados_obj = VacinadosTransform(vacinacao_hash)
#   2.1 -> Generation of the database of vaccinated people (optional if data already exists)
#vacinados_df = vacinados_obj.generate_vacinados()
#vacinados_df.to_csv(os.path.join(path_to_data["VACINACAO POR PESSOA"], name_of_data["VACINACAO POR PESSOA"]))
#   2.2 -> Load and transform the database of vaccinated people.
vacinados_df = vacinados_obj.load_and_transform(nrows=None)

# 3. GENERATE NEW DATA -> Join "Vacine Já" database to "Vacinados" database -> "Vacine Já +" database.
vacinejaplus_df = vacineja_obj.join_ja_vacinados(vacinados_df)

if verbose:
    print(f"Dimensão Vacine Já: {vacineja_df.shape}")
    print(f"Dimensão Vacinados: {vacinados_df.shape}")
    print(f"Dimensão Vacine Já +: {vacinejaplus_df.shape}")
# 4. Prepare the GAL data.
testes_fname = os.path.join(path_to_data["TESTES COVID-19"], name_of_data["TESTES COVID-19"])
teste_gal_obj = TestesGalTransform(testes_fname, testes_path=path_to_data["TESTES COVID-19"])
#   4.1 -> From the separate files, generate the aggregated data regarding all available months of 2021.
#teste_gal_obj.join_separate_files(output_name=name_of_data["TESTES COVID-19"])
#   4.2 -> With the aggregated data generated, perform the data transformation.
df_gal = teste_gal_obj.load_and_transform(nrows=None)

# 5. Prepare the IntegraSUS data.
testes_fname = os.path.join(path_to_data["TESTES COVID-19 INTEGRA"], name_of_data["TESTES COVID-19 INTEGRA"])
testes_integra_obj = TestesIntegraTransform(testes_fname)
testes_integra_df = testes_integra_obj.load_and_transform()

# 6. Integrate results from "GAL" and "IntegraSUS" databases:  
gal_integra_df = teste_gal_obj.compare_integraSUS(testes_integra_df)

# 7. GENERATE NEW DATA -> Integrate results from "Vacine Já +" and "GAL-INTEGRA" databases: Vacine Já ++
joiner = JoinVacinadosTestes(gal_integra_df, vacinejaplus_df)
vacineja2plus_df = joiner.integrate_vacineja_galintegra()
if verbose:
    print(f"Dimensão GAL+IntegraSUS: {gal_integra_df.shape}")
    print(f"Testes cruzados com sucesso: {vacineja2plus_df['RESULTADO FINAL GAL-INTEGRASUS'].notnull().sum()}")

# 8. GENERATE NEW DATA -> Integrate "Vacine Já ++" with the outcomes (Deaths and hospitalizations): "Vacine Já ++O"
outcome_joiner = JoinOutcomes(vacineja2plus_df)
#   8.1 -> Integrate Deaths
vacineja2plus_df = outcome_joiner.join_obitos(os.path.join(path_to_data["OBITOS COVID-19"], name_of_data["OBITOS COVID-19"]), verbose=verbose)
#   8.2 -> Integrate Hospitalizations
vacineja2plus_df = outcome_joiner.join_hospitalization(os.path.join(path_to_data["HOSPITALIZACAO COVID-19"], name_of_data["HOSPITALIZACAO COVID-19"]), verbose=verbose)
vacineja2plus_df = outcome_joiner.rename_columns()

#9. DEFINE COHORT, ELIGIBILITY AND PERFORM THE MATCHING.
init_cohort = dt.date(2021, 1, 21)
final_cohort = dt.date(2021, 8, 31)
cohort_def = DefineCohortSettings(vacineja2plus_df, init_cohort, final_cohort)
vacineja2plusO_df = cohort_def.define_eligibility()

now = str(dt.datetime.timestamp(dt.datetime.now())).split(".")[0]
vacineja2plusO_df.to_csv(os.path.join("output", f"ELIGIBILIDADE_CADASTRADOS_VACINAS_COORTE_FORTALEZA_{now}.csv"))

if to_match:
    matching_info = {
        "AGE MIN": 50,
        "AGE DELTA MINUS": 1,
        "AGE DELTA PLUS": 1
    }
    age_thr_coronavac = 60
    age_thr_astrazeneca = 18
    age_thr_pfizer = 18
    # 10. Perform MATCHING with the defined cohort period FOR EACH VACCINE.
    matching_corona, df_join1 = cohort_def.dynamical_matching(vaccine="CORONAVAC", verbose=verbose, age_thr=age_thr_coronavac, seed=seed)
    df_join1.to_csv(os.path.join("output", f"control_reservoir_corona_{now}.csv"))
    matching_corona.to_csv(os.path.join("output", f"pareados_corona_{now}.csv"))
    matching_astra, df_join2 = cohort_def.dynamical_matching(vaccine="ASTRAZENECA", verbose=verbose, age_thr=age_thr_astrazeneca, seed=seed)
    matching_astra.to_csv(os.path.join("output", f"pareados_astra_{now}.csv"))
    df_join2.to_csv(os.path.join("output", f"control_reservoir_astra_{now}.csv"))
    matching_pfizer, df_join3 = cohort_def.dynamical_matching(vaccine="PFIZER", verbose=verbose, age_thr=age_thr_pfizer, seed=seed)
    matching_pfizer.to_csv(os.path.join("output", f"pareados_pfizer_{now}.csv"))
    df_join3.to_csv(os.path.join("output", f"control_reservoir_pfizer_{now}.csv"))
    # 11. Generate table of SURVIVAL TIMES for analysis
    survival_tb_corona = oututils.calculate_riskdays(matching_corona)
    survival_tb_astra = oututils.calculate_riskdays(matching_astra)
    survival_tb_pfizer = oututils.calculate_riskdays(matching_pfizer)
    #   11.3 SAVE FILES
    survival_tb_corona.to_csv(os.path.join("output", "sobrevida_coronavac.csv"))
    survival_tb_astra.to_csv(os.path.join("output", "sobrevida_astrazeneca.csv"))
    survival_tb_pfizer.to_csv(os.path.join("output", "sobrevida_pfizer.csv"))