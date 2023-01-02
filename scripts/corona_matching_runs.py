import os
import numpy as np
os.chdir("..")

# JAN-AUG COHORT
for seed in np.arange(1, 31, 1):
    print(f"JAN-AUG COHORT -> Seed {seed} CORONAVAC")
    # ALLPOP CORONAVAC
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 0 --suffix NOVO_D1D2REG")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 0 --suffix NOVO_D1D2REG")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 7 --suffix NOVO_D1D2REG")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 7 --suffix NOVO_D1D2REG")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 14 --suffix NOVO_D1D2REG")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 14 --suffix NOVO_D1D2REG")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 0 --suffix NOVO")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 0 --suffix NOVO")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 7 --suffix NOVO")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 7 --suffix NOVO")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 14 --suffix NOVO")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 14 --suffix NOVO")
    
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --suffix PRI_NA_COORTEX")

    # VACCINEPOP CORONAVAC
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")

    #print(f"JAN-JUN COHORT -> Seed {seed} CORONAVAC")
    # ALLPOP CORONAVAC 
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test ALL --suffix PRI_NA_COORTEX")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --suffix PRI_NA_COORTEX")

    # VACCINEPOP CORONAVAC 
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")

    #print(f"JAN-AUG COHORT -> Seed {seed} ASTRAZENECA")
    ## ALLPOP ASTRAZENECA
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 0 --pop_test ALL --suffix PRI_NA_COORTE")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 2 --pop_test ALL --suffix PRI_NA_COORTE")

    ## VACCINEPOP ASTRAZENECA
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 0 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 2 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")

    #print(f"JAN-JUN COHORT -> Seed {seed} ASTRAZENECA")
    ## ALLPOP ASTRAZENECA 
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 0 --pop_test ALL --suffix PRI_NA_COORTE")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 2 --pop_test ALL --suffix PRI_NA_COORTE")

    ## VACCINEPOP ASTRAZENECA 
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 0 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --age_range 18 200 --seed {seed} --hdi_index 2 --pop_test VACCINE --suffix VACPOPUL_PRI_NA_COORTE")
