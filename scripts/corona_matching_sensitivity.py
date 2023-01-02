import os
import numpy as np
os.chdir("..")

# JAN-AUG COHORT
for seed in np.arange(21, 22, 1):
    print(f"JAN-AUG COHORT -> Seed {seed} CORONAVAC")
    print("SENSITIVITY - DELAY 6 DAYS")
    # ALLPOP CORONAVAC
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 0 --suffix NOVO_D1D2REG_DELAY6 --delay_vaccine 6")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 0 --suffix NOVO_D1D2REG_DELAY6 --delay_vaccine 6")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 7 --suffix NOVO_D1D2REG_DELAY6 --delay_vaccine 6")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 7 --suffix NOVO_D1D2REG_DELAY6 --delay_vaccine 6")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 14 --suffix NOVO_D1D2REG_DELAY6 --delay_vaccine 6")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 14 --suffix NOVO_D1D2REG_DELAY6 --delay_vaccine 6")

    print("SENSITIVITY - DELAY 13 DAYS")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 0 --suffix NOVO_D1D2REG_DELAY13 --delay_vaccine 13")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 0 --suffix NOVO_D1D2REG_DELAY13 --delay_vaccine 13")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 7 --suffix NOVO_D1D2REG_DELAY13 --delay_vaccine 13")
    #os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 7 --suffix NOVO_D1D2REG_DELAY13 --delay_vaccine 13")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D1 --days_after 14 --suffix NOVO_D1D2REG_DELAY13 --delay_vaccine 13")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --dose DATA D2 --days_after 14 --suffix NOVO_D1D2REG_DELAY13 --delay_vaccine 13")
    
    
