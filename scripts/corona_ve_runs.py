import os
import numpy as np
os.chdir("..")

for seed in np.arange(2,31,1):
    print(f"JAN-AUG COHORT -> Seed {seed}")
    
    # CORONAVAC
    print("CoronaVac")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D1 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D2 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D1 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D2 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D1 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D2 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D1 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D2 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D1 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D2 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D1 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO_D1D2REG --dose D2 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")

