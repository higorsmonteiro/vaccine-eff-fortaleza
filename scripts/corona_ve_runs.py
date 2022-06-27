import os
os.chdir("..")

# JAN-AUG COHORT
for seed in [2,3,4,5,6,7,8,9,10]:
    print(f"JAN-AUG COHORT -> Seed {seed}")
    
    # CORONAVAC
    print("CoronaVac")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D1 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D2 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D1 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D2 --days_after 14 --bootstrap_n 1000 --events ALL --t_min 0")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D1 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D2 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D1 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D2 --days_after 7 --bootstrap_n 1000 --events ALL --t_min 0")
    
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D1 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D2 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D1 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --dose D2 --days_after 0 --bootstrap_n 1000 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix NOVO --bootstrap_n 50 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix NOVO --bootstrap_n 50 --events ALL --t_min 14")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")

    # ASTRAZENECA
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")#
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")#

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")

    print(f"JAN-JUN COHORT -> Seed {seed}")
    
    # CORONAVAC
    print("CoronaVac")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix PRI_NA_COORTEX --bootstrap_n 100 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix PRI_NA_COORTEX --bootstrap_n 100 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix PRI_NA_COORTEX --bootstrap_n 100 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix PRI_NA_COORTEX --bootstrap_n 100 --events ALL --t_min 14")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")

    # ASTRAZENECA
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 0")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")

    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 0 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
    #os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine ASTRAZENECA --seed {seed} --hdi_index 2 --suffix VACPOPUL_PRI_NA_COORTE --bootstrap_n 20 --events ALL --t_min 14")
