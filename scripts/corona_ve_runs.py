import os

os.chdir("..")

# JAN-AUG COHORT
for seed in [1,2,3]:
    print(f"JAN-AUG COHORT -> Seed {seed}")
    # ALLPOP CORONAVAC
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 0")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 0")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 14")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 14")

for seed in [1,2,3]:
    print(f"JAN-JUN COHORT -> Seed {seed}")
    # ALLPOP CORONAVAC
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 0")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 0")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 0")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix TODAPOPUL --bootstrap_n 200 --events ALL --t_min 14")

    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 0 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 1 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 14")
    os.system(f"python ve_estimate.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --seed {seed} --hdi_index 2 --suffix VACPOPUL --bootstrap_n 200 --events ALL --t_min 14")
