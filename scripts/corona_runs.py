import os

os.chdir("..")

# JAN-AUG COHORT
for seed in [1,2,3]:
    print(f"JAN-AUG COHORT -> Seed {seed}")
    # ALL POP CORONAVAC 1
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test ALL --suffix TODAPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 1 --pop_test ALL --suffix TODAPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --suffix TODAPOPUL")

    # VACCINEPOP CORONAVAC 1
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test VACCINE --suffix VACPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 1 --pop_test VACCINE --suffix VACPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-08-31 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test VACCINE --suffix VACPOPUL")

# JAN-JUN COHORT
for seed in [1,2,3]:
    print(f"JAN-JUN COHORT -> Seed {seed}")
    # ALL POP CORONAVAC 1
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test ALL --suffix TODAPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 1 --pop_test ALL --suffix TODAPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test ALL --suffix TODAPOPUL")

    # VACCINEPOP CORONAVAC 1
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 0 --pop_test VACCINE --suffix VACPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 1 --pop_test VACCINE --suffix VACPOPUL")
    os.system(f"python perform_matching.py --start 2021-01-21 --end 2021-06-30 --vaccine CORONAVAC --age_range 60 200 --seed {seed} --hdi_index 2 --pop_test VACCINE --suffix VACPOPUL")
