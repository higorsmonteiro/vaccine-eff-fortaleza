import pandas as pd
import numpy as np
import os

path_old = os.path.join("..", "output", "data", "eligibility_population_21JAN2021_31AUG2021_newbairro.csv")

df = pd.read_csv(path_old, dtype={"cpf":str})

df["Data da Solicitação(GAL)"] = pd.to_datetime(df["Data da Solicitação(GAL)"], errors="coerce")
df["Data da Coleta(GAL)"] = pd.to_datetime(df["Data da Coleta(GAL)"], errors="coerce")
df["data_obito(OBITO COVID)"] = pd.to_datetime(df["data_obito(OBITO COVID)"], errors="coerce")

cols = ["Data da Solicitação(GAL)", "Data da Coleta(GAL)", "data_obito(OBITO COVID)"]
df["TESTE SOLICITACAO"] = df[cols].apply(lambda x: "INCONSISTENTE" if x[cols[0]]>x[cols[2]] and not pd.isna(x[cols[0]]) else "CONSISTENTE", axis=1)
df["TESTE COLETA"] = df[cols].apply(lambda x: "INCONSISTENTE" if x[cols[1]]>x[cols[2]] and not pd.isna(x[cols[0]]) else "CONSISTENTE", axis=1)
df.to_csv(os.path.join("..", "output", "data", "eligibility_population_21JAN2021_31AUG2021_newbairro1.csv"), index=False)

