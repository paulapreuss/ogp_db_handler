import ast

import pandas as pd
from pathlib import Path
import json

cd = Path.cwd()

esd_old = pd.read_csv(cd / "mysql_wam" / f"energysystemdesign.csv", index_col="id")
esd_new = pd.read_csv(cd / "postgres_csv" / f"steps_energysystemdesign.csv", index_col="id")
id_mapping = pd.read_csv(cd / "proj_id_mapping.csv", sep=";", index_col="id").to_dict()
id_mapping = {ast.literal_eval(v): k for k, v in id_mapping["mapping"].items()}

table_reformat = esd_old.copy()

table_reformat["project_id"] = esd_old.apply(lambda row: id_mapping.get((row.name, row.project_id)), axis=1)
table_reformat.dropna(subset=["project_id"], inplace=True)

table_reformat.reset_index(inplace=True)
table_reformat.drop(columns="id", inplace=True)

for col in table_reformat.columns:
    if "is_selected" in col or "design" in col:
        table_reformat[col] = table_reformat[col].apply(lambda x: bool(int(x)))
    elif "lifetime" in col or col == "project_id":
        table_reformat[col] = table_reformat[col].apply(lambda x: int(x))
    else:
        table_reformat[col] = table_reformat[col].apply(lambda x: float(x))


table_reformat = table_reformat[esd_new.columns.tolist()]
table_reformat.to_csv(f"steps_energysystemdesign.csv", index=True)



