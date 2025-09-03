import ast

import pandas as pd
from pathlib import Path

cd = Path.cwd()

grid_des_old = pd.read_csv(cd / "mysql_wam" / f"griddesign.csv", index_col="id")
grid_des_new = pd.read_csv(cd / "postgres_csv" / f"steps_griddesign.csv", index_col="id")
id_mapping = pd.read_csv(cd / "proj_id_mapping.csv", sep=";", index_col="id").to_dict()
id_mapping = {ast.literal_eval(v): k for k, v in id_mapping["mapping"].items()}

table_reformat = grid_des_old.copy()

table_reformat["project_id"] = grid_des_old.apply(lambda row: id_mapping.get((row.name, row.project_id)), axis=1)
table_reformat.dropna(subset=["project_id"], inplace=True)
table_reformat["project_id"] = table_reformat["project_id"].apply(lambda x: int(x))

table_reformat.reset_index(inplace=True)
table_reformat.drop(columns="id", inplace=True)

for col in table_reformat.columns:
    for comp in ["distribution_cable", "connection_cable", "pole", "mg", "shs"]:
        if comp in col:
            col_name = comp + "_" + col.split(comp, 1)[1]
            if "lifetime" in col or "n_connections" in col:
                table_reformat[col_name] = table_reformat[col].apply(lambda x: int(x))
            else:
                table_reformat[col_name] = table_reformat[col].apply(lambda x: float(x))
            table_reformat.drop(columns=[col], inplace=True)

table_reformat["shs__include"] = table_reformat["shs__max_grid_cost"].apply(lambda x: False if x == 999 else True)
table_reformat = table_reformat[grid_des_new.columns.tolist()]

table_reformat.to_csv(f"steps_griddesign.csv", index=True)