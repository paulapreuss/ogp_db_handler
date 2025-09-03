import ast

import numpy as np
import pandas as pd
from pathlib import Path

cd = Path.cwd()

res_old = pd.read_csv(cd / "mysql_wam" / f"results.csv", index_col="id")
res_new = pd.read_csv(cd / "postgres_csv" / f"optimization_results.csv", index_col="id")
id_mapping = pd.read_csv(cd / "proj_id_mapping.csv", sep=";", index_col="id").to_dict()
id_mapping = {ast.literal_eval(v): k for k, v in id_mapping["mapping"].items()}
simulation_ids = pd.read_csv(cd / "postgres_csv" / f"optimization_simulation.csv", index_col="project_id")


table_reformat = res_old.copy()

table_reformat["project_id"] = res_old.apply(lambda row: id_mapping.get((row.name, row.project_id)), axis=1)
table_reformat.dropna(subset=["project_id"], inplace=True)
table_reformat["project_id"] = table_reformat["project_id"].apply(lambda x: int(x))
table_reformat["simulation_id"] = table_reformat["project_id"].apply(lambda x: simulation_ids.loc[x]["id"])
table_reformat.reset_index(inplace=True)
table_reformat.drop(columns="id", inplace=True)

table_reformat.rename(columns={"upfront_invest_diesel_gen": "upfront_invest_diesel_genset"}, inplace=True)

table_reformat["lcoe_share_supply"] = (
        (table_reformat["epc_total"] - table_reformat["cost_grid"]) / table_reformat["epc_total"] * 100
)
table_reformat["lcoe_share_grid"] = 100 - table_reformat["lcoe_share_supply"]
assets = ["grid", "diesel_genset", "inverter", "rectifier", "battery", "pv"]
table_reformat["upfront_invest_total"] = sum(
    [getattr(table_reformat, f"upfront_invest_{key}") for key in assets]
)

for col in table_reformat.columns:
    if col.startswith("n_") or col.startswith("length_") or col == "infeasible":
        table_reformat[col] = table_reformat[col].astype("Int64")

intersection = list(set(res_old.columns) - set(res_new.columns))

table_reformat = table_reformat[res_new.columns.tolist()]

table_reformat.to_csv(f"optimization_results.csv", index=True)