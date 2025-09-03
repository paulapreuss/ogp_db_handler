import ast

import pandas as pd
from pathlib import Path
import json

cd = Path.cwd()

demand_old = pd.read_csv(cd / "mysql_wam" / f"demand.csv", index_col="id")
customdemand_old = pd.read_csv(cd / "mysql_wam" / f"customdemand.csv", index_col="id")

demand_new = pd.read_csv(cd / "postgres_csv" / f"steps_customdemand.csv", index_col="id")

id_mapping = pd.read_csv(cd / "proj_id_mapping.csv", sep=";", index_col="id").to_dict()
id_mapping = {ast.literal_eval(v): k for k, v in id_mapping["mapping"].items()}

table_reformat = pd.DataFrame()
table_reformat["very_low"] = demand_old["custom_share_1"] / 100
table_reformat["low"] = demand_old["custom_share_2"] / 100
table_reformat["middle"] = demand_old["custom_share_3"] / 100
table_reformat["high"] = demand_old["custom_share_4"] / 100
table_reformat["very_high"] = demand_old["custom_share_5"] / 100
table_reformat["annual_total_consumption"] = demand_old["average_daily_energy"]
table_reformat["annual_peak_load"] = demand_old["maximum_peak_load"]
table_reformat["project_id"] = demand_old.apply(lambda row: id_mapping.get((row.name, row.project_id)), axis=1)
table_reformat.dropna(subset=["project_id"], inplace=True)
table_reformat["project_id"] = table_reformat["project_id"].apply(lambda x: int(x))
table_reformat["uploaded_data"] = customdemand_old["data"].apply(lambda x: json.loads(x))
table_reformat.reset_index(inplace=True)
table_reformat.drop(columns="id", inplace=True)

table_reformat.to_csv(f"steps_customdemand.csv", index=True)