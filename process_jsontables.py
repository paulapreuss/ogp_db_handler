import ast
from io import StringIO

import pandas as pd
from pathlib import Path
import json

cd = Path.cwd()

json_tables = ["nodes", "links", "energyflow", "emissions", "durationcurve", "demandcoverage"]

for json_table in json_tables:
    table_old = pd.read_csv(cd / "mysql_wam" / f"{json_table}.csv", index_col="id")
    table_new = pd.read_csv(cd / "postgres_csv" / f"optimization_{json_table}.csv", index_col="id")
    users = pd.read_csv(cd / "mysql_csv" / "user.csv", index_col="id")
    id_mapping = pd.read_csv(cd / "proj_id_mapping.csv", sep=";", index_col="id").to_dict()
    id_mapping = {ast.literal_eval(v): k for k, v in id_mapping["mapping"].items()}

    table_reformat = table_old.copy()
    # table_reformat.data = table_old.data.apply(lambda x: json.loads(x))
    table_reformat.data = table_old.data.apply(lambda x: json.dumps(pd.DataFrame(json.loads(json.loads(x))).to_json()))
    table_reformat["project_id"] = table_old.apply(lambda row: id_mapping.get((row.name, row.project_id)), axis=1)
    table_reformat.dropna(inplace=True)
    table_reformat["project_id"] = table_reformat["project_id"].apply(lambda x: int(x))
    table_reformat = table_reformat[["data", "project_id"]]
    table_reformat.reset_index(inplace=True)
    table_reformat.drop(columns="id", inplace=True)
    table_reformat.to_csv(f"optimization_{json_table}.csv", index=True)

# import pdb; pdb.set_trace()