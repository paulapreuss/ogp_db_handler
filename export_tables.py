import os
import subprocess
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()


def export_all_tables(db_url, out_dir):
    """Export all tables from a database to CSV"""
    os.makedirs(out_dir, exist_ok=True)
    engine = sqlalchemy.create_engine(db_url, echo=True, pool_recycle=3600)
    insp = sqlalchemy.inspect(engine)

    tables = insp.get_table_names()
    for t in tables:
        if "weatherdata" in t:
            continue
        try:
            with engine.connect() as conn:  # fresh connection
                df = pd.read_sql(f"SELECT * FROM {t}", conn)
                out_path = os.path.join(out_dir, f"{t}.csv")
                df.to_csv(out_path, index=False)
                print(f"Exported {t} -> {out_path}")
        except Exception as e:
            print(f"⚠️ Failed to export {t}: {e}")

def import_all_csvs(db_url, in_dir):
    """Import all CSVs into a PostgreSQL database (tables must already exist)"""
    engine = sqlalchemy.create_engine(db_url)

    for f in os.listdir(in_dir):
        if f.endswith(".csv"):
            table = os.path.splitext(f)[0]
            path = os.path.join(in_dir, f)
            df = pd.read_csv(path)
            df.to_sql(table, engine, if_exists="append", index=False)
            print(f"Imported {path} -> {table}")


def dump_mysql_schema(user, password, host, db, out_file):
    """Dump MySQL schema only (no data)"""
    cmd = [
        r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe",
        f"-u{user}",
        f"-p{password}",
        "-h", host,
        "--no-data",
        db,
    ]
    with open(out_file, "w") as f:
        subprocess.run(cmd, check=True, stdout=f)
    print(f"MySQL schema exported -> {out_file}")


def dump_postgres_schema(user, host, db, out_file):
    """Dump PostgreSQL schema only (no data)"""
    cmd = [
        "pg_dump",
        "-U", user,
        "-h", host,
        "--schema-only",
        db,
    ]
    with open(out_file, "w") as f:
        subprocess.run(cmd, check=True, stdout=f)
    print(f"PostgreSQL schema exported -> {out_file}")


if __name__ == "__main__":
    # --- CONFIG ---

    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASS = os.getenv("MYSQL_PASS")
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_DB = os.getenv("MYSQL_DB")

    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASS = os.getenv("POSTGRES_PASS")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_DB = os.getenv("POSTGRES_DB")

    MYSQL_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}/{MYSQL_DB}"
    POSTGRES_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}/{POSTGRES_DB}"

    # --- EXPORT MYSQL DATA ---
    # export_all_tables(MYSQL_URL, "mysql_csv")

    # --- EXPORT POSTGRES DATA ---
    export_all_tables(POSTGRES_URL, "postgres_csv")

    # --- EXPORT MYSQL SCHEMA ---
    dump_mysql_schema(MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_DB, "mysql_schema.sql")

    # --- EXPORT POSTGRES SCHEMA ---
    dump_postgres_schema(POSTGRES_USER, POSTGRES_HOST, POSTGRES_DB, "postgres_schema.sql")

    # --- TO IMPORT INTO POSTGRES (uncomment after editing CSVs) ---
    # import_all_csvs(POSTGRES_URL, "edited_mysql_csv")
