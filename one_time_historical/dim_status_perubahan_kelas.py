import getpass
import pandas as pd
from sqlalchemy import create_engine

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_table(table_name):
    print(f"Extracting {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", source_engine)

# === TRANSFORM ===
def get_max_surrogate(table, id_column):
    query = f"SELECT MAX({id_column}) AS max_id FROM {table}"
    result = pd.read_sql(query, target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

def transform_perubahan_kelas(df):
    print("Transforming status perubahan kelas dimension...")

    df = df[['action']].drop_duplicates()
    df['status'] = df['action'].str.strip().str.capitalize()

    last_id = get_max_surrogate('dim_status_perubahan_kelas', 'perubahan_kelas_id')
    df['perubahan_kelas_id'] = range(last_id + 1, last_id + 1 + len(df))

    return df[['perubahan_kelas_id', 'status']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl_status_perubahan_kelas():
    df = extract_table("detail_frs")
    dim_status = transform_perubahan_kelas(df)
    load_table(dim_status, "dim_status_perubahan_kelas")

if __name__ == "__main__":
    run_etl_status_perubahan_kelas()
