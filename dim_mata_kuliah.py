import getpass
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap'

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

def transform_dim_mata_kuliah(df_mk, df_kelas):
    print("Transforming mata kuliah dimension...")

    # JOIN dengan kelas
    df = df_kelas.merge(df_mk, on="kode_mata_kuliah", how="left")

    # TRANSFORM
    df['nama_mata_kuliah'] = df['nama'].str.strip().str.title()
    df['nama_kelas'] = df['nama_kelas'].str.strip().str.upper()
    df['dosen'] = df['dosen'].str.strip().str.title()

    # Tambahan kolom untuk SCD Type 2
    df['roweffectivedate'] = pd.Timestamp(datetime.now().date())
    df['rowexpirationdate'] = pd.Timestamp(datetime.max.date())
    df['currentrowindicator'] = 'Current'

    # Surrogate key
    last_id = get_max_surrogate('dim_mata_kuliah', 'mata_kuliah_id')
    df['mata_kuliah_id'] = range(last_id + 1, last_id + 1 + len(df))

    # PILIH KOLOM SESUAI TARGET
    return df[['mata_kuliah_id', 'kode_mata_kuliah', 'nama_mata_kuliah', 'sks',
               'nama_kelas', 'dosen', 'kapasitas', 
               'roweffectivedate', 'rowexpirationdate', 'currentrowindicator']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl_dim_mk():
    df_mk = extract_table("mata_kuliah")
    df_kelas = extract_table("kelas")

    dim_mk = transform_dim_mata_kuliah(df_mk, df_kelas)
    load_table(dim_mk, "dim_mata_kuliah")

if __name__ == "__main__":
    run_etl_dim_mk()
