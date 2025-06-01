import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# === CONFIGURATION ===
password = ""
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

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

def normalize_degree(nama):
    # Contoh: normalisasi gelar (bisa disesuaikan)
    replacements = {
        'S.T': 'ST', 'S.T.': 'ST', 
        'M.T': 'MT', 'M.T.': 'MT',
        'Ph.D': 'PhD', 'Ph.D.': 'PhD'
    }
    for old, new in replacements.items():
        nama = nama.replace(old, new)
    return nama

def transform_dosen_wali(df):
    print("Transforming dosen wali dimension...")

    # Transform nama
    df['nama'] = df['nama'].str.strip().str.title()
    df['nama'] = df['nama'].apply(normalize_degree)

    # Transform email
    df['email'] = df['email'].str.strip().str.lower()

    # Dedup
    df = df.drop_duplicates(subset=['nama', 'email'])

    # Surrogate key
    last_id = get_max_surrogate('dim_dosen_wali', 'dosen_wali_id')
    df['dosen_wali_id'] = range(last_id + 1, last_id + 1 + len(df))

    return df[['dosen_wali_id', 'nama', 'email']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl_dosen_wali():
    df = extract_table("dosen_wali")
    dim_dosen = transform_dosen_wali(df)
    load_table(dim_dosen, "dim_dosen_wali")

if __name__ == "__main__":
    run_etl_dosen_wali()
