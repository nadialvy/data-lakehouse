import getpass
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import re

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_table(table_name):
    print(f"Extracting {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", source_engine)

# === SURROGATE KEY ===
def get_max_surrogate(table, id_column):
    query = f"SELECT MAX({id_column}) AS max_id FROM {table}"
    result = pd.read_sql(query, target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

# === CLEANING ===
def clean_text(text):
    if pd.isna(text): return text
    text = text.strip().title()
    text = re.sub(r'\s+', ' ', text)
    return text

def log_typo_column(df, column_name):
    original = df[column_name].copy()
    cleaned = df[column_name].apply(clean_text)
    df[column_name] = cleaned
    log_df = pd.DataFrame({'original': original, 'cleaned': cleaned})
    typo_log = log_df[log_df['original'] != log_df['cleaned']].drop_duplicates()
    if not typo_log.empty:
        typo_log.to_csv(f"typo_log_{column_name}.csv", index=False)
        print(f"🔍 {len(typo_log)} typo logged in typo_log_{column_name}.csv")
    return df

# === TRANSFORM ===
def transform_dim_mata_kuliah(df_mk, df_kelas):
    print("Transforming mata kuliah dimension...")

    df = df_kelas.merge(df_mk, on="kode_mata_kuliah", how="left")

    # Normalisasi kolom teks
    df['kode_mata_kuliah'] = df['kode_mata_kuliah'].str.strip()

    df = log_typo_column(df, 'nama')
    df = log_typo_column(df, 'nama_kelas')
    df = log_typo_column(df, 'dosen')

    df['nama_kelas'] = df['nama_kelas'].str.upper()

    # Tambahan kolom SCD2
    df['row_effective_date'] = pd.Timestamp(datetime.now().date())
    df['row_expiration_date'] = pd.Timestamp(datetime.max.date())
    df['current_row_flag'] = 'Current'

    # Surrogate key
    last_id = get_max_surrogate('dim_mata_kuliah', 'mata_kuliah_id')
    df['mata_kuliah_id'] = range(last_id + 1, last_id + 1 + len(df))

    return df[['mata_kuliah_id', 'kode_mata_kuliah', 'nama', 'sks',
               'nama_kelas', 'dosen', 'kapasitas',
               'row_effective_date', 'row_expiration_date', 'current_row_flag']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")

    existing_kode_mata_kuliah = pd.read_sql(f"SELECT kode_mata_kuliah FROM {table_name}", target_engine)
    if not existing_kode_mata_kuliah.empty:
        existing = existing_kode_mata_kuliah['kode_mata_kuliah'].str.lower().tolist()
        df_filtered = df[~df['kode_mata_kuliah'].str.lower().isin(existing)].copy()
    else:
        df_filtered = df.copy()

    if df_filtered.empty:
        print("✅ Tidak ada data baru untuk dimasukkan (semua sudah ada).")
        return

    df_filtered.to_sql(table_name, target_engine, if_exists='append', index=False)
    print(f"✅ {len(df_filtered)} baris dimuat ke {table_name}.")

# === MAIN ===
def run_etl_dim_mk():
    df_mk = extract_table("mata_kuliah")
    df_kelas = extract_table("kelas")

    dim_mk = transform_dim_mata_kuliah(df_mk, df_kelas)
    load_table(dim_mk, "dim_mata_kuliah")

if __name__ == "__main__":
    run_etl_dim_mk()
