import getpass
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

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

def transform_mahasiswa(df_mhs, df_jur, df_dosen):
    print("Transforming mahasiswa dimension...")

    # Join ke jurusan & dosen wali
    df = df_mhs.merge(df_jur, on="jurusan_id", how="left")
    df = df.merge(df_dosen, left_on="dosen_wali_id", right_on="id", suffixes=('', '_dosen'))

    # Trim & format
    df['nama'] = df['nama'].str.strip().str.title()
    df['email'] = df['email'].str.strip().str.lower()
    df['nama_jurusan'] = df['nama_jurusan'].str.strip().str.title()
    df['nama_dosen'] = df['nama_dosen'].str.strip().str.title()

    # Validasi email sederhana
    df = df[df['email'].str.contains('@', na=False)]

    # Dedup berdasarkan NRP (anggap NRP unik)
    df = df.drop_duplicates(subset='nrp')

    # Surrogate key
    last_id = get_max_surrogate('dim_mahasiswa', 'mahasiswa_id')
    df['mahasiswa_id'] = range(last_id + 1, last_id + 1 + len(df))

    # Kolom target
    return df[['mahasiswa_id', 'nrp', 'nama', 'email', 'nama_jurusan', 'nama_dosen']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl_mahasiswa():
    df_mhs = extract_table("mahasiswa")
    df_jur = extract_table("jurusan")
    df_dosen = extract_table("dosen_wali")

    df_dosen = df_dosen.rename(columns={"nama": "nama_dosen"})

    dim_mahasiswa = transform_mahasiswa(df_mhs, df_jur, df_dosen)
    load_table(dim_mahasiswa, "dim_mahasiswa")

if __name__ == "__main__":
    run_etl_mahasiswa()
