import getpass
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import numpy as np

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_table(table_name, engine):
    print(f"Extracting {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)

# === SURROGATE ===
def get_max_surrogate(table, id_column):
    result = pd.read_sql(f"SELECT MAX({id_column}) as max_id FROM {table}", target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

# === TRANSFORM ===
def transform_dim_mata_kuliah(df_mk, df_kelas):
    df = df_kelas.merge(df_mk, on="kode_mata_kuliah", how="left")

    df['kode_mata_kuliah'] = df['kode_mata_kuliah'].str.strip()
    df['nama'] = df['nama'].str.strip().str.title()
    df['nama_kelas'] = df['nama_kelas'].str.strip().str.upper()
    df['dosen'] = df['dosen'].str.strip().str.title()

    df['row_effective_date'] = pd.Timestamp(datetime.now().date())
    df['row_expiration_date'] = pd.Timestamp(datetime.max.date())
    df['current_row_flag'] = 'Current'

    return df[['kode_mata_kuliah', 'nama', 'sks', 'nama_kelas', 'dosen', 'kapasitas',
               'row_effective_date', 'row_expiration_date', 'current_row_flag']]

# === LOAD ===
def incremental_scd2_load(df_new):
    df_existing = extract_table("dim_mata_kuliah", target_engine)
    df_existing = df_existing[df_existing['current_row_flag'] == 'Current'].copy()

    # Join by natural key
    merged = df_existing.merge(df_new, 
        on=['kode_mata_kuliah', 'nama_kelas'], 
        suffixes=('_old', '_new'))

    # Detect changed SCD2 cols
    changed = merged[
        (merged['dosen_old'] != merged['dosen_new']) |
        (merged['kapasitas_old'] != merged['kapasitas_new'])
    ]

    # Step 1: Expire old rows
    if not changed.empty:
        print(f"🔁 {len(changed)} perubahan ditemukan, menandai baris lama sebagai expired...")

        with target_engine.begin() as conn:
            for _, row in changed.iterrows():
                update = text("""
                    UPDATE dim_mata_kuliah
                    SET row_expiration_date = :expire_date, current_row_flag = 'Expired'
                    WHERE kode_mata_kuliah = :kode AND nama_kelas = :kelas AND current_row_flag = 'Current'
                """)
                conn.execute(update, {
                    "expire_date": datetime.now().date(),
                    "kode": row['kode_mata_kuliah'],
                    "kelas": row['nama_kelas']
                })

    # Step 2: Insert changed + new records
    unchanged_keys = df_existing[['kode_mata_kuliah', 'nama_kelas']].apply(tuple, axis=1)
    new_keys = df_new[['kode_mata_kuliah', 'nama_kelas']].apply(tuple, axis=1)
    keys_to_insert = ~new_keys.isin(unchanged_keys)

    to_insert = df_new[keys_to_insert].copy()
    changed_keys = changed[['kode_mata_kuliah', 'nama_kelas']].apply(tuple, axis=1)
    to_insert_changed = df_new[df_new[['kode_mata_kuliah', 'nama_kelas']].apply(tuple, axis=1).isin(changed_keys)]

    to_insert = pd.concat([to_insert, to_insert_changed])

    if not to_insert.empty:
        last_id = get_max_surrogate('dim_mata_kuliah', 'mata_kuliah_id')
        to_insert = to_insert.drop_duplicates(subset=['kode_mata_kuliah', 'nama_kelas', 'dosen', 'kapasitas'])
        to_insert['mata_kuliah_id'] = range(last_id + 1, last_id + 1 + len(to_insert))

        print(f"✅ Menambahkan {len(to_insert)} baris baru ke dim_mata_kuliah...")
        to_insert.to_sql("dim_mata_kuliah", target_engine, if_exists='append', index=False)
    else:
        print("✅ Tidak ada baris baru yang perlu dimasukkan.")

# === MAIN ===
def run_incremental_dim_mk():
    df_mk = extract_table("mata_kuliah", source_engine)
    df_kelas = extract_table("kelas", source_engine)

    transformed = transform_dim_mata_kuliah(df_mk, df_kelas)
    incremental_scd2_load(transformed)

if __name__ == "__main__":
    run_incremental_dim_mk()
