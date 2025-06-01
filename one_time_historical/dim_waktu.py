import getpass
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_date_column(table, column):
    print(f"Extracting {table}.{column}...")
    return pd.read_sql(f"SELECT {column} as tanggal FROM {table} WHERE {column} IS NOT NULL", source_engine)

# === TRANSFORM ===
def get_max_surrogate(table, id_column):
    query = f"SELECT MAX({id_column}) AS max_id FROM {table}"
    result = pd.read_sql(query, target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

def derive_semester(row):
    month = row['tanggal'].month
    return 'Genap' if month <= 6 else 'Ganjil'

def transform_dim_waktu(*dateframes):
    print("Transforming waktu dimension...")

    # Gabung semua tanggal jadi satu kolom
    df_all = pd.concat(dateframes, ignore_index=True).dropna()
    df_all['tanggal'] = pd.to_datetime(df_all['tanggal']).dt.date
    df_all = df_all.drop_duplicates(subset=['tanggal']).sort_values('tanggal')

    df_all['hari'] = pd.to_datetime(df_all['tanggal']).dt.day_name()
    df_all['bulan'] = pd.to_datetime(df_all['tanggal']).dt.month_name()
    df_all['tahun'] = pd.to_datetime(df_all['tanggal']).dt.year
    df_all['semester_akademik'] = df_all.apply(derive_semester, axis=1)

    last_id = get_max_surrogate('dim_waktu', 'waktu_id')
    df_all['waktu_id'] = range(last_id + 1, last_id + 1 + len(df_all))

    return df_all[['waktu_id', 'tanggal', 'hari', 'bulan', 'tahun', 'semester_akademik']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl_waktu():
    df1 = extract_date_column("frs", "tanggal_disetujui")
    df2 = extract_date_column("pembayaran", "tanggal_bayar")
    df3 = extract_date_column("detail_frs", "tanggal")
    df4 = extract_date_column("pembatalan_frs", "tanggal_pengajuan")

    dim_waktu = transform_dim_waktu(df1, df2, df3, df4)
    load_table(dim_waktu, "dim_waktu")

if __name__ == "__main__":
    run_etl_waktu()
