import getpass
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_date_column(table, column):
    print(f"Extracting {table}.{column}...")
    return pd.read_sql(f"SELECT {column} as tanggal FROM {table} WHERE {column} IS NOT NULL", source_engine)

def extract_existing_dates():
    print("Extracting existing dates from dim_waktu...")
    try:
        df = pd.read_sql("SELECT tanggal FROM dim_waktu", target_engine)
        return pd.to_datetime(df['tanggal']).dt.date  # <-- Return Series
    except:
        return pd.Series([], dtype="object")

# === TRANSFORM ===
def get_max_surrogate(table, id_column):
    query = f"SELECT MAX({id_column}) AS max_id FROM {table}"
    result = pd.read_sql(query, target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

def derive_semester(row):
    month = row['tanggal'].month
    return 'Genap' if month <= 6 else 'Ganjil'

def transform_dim_waktu(dateframes, existing_dates):
    print("Transforming waktu dimension...")

    df_all = pd.concat(dateframes, ignore_index=True).dropna()
    # Drop tanggal yang out of bounds
    df_all = df_all[df_all['tanggal'] <= datetime(2262, 4, 11).date()]
    df_all = df_all[df_all['tanggal'] >= datetime(1900, 1, 1).date()]  # optionally drop far past

    # Konversi aman
    df_all['tanggal'] = pd.to_datetime(df_all['tanggal'], errors='coerce').dt.date
    df_all = df_all.dropna(subset=['tanggal'])

    df_all = df_all.drop_duplicates(subset=['tanggal'])

    # FILTER tanggal baru
    df_new = df_all[~df_all['tanggal'].isin(existing_dates)].sort_values('tanggal')

    if df_new.empty:
        print("✅ Tidak ada tanggal baru yang perlu ditambahkan.")
        return pd.DataFrame()

    print(f"📅 Tanggal baru yang akan dimasukkan ({len(df_new)}):")
    for tgl in df_new['tanggal']:
        print(f" - {tgl}")

    df_new['hari'] = pd.to_datetime(df_new['tanggal']).dt.day_name()
    df_new['bulan'] = pd.to_datetime(df_new['tanggal']).dt.month_name()
    df_new['tahun'] = pd.to_datetime(df_new['tanggal']).dt.year
    df_new['semester_akademik'] = df_new.apply(derive_semester, axis=1)

    last_id = get_max_surrogate('dim_waktu', 'waktu_id')
    df_new['waktu_id'] = range(last_id + 1, last_id + 1 + len(df_new))

    return df_new[['waktu_id', 'tanggal', 'hari', 'bulan', 'tahun', 'semester_akademik']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    if df.empty:
        print("❎ Tidak ada data untuk dimuat.")
        return
    df.to_sql(table_name, target_engine, if_exists='append', index=False)
    print(f"✅ {len(df)} baris berhasil dimuat ke {table_name}.")

# === MAIN ===
def run_incremental_etl_waktu():
    df1 = extract_date_column("frs", "tanggal_disetujui")
    df2 = extract_date_column("pembayaran", "tanggal_bayar")
    df3 = extract_date_column("detail_frs", "tanggal")
    df4 = extract_date_column("pembatalan_frs", "tanggal_pengajuan")

    existing_dates = extract_existing_dates()
    dim_waktu = transform_dim_waktu([df1, df2, df3, df4], existing_dates)
    load_table(dim_waktu, "dim_waktu")

if __name__ == "__main__":
    run_incremental_etl_waktu()
