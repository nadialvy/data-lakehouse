import getpass
import pandas as pd
from sqlalchemy import create_engine

# === CONFIG ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

def extract_table(name):
    print(f"Extracting {name}...")
    return pd.read_sql(f"SELECT * FROM {name}", source_engine)

def extract_dim(name):
    return pd.read_sql(f"SELECT * FROM {name}", target_engine)

def get_max_id(table, col):
    result = pd.read_sql(f"SELECT MAX({col}) as max_id FROM {table}", target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

# === TRANSFORM ===
def transform(df_frs, df_mhs_raw, df_log, df_detail, df_kelas, df_mk, df_waktu, df_status, df_nilai, df_mhs_dim):
    print("Transforming fact_persetujuan_frs...")

    # Cast data type
    df_frs['nrp'] = df_frs['nrp'].astype(str)
    df_mhs_raw['nrp'] = df_mhs_raw['nrp'].astype(str)
    df_nilai['nrp'] = df_nilai['nrp'].astype(str)
    df_mhs_dim['nrp'] = df_mhs_dim['nrp'].astype(str)
    df_log['tanggal'] = pd.to_datetime(df_log['tanggal'])
    df_waktu['tanggal'] = pd.to_datetime(df_waktu['tanggal'])

    # Merge mahasiswa_id dan dosen_wali_id
    df_frs = df_frs.merge(df_mhs_dim[['mahasiswa_id', 'nrp']], on='nrp', how='left')
    df_frs = df_frs.merge(df_mhs_raw[['nrp', 'dosen_wali_id']], on='nrp', how='left')

    # Ambil status log terakhir per FRS
    df_log_latest = df_log.sort_values('tanggal').drop_duplicates(subset='frs_id', keep='last')
    df_log_latest = df_log_latest.rename(columns={
        'status': 'status_log',
        'tanggal': 'tanggal_log'
    })
    df_frs = df_frs.merge(df_log_latest[['frs_id', 'status_log', 'tanggal_log']], left_on='id', right_on='frs_id', how='left')

    # Lookup waktu_id dari tanggal log
    df_frs = df_frs.merge(
        df_waktu.rename(columns={'waktu_id': 'waktu_id_log', 'tanggal': 'tanggal_log'}),
        left_on='tanggal_log',
        right_on='tanggal_log',
        how='left'
    )

    # Mapping status ke status_id
    df_frs = df_frs.merge(df_status, left_on='status_log', right_on='status', how='left')

    # Hitung jumlah SKS diambil (hanya yang ADD)
    df_detail_add = df_detail[df_detail['action'] == 'ADD']
    df_detail_add = df_detail_add.merge(df_kelas, left_on='kelas_id', right_on='id', how='left')
    df_detail_add = df_detail_add.merge(df_mk[['mata_kuliah_id', 'kode_mata_kuliah', 'sks']], on='kode_mata_kuliah', how='left')
    sks_per_frs = df_detail_add.groupby('frs_id')['sks'].sum().reset_index().rename(columns={'sks': 'jumlah_sks'})
    df_frs = df_frs.merge(sks_per_frs, left_on='id', right_on='frs_id', how='left')
    df_frs['jumlah_sks'] = df_frs['jumlah_sks'].fillna(0)

    # Hitung IPK terakhir: nilai dari semester sebelumnya
    nilai_prev = df_nilai.merge(df_frs[['nrp', 'semester']], on='nrp', how='inner')
    nilai_prev = nilai_prev[nilai_prev['semester_x'] < nilai_prev['semester_y']]
    ipk = nilai_prev.groupby('nrp')['nilai'].mean().round(2).reset_index().rename(columns={'nilai': 'ipk_terakhir'})
    df_frs = df_frs.merge(ipk, on='nrp', how='left')
    df_frs['ipk_terakhir'] = df_frs['ipk_terakhir'].fillna(0)

    # Generate surrogate key
    last_id = get_max_id('fact_persetujuan_frs', 'persetujuan_frs_id')
    df_frs['persetujuan_frs_id'] = range(last_id + 1, last_id + 1 + len(df_frs))

    return df_frs[['persetujuan_frs_id', 'mahasiswa_id', 'dosen_wali_id', 'semester', 'status_id', 'jumlah_sks', 'ipk_terakhir', 'waktu_id_log']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl():
    df_frs = extract_table("frs")
    df_mhs_raw = extract_table("mahasiswa")
    df_log = extract_table("log_frs")
    df_detail = extract_table("detail_frs")
    df_kelas = extract_table("kelas")
    df_nilai = extract_table("nilai_mahasiswa")

    df_mhs_dim = extract_dim("dim_mahasiswa")
    df_mk = extract_dim("dim_mata_kuliah")
    df_waktu = extract_dim("dim_waktu")
    df_status = extract_dim("dim_status")

    df_result = transform(df_frs, df_mhs_raw, df_log, df_detail, df_kelas, df_mk, df_waktu, df_status, df_nilai, df_mhs_dim)
    load_table(df_result, 'fact_persetujuan_frs')

if __name__ == "__main__":
    run_etl()
