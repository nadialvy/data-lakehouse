import getpass
import pandas as pd
from sqlalchemy import create_engine

# Configuration
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

def extract_table(table_name):
    print(f"Extracting {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", source_engine)

def extract_dim(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", target_engine)

def get_max_surrogate(table, id_column):
    result = pd.read_sql(f"SELECT MAX({id_column}) as max_id FROM {table}", target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

def transform_fact(df_batal, df_frs, df_nilai, df_waktu, df_mahasiswa):
    print("Transforming fact_pembatalan_frs...")

    df = df_batal.merge(df_frs[['id', 'nrp', 'tanggal_disetujui', 'semester']], left_on='frs_id', right_on='id')
    df['tanggal_pengajuan'] = pd.to_datetime(df['tanggal_pengajuan'])
    df['tanggal_disetujui'] = pd.to_datetime(df['tanggal_disetujui'])
    df['nrp'] = df['nrp'].astype(str)
    df_mahasiswa['nrp'] = df_mahasiswa['nrp'].astype(str)

    df = df.merge(df_mahasiswa[['mahasiswa_id', 'nrp']], on='nrp', how='left')

    nilai_prev = df_nilai.copy()
    nilai_prev['nrp'] = nilai_prev['nrp'].astype(str)
    nilai_prev = nilai_prev.merge(df[['nrp', 'semester']], on='nrp')
    nilai_prev = nilai_prev[nilai_prev['semester_x'] < nilai_prev['semester_y']]
    ipk = nilai_prev.groupby('nrp')['nilai'].mean().round(2).reset_index().rename(columns={'nilai': 'ipk_terakhir'})
    df = df.merge(ipk, on='nrp', how='left')
    df['ipk_terakhir'] = df['ipk_terakhir'].fillna(0.0)
    df_waktu['tanggal'] = pd.to_datetime(df_waktu['tanggal'])

    df = df.merge(df_waktu[['waktu_id', 'tanggal']].rename(columns={'waktu_id': 'waktu_pengajuan_id', 'tanggal': 'tanggal_pengajuan'}), 
                  on='tanggal_pengajuan', how='left')
    df = df.merge(df_waktu[['waktu_id', 'tanggal']].rename(columns={'waktu_id': 'waktu_verifikasi_id', 'tanggal': 'tanggal_disetujui'}), 
                  on='tanggal_disetujui', how='left')

    df['lama_verifikasi_pembatalan'] = (df['tanggal_disetujui'] - df['tanggal_pengajuan']).dt.days.fillna(0).astype(int)

    last_id = get_max_surrogate('fact_pembatalan_frs', 'pembatalan_frs_id')
    df['pembatalan_frs_id'] = range(last_id + 1, last_id + 1 + len(df))

    return df[['pembatalan_frs_id', 'mahasiswa_id', 'waktu_pengajuan_id', 'waktu_verifikasi_id',
               'ipk_terakhir', 'lama_verifikasi_pembatalan']]

def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

def run_etl():
    df_batal = extract_table('pembatalan_frs')
    df_frs = extract_table('frs')
    df_nilai = extract_table('nilai_mahasiswa')
    df_waktu = extract_dim('dim_waktu')
    df_mahasiswa = extract_dim('dim_mahasiswa')

    df_fact = transform_fact(df_batal, df_frs, df_nilai, df_waktu, df_mahasiswa)
    load_table(df_fact, 'fact_pembatalan_frs')

if __name__ == "__main__":
    run_etl()
