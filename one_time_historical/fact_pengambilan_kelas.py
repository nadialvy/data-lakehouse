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

def extract_dim_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", target_engine)

def get_max_surrogate(table, id_column):
    query = f"SELECT MAX({id_column}) AS max_id FROM {table}"
    result = pd.read_sql(query, target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

# === TRANSFORM ===
def transform_fact(df_detail, df_frs, df_kelas, df_mk, df_nilai, df_mahasiswa, df_pembayaran, df_waktu):
    print("Transforming fact_pengambilan_kelas...")

    # Join detail_frs → kelas → mata_kuliah
    df = df_detail.merge(df_kelas, left_on="kelas_id", right_on="id", how="left")
    df = df.merge(df_mk, on="kode_mata_kuliah", how="left")

    # Join detail_frs → frs → nrp + semester
    df = df.merge(df_frs[['id', 'nrp', 'semester']], left_on='frs_id', right_on='id', suffixes=('', '_frs'))

    # === Baru setelah ini casting nrp & semester ===
    df['nrp'] = df['nrp'].astype(str)
    df_frs['nrp'] = df_frs['nrp'].astype(str)
    df_mahasiswa['nrp'] = df_mahasiswa['nrp'].astype(str)
    df_nilai['nrp'] = df_nilai['nrp'].astype(str)
    df_pembayaran['nrp'] = df_pembayaran['nrp'].astype(str)

    df['semester'] = df['semester'].astype(int)
    df_frs['semester'] = df_frs['semester'].astype(int)
    df_pembayaran['semester'] = df_pembayaran['semester'].astype(int)
    df_nilai['semester'] = df_nilai['semester'].astype(int)

    # Join detail_frs → frs → nrp + semester
    df = df.merge(df_frs[['id', 'nrp', 'semester']], left_on='frs_id', right_on='id', suffixes=('', '_frs'))

    # Join ke dim_mahasiswa untuk get mahasiswa_id
    df = df.merge(df_mahasiswa[['mahasiswa_id', 'nrp']], on="nrp", how="left")

    # Lookup sks dari mata_kuliah
    df['sks_diambil'] = df['sks']

    # Flag is_drop
    df['is_drop'] = df['action'].apply(lambda x: 1 if x.upper() == "DROP" else 0)

    # Hitung IPK terakhir dari semester sebelumnya
    nilai_prev = df_nilai.merge(
        df[['nrp', 'semester']].rename(columns={'semester': 'semester_ref'}),
        on='nrp'
    )
    nilai_prev = nilai_prev[nilai_prev['semester'] < nilai_prev['semester_ref']]

    ipk = (
        nilai_prev.groupby('nrp')['nilai']
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={'nilai': 'ipk_terakhir'})
    )
    df = df.merge(ipk, on="nrp", how="left")
    df['ipk_terakhir'] = df['ipk_terakhir'].fillna(0.0)

    # Flag sudah bayar
    df = df.merge(df_pembayaran[['nrp', 'semester']], on=['nrp', 'semester'], how="left", indicator=True)
    df['sudah_bayar_flag'] = df['_merge'].apply(lambda x: 1 if x == 'both' else 0)
    df.drop(columns=['_merge'], inplace=True)

    # Lookup waktu_id dari tanggal
    df = df.merge(df_waktu[['waktu_id', 'tanggal']], left_on='tanggal', right_on='tanggal', how='left')

    # Generate surrogate key
    last_id = get_max_surrogate('fact_pengambilan_kelas', 'pengambilan_kelas_id')
    df['pengambilan_kelas_id'] = range(last_id + 1, last_id + 1 + len(df))

    return df[['pengambilan_kelas_id', 'mahasiswa_id', 'mata_kuliah_id', 'waktu_id',
               'sks_diambil', 'ipk_terakhir', 'sudah_bayar_flag', 'is_drop']]

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ===
def run_etl_fact_pengambilan_kelas():
    # Source
    df_detail = extract_table('detail_frs')
    df_frs = extract_table('frs')
    df_kelas = extract_table('kelas')
    df_nilai = extract_table('nilai_mahasiswa')
    df_pembayaran = extract_table('pembayaran')

    # Dim
    df_mahasiswa = extract_dim_table('dim_mahasiswa')
    df_mk = extract_dim_table('dim_mata_kuliah')
    df_waktu = extract_dim_table('dim_waktu')

    df_fact = transform_fact(df_detail, df_frs, df_kelas, df_mk, df_nilai, df_mahasiswa, df_pembayaran, df_waktu)
    load_table(df_fact, "fact_pengambilan_kelas")

if __name__ == "__main__":
    run_etl_fact_pengambilan_kelas()
