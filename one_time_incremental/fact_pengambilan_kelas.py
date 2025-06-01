import getpass
import pandas as pd
from sqlalchemy import create_engine

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

def extract_dim_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", target_engine)

def get_max_surrogate(table, id_column):
    query = f"SELECT MAX({id_column}) AS max_id FROM {table}"
    result = pd.read_sql(query, target_engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0

def get_existing_keys():
    query = """
        SELECT mahasiswa_id, mata_kuliah_id, waktu_id
        FROM fact_pengambilan_kelas
    """
    return pd.read_sql(query, target_engine)

# === TRANSFORM ===
def transform_fact(df_detail, df_frs, df_kelas, df_mk, df_nilai, df_mahasiswa, df_pembayaran, df_waktu):
    print("Transforming fact_pengambilan_kelas...")

    df = df_detail.merge(df_kelas, left_on="kelas_id", right_on="id", how="left")
    df = df.merge(df_mk, on="kode_mata_kuliah", how="left")
    df = df.merge(df_frs[['id', 'nrp', 'semester', 'tanggal_disetujui']],
                  left_on='frs_id', right_on='id', suffixes=('', '_frs'))

    df['nrp'] = df['nrp'].astype(str)
    df_frs['nrp'] = df_frs['nrp'].astype(str)
    df_mahasiswa['nrp'] = df_mahasiswa['nrp'].astype(str)
    df_nilai['nrp'] = df_nilai['nrp'].astype(str)
    df_pembayaran['nrp'] = df_pembayaran['nrp'].astype(str)

    df['semester'] = df['semester'].astype(int)
    df_pembayaran['semester'] = df_pembayaran['semester'].astype(int)
    df_nilai['semester'] = df_nilai['semester'].astype(int)

    df = df.merge(df_mahasiswa[['mahasiswa_id', 'nrp']], on="nrp", how="left")

    df['sks_diambil'] = df['sks']
    df['is_drop'] = df['action'].apply(lambda x: 1 if str(x).upper() == "DROP" else 0)

    nilai_prev = df_nilai.merge(df[['nrp', 'semester']].rename(columns={'semester': 'semester_ref'}), on='nrp')
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

    df = df.merge(df_pembayaran[['nrp', 'semester']], on=['nrp', 'semester'], how="left", indicator=True)
    df['sudah_bayar_flag'] = df['_merge'].apply(lambda x: 1 if x == 'both' else 0)
    df.drop(columns=['_merge'], inplace=True)

    df = df.merge(df_waktu[['waktu_id', 'tanggal']], left_on='tanggal_disetujui', right_on='tanggal', how='left')

    missing_waktu = df[df['waktu_id'].isnull()]
    if not missing_waktu.empty:
        print("❌ Beberapa tanggal_disetujui tidak ditemukan di dim_waktu:")
        print(missing_waktu[['tanggal_disetujui']].drop_duplicates())
        print(f"Total baris yang akan di-drop: {len(missing_waktu)}")

    df = df[df['waktu_id'].notnull()]

    df = df[['mahasiswa_id', 'mata_kuliah_id', 'waktu_id',
             'sks_diambil', 'ipk_terakhir', 'sudah_bayar_flag', 'is_drop']]

    return df

# === DEDUPLICATION ===
def filter_new_rows(df_new, df_existing_keys):
    print("🔍 Filtering existing rows...")
    df_new = df_new.merge(df_existing_keys, on=['mahasiswa_id', 'mata_kuliah_id', 'waktu_id'], how='left', indicator=True)
    df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])
    print(f"✅ Ditemukan {len(df_new)} baris baru untuk dimuat.")
    return df_new

# === LOAD ===
def load_table(df, table_name, last_id):
    print(f"⬆️ Loading to {table_name}...")
    df = df.copy()
    df.insert(0, 'pengambilan_kelas_id', range(last_id + 1, last_id + 1 + len(df)))
    df.to_sql(table_name, target_engine, if_exists='append', index=False)
    print("✅ Load selesai.")

# === MAIN ===
def run_etl_incremental():
    df_detail = extract_table('detail_frs')
    df_frs = extract_table('frs')
    df_kelas = extract_table('kelas')
    df_nilai = extract_table('nilai_mahasiswa')
    df_pembayaran = extract_table('pembayaran')

    df_mahasiswa = extract_dim_table('dim_mahasiswa')
    df_mk = extract_dim_table('dim_mata_kuliah')
    df_waktu = extract_dim_table('dim_waktu')

    df_existing_keys = get_existing_keys()
    df_fact = transform_fact(df_detail, df_frs, df_kelas, df_mk, df_nilai, df_mahasiswa, df_pembayaran, df_waktu)
    df_fact_filtered = filter_new_rows(df_fact, df_existing_keys)

    if not df_fact_filtered.empty:
        last_id = get_max_surrogate('fact_pengambilan_kelas', 'pengambilan_kelas_id')
        load_table(df_fact_filtered, 'fact_pengambilan_kelas', last_id)
    else:
        print("📭 Tidak ada baris baru yang dimuat.")

if __name__ == "__main__":
    run_etl_incremental()
