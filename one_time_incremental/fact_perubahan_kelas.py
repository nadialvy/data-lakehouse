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
        FROM fact_perubahan_kelas
    """
    return pd.read_sql(query, target_engine)

# === TRANSFORM ===
def transform_fact(df_detail, df_kelas, df_dim_mk, df_dim_mhs, df_dim_status, df_dim_waktu):
    print("Transforming fact_perubahan_kelas...")

    # JOIN untuk dapatkan semua ID dimensi
    df_detail = df_detail.merge(df_kelas[['id', 'kode_mata_kuliah']], left_on='kelas_id', right_on='id', how='left')
    df_detail = df_detail.merge(df_dim_mk[['kode_mata_kuliah', 'mata_kuliah_id']], on='kode_mata_kuliah', how='left')

    frs = extract_table("frs")
    frs['nrp'] = frs['nrp'].astype(str)
    df_detail = df_detail.merge(frs[['id', 'nrp', 'semester']], left_on='frs_id', right_on='id', how='left')

    df_dim_mhs['nrp'] = df_dim_mhs['nrp'].astype(str)
    df_detail = df_detail.merge(df_dim_mhs[['mahasiswa_id', 'nrp']], on='nrp', how='left')

    df_detail = df_detail.merge(df_dim_waktu[['waktu_id', 'tanggal']], on='tanggal', how='left')

    df_detail['action'] = df_detail['action'].str.strip().str.upper()
    df_dim_status['status'] = df_dim_status['status'].str.strip().str.upper()
    df_detail = df_detail.merge(df_dim_status, left_on='action', right_on='status', how='left')

    # Hitung jumlah ADD dan DROP per kombinasi unik
    jumlah_add = df_detail[df_detail['action'] == 'ADD'].groupby(
        ['mahasiswa_id', 'mata_kuliah_id', 'semester']
    ).size().reset_index(name='jumlah_add')

    jumlah_drop = df_detail[df_detail['action'] == 'DROP'].groupby(
        ['mahasiswa_id', 'mata_kuliah_id', 'semester']
    ).size().reset_index(name='jumlah_drop')

    df_result = df_detail.drop_duplicates(subset=['mahasiswa_id', 'mata_kuliah_id', 'semester'])
    df_result = df_result.merge(jumlah_add, on=['mahasiswa_id', 'mata_kuliah_id', 'semester'], how='left')
    df_result = df_result.merge(jumlah_drop, on=['mahasiswa_id', 'mata_kuliah_id', 'semester'], how='left')

    df_result['jumlah_add'] = df_result['jumlah_add'].fillna(0).astype(int)
    df_result['jumlah_drop'] = df_result['jumlah_drop'].fillna(0).astype(int)

    # SKS setelah perubahan (hanya ADD yang dihitung)
    df_sks = df_detail[df_detail['action'] == 'ADD'].merge(
        df_dim_mk[['mata_kuliah_id', 'sks']], on='mata_kuliah_id', how='left'
    )
    sks_sum = df_sks.groupby(['mahasiswa_id', 'mata_kuliah_id', 'semester'])['sks'].sum().reset_index(name='sks_setelah_perubahan')
    df_result = df_result.merge(sks_sum, on=['mahasiswa_id', 'mata_kuliah_id', 'semester'], how='left')
    df_result['sks_setelah_perubahan'] = df_result['sks_setelah_perubahan'].fillna(0).astype(int)

    # Ambil kolom yang dibutuhkan
    return df_result[['mata_kuliah_id', 'mahasiswa_id', 'status_perubahan_kelas_id',
                      'waktu_id', 'jumlah_drop', 'jumlah_add', 'sks_setelah_perubahan']]

# === FILTER ===
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
    df.insert(0, 'perubahan_kelas_id', range(last_id + 1, last_id + 1 + len(df)))
    df.to_sql(table_name, target_engine, if_exists='append', index=False)
    print("✅ Load selesai.")

# === MAIN ===
def run_etl_incremental():
    df_detail = extract_table('detail_frs')
    df_kelas = extract_table('kelas')

    df_dim_mk = extract_dim_table('dim_mata_kuliah')
    df_dim_mhs = extract_dim_table('dim_mahasiswa')
    df_dim_status = extract_dim_table('dim_status_perubahan_kelas')
    df_dim_waktu = extract_dim_table('dim_waktu')

    df_existing_keys = get_existing_keys()
    df_fact = transform_fact(df_detail, df_kelas, df_dim_mk, df_dim_mhs, df_dim_status, df_dim_waktu)
    df_fact_filtered = filter_new_rows(df_fact, df_existing_keys)

    if not df_fact_filtered.empty:
        last_id = get_max_surrogate('fact_perubahan_kelas', 'perubahan_kelas_id')
        load_table(df_fact_filtered, 'fact_perubahan_kelas', last_id)
    else:
        print("📭 Tidak ada baris baru yang dimuat.")

if __name__ == "__main__":
    run_etl_incremental()
