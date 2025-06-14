import pandas as pd
from sqlalchemy import create_engine

# === CONFIGURASI KONEKSI ===
password = ""  # isi dengan password MySQL-mu
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

def extract_table(name):
    print(f"[EXTRACT] {name}")
    return pd.read_sql(f"SELECT * FROM {name}", source_engine)

def extract_dim(name):
    print(f"[EXTRACT DIM] {name}")
    return pd.read_sql(f"SELECT * FROM {name}", target_engine)

def get_max_id(table, col):
    df = pd.read_sql(f"SELECT MAX({col}) AS max_id FROM {table}", target_engine)
    return int(df['max_id'][0]) if df['max_id'][0] is not None else 0

# === TRANSFORM ===
def transform(
    df_frs, df_mhs_raw, df_log, df_detail, df_kelas,
    df_nilai, df_dim_mhs, df_dim_waktu
):
    print("[TRANSFORM] fact_persetujuan_frs")

    # 1) Format kolom NRP dan ubah log.tanggal → datetime
    df_frs['nrp']        = df_frs['nrp'].astype(str)
    df_mhs_raw['nrp']    = df_mhs_raw['nrp'].astype(str)
    df_nilai['nrp']      = df_nilai['nrp'].astype(str)
    df_dim_mhs['nrp']    = df_dim_mhs['nrp'].astype(str)
    df_log['tanggal']    = pd.to_datetime(df_log['tanggal'])
    df_dim_waktu['tanggal'] = pd.to_datetime(df_dim_waktu['tanggal'])

    # 2) Tambahkan kolom date‐only (strip jam, menit, detik)
    df_log['tanggal_date']      = df_log['tanggal'].dt.normalize()
    df_dim_waktu['tanggal_date'] = df_dim_waktu['tanggal'].dt.normalize()

    # 3) Merge untuk dapatkan mahasiswa_id & dosen_wali_id
    df = df_frs.merge(
        df_dim_mhs[['mahasiswa_id', 'nrp']],
        on='nrp', how='left'
    ).merge(
        df_mhs_raw[['nrp', 'dosen_wali_id']],
        on='nrp', how='left'
    )

    # 4) Ambil log terakhir (per frs_id), sekaligus simpan tanggal_date
    df_log_latest = (
        df_log
        .sort_values('tanggal')
        .drop_duplicates(subset='frs_id', keep='last')
        .rename(columns={'status': 'status_log'})
    )
    # debug: cek beberapa tanggal_date unik di log dan di dim_waktu
    print("[DEBUG] Unique tanggal_date di log_frs:", 
          df_log_latest['tanggal_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df_log_latest['tanggal_date'].nunique())
    print("[DEBUG] Unique tanggal_date di dim_waktu:", 
          df_dim_waktu['tanggal_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df_dim_waktu['tanggal_date'].nunique())

    df = df.merge(
        df_log_latest[['frs_id', 'status_log', 'tanggal_date']],
        left_on='id', right_on='frs_id', how='left'
    )

    # 5) Merge ke dim_waktu berdasarkan tanggal_date
    df = df.merge(
        df_dim_waktu.rename(columns={'waktu_id': 'waktu_persetujuan_id'}),
        on='tanggal_date',
        how='left'
    )

    # 6) Mapping status_log → is_frs_disetujui (1/0)
    df['is_frs_disetujui'] = df['status_log'].apply(
        lambda s: 1 if str(s).upper() == 'DISETUJUI' else 0
    )

    # 7) Hitung jumlah SKS (hanya yang action='ADD')
    df_detail_add = df_detail[df_detail['action'] == 'ADD'].copy()
    df_detail_add = df_detail_add.merge(
        df_kelas[['id', 'kode_mata_kuliah']],
        left_on='kelas_id', right_on='id', how='left'
    )
    df_dim_mk = extract_dim("dim_mata_kuliah")[['kode_mata_kuliah', 'sks']]
    df_detail_add = df_detail_add.merge(
        df_dim_mk, on='kode_mata_kuliah', how='left'
    )
    sks_per_frs = (
        df_detail_add
        .groupby('frs_id')['sks']
        .sum()
        .reset_index()
        .rename(columns={'sks': 'jumlah_sks'})
    )
    df = df.merge(
        sks_per_frs, left_on='id', right_on='frs_id', how='left'
    )
    df['jumlah_sks'] = df['jumlah_sks'].fillna(0).astype(int)


    # 9) Filter baris yang berhasil match ke dim_waktu
    #    (agar waktu_persetujuan_id tidak null)
    df = df[df['waktu_persetujuan_id'].notna()]

    # 10) Generate surrogate key persetujuan_frs_id
    last_id = get_max_id('fact_persetujuan_frs', 'persetujuan_frs_id')
    df['persetujuan_frs_id'] = range(last_id + 1, last_id + 1 + len(df))

    # 11) Pilih kolom final sesuai DDL
    df_final = df[[
        'persetujuan_frs_id',
        'mahasiswa_id',
        'dosen_wali_id',
        'waktu_persetujuan_id',
        'is_frs_disetujui',
        'jumlah_sks',
    ]].copy()

    return df_final

# === LOAD ===
def load_table(df, table_name):
    print(f"[LOAD] {table_name} → {len(df)} baris")
    df.to_sql(
        table_name,
        target_engine,
        if_exists='append',
        index=False
    )

# === MAIN ETL ===
def run_etl():
    # Extract dari source OLTP
    df_frs       = extract_table("frs")
    df_mhs_raw   = extract_table("mahasiswa")
    df_log       = extract_table("log_frs")
    df_detail    = extract_table("detail_frs")
    df_kelas     = extract_table("kelas")
    df_nilai     = extract_table("nilai_mahasiswa")

    # Extract dimensi (OLAP)
    df_dim_mhs   = extract_dim("dim_mahasiswa")
    df_dim_waktu = extract_dim("dim_waktu")

    # Transformasi
    df_fact = transform(
        df_frs, df_mhs_raw, df_log, df_detail, df_kelas,
        df_nilai, df_dim_mhs, df_dim_waktu
    )

    # Load ke fact_persetujuan_frs
    load_table(df_fact, 'fact_persetujuan_frs')

if __name__ == "__main__":
    run_etl()
