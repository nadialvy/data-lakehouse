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

def get_max_waktu_date_from_fact():
    """
    Cari tanggal maksimum (date-only) yang sudah ada di fact_persetujuan_frs.
    Caranya: Ambil max(waktu_persetujuan_id) lalu lookup di dim_waktu.
    Jika belum ada row di fact, kembalikan None.
    """
    sql = """
        SELECT f.waktu_persetujuan_id
        FROM fact_persetujuan_frs AS f
        ORDER BY f.waktu_persetujuan_id DESC
        LIMIT 1
    """
    df = pd.read_sql(sql, target_engine)
    if df.empty:
        return None

    max_waktu_id = int(df.at[0, 'waktu_persetujuan_id'])
    # Sekarang ambil tanggal_date di dim_waktu untuk max_waktu_id
    sql2 = f"""
        SELECT tanggal AS tanggal_full
        FROM dim_waktu
        WHERE waktu_id = {max_waktu_id}
        LIMIT 1
    """
    df2 = pd.read_sql(sql2, target_engine)
    if df2.empty:
        return None

    # Normalize → ambil date saja
    ts = pd.to_datetime(df2.at[0, 'tanggal_full']).normalize()
    print(f"[WATERMARK] Last loaded tanggal_date di fact = {ts.date()}")
    return ts

# === TRANSFORM ===
def transform_incremental(
    df_frs, df_mhs_raw, df_log, df_detail, df_kelas,
    df_nilai, df_dim_mhs, df_dim_waktu, watermark_date
):
    """
    Sama seperti transform() sebelumnya, tapi cuma proses baris log_frs di atas watermark_date.
    watermark_date: pd.Timestamp (date-only) atau None (jika full load).
    """
    print("[TRANSFORM-INC] fact_persetujuan_frs")

    # 1) Format NRP dan ubah log.tanggal jadi datetime + buat tanggal_date
    df_frs['nrp']        = df_frs['nrp'].astype(str)
    df_mhs_raw['nrp']    = df_mhs_raw['nrp'].astype(str)
    df_nilai['nrp']      = df_nilai['nrp'].astype(str)
    df_dim_mhs['nrp']    = df_dim_mhs['nrp'].astype(str)
    df_log['tanggal']    = pd.to_datetime(df_log['tanggal'])
    df_dim_waktu['tanggal'] = pd.to_datetime(df_dim_waktu['tanggal'])

    df_log['tanggal_date']      = df_log['tanggal'].dt.normalize()
    df_dim_waktu['tanggal_date'] = df_dim_waktu['tanggal'].dt.normalize()

    # 2) Jika watermark_date tidak None, filter hanya log_frs setelah tanggal itu
    if watermark_date is not None:
        mask_new = df_log['tanggal_date'] > watermark_date
        df_log = df_log[mask_new].copy()
        print(f"[FILTER] Baris log_frs setelah {watermark_date.date()} = {len(df_log)}")
        if df_log.empty:
            print("[TRANSFORM-INC] Tidak ada data baru di log_frs → skip transform")
            return pd.DataFrame(columns=[
                'persetujuan_frs_id',
                'mahasiswa_id',
                'dosen_wali_id',
                'waktu_persetujuan_id',
                'is_frs_disetujui',
                'jumlah_sks',
            ])

    # 3) Merge untuk dapatkan mahasiswa_id & dosen_wali_id
    df = df_frs.merge(
        df_dim_mhs[['mahasiswa_id', 'nrp']],
        on='nrp', how='left'
    ).merge(
        df_mhs_raw[['nrp', 'dosen_wali_id']],
        on='nrp', how='left'
    )

    # 4) Ambil log terakhir *hanya untuk frs_id yang masuk df_log versi baru*
    #    (df_log sudah berisi only new records)
    df_log_latest = (
        df_log
        .sort_values('tanggal')
        .drop_duplicates(subset='frs_id', keep='last')
        .rename(columns={'status': 'status_log'})
    )
    # debug: cek uniq tanggal_date (optional)
    print("[DEBUG] Unique tanggal_date di log_frs (baru):", 
          df_log_latest['tanggal_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df_log_latest['tanggal_date'].nunique())
    print("[DEBUG] Unique tanggal_date di dim_waktu:", 
          df_dim_waktu['tanggal_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df_dim_waktu['tanggal_date'].nunique())

    # Gabungkan log terbaru ke main df → tapi kita butuh data FR S‐nya
    df = df.merge(
        df_log_latest[['frs_id', 'status_log', 'tanggal_date']],
        left_on='id', right_on='frs_id', how='inner'
    )
    # catatan: pakai how='inner' supaya hanya FRS yang ada di df_log_latest

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

    # 8) IPK default (tanpa perhitungan semester)
    df['ipk_terakhir'] = 0

    # 9) Filter baris yang match ke dim_waktu (waktu_persetujuan_id must not null)
    df = df[df['waktu_persetujuan_id'].notna()]

    # 10) Generate surrogate key (incremental)
    last_id = get_max_waktu_date_from_fact()
    # get_max_waktu_date_from_fact() sudah diambil di luar. Kita perlu ID, bukan tanggal.
    # Kita bisa panggil lagi untuk ID langsung:
    sql_max_id = "SELECT MAX(persetujuan_frs_id) AS last_id FROM fact_persetujuan_frs"
    df_lastid = pd.read_sql(sql_max_id, target_engine)
    prev_id = int(df_lastid.at[0, 'last_id']) if not df_lastid.empty and pd.notna(df_lastid.at[0, 'last_id']) else 0
    df['persetujuan_frs_id'] = range(prev_id + 1, prev_id + 1 + len(df))

    # 11) Pilih kolom final
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
    if df.empty:
        print(f"[LOAD] {table_name} → tidak ada record baru (0 baris)")
        return
    print(f"[LOAD] {table_name} → {len(df)} baris")
    df.to_sql(
        table_name,
        target_engine,
        if_exists='append',
        index=False
    )

# === MAIN ETL INCREMENTAL ===
def run_etl_incremental():
    # 1) Bentuk watermark_date dari fact
    watermark_date = get_max_waktu_date_from_fact()  # pd.Timestamp (date-only) atau None

    # 2) Extract semua source
    df_frs       = extract_table("frs")
    df_mhs_raw   = extract_table("mahasiswa")
    df_log       = extract_table("log_frs")
    df_detail    = extract_table("detail_frs")
    df_kelas     = extract_table("kelas")
    df_nilai     = extract_table("nilai_mahasiswa")

    # 3) Extract dimensi
    df_dim_mhs   = extract_dim("dim_mahasiswa")
    df_dim_waktu = extract_dim("dim_waktu")

    # 4) Transform incremental
    df_fact_new = transform_incremental(
        df_frs, df_mhs_raw, df_log, df_detail, df_kelas,
        df_nilai, df_dim_mhs, df_dim_waktu, watermark_date
    )

    # 5) Load hasil incremental
    load_table(df_fact_new, 'fact_persetujuan_frs')

if __name__ == "__main__":
    run_etl_incremental()
