import getpass
import pandas as pd
from sqlalchemy import create_engine

# === CONFIGURASI KONEKSI ===
password = getpass.getpass("Masukkan password: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

def extract_table(table_name):
    print(f"[EXTRACT] {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", source_engine)

def extract_dim(table_name):
    print(f"[EXTRACT DIM] {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", target_engine)

def get_max_surrogate(table, id_column):
    """
    Mengembalikan nilai MAX(id_column) dari table di database TARGET.
    Jika tabel kosong, kembalikan 0.
    """
    sql = f"SELECT MAX({id_column}) as max_id FROM {table}"
    df = pd.read_sql(sql, target_engine)
    return int(df['max_id'][0]) if df['max_id'][0] is not None else 0

def get_max_pengajuan_date_from_fact():
    """
    Cari nilai maksimum waktu_pengajuan_id di fact_pembatalan_frs,
    lalu lookup tanggal di dim_waktu untuk ID tersebut.
    Hasilnya: pd.Timestamp (normalize → date-only) atau None jika fact kosong.
    """
    sql = "SELECT MAX(waktu_pengajuan_id) AS last_peng_id FROM fact_pembatalan_frs"
    df = pd.read_sql(sql, target_engine)
    if df.empty or pd.isna(df.at[0, 'last_peng_id']):
        return None

    last_peng_id = int(df.at[0, 'last_peng_id'])
    sql2 = f"SELECT tanggal FROM dim_waktu WHERE waktu_id = {last_peng_id} LIMIT 1"
    df2 = pd.read_sql(sql2, target_engine)
    if df2.empty:
        return None

    ts = pd.to_datetime(df2.at[0, 'tanggal']).normalize()
    print(f"[WATERMARK] Last loaded tanggal_pengajuan_date = {ts.date()}")
    return ts

def transform_incremental(
    df_batal, df_frs, df_nilai, df_waktu, df_mahasiswa, watermark_date
):
    """
    Transformasi incremental untuk fact_pembatalan_frs.
    Hanya memproses baris di df_batal yang tanggal_pengajuan_date > watermark_date.
    """
    print("[TRANSFORM-INC] fact_pembatalan_frs...")

    # 1) Merge dasar: df_batal + info FRS (nrp, tanggal_disetujui, semester)
    #    Pastikan kolom 'frs_id' di df_batal cocok dengan 'id' di df_frs.
    df = df_batal.merge(
        df_frs[['id', 'nrp', 'tanggal_disetujui', 'semester']],
        left_on='frs_id', right_on='id', how='left'
    )

    # 2) Setelah merge, parse tanggal di df (tanggal_pengajuan dari df_batal, 
    #    dan tanggal_disetujui ada di hasil merge dari df_frs).
    df['tanggal_pengajuan'] = pd.to_datetime(
        df['tanggal_pengajuan'], errors='coerce'
    )
    df['tanggal_disetujui'] = pd.to_datetime(
        df['tanggal_disetujui'], errors='coerce'
    )
    df['tanggal_pengajuan_date'] = df['tanggal_pengajuan'].dt.normalize()
    df['tanggal_disetujui_date'] = df['tanggal_disetujui'].dt.normalize()

    # 3) Jika watermark_date ada, filter baris pembatalan yang lebih baru
    if watermark_date is not None:
        mask = df['tanggal_pengajuan_date'] > watermark_date
        new_count = mask.sum()
        print(f"[FILTER] Baris pembatalan_frs setelah {watermark_date.date()} = {new_count}")
        if new_count == 0:
            print("[TRANSFORM-INC] Tidak ada data baru → skip transform")
            return pd.DataFrame(columns=[
                'pembatalan_frs_id',
                'mahasiswa_id',
                'waktu_pengajuan_id',
                'waktu_verifikasi_id',
                'ipk_terakhir',
                'lama_verifikasi_pembatalan'
            ])
        df = df[mask].copy()
    else:
        print("[FILTER] Full load karena watermark_date = None")

    # 4) Drop baris jika parse tanggal gagal (NaT)
    before_drop = len(df)
    df = df.dropna(subset=['tanggal_pengajuan', 'tanggal_disetujui'])
    print(f"[CLEAN] Dropped {before_drop - len(df)} baris karena tanggal invalid")

    # 5) Merge ke dim_mahasiswa untuk dapatkan mahasiswa_id
    df['nrp'] = df['nrp'].astype(str)
    df_mahasiswa['nrp'] = df_mahasiswa['nrp'].astype(str)
    df = df.merge(
        df_mahasiswa[['mahasiswa_id', 'nrp']],
        on='nrp', how='left'
    )

    # 6) Hitung IPK terakhir
    df_nilai['nrp'] = df_nilai['nrp'].astype(str)
    df_nilai_prev = df_nilai.merge(
        df[['nrp', 'semester']].rename(columns={'semester': 'semester_batal'}),
        on='nrp', how='inner'
    )
    df_nilai_prev = df_nilai_prev[
        df_nilai_prev['semester'] < df_nilai_prev['semester_batal']
    ]
    ipk_df = (
        df_nilai_prev
        .groupby('nrp')['nilai']
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={'nilai': 'ipk_terakhir'})
    )
    df = df.merge(ipk_df, on='nrp', how='left')
    df['ipk_terakhir'] = df['ipk_terakhir'].fillna(0.0)

    # 7) Siapkan df_waktu: parse & buat tanggal_date
    df_waktu['tanggal'] = pd.to_datetime(df_waktu['tanggal'], errors='coerce')
    df_waktu['tanggal_date'] = df_waktu['tanggal'].dt.normalize()

    # (Opsional) Debug singkat
    print("[DEBUG] Unique pengajuan_date:", df['tanggal_pengajuan_date'].dt.strftime('%Y-%m-%d').unique()[:3])
    print("[DEBUG] Unique disetujui_date:", df['tanggal_disetujui_date'].dt.strftime('%Y-%m-%d').unique()[:3])
    print("[DEBUG] Unique tanggal_date dim_waktu:", df_waktu['tanggal_date'].dt.strftime('%Y-%m-%d').unique()[:3])

    # 8) Merge untuk mendapatkan waktu_pengajuan_id
    df = df.merge(
        df_waktu.rename(columns={'waktu_id': 'waktu_pengajuan_id'}),
        left_on='tanggal_pengajuan_date',
        right_on='tanggal_date',
        how='left'
    )

    # 9) Merge untuk mendapatkan waktu_verifikasi_id
    df = df.merge(
        df_waktu.rename(columns={'waktu_id': 'waktu_verifikasi_id'}),
        left_on='tanggal_disetujui_date',
        right_on='tanggal_date',
        how='left',
        suffixes=('_pgj', '_ver')
    )

    # Drop baris jika merge gagal ke dim_waktu
    before_drop2 = len(df)
    df = df.dropna(subset=['waktu_pengajuan_id', 'waktu_verifikasi_id'])
    print(f"[CLEAN] Dropped {before_drop2 - len(df)} baris karena waktu_id NaN")

    # Ubah tipe ke int
    df['waktu_pengajuan_id'] = df['waktu_pengajuan_id'].astype(int)
    df['waktu_verifikasi_id'] = df['waktu_verifikasi_id'].astype(int)

    # 10) Hitung lama verifikasi pembatalan
    df['lama_verifikasi_pembatalan'] = (
        (df['tanggal_disetujui'] - df['tanggal_pengajuan']).dt.days
    ).fillna(0).astype(int)

    # 11) Generate surrogate key incremental
    last_id = get_max_surrogate('fact_pembatalan_frs', 'pembatalan_frs_id')
    df['pembatalan_frs_id'] = range(last_id + 1, last_id + 1 + len(df))

    # 12) Pilih kolom final sesuai DDL
    df_final = df[[
        'pembatalan_frs_id',
        'mahasiswa_id',
        'waktu_pengajuan_id',
        'waktu_verifikasi_id',
        'ipk_terakhir',
        'lama_verifikasi_pembatalan'
    ]].copy()

    return df_final

def load_table(df, table_name):
    if df.empty:
        print(f"[LOAD] {table_name} → tidak ada baris baru (0 baris)")
    else:
        print(f"[LOAD] {table_name} → {len(df)} baris baru")
        df.to_sql(table_name, target_engine, if_exists='append', index=False)

def run_etl_incremental():
    # 1. Cari watermark_date dari fact_pembatalan_frs
    watermark_date = get_max_pengajuan_date_from_fact()

    # 2. Extract semua data
    df_batal     = extract_table('pembatalan_frs')
    df_frs       = extract_table('frs')
    df_nilai     = extract_table('nilai_mahasiswa')
    df_waktu     = extract_dim('dim_waktu')
    df_mahasiswa = extract_dim('dim_mahasiswa')

    # 3. Transform incremental
    df_incremental = transform_incremental(
        df_batal, df_frs, df_nilai, df_waktu, df_mahasiswa, watermark_date
    )

    # 4. Load hasil incremental
    load_table(df_incremental, 'fact_pembatalan_frs')

if __name__ == "__main__":
    run_etl_incremental()
