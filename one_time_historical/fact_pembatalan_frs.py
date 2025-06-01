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
    sql = f"SELECT MAX({id_column}) as max_id FROM {table}"
    df = pd.read_sql(sql, target_engine)
    return int(df['max_id'][0]) if df['max_id'][0] is not None else 0

def transform_fact(df_batal, df_frs, df_nilai, df_waktu, df_mahasiswa):
    print("[TRANSFORM] fact_pembatalan_frs...")

    # --- 1. Merge dasar: df_batal + info FRS (nrp, tanggal_disetujui, semester) ---
    df = df_batal.merge(
        df_frs[['id', 'nrp', 'tanggal_disetujui', 'semester']],
        left_on='frs_id', right_on='id', how='left'
    )

    # --- 2. Konversi tanggal dengan errors='coerce' ---
    # Kalau ada nilai di luar jangkauan pandas, otomatis jadi NaT
    df['tanggal_pengajuan']  = pd.to_datetime(
        df['tanggal_pengajuan'], errors='coerce'
    )
    df['tanggal_disetujui'] = pd.to_datetime(
        df['tanggal_disetujui'], errors='coerce'
    )

    # Setelah coerce, kita drop baris jika TIDAK ada tanggal valid di kedua kolom
    # Karena kita perlu keduanya untuk join ke dim_waktu
    sebelum_drop = len(df)
    df = df.dropna(subset=['tanggal_pengajuan', 'tanggal_disetujui'])
    print(f"[CLEAN] Dropped {sebelum_drop - len(df)} baris karena tanggal invalid")

    # Pastikan NRP adalah string
    df['nrp'] = df['nrp'].astype(str)
    df_mahasiswa['nrp'] = df_mahasiswa['nrp'].astype(str)

    # --- 3. Merge untuk mendapatkan mahasiswa_id dari dim_mahasiswa ---
    df = df.merge(
        df_mahasiswa[['mahasiswa_id', 'nrp']],
        on='nrp', how='left'
    )

    # --- 4. Hitung IPK terakhir (rata2 nilai di semester sebelumnya) ---
    # Jika kamu ingin skip IPK, bisa ubah bagian ini menjadi df['ipk_terakhir']=0
    df_nilai['nrp'] = df_nilai['nrp'].astype(str)
    # Merge nilai_mahasiswa dengan df (yang punya kolom 'semester' sebagai semester_batal)
    df_nilai_prev = df_nilai.merge(
        df[['nrp', 'semester']].rename(columns={'semester': 'semester_batal'}),
        on='nrp', how='inner'
    )
    # Pastikan nama kolom semester di df_nilai adalah 'semester'
    # Hasil merge punya kolom 'semester_x' (semester di nilai) dan 'semester_batal'
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

    # --- 5. Siapkan kolom date-only untuk merge ke dim_waktu ---
    df['tanggal_pengajuan_date']  = df['tanggal_pengajuan'].dt.normalize()
    df['tanggal_disetujui_date'] = df['tanggal_disetujui'].dt.normalize()
    df_waktu['tanggal'] = pd.to_datetime(df_waktu['tanggal'], errors='coerce')
    df_waktu['tanggal_date'] = df_waktu['tanggal'].dt.normalize()

    # Debug: cek 5 tanggal_date unik di kedua dataset
    print("[DEBUG] Unique tanggal_pengajuan_date di fact_batal:", 
          df['tanggal_pengajuan_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df['tanggal_pengajuan_date'].nunique())
    print("[DEBUG] Unique tanggal_disetujui_date di fact_batal:", 
          df['tanggal_disetujui_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df['tanggal_disetujui_date'].nunique())
    print("[DEBUG] Unique tanggal_date di dim_waktu:", 
          df_waktu['tanggal_date'].dt.strftime('%Y-%m-%d').unique()[:5], 
          "… total", df_waktu['tanggal_date'].nunique())

    # --- 6. Merge untuk mendapatkan waktu_pengajuan_id ---
    df = df.merge(
        df_waktu.rename(columns={'waktu_id': 'waktu_pengajuan_id'}),
        left_on='tanggal_pengajuan_date',
        right_on='tanggal_date',
        how='left'
    )

    # --- 7. Merge untuk mendapatkan waktu_verifikasi_id ---
    df = df.merge(
        df_waktu.rename(columns={'waktu_id': 'waktu_verifikasi_id'}),
        left_on='tanggal_disetujui_date',
        right_on='tanggal_date',
        how='left',
        suffixes=('_pgj', '_ver')  # agar kolom tidak tabrakan
    )

    # ― Setelah merge, kita drop baris yang waktu_pengajuan_id atau waktu_verifikasi_id = NaN
    sebelum_drop_waktu = len(df)
    df = df.dropna(subset=['waktu_pengajuan_id', 'waktu_verifikasi_id'])
    print(f"[CLEAN] Dropped {sebelum_drop_waktu - len(df)} baris karena waktu_id NaN")

    # ― Ubah ke integer (setelah drop NaN)
    df['waktu_pengajuan_id']  = df['waktu_pengajuan_id'].astype(int)
    df['waktu_verifikasi_id'] = df['waktu_verifikasi_id'].astype(int)

    # --- 8. Hitung selisih hari verifikasi pembatalan ---
    df['lama_verifikasi_pembatalan'] = (
        (df['tanggal_disetujui'] - df['tanggal_pengajuan']).dt.days
    ).fillna(0).astype(int)

    # --- 9. Generate surrogate key (pembatalan_frs_id) ---
    last_id = get_max_surrogate('fact_pembatalan_frs', 'pembatalan_frs_id')
    df['pembatalan_frs_id'] = range(last_id + 1, last_id + 1 + len(df))

    # --- 10. Pilih kolom final sesuai DDL (tanpa kolom 'semester', 'id', dsb.) ---
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
    print(f"[LOAD] {table_name} → {len(df)} baris")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

def run_etl():
    # Extract semua tabel
    df_batal     = extract_table('pembatalan_frs')
    df_frs       = extract_table('frs')
    df_nilai     = extract_table('nilai_mahasiswa')
    df_waktu     = extract_dim('dim_waktu')
    df_mahasiswa = extract_dim('dim_mahasiswa')

    # Transformasi fact
    df_fact = transform_fact(df_batal, df_frs, df_nilai, df_waktu, df_mahasiswa)

    # Load ke database OLAP
    load_table(df_fact, 'fact_pembatalan_frs')

if __name__ == "__main__":
    run_etl()
