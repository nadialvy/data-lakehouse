import getpass
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

# === CONFIGURATION ===
password = ""
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_table(table_name):
    print(f"Extracting {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", source_engine)

# === TRANSFORM ===
def clean_dosen(name):
    if not isinstance(name, str):
        return name
    name = name.strip().title()
    name = name.replace('.', '. ').replace('  ', ' ')
    name = name.replace('St ', 'St. ').replace('Dr ', 'Dr. ').replace('Phd', 'Ph.D').replace('Ph.D.', 'Ph.D')
    name = name.replace('Mkom', 'M.Kom').replace('Skom', 'S.Kom').replace('Spt', 'S.Pt')
    return name.strip()

def transform_mahasiswa(df_mhs, df_jur, df_dosen):
    print("Transforming mahasiswa dimension...")

    # Rename kolom nama mahasiswa agar tidak ketindih saat merge
    df_mhs = df_mhs.rename(columns={'nama': 'nama_mahasiswa'})

    # Merge ke jurusan
    df = df_mhs.merge(df_jur, on="jurusan_id", how="left")

    # Merge ke dosen, hanya ambil kolom yang diperlukan
    df = df.merge(df_dosen[['id', 'nama']], left_on="dosen_wali_id", right_on="id", how="left")
    df = df.rename(columns={'nama': 'nama_dosen_wali'})

    # Bersihin data
    df['nama_mahasiswa'] = df['nama_mahasiswa'].str.strip().str.title()
    df['email'] = df['email'].str.strip().str.lower()
    df['nama_jurusan'] = df['nama_jurusan'].str.strip().str.title()
    df['nama_dosen_wali'] = df['nama_dosen_wali'].apply(clean_dosen)

    # Validasi email sederhana
    df = df[df['email'].str.contains('@', na=False)]

    # Drop duplicate
    df = df.drop_duplicates(subset='nrp').copy()

    # Surrogate key
    df['mahasiswa_id'] = range(1, len(df) + 1)

    return df[['mahasiswa_id', 'nrp', 'nama_mahasiswa', 'email', 'nama_jurusan', 'nama_dosen_wali']] \
        .rename(columns={'nama_mahasiswa': 'nama'})

# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    try:
        df.to_sql(table_name, target_engine, if_exists='append', index=False)
    except IntegrityError as e:
        print("❌ Gagal memuat data: Duplikasi PRIMARY KEY terdeteksi.")
        existing = pd.read_sql(f"SELECT * FROM {table_name}", target_engine)
        conflict = pd.merge(existing, df, on='mahasiswa_id', suffixes=('_lama', '_baru'))
        print("\nBerikut data yang konflik:\n", conflict[['mahasiswa_id', 'nrp_lama', 'nama_lama', 'nama_baru']].head())
        non_conflict = df[~df['mahasiswa_id'].isin(conflict['mahasiswa_id'])]
        print("\n✅ Data yang berhasil disiapkan untuk insert:\n", non_conflict.head())

# === MAIN ===
def run_etl_mahasiswa():
    df_mhs = extract_table("mahasiswa")
    print("Kolom df_mhs:", df_mhs.columns.tolist())

    df_jur = extract_table("jurusan")
    df_dosen = extract_table("dosen_wali")

    dim_mahasiswa = transform_mahasiswa(df_mhs, df_jur, df_dosen)
    load_table(dim_mahasiswa, "dim_mahasiswa")

if __name__ == "__main__":
    run_etl_mahasiswa()
