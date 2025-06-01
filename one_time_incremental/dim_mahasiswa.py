import getpass
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

# === CONFIGURATION ===
password = getpass.getpass("Masukkan password DB: ")
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# Fungsi pembersih dosen sama seperti ETL
def clean_dosen(name):
    if not isinstance(name, str):
        return name
    name = name.strip().title()
    name = name.replace('.', '. ').replace('  ', ' ')
    name = name.replace('St ', 'St. ').replace('Dr ', 'Dr. ').replace('Phd', 'Ph.D').replace('Ph.D.', 'Ph.D')
    name = name.replace('Mkom', 'M.Kom').replace('Skom', 'S.Kom').replace('Spt', 'S.Pt')
    return name.strip()

# Fungsi transform mahasiswa seperti sebelumnya
def transform_mahasiswa(df_mhs, df_jur, df_dosen):
    df_mhs = df_mhs.rename(columns={'nama': 'nama_mahasiswa'})
    df = df_mhs.merge(df_jur, on="jurusan_id", how="left")
    df = df.merge(df_dosen[['id', 'nama']], left_on="dosen_wali_id", right_on="id", how="left")
    df = df.rename(columns={'nama': 'nama_dosen_wali'})

    df['nama_mahasiswa'] = df['nama_mahasiswa'].str.strip().str.title()
    df['email'] = df['email'].str.strip().str.lower()
    df['nama_jurusan'] = df['nama_jurusan'].str.strip().str.title()
    df['nama_dosen_wali'] = df['nama_dosen_wali'].apply(clean_dosen)
    df = df[df['email'].str.contains('@', na=False)]
    df = df.drop_duplicates(subset='nrp').copy()
    df['mahasiswa_id'] = range(1, len(df) + 1)

    return df[['mahasiswa_id', 'nrp', 'nama_mahasiswa', 'email', 'nama_jurusan', 'nama_dosen_wali']] \
        .rename(columns={'nama_mahasiswa': 'nama'})

# Load dan compare
def compare_and_update():
    print("🔍 Extracting source data...")
    df_mhs = pd.read_sql("SELECT * FROM mahasiswa", source_engine)
    df_jur = pd.read_sql("SELECT * FROM jurusan", source_engine)
    df_dosen = pd.read_sql("SELECT * FROM dosen_wali", source_engine)
    df_oltp = transform_mahasiswa(df_mhs, df_jur, df_dosen)
    df_oltp['nrp'] = df_oltp['nrp'].astype(str)

    print("📦 Extracting OLAP data...")
    df_olap = pd.read_sql("SELECT * FROM dim_mahasiswa", target_engine)

    # Gabung dan cari yang berbeda
    df_join = df_oltp.merge(df_olap, on="nrp", suffixes=("_new", "_old"))
    changes = []

    for _, row in df_join.iterrows():
        row_diff = {}
        for col in ['nama', 'email', 'nama_jurusan', 'nama_dosen_wali']:
            if row[f'{col}_new'] != row[f'{col}_old']:
                row_diff[col] = (row[f'{col}_old'], row[f'{col}_new'])
        if row_diff:
            changes.append({
                'nrp': row['nrp'],
                'changes': row_diff
            })

    if not changes:
        print("✅ Tidak ada perubahan data mahasiswa.")
        return

    print(f"🔁 Ditemukan {len(changes)} baris dengan perubahan:")
    for change in changes:
        print(f"\nNRP: {change['nrp']}")
        for k, v in change['changes'].items():
            print(f" - {k}: '{v[0]}' -> '{v[1]}'")

    # Update OLAP
    with target_engine.begin() as conn: 
        for change in changes:
            nrp = change['nrp']
            new_data = df_oltp[df_oltp['nrp'] == nrp].iloc[0]
            update_query = text("""
                UPDATE dim_mahasiswa
                SET nama = :nama,
                    email = :email,
                    nama_jurusan = :nama_jurusan,
                    nama_dosen_wali = :nama_dosen_wali
                WHERE nrp = :nrp
            """)
            conn.execute(update_query, {
                'nama': new_data['nama'],
                'email': new_data['email'],
                'nama_jurusan': new_data['nama_jurusan'],
                'nama_dosen_wali': new_data['nama_dosen_wali'],
                'nrp': new_data['nrp']
            })
    print(f"\n✅ Selesai update {len(changes)} baris ke OLAP.")

if __name__ == "__main__":
    compare_and_update()
