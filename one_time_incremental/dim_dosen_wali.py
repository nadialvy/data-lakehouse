import pandas as pd
from sqlalchemy import create_engine, text
import getpass

# === CONFIGURATION ===
password = ""
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/frs_paling_fix'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@localhost/olap_frs'

source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_table(table_name, engine):
    print(f"Extracting {table_name}...")
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)

# === TRANSFORM ===
def normalize_degree(nama):
    replacements = {
        'S.T': 'ST', 'S.T.': 'ST',
        'M.T': 'MT', 'M.T.': 'MT',
        'Ph.D': 'PhD', 'Ph.D.': 'PhD'
    }
    for old, new in replacements.items():
        nama = nama.replace(old, new)
    return nama

def transform_dosen_wali(df):
    print("Transforming dosen wali dimension...")

    df['nama'] = df['nama'].str.strip().str.title().apply(normalize_degree)
    df['email'] = df['email'].str.strip().str.lower()
    df = df.drop_duplicates(subset='email')

    return df[['nama', 'email']]

# === NORMALISASI EXISTING ===
def normalize_dim_dosen(df):
    df['nama'] = df['nama'].str.strip().str.title().apply(normalize_degree)
    df['email'] = df['email'].str.strip().str.lower()
    return df

# === LOAD dengan REPLACE ===
def incremental_replace(df_new):
    print("Checking for changed or new rows...")

    df_existing = extract_table("dim_dosen_wali", target_engine)
    df_existing = normalize_dim_dosen(df_existing)
    df_new = normalize_dim_dosen(df_new)

    merged = df_existing.merge(df_new, on='email', suffixes=('_old', '_new'))

    changed = merged[merged['nama_old'] != merged['nama_new']]

    print(f"\n🔍 Jumlah dosen dengan nama berubah: {len(changed)}")

    # DELETE yang berubah
    email_to_delete = changed['email'].tolist()
    delete_query = text("DELETE FROM dim_dosen_wali WHERE email = :email")
    with target_engine.begin() as conn:
        for email in email_to_delete:
            conn.execute(delete_query, {"email": email})

    # Get last surrogate ID
    last_id = pd.read_sql("SELECT MAX(dosen_wali_id) as max_id FROM dim_dosen_wali", target_engine)['max_id'][0]
    last_id = last_id if last_id is not None else 0

    # INSERT ulang data yang berubah + data baru (yang belum ada di existing)
    to_insert = df_new[df_new['email'].isin(email_to_delete)]
    new_emails = set(df_new['email']) - set(df_existing['email'])
    to_insert = pd.concat([to_insert, df_new[df_new['email'].isin(new_emails)]])

    to_insert = to_insert.drop_duplicates(subset='email').copy()
    to_insert['dosen_wali_id'] = range(last_id + 1, last_id + 1 + len(to_insert))

    to_insert = to_insert[['dosen_wali_id', 'nama', 'email']]
    to_insert.to_sql("dim_dosen_wali", con=target_engine, if_exists='append', index=False)

    print(f"✅ {len(to_insert)} baris dimasukkan (baru atau diperbarui).")

# === MAIN ===
def run_incremental_etl():
    df = extract_table("dosen_wali", source_engine)
    transformed = transform_dosen_wali(df)
    incremental_replace(transformed)

if __name__ == "__main__":
    run_incremental_etl()
