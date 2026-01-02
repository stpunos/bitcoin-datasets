import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

def get_snowflake_conn():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Could not connect to Snowflake: {e}")
        return None

def upload_folder(conn, folder_name):
    folder_path = os.path.join(DATA_DIR, folder_name)
    if not os.path.exists(folder_path):
        return

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                print(f"Processing {file_path}...")
                
                try:
                    df = pd.read_csv(file_path)
                    
                    # sanitize columns
                    df.columns = [c.upper().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_') for c in df.columns]
                    
                    # Table name based on file name or folder? Let's use folder_filename convention
                    # e.g. NEWHEDGE_BITCOIN_DASHBOARD_DATA
                    table_name = f"{folder_name.upper()}_{file.replace('.csv', '').upper()}"
                    
                    # Write to snowflake
                    success, n_chunks, n_rows, _ = write_pandas(
                        conn,
                        df,
                        table_name,
                        auto_create_table=True
                    )
                    
                    if success:
                        print(f"Uploaded {n_rows} rows to {table_name}")
                    else:
                        print(f"Failed to upload {file}")
                        
                except Exception as e:
                    print(f"Error uploading {file}: {e}")

def main():
    conn = get_snowflake_conn()
    if not conn:
        print("Skipping Snowflake update (no connection).")
        return

    # Upload subdirectories
    for folder in ['newhedge', 'coindesk']:
        # For coindesk, we have nested folders like 'price', 'news'.
        # This simple walker might need adjustment if we want specific table names.
        # But for now, let's walk the DATA_DIR directly or just specific roots
         upload_folder(conn, folder)
    
    conn.close()

if __name__ == "__main__":
    main()
