import os
import requests
import pandas as pd
import yaml
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone
import uuid
from dotenv import load_dotenv
import logging

# Load environment variables for local development
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.yml')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'coindesk')

def load_config(path: str) -> dict:
    if not os.path.exists(path):
        logger.error(f"Config file not found at {path}")
        return {}
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_api_key():
    from dotenv import load_dotenv
    load_dotenv()
    return os.getenv('CRYPTOCOMPARE_API_KEY') or os.getenv('API_KEY')

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
        logger.error(f"Could not connect to Snowflake: {e}")
        return None

def check_table_status(conn, table_name):
    """
    Returns (exists, row_count)
    """
    try:
        cursor = conn.cursor()
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
        if cursor.fetchone():
            # Get count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = cursor.fetchone()
            count = result[0] if result else 0
            return True, count
        return False, 0
    except Exception as e:
        logger.error(f"Error checking table status for {table_name}: {e}")
        return False, 0

def get_table_columns(conn, table_name):
    """
    Returns a list of uppercase column names for the table.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        columns = [row[0].upper() for row in cursor.fetchall()]
        return columns
    except Exception as e:
        logger.error(f"Error fetching columns for {table_name}: {e}")
        return []

def perform_merge(conn, df, table_name, unique_key):
    """
    Performs a MERGE operation into the target table using a temporary staging table.
    """
    # Create a temporary staging table name
    stage_table = f"{table_name}_STAGE_{uuid.uuid4().hex[:8]}".upper()
    
    try:
        # 1. Upload to Stage (Auto-create temp table)
        # We assume columns are already upper-cased in df
        success, _, _, _ = write_pandas(
            conn,
            df,
            stage_table,
            auto_create_table=True,
            table_type="TEMPORARY",
            quote_identifiers=True  # Quote identifiers to handle reserved keywords
        )
        if not success:
            logger.error(f"Failed to upload to staging table {stage_table}")
            return

        # 2. Construct MERGE query
        # Identify columns to update (all columns except the unique key)
        columns = [c for c in df.columns]
        
        # Ensure unique_key is in columns
        if unique_key not in columns:
            logger.error(f"Error: Unique key {unique_key} not in dataframe columns: {columns}")
            return

        # Quote column names to handle reserved keywords like TO, FROM
        update_clause = ", ".join([f't."{col}" = s."{col}"' for col in columns if col != unique_key])
        insert_cols = ", ".join([f'"{col}"' for col in columns])
        insert_vals = ", ".join([f's."{col}"' for col in columns])

        merge_sql = f"""
        MERGE INTO {table_name} t
        USING {stage_table} s
        ON t."{unique_key}" = s."{unique_key}"
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """
        
        # 3. Execute Merge
        cursor = conn.cursor()
        cursor.execute(merge_sql)
        logger.info(f"Merged data into {table_name} successfully.")

    except Exception as e:
        logger.error(f"Error during MERGE for {table_name}: {e}")
    finally:
        # Temp tables drop automatically at session end, but good practice to clean up if long running
        try:
             conn.cursor().execute(f"DROP TABLE IF EXISTS {stage_table}")
        except:
            pass

def upload_and_fetch_from_snowflake(df, table_name, unique_key=None):
    """
    1. Uploads/Merges fresh df to Snowflake.
    2. Downloads the full unique dataset.
    """
    conn = get_snowflake_conn()
    if not conn:
        logger.warning("Skipping Snowflake operations (no connection). Returning original DF.")
        return df

    try:
        # Standardize columns to uppercase for Snowflake consistency
        df.columns = [c.upper().replace(' ', '_').replace('-', '_') for c in df.columns]
        
        # Check if table exists and get row count
        table_exists, row_count = check_table_status(conn, table_name)
        
        if not table_exists:
            logger.error(f"Error: Table {table_name} does not exist. Please run schemachange first.")
            return df

        # Filter DF columns to match Snowflake table columns
        table_cols = get_table_columns(conn, table_name)
        if table_cols:
            original_cols = df.columns.tolist()
            matching_cols = [c for c in df.columns if c in table_cols]
            df = df[matching_cols].copy()
            logger.info(f"Filtered {table_name} DataFrame to {len(df.columns)} columns matching Snowflake schema.")

        if df.empty or len(df.columns) == 0:
            logger.warning(f"Warning: No columns in {table_name} DataFrame match the Snowflake schema. Skipping upload.")
            logger.warning(f"DataFrame had columns: {original_cols if 'original_cols' in locals() else df.columns.tolist()}")
            logger.warning(f"Snowflake table expected: {table_cols if table_cols else 'Could not fetch table columns'}")
            return df

        # Logic for Bulk vs Delta
        if row_count >= 1 and unique_key and unique_key.upper() in df.columns:
            # Incremental load: Merge
            logger.info(f"Table {table_name} has {row_count} rows. Performing MERGE (Delta Load) on {unique_key}...")
            perform_merge(conn, df, table_name, unique_key.upper())
        else:
            # Bulk load or Append (no unique key)
            load_type = "Bulk Load (Empty Table)" if row_count == 0 else "Append (No Unique Key)"
            logger.info(f"Table {table_name} has {row_count} rows. Performing {load_type}...")
            # Use quote_identifiers=True to handle reserved keywords like TO, FROM
            write_pandas(conn, df, table_name, auto_create_table=False, quote_identifiers=True)

        # 2. Export (Full Dataset)
        sort_col = "TIMESTAMP" if "TIMESTAMP" in df.columns else ("TIME" if "TIME" in df.columns else df.columns[0])
        query = f'SELECT DISTINCT * FROM {table_name} ORDER BY "{sort_col}" DESC'

        logger.info(f"Fetching full updated data from {table_name}...")
        cursor = conn.cursor()
        cursor.execute(query)
        result_df = cursor.fetch_pandas_all()

        logger.info(f"Retrieved {len(result_df)} rows from Snowflake.")
        return result_df

    except Exception as e:
        logger.error(f"Snowflake Error for {table_name}: {e}")
        return df 
    finally:
        conn.close()

def process_and_save(key: str, url: str, api_key: str):
    # --- Always use limit 2000 and merge strategy ---
    limit_val = 2000
    table_name = f"COINDESK_{key.upper()}"
    logger.info(f"[{key}] Fetching with limit: {limit_val}")
    
    # Inject API Key
    if '{API_KEY}' in url:
        if not api_key:
            logger.warning(f"Skipping {key}: API key required but not found.")
            return
        url = url.replace('{API_KEY}', api_key)

    # Inject Limit
    if '{LIMIT}' in url:
        url = url.replace('{LIMIT}', str(limit_val))

    logger.info(f"Fetching {key} from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        df = None
        unique_key = None
        
        # --- Parsing Logic ---
        if key == 'pricemultifull':
            # Structure: {"RAW":{"BTC":{"USD":{...}}}}
            try:
                raw_data = data.get('RAW', {}).get('BTC', {}).get('USD', {})
                if raw_data:
                    df = pd.DataFrame([raw_data])
                    # No unique key for price log, we just append snapshots
            except AttributeError:
                pass

        elif key in ['histoday', 'histohour', 'hourly_social_data']:
            # Structure: {"Data": {"Data": [...]}}
            try:
                if 'Data' in data and isinstance(data['Data'], dict) and 'Data' in data['Data']:
                     df = pd.DataFrame(data['Data']['Data'])
                elif 'Data' in data and isinstance(data['Data'], list):
                     df = pd.DataFrame(data['Data'])
                
                # Identify Merge Key
                if df is not None:
                     # OHLC Specific: Map volumeto -> volume
                     if key in ['histoday', 'histohour']:
                         if 'volumeto' in df.columns:
                             df['volume'] = df['volumeto']
                         
                         # Remove original volume and conversion columns
                         cols_to_drop = [c for c in ['volumeto', 'volumefrom', 'conversionType', 'conversionSymbol'] if c in df.columns]
                         if cols_to_drop:
                             df.drop(columns=cols_to_drop, inplace=True)

                     if 'time' in df.columns: unique_key = 'TIME' # Will be uppercased later
            except Exception:
                pass

        elif key == 'blockchain_balancedistribution':
             try:
                 if 'Data' in data and isinstance(data['Data'], dict) and 'Data' in data['Data']:
                     items_list = data['Data']['Data']
                     if items_list and isinstance(items_list, list) and len(items_list) > 0:
                         # Check if first item has balance_distribution
                         if 'balance_distribution' in items_list[0]:
                             df = pd.json_normalize(
                                 items_list,
                                 record_path=['balance_distribution'],
                                 meta=['id', 'symbol', 'partner_symbol', 'time'],
                                 errors='ignore'
                             )
                             logger.info(f"Blockchain balance distribution: Parsed {len(df)} rows with columns: {list(df.columns)}")
                             # Handle unique key for exploded data
                             if 'time' in df.columns and 'from' in df.columns and 'to' in df.columns:
                                 df['merge_key'] = df['time'].astype(str) + "_" + df['from'].astype(str) + "_" + df['to'].astype(str)
                                 unique_key = 'MERGE_KEY'
                                 logger.info(f"Created merge_key for blockchain data with {len(df)} records")
                         else:
                             df = pd.DataFrame(items_list)
             except Exception as e:
                 logger.error(f"Error parsing blockchain_balancedistribution: {e}")
                 
        elif key in ['tadingsignals', 'tradingsignals']:
             try:
                 if 'Data' in data and isinstance(data['Data'], dict):
                    flat_data = {}

                    # Map new API field names to old Snowflake column names
                    field_mapping = {
                        'addressesNetGrowth': 'ltHandsTh',
                        'concentrationVar': 'concentration',
                        'largetxsVar': 'largeSurplus',
                        'inOutVar': 'inOutVar'  # This one stays the same
                    }

                    for signal_name, signal_data in data['Data'].items():
                        # Map to old field name if available
                        mapped_name = field_mapping.get(signal_name, signal_name)

                        if isinstance(signal_data, dict):
                             for k, v in signal_data.items():
                                 # Skip metadata fields, only keep sentiment and value
                                 if k in ['sentiment', 'value']:
                                     flat_data[f"{mapped_name}_{k}"] = v if v is not None else None
                        else:
                             flat_data[mapped_name] = signal_data if signal_data is not None else None

                    # Add fetched_at timestamp
                    flat_data['fetched_at'] = datetime.now(timezone.utc).isoformat()
                    df = pd.DataFrame([flat_data])
                    logger.info(f"Trading signals parsed successfully with {len(flat_data)} fields")
             except Exception as e:
                 logger.error(f"Error parsing tradingsignals: {e}")
                 pass

        elif key == 'news':
            if 'Data' in data and isinstance(data['Data'], list):
                df = pd.DataFrame(data['Data'])
                # News might have an ID
                if 'id' in df.columns: unique_key = 'ID'
        
        else:
            # Fallback
            if 'Data' in data:
                 if isinstance(data['Data'], list):
                     df = pd.DataFrame(data['Data'])
                 elif isinstance(data['Data'], dict) and 'Data' in data['Data']:
                      df = pd.DataFrame(data['Data']['Data'])
                 else:
                      df = pd.DataFrame([data['Data']]) if isinstance(data['Data'], dict) else pd.DataFrame([data])
            else:
                 if isinstance(data, list):
                     df = pd.DataFrame(data)
                 else:
                     df = pd.DataFrame([data])

        # --- Snowflake & Saving Logic ---
        
        if df is not None and not df.empty:
            # Add timestamp if completely missing
            if 'timestamp' not in df.columns and 'time' not in df.columns and 'TIMESTAMP' not in df.columns:
                df['fetched_at'] = datetime.now(timezone.utc).isoformat()
            
            # Prepare Table Name
            table_name = f"COINDESK_{key.upper()}"
            
            # Upload to Snowflake and get back the FULL updated table
            final_df = upload_and_fetch_from_snowflake(df, table_name, unique_key)
            
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            file_path = os.path.join(OUTPUT_DIR, f'{key}.csv')
            
            final_df.to_csv(file_path, index=False)
            logger.info(f"Exported {len(final_df)} rows to {file_path} (Full Dataset).")

        else:
            logger.warning(f"Warning: No valid data extracted for {key}")

    except Exception as e:
        logger.error(f"Error processing {key}: {e}")

if __name__ == "__main__":
    logger.info(f"Loading config from {CONFIG_FILE}")
    config = load_config(CONFIG_FILE)

    api_key = get_api_key()

    for key, url in config.items():
        process_and_save(key, url, api_key)
