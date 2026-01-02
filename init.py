import os
import subprocess
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    # Required environment variables for schemachange
    env_vars = {
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'),
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA')
    }
    
    # Filter out None values
    env_vars = {k: v for k, v in env_vars.items() if v is not None}
    
    # Construct schemachange command
    command = [
        "schemachange",
        "deploy",
        "-f", "migrations",
        "-a", env_vars.get('SNOWFLAKE_ACCOUNT'),
        "-u", env_vars.get('SNOWFLAKE_USER'),
        "-r", env_vars.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        "-w", env_vars.get('SNOWFLAKE_WAREHOUSE'),
        "-d", env_vars.get('SNOWFLAKE_DATABASE'),
        "-s", env_vars.get('SNOWFLAKE_SCHEMA'),
        "--change-history-table", "BITCOIN_DATA.METADATA.CHANGE_HISTORY",
        "--create-change-history-table"
    ]
    
    # Pass password via env var (schemachange looks for SNOWFLAKE_PASSWORD)
    os.environ['SNOWFLAKE_PASSWORD'] = env_vars.get('SNOWFLAKE_PASSWORD', '')
    
    print("Running schemachange migrations...")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running schemachange: {e}")
        print(e.stdout)
        print(e.stderr)

if __name__ == "__main__":
    main()
