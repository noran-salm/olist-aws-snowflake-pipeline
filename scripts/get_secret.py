"""
scripts/get_secret.py
Fetches Snowflake credentials from AWS Secrets Manager.
Usage: eval $(python3 scripts/get_secret.py)
"""
import boto3
import json
import sys

def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

if __name__ == "__main__":
    try:
        creds = get_secret("olist/snowflake/credentials")
        print(f"export SNOWFLAKE_ACCOUNT='{creds['account']}'")
        print(f"export SNOWFLAKE_USER='{creds['user']}'")
        print(f"export SNOWFLAKE_PASSWORD='{creds['password']}'")
        print(f"export SNOWFLAKE_ROLE='{creds.get('role','SYSADMIN')}'")
        print(f"export SNOWFLAKE_DATABASE='{creds.get('database','OLIST_DW')}'")
        print(f"export SNOWFLAKE_WAREHOUSE='{creds.get('warehouse','OLIST_WH')}'")
        print(f"export SNOWFLAKE_SCHEMA='{creds.get('schema','STAGING')}'")
    except Exception as e:
        print(f"# ERROR fetching secret: {e}", file=sys.stderr)
        sys.exit(1)
