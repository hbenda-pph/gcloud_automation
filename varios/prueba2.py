from google.cloud import bigquery
import sys

def run_query():
    try:
        print(f"Python version: {sys.version}")
        print(f"protobuf version: {google.protobuf.__version__}")
        
        client = bigquery.Client(project="platform-partners-des")
        query_job = client.query("SELECT 1 AS test")
        
        # Método más robusto para obtener resultados
        results = list(query_job.result())
        return results[0].test if results else None
        
    except Exception as e:
        print(f"Error type: {type(e).__name__}")
        print(f"Full error: {str(e)}")
        raise

import google.protobuf
print("Result:", run_query())
