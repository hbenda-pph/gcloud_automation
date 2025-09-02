from google.cloud import bigquery
import os

print("--- Diagnóstico ---")
print("Credenciales activas:", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
print("Proyecto default:", bigquery.Client().project)

try:
    test_client = bigquery.Client()
    print("Tipo del cliente:", type(test_client))
    test_job = test_client.query("SELECT 1 AS test")
    print("Tipo del job:", type(test_job))
    test_job.result()  # Esta línea debe fallar si hay problema real
    print("¡Consulta ejecutada correctamente!")
except Exception as e:
    print(f"Error diagnóstico: {str(e)}")
