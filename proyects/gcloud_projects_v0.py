#!/usr/bin/env python3
"""
Script simple para leer datos de una tabla de BigQuery
"""
from google.cloud import bigquery

PROJECT_ID = "platform-partners-des"
DATASET_NAME = "settings"
TABLE_NAME = "companies"


def main():
    try:
        print(f"Conectando a proyecto: {PROJECT_ID}")
        print(f"Dataset: {DATASET_NAME}")
        print(f"Tabla: {TABLE_NAME}")
        
        # Crear cliente de BigQuery
        print("Creando cliente BigQuery...")
        client = bigquery.Client(project=PROJECT_ID)
        print("Cliente BigQuery creado exitosamente")
        
        # Consulta SQL
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name
            FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
            ORDER BY company_id
        """

        print(f"Ejecutando consulta...")
        print(f"Query: {query}")
        
        # Ejecutar consulta
        print("Iniciando query_job...")
        query_job = client.query(query)
        print("Query job creado, obteniendo resultados...")
        results = query_job.result()
        print("Resultados obtenidos")
        
        print("Consulta ejecutada exitosamente")
        print("=" * 50)
        
        count = 0
        print("Iniciando iteración sobre resultados...")
        for row in results:
            try:
                count += 1
                print(f"Fila {count}:")
                print(f"  Tipo de row: {type(row)}")
                print(f"  Row completo: {row}")
                print(f"  company_id: {row.company_id}")
                print(f"  company_name: {row.company_name}")
                print(f"  company_new_name: {row.company_new_name}")
                print("-" * 30)
            except Exception as row_error:
                print(f"ERROR en fila {count}: {str(row_error)}")
                print(f"Row problemático: {row}")
                print(f"Tipo de row: {type(row)}")
                break
            
        print(f"Total de filas procesadas: {count}")
        
    except Exception as e:
        print(f"ERROR GENERAL: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        import traceback
        print("Traceback completo:")
        traceback.print_exc()

if __name__ == "__main__":
    main() 
