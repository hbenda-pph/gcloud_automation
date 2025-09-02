#!/usr/bin/env python3
"""
Script simple para leer datos de una tabla de BigQuery y generar comandos de creación de proyectos GCP
"""
from google.cloud import bigquery
import subprocess
import sys

PROJECT_SOURCE = "platform-partners-des"
DATASET_NAME = "settings"
TABLE_NAME = "companies"


def generate_project_id(company_new_name, company_id):
    """
    Genera un project_id válido para GCP basado en el company_new_name
    Trunca hasta el primer guión encontrado y agrega company_id para hacerlo único
    """
    if not company_new_name:
        return None
    
    # Truncar hasta el primer guión
    if '-' in company_new_name:
        company_new_name = company_new_name.split('-')[0]
    
    # Convertir a minúsculas, reemplazar espacios con guiones y remover caracteres especiales
    project_id = company_new_name.lower()
    project_id = project_id.replace(' ', '-')
    project_id = project_id.replace('&', 'and')
    project_id = project_id.replace('(', '')
    project_id = project_id.replace(')', '')
    project_id = project_id.replace('.', '')
    project_id = project_id.replace(',', '')
    project_id = project_id.replace("'", '')
    project_id = project_id.replace('"', '')
    
    # Remover caracteres no válidos para project_id
    import re
    project_id = re.sub(r'[^a-z0-9-]', '', project_id)
    
    # Asegurar que no empiece ni termine con guión
    project_id = project_id.strip('-')
    
    # Agregar company_id para hacer el project_id único
    project_id = f"{project_id}-{company_id}"
    
    # Limitar longitud (GCP project_id máximo 30 caracteres)
    if len(project_id) > 30:
        # Si es muy largo, truncar la parte del nombre pero mantener el ID
        max_name_length = 30 - len(str(company_id)) - 1  # -1 por el guión
        project_id = f"{project_id[:max_name_length]}-{company_id}"
    
    return project_id


def generate_gcp_commands(row):
    """
    Genera los comandos de creación de proyecto GCP basado en los datos de la empresa
    """
    company_id = row.company_id
    company_name = row.company_name
    company_new_name = row.company_new_name
    
    # Generar project_id
    project_id = generate_project_id(company_new_name, company_id)
    
    if not project_id:
        print(f"⚠️  No se pudo generar project_id para: {company_name}")
        return None
    
    # Comando para crear el proyecto (sin --set-as-default)
    create_project_cmd = f"gcloud projects create {project_id} --name=\"{company_new_name}\""
    
    # Comando para habilitar APIs necesarias
    enable_apis_cmd = f"gcloud services enable bigquery.googleapis.com --project={project_id}"
    
    # Comandos para crear datasets de BigQuery
    datasets = ["settings", "fivetran", "bronze", "silver", "gold", "management"]
    create_datasets_cmds = []
    for dataset in datasets:
        create_datasets_cmds.append(f"bq mk --project_id={project_id} --dataset --location=US {dataset}")
    
    # Comandos para crear cuenta de servicio
    create_service_account_cmd = f"gcloud iam service-accounts create fivetran-account-service --display-name=\"Fivetran Account Service\" --project={project_id}"
    add_bigquery_admin_role_cmd = f"gcloud projects add-iam-policy-binding {project_id} --member=serviceAccount:fivetran-account-service@{project_id}.iam.gserviceaccount.com --role=roles/bigquery.admin"
    
    return {
        'company_id': company_id,
        'company_name': company_name,
        'company_new_name': company_new_name,
        'project_id': project_id,
        'create_project_cmd': create_project_cmd,
        'enable_apis_cmd': enable_apis_cmd,
        'create_datasets_cmds': create_datasets_cmds,
        'create_service_account_cmd': create_service_account_cmd,
        'add_bigquery_admin_role_cmd': add_bigquery_admin_role_cmd
    }


def generate_delete_commands(row):
    """
    Genera los comandos para eliminar proyecto GCP basado en los datos de la empresa
    """
    company_id = row.company_id
    company_name = row.company_name
    company_new_name = row.company_new_name
    
    # Generar project_id
    project_id = generate_project_id(company_new_name, company_id)
    
    if not project_id:
        print(f"⚠️  No se pudo generar project_id para: {company_name}")
        return None
    
    # Comando para eliminar el proyecto
    delete_project_cmd = f"gcloud projects delete {project_id} --quiet"
    
    return {
        'company_id': company_id,
        'company_name': company_name,
        'company_new_name': company_new_name,
        'project_id': project_id,
        'delete_project_cmd': delete_project_cmd
    }


def execute_command(command, dry_run=True):
    """
    Ejecuta un comando del sistema
    """
    if dry_run:
        print(f"🔍 DRY-RUN: {command}")
        return True
    else:
        print(f"🚀 EJECUTANDO: {command}")
        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ ÉXITO: {command}")
                return True
            else:
                print(f"❌ ERROR: {command}")
                print(f"   Error: {result.stderr}")
                return False
        except Exception as e:
            print(f"❌ EXCEPCIÓN: {command}")
            print(f"   Error: {str(e)}")
            return False


def update_company_project_in_bq(company_id, project_id):
    """
    Actualiza el campo company_project en la tabla companies para el company_id dado
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_SOURCE)
    table_ref = f"{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}"
    query = f"""
        UPDATE `{table_ref}`
        SET company_project_id = @project_id
        WHERE company_id = @company_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("company_id", "INT64", company_id),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Esperar a que termine
    print(f"✅ Campo company_project actualizado para company_id={company_id} con project_id={project_id}")


def execute_project_creation(commands, dry_run=True):
    """
    Ejecuta la secuencia completa de creación de proyecto
    """
    print(f"\n{'='*80}")
    print(f"🏗️  {'DRY-RUN' if dry_run else 'EJECUCIÓN REAL'} - {commands['company_name']}")
    print(f"{'='*80}")
    
    success_count = 0
    total_commands = 0
    
    # 1. Crear proyecto
    total_commands += 1
    if execute_command(commands['create_project_cmd'], dry_run):
        success_count += 1
    
    # 2. Habilitar APIs
    total_commands += 1
    if execute_command(commands['enable_apis_cmd'], dry_run):
        success_count += 1
    
    # 3. Crear datasets
    for cmd in commands['create_datasets_cmds']:
        total_commands += 1
        if execute_command(cmd, dry_run):
            success_count += 1
    
    # 4. Crear cuenta de servicio
    total_commands += 1
    if execute_command(commands['create_service_account_cmd'], dry_run):
        success_count += 1
    
    # 5. Asignar rol de BigQuery Admin
    total_commands += 1
    if execute_command(commands['add_bigquery_admin_role_cmd'], dry_run):
        success_count += 1
    
    all_success = (success_count == total_commands)
    print(f"\n📊 RESUMEN: {success_count}/{total_commands} comandos {'simulados' if dry_run else 'ejecutados'} exitosamente")
    
    # Si todo fue exitoso y no es dry_run, actualizar en BigQuery
    if all_success and not dry_run:
        update_company_project_in_bq(commands['company_id'], commands['project_id'])
    
    return all_success


def execute_project_deletion(commands, dry_run=True):
    """
    Ejecuta la secuencia de eliminación de proyecto
    """
    print(f"\n{'='*80}")
    print(f"🗑️  {'DRY-RUN' if dry_run else 'EJECUCIÓN REAL'} - ELIMINACIÓN - {commands['company_name']}")
    print(f"{'='*80}")
    
    success_count = 0
    total_commands = 0
    
    # 1. Eliminar proyecto
    total_commands += 1
    if execute_command(commands['delete_project_cmd'], dry_run):
        success_count += 1
    
    print(f"\n📊 RESUMEN: {success_count}/{total_commands} comandos {'simulados' if dry_run else 'ejecutados'} exitosamente")
    return success_count == total_commands


def dry_run_mode():
    """
    Modo de ejecución en seco - solo muestra los comandos
    """
    print("🔍 MODO DRY-RUN - Solo mostrando comandos (no se ejecutarán)")
    print("=" * 80)
    
    try:
        print(f"Conectando a proyecto: {PROJECT_SOURCE}")
        print(f"Dataset: {DATASET_NAME}")
        print(f"Tabla: {TABLE_NAME}")
        
        # Crear cliente de BigQuery
        print("Creando cliente BigQuery...")
        client = bigquery.Client(project=PROJECT_SOURCE)
        print("Cliente BigQuery creado exitosamente")
        
        # Consulta SQL
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name,
                company_project_id
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            ORDER BY company_id
        """

        print(f"Ejecutando consulta...")
        
        # Ejecutar consulta
        query_job = client.query(query)
        results = query_job.result()
        
        print("Consulta ejecutada exitosamente")
        print("=" * 80)
        
        count = 0
        successful_commands = 0
        print("Generando comandos de creación de proyectos GCP...")
        print()
        
        for row in results:
            try:
                count += 1
                print(f"📋 EMPRESA {count}:")
                print(f"  company_id: {row.company_id}")
                print(f"  company_name: {row.company_name}")
                print(f"  company_new_name: {row.company_new_name}")
                print(f"  company_project_id: {row.company_project_id}")
                
                if row.company_project_id is not None:
                    print(f"  ⚠️  Ya existe un project_id registrado ({row.company_project_id}), se omite la creación.")
                    print("-" * 80)
                    continue
                
                # Generar comandos
                commands = generate_gcp_commands(row)
                
                if commands:
                    successful_commands += 1
                    print(f"  🎯 project_id generado: {commands['project_id']}")
                    print()
                    print("  📝 COMANDOS GENERADOS:")
                    print(f"    # Crear proyecto")
                    print(f"    {commands['create_project_cmd']}")
                    print()
                    print(f"    # Habilitar APIs")
                    print(f"    {commands['enable_apis_cmd']}")
                    print()
                    print(f"    # Crear datasets BigQuery")
                    for i, cmd in enumerate(commands['create_datasets_cmds'], 1):
                        print(f"    {cmd}")
                    print()
                    print(f"    # Crear cuenta de servicio Fivetran")
                    print(f"    {commands['create_service_account_cmd']}")
                    print()
                    print(f"    # Asignar rol de Administrador de BigQuery")
                    print(f"    {commands['add_bigquery_admin_role_cmd']}")
                    print()
                else:
                    print("  ❌ No se pudieron generar comandos")
                
                print("-" * 80)
                
            except Exception as row_error:
                print(f"❌ ERROR en fila {count}: {str(row_error)}")
                print(f"Row problemático: {row}")
                print(f"Tipo de row: {type(row)}")
                print("-" * 80)
        
        print(f"📊 RESUMEN:")
        print(f"  Total de empresas procesadas: {count}")
        print(f"  Comandos generados exitosamente: {successful_commands}")
        print(f"  Errores: {count - successful_commands}")
        
    except Exception as e:
        print(f"❌ ERROR GENERAL: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        import traceback
        print("Traceback completo:")
        traceback.print_exc()


def real_execution_mode():
    """
    Modo de ejecución real - ejecuta los comandos
    """
    print("🚀 MODO EJECUCIÓN REAL - Ejecutando comandos")
    print("⚠️  ADVERTENCIA: Esto creará proyectos reales en GCP")
    print("=" * 80)
    
    # Confirmación del usuario
    confirm = input("¿Estás seguro de que quieres continuar? (escribe 'SI' para confirmar): ")
    if confirm != "SI":
        print("❌ Ejecución cancelada por el usuario")
        return
    
    try:
        print(f"Conectando a proyecto: {PROJECT_SOURCE}")
        print(f"Dataset: {DATASET_NAME}")
        print(f"Tabla: {TABLE_NAME}")
        
        # Crear cliente de BigQuery
        print("Creando cliente BigQuery...")
        client = bigquery.Client(project=PROJECT_SOURCE)
        print("Cliente BigQuery creado exitosamente")
        
        # Consulta SQL
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name,
                company_project_id
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            ORDER BY company_id
        """

        print(f"Ejecutando consulta...")
        
        # Ejecutar consulta
        query_job = client.query(query)
        results = query_job.result()
        
        print("Consulta ejecutada exitosamente")
        print("=" * 80)
        
        count = 0
        successful_projects = 0
        failed_projects = 0
        
        for row in results:
            try:
                count += 1
                print(f"📋 EMPRESA {count}:")
                print(f"  company_id: {row.company_id}")
                print(f"  company_name: {row.company_name}")
                print(f"  company_new_name: {row.company_new_name}")
                print(f"  company_project_id: {row.company_project_id}")
                
                if row.company_project_id is not None:
                    print(f"  ⚠️  Ya existe un project_id registrado ({row.company_project_id}), se omite la creación.")
                    print("-" * 80)
                    continue
                
                # Generar comandos
                commands = generate_gcp_commands(row)
                
                if commands:
                    # Ejecutar secuencia de creación
                    if execute_project_creation(commands, dry_run=False):
                        successful_projects += 1
                    else:
                        failed_projects += 1
                else:
                    failed_projects += 1
                    print(f"❌ No se pudieron generar comandos para: {row.company_name}")
                
            except Exception as row_error:
                failed_projects += 1
                print(f"❌ ERROR en fila {count}: {str(row_error)}")
                print(f"Row problemático: {row}")
        
        print(f"\n📊 RESUMEN FINAL:")
        print(f"  Total de empresas procesadas: {count}")
        print(f"  Proyectos creados exitosamente: {successful_projects}")
        print(f"  Proyectos fallidos: {failed_projects}")
        
    except Exception as e:
        print(f"❌ ERROR GENERAL: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        import traceback
        print("Traceback completo:")
        traceback.print_exc()
    finally:
        # Volver a dejar el proyecto principal como activo
        execute_command('gcloud config set project platform-partners-pro', dry_run=False)


def delete_projects_dry_run():
    """
    Modo de ejecución en seco para eliminación - solo muestra los comandos
    """
    print("🔍 MODO DRY-RUN - ELIMINACIÓN - Solo mostrando comandos (no se ejecutarán)")
    print("=" * 80)
    
    try:
        print(f"Conectando a proyecto: {PROJECT_SOURCE}")
        print(f"Dataset: {DATASET_NAME}")
        print(f"Tabla: {TABLE_NAME}")
        
        # Crear cliente de BigQuery
        print("Creando cliente BigQuery...")
        client = bigquery.Client(project=PROJECT_SOURCE)
        print("Cliente BigQuery creado exitosamente")
        
        # Consulta SQL
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            ORDER BY company_id
        """

        print(f"Ejecutando consulta...")
        
        # Ejecutar consulta
        query_job = client.query(query)
        results = query_job.result()
        
        print("Consulta ejecutada exitosamente")
        print("=" * 80)
        
        count = 0
        successful_commands = 0
        print("Generando comandos de eliminación de proyectos GCP...")
        print()
        
        for row in results:
            try:
                count += 1
                print(f"📋 EMPRESA {count}:")
                print(f"  company_id: {row.company_id}")
                print(f"  company_name: {row.company_name}")
                print(f"  company_new_name: {row.company_new_name}")
                
                # Generar comandos de eliminación
                commands = generate_delete_commands(row)
                
                if commands:
                    successful_commands += 1
                    print(f"  🎯 project_id a eliminar: {commands['project_id']}")
                    print()
                    print("  📝 COMANDOS DE ELIMINACIÓN GENERADOS:")
                    print(f"    # Eliminar proyecto")
                    print(f"    {commands['delete_project_cmd']}")
                else:
                    print("  ❌ No se pudieron generar comandos de eliminación")
                
                print("-" * 80)
                
            except Exception as row_error:
                print(f"❌ ERROR en fila {count}: {str(row_error)}")
                print(f"Row problemático: {row}")
                print(f"Tipo de row: {type(row)}")
                print("-" * 80)
        
        print(f"📊 RESUMEN:")
        print(f"  Total de empresas procesadas: {count}")
        print(f"  Comandos de eliminación generados exitosamente: {successful_commands}")
        print(f"  Errores: {count - successful_commands}")
        
    except Exception as e:
        print(f"❌ ERROR GENERAL: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        import traceback
        print("Traceback completo:")
        traceback.print_exc()


def delete_projects_real():
    """
    Modo de ejecución real para eliminación - ejecuta los comandos
    """
    print("🗑️  MODO EJECUCIÓN REAL - ELIMINACIÓN")
    print("⚠️  ADVERTENCIA: Esto ELIMINARÁ proyectos reales en GCP")
    print("⚠️  ADVERTENCIA: Esta acción NO SE PUEDE DESHACER")
    print("=" * 80)
    
    # Confirmación del usuario
    confirm = input("¿Estás SEGURO de que quieres ELIMINAR los proyectos? (escribe 'ELIMINAR' para confirmar): ")
    if confirm != "ELIMINAR":
        print("❌ Eliminación cancelada por el usuario")
        return
    
    # Doble confirmación
    confirm2 = input("¿Estás COMPLETAMENTE SEGURO? Esta acción es IRREVERSIBLE (escribe 'SI' para confirmar): ")
    if confirm2 != "SI":
        print("❌ Eliminación cancelada por el usuario")
        return
    
    try:
        print(f"Conectando a proyecto: {PROJECT_SOURCE}")
        print(f"Dataset: {DATASET_NAME}")
        print(f"Tabla: {TABLE_NAME}")
        
        # Crear cliente de BigQuery
        print("Creando cliente BigQuery...")
        client = bigquery.Client(project=PROJECT_SOURCE)
        print("Cliente BigQuery creado exitosamente")
        
        # Consulta SQL
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            ORDER BY company_id
        """

        print(f"Ejecutando consulta...")
        
        # Ejecutar consulta
        query_job = client.query(query)
        results = query_job.result()
        
        print("Consulta ejecutada exitosamente")
        print("=" * 80)
        
        count = 0
        successful_deletions = 0
        failed_deletions = 0
        
        for row in results:
            try:
                count += 1
                
                # Generar comandos de eliminación
                commands = generate_delete_commands(row)
                
                if commands:
                    # Ejecutar secuencia de eliminación
                    if execute_project_deletion(commands, dry_run=False):
                        successful_deletions += 1
                    else:
                        failed_deletions += 1
                else:
                    failed_deletions += 1
                    print(f"❌ No se pudieron generar comandos de eliminación para: {row.company_name}")
                
            except Exception as row_error:
                failed_deletions += 1
                print(f"❌ ERROR en fila {count}: {str(row_error)}")
                print(f"Row problemático: {row}")
        
        print(f"\n📊 RESUMEN FINAL:")
        print(f"  Total de empresas procesadas: {count}")
        print(f"  Proyectos eliminados exitosamente: {successful_deletions}")
        print(f"  Proyectos fallidos: {failed_deletions}")
        
    except Exception as e:
        print(f"❌ ERROR GENERAL: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        import traceback
        print("Traceback completo:")
        traceback.print_exc()
    finally:
        # Volver a dejar el proyecto principal como activo
        execute_command('gcloud config set project platform-partners-pro', dry_run=False)


def main():
    """
    Función principal que permite elegir entre dry-run y ejecución real
    """
    print("🔧 SCRIPT DE CREACIÓN/ELIMINACIÓN DE PROYECTOS GCP")
    print("=" * 60)
    print("1. Modo DRY-RUN - Crear proyectos (solo mostrar comandos)")
    print("2. Modo EJECUCIÓN REAL - Crear proyectos")
    print("3. Modo DRY-RUN - Eliminar proyectos (solo mostrar comandos)")
    print("4. Modo EJECUCIÓN REAL - Eliminar proyectos")
    print("=" * 60)
    
    choice = input("Selecciona el modo (1, 2, 3 o 4): ").strip()
    
    if choice == "1":
        dry_run_mode()
    elif choice == "2":
        real_execution_mode()
    elif choice == "3":
        delete_projects_dry_run()
    elif choice == "4":
        delete_projects_real()
    else:
        print("❌ Opción inválida. Saliendo...")
        sys.exit(1)


if __name__ == "__main__":
    main() 
