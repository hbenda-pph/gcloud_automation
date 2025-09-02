#!/usr/bin/env python3
"""
Script para asignar permisos de vistas autorizadas a todas las tablas "call" 
en el dataset "silver" de cada proyecto según la tabla companies
"""
from google.cloud import bigquery
import subprocess
import sys

PROJECT_SOURCE = "platform-partners-des"
DATASET_NAME = "settings"
TABLE_NAME = "companies"


def get_companies_with_projects():
    """
    Obtiene todas las empresas que tienen un company_project_id asignado
    """
    try:
        client = bigquery.Client(project=PROJECT_SOURCE)
        
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name,
                company_project_id
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            WHERE company_project_id IS NOT NULL
            ORDER BY company_id
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        return list(results)
        
    except Exception as e:
        print(f"❌ ERROR al obtener empresas con proyectos: {str(e)}")
        return []


def get_call_table_info(project_id):
    """
    Obtiene información de la tabla "call" en el dataset servicetitan_<project_id>
    """
    try:
        client = bigquery.Client(project=project_id)
        
        # Convertir project_id: cambiar guiones por guiones bajos
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        
        query = f"""
            SELECT 
                'call' as table_id,
                'call' as table_name,
                '{dataset_name}' as dataset_id
            FROM `{project_id}.{dataset_name}.__TABLES__`
            WHERE table_id = 'call'
            LIMIT 1
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        # Si la tabla existe, retornar la información
        for row in results:
            return row
        
        # Si no se encontró la tabla, retornar None
        return None
        
    except Exception as e:
        print(f"❌ ERROR al verificar tabla call en proyecto {project_id}, dataset {dataset_name}: {str(e)}")
        return None


def check_service_account_exists(project_id):
    """
    Verifica si la cuenta de servicio data-analytics existe
    """
    try:
        service_account_email = f"data-analytics@{project_id}.iam.gserviceaccount.com"
        check_cmd = f"gcloud iam service-accounts describe {service_account_email} --project={project_id}"
        
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ ERROR al verificar cuenta de servicio: {str(e)}")
        return False


def create_data_analytics_service_account(project_id, dry_run=True):
    """
    Crea la cuenta de servicio data-analytics si no existe
    """
    try:
        service_account_email = f"data-analytics@{project_id}.iam.gserviceaccount.com"
        
        print(f"🔧 Verificando cuenta de servicio para análisis de datos...")
        print(f"   Service Account: {service_account_email}")
        
        # Primero verificar si ya existe
        if not dry_run and check_service_account_exists(project_id):
            print(f"ℹ️  La cuenta de servicio ya existe, saltando creación")
            return True
        
        create_cmd = f"gcloud iam service-accounts create data-analytics --display-name=\"Data Analytics Service Account\" --project={project_id}"
        
        if dry_run:
            print(f"🔍 DRY-RUN: {create_cmd}")
            return True
        else:
            print(f"🚀 EJECUTANDO: {create_cmd}")
            result = subprocess.run(create_cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Cuenta de servicio creada exitosamente")
                return True
            else:
                # Si ya existe, no es un error
                if "already exists" in result.stderr:
                    print(f"ℹ️  La cuenta de servicio ya existe")
                    return True
                else:
                    print(f"❌ ERROR: {result.stderr}")
                    return False
                    
    except Exception as e:
        print(f"❌ ERROR al crear cuenta de servicio: {str(e)}")
        return False


def check_permission_exists(project_id, dataset_id, table_id, central_project):
    """
    Verifica si el permiso ya existe para la cuenta data-analytics
    """
    try:
        service_account_email = f"data-analytics@{central_project}.iam.gserviceaccount.com"
        check_cmd = f"bq show --format=json {project_id}:{dataset_id}.{table_id}"
        
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            import json
            table_info = json.loads(result.stdout)
            
            # Verificar si el service account ya tiene permisos
            if 'access' in table_info:
                for access in table_info['access']:
                    if 'userByEmail' in access and access['userByEmail'] == service_account_email:
                        return True
                    if 'specialGroup' in access and 'serviceAccounts' in access['specialGroup']:
                        # Verificar si está en el grupo de service accounts
                        return True
            
        return False
        
    except Exception as e:
        print(f"❌ ERROR al verificar permisos: {str(e)}")
        return False


def assign_data_viewer_permission(project_id, dataset_id, table_id, central_project, dry_run=True):
    """
    Asigna permiso de lectura (dataViewer) a la cuenta data-analytics del proyecto central
    """
    try:
        service_account_email = f"data-analytics@{central_project}.iam.gserviceaccount.com"
        
        print(f"🔧 Verificando permisos de lectura para la cuenta de análisis...")
        print(f"   Tabla: {project_id}:{dataset_id}.{table_id}")
        print(f"   Service Account: {service_account_email}")
        
        # Verificar si el permiso ya existe
        if not dry_run and check_permission_exists(project_id, dataset_id, table_id, central_project):
            print(f"ℹ️  El permiso ya existe, saltando asignación")
            return "SKIP"
        
        # Comando para asignar permiso de lectura directo
        #cmd = f"bq add-iam-policy-binding --project_id={project_id} --member=serviceAccount:{service_account_email} --role=roles/bigquery.dataViewer {project_id}:{dataset_id}.{table_id}"
        cmd = f"bq add-iam-policy-binding --project_id={project_id} --member=VIEW:platform-partners-des.silver.vw_consolidated_call --role=roles/bigquery.dataViewer {project_id}:{dataset_id}.{table_id}"
        
        print(f"🔧 Asignando permiso de lectura a la cuenta de análisis...")
        print(f"   Permiso: roles/bigquery.dataViewer")
        
        return cmd
        
    except Exception as e:
        print(f"❌ ERROR al generar comando de permiso para {table_id}: {str(e)}")
        return None


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


def process_company_call_table(company, dry_run=True):
    """
    Procesa la tabla call de una empresa específica
    """
    company_id = company.company_id
    company_name = company.company_name
    company_new_name = company.company_new_name
    project_id = company.company_project_id
    
    print(f"\n{'='*80}")
    print(f"🏢 PROCESANDO EMPRESA: {company_name}")
    print(f"   Project ID: {project_id}")
    print(f"{'='*80}")
    
    # Verificar que existe la tabla call en el dataset silver
    call_table = get_call_table_info(project_id)
    
    if not call_table:
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        print(f"⚠️  No se encontró la tabla 'call' en el dataset '{dataset_name}' del proyecto {project_id}")
        return {
            'company_id': company_id,
            'company_name': company_name,
            'project_id': project_id,
            'table_found': False,
            'permission_assigned': False,
            'error': True
        }
    
    print(f"📋 Tabla 'call' encontrada en el dataset '{call_table.dataset_id}': {call_table.table_id}")
    
    # Configuración del proyecto central
    central_project = "platform-partners-des"  # Proyecto central donde está la cuenta data-analytics
    
    print(f"\n🔐 Asignando permisos de lectura para vista consolidada...")
    print(f"   Tabla: {project_id}:{call_table.dataset_id}.{call_table.table_id}")
    print(f"   Proyecto central: {central_project}")
    
    try:
        # Primero crear la cuenta de servicio si no existe
        if not create_data_analytics_service_account(central_project, dry_run):
            print(f"❌ No se pudo crear/verificar la cuenta de servicio para {company_name}")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'table_found': True,
                'permission_assigned': False,
                'error': True
            }
        
        # Generar comando de asignación de permiso
        cmd = assign_data_viewer_permission(
            project_id=project_id,
            dataset_id=call_table.dataset_id,
            table_id=call_table.table_id,
            central_project=central_project,
            dry_run=dry_run
        )
        
        if cmd == "SKIP":
            print(f"ℹ️  Permiso ya existe para {company_name}, saltando")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'table_found': True,
                'permission_assigned': True,  # Ya existe, considerarlo como asignado
                'error': False
            }
        elif cmd:
            if execute_command(cmd, dry_run):
                print(f"✅ Permiso asignado exitosamente para {company_name}")
                return {
                    'company_id': company_id,
                    'company_name': company_name,
                    'project_id': project_id,
                    'table_found': True,
                    'permission_assigned': True,
                    'error': False
                }
            else:
                print(f"❌ Error al asignar permiso para {company_name}")
                return {
                    'company_id': company_id,
                    'company_name': company_name,
                    'project_id': project_id,
                    'table_found': True,
                    'permission_assigned': False,
                    'error': True
                }
        else:
            print(f"❌ No se pudo generar comando para {company_name}")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'table_found': True,
                'permission_assigned': False,
                'error': True
            }
            
    except Exception as e:
        print(f"❌ ERROR procesando tabla call para {company_name}: {str(e)}")
        return {
            'company_id': company_id,
            'company_name': company_name,
            'project_id': project_id,
            'table_found': True,
            'permission_assigned': False,
            'error': True
        }


def dry_run_mode():
    """
    Modo de ejecución en seco - solo muestra los comandos
    """
    print("🔍 MODO DRY-RUN - Solo mostrando comandos de asignación de permisos (no se ejecutarán)")
    print("=" * 80)
    
    # Obtener empresas con proyectos
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados")
    print("=" * 80)
    
    total_tables = 0
    total_permissions = 0
    total_errors = 0
    
    for company in companies:
        result = process_company_call_table(company, dry_run=True)
        if result['table_found']:
            total_tables += 1
        if result['permission_assigned']:
            total_permissions += 1
        if result['error']:
            total_errors += 1
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN GENERAL:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de tablas 'call' encontradas: {total_tables}")
    print(f"   Total de permisos a asignar: {total_permissions}")
    print(f"   Total de errores: {total_errors}")
    print(f"{'='*80}")


def real_execution_mode():
    """
    Modo de ejecución real - ejecuta los comandos
    """
    print("🚀 MODO EJECUCIÓN REAL - Ejecutando comandos")
    print("⚠️  ADVERTENCIA: Esto asignará permisos de lectura reales en GCP")
    print("=" * 80)
    
    # Confirmación del usuario
    confirm = input("¿Estás seguro de que quieres continuar? (escribe 'SI' para confirmar): ")
    if confirm != "SI":
        print("❌ Ejecución cancelada por el usuario")
        return
    
    # Obtener empresas con proyectos
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados")
    print("=" * 80)
    
    total_tables = 0
    total_permissions = 0
    total_errors = 0
    successful_companies = 0
    failed_companies = 0
    
    for company in companies:
        try:
            result = process_company_call_table(company, dry_run=False)
            if result['table_found']:
                total_tables += 1
            if result['permission_assigned']:
                total_permissions += 1
            if result['error']:
                total_errors += 1
            
            if not result['error']:
                successful_companies += 1
            else:
                failed_companies += 1
                
        except Exception as e:
            failed_companies += 1
            total_errors += 1
            print(f"❌ ERROR procesando empresa {company.company_name}: {str(e)}")
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN FINAL:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Empresas exitosas: {successful_companies}")
    print(f"   Empresas con errores: {failed_companies}")
    print(f"   Total de tablas 'call' encontradas: {total_tables}")
    print(f"   Total de permisos asignados: {total_permissions}")
    print(f"   Total de errores: {total_errors}")
    print(f"{'='*80}")
    
    # Volver a dejar el proyecto principal como activo
    execute_command('gcloud config set project platform-partners-des', dry_run=False)


def list_call_tables_only():
    """
    Solo lista las tablas call encontradas sin asignar permisos
    """
    print("📋 MODO LISTADO - Solo mostrando tablas 'call' encontradas en datasets servicetitan_*")
    print("   (Para asignar permisos de lectura para la vista consolidada)")
    print("=" * 80)
    
    # Obtener empresas con proyectos
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados")
    print("=" * 80)
    
    total_tables = 0
    
    for company in companies:
        company_id = company.company_id
        company_name = company.company_name
        project_id = company.company_project_id
        
        print(f"\n🏢 EMPRESA: {company_name}")
        print(f"   Project ID: {project_id}")
        
        # Verificar que existe la tabla call en el dataset servicetitan_*
        call_table = get_call_table_info(project_id)
        
        if not call_table:
            dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
            print(f"   ⚠️  No se encontró la tabla 'call' en el dataset '{dataset_name}'")
        else:
            print(f"   📋 Tabla 'call' encontrada en dataset '{call_table.dataset_id}': {call_table.table_id}")
            total_tables += 1
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de tablas 'call' encontradas: {total_tables}")
    print(f"{'='*80}")


def main():
    """
    Función principal que permite elegir entre diferentes modos
    """
    print("🔧 SCRIPT DE ASIGNACIÓN DE PERMISOS PARA VISTA CONSOLIDADA")
    print("=" * 60)
    print("1. Modo LISTADO - Solo mostrar tablas 'call' encontradas en datasets servicetitan_*")
    print("2. Modo DRY-RUN - Mostrar comandos de asignación de permisos")
    print("3. Modo EJECUCIÓN REAL - Asignar permisos de lectura para vista consolidada")
    print("=" * 60)
    
    choice = input("Selecciona el modo (1, 2 o 3): ").strip()
    
    if choice == "1":
        list_call_tables_only()
    elif choice == "2":
        dry_run_mode()
    elif choice == "3":
        real_execution_mode()
    else:
        print("❌ Opción inválida. Saliendo...")
        sys.exit(1)


if __name__ == "__main__":
    main() 

