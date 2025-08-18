#!/usr/bin/env python3
"""
Script para crear vistas autorizadas usando la API REST de BigQuery
"""
import requests
import json
import time
import logging
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery
import subprocess
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bigquery_authorization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PROJECT_SOURCE = "platform-partners-qua"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Configuración de vistas en silver a autorizar
SILVER_VIEWS = [
    "vw_master_tracker_export",
    "vw_new_customer_list",
    "vw_sold_estimates"
]

def get_access_token():
    """
    Obtiene el token de acceso para la API REST
    """
    try:
        logger.info("Obteniendo token de acceso...")
        credentials, project = default()
        credentials.refresh(Request())
        token = credentials.token
        logger.info("Token de acceso obtenido exitosamente")
        return token
    except Exception as e:
        logger.error(f"ERROR al obtener token de acceso: {str(e)}")
        print(f"❌ ERROR al obtener token de acceso: {str(e)}")
        return None


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


def check_servicetitan_dataset_exists(project_id):
    """
    Verifica si existe el dataset servicetitan_* en el proyecto
    """
    try:
        client = bigquery.Client(project=project_id)
        
        # Convertir project_id: cambiar guiones por guiones bajos
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        
        # Verificar si el dataset existe
        try:
            dataset = client.get_dataset(dataset_name)
            return {
                'dataset_id': dataset_name,
                'project_id': project_id,
                'exists': True
            }
        except Exception:
            return {
                'dataset_id': dataset_name,
                'project_id': project_id,
                'exists': False
            }
        
    except Exception as e:
        print(f"❌ ERROR al verificar dataset servicetitan en proyecto {project_id}: {str(e)}")
        return None


def check_bronze_dataset_exists(project_id):
    """
    Verifica si existe el dataset bronze en el proyecto
    """
    try:
        logger.info(f"Verificando dataset bronze en proyecto {project_id}")
        client = bigquery.Client(project=project_id)
        
        dataset_name = "bronze"
        
        # Verificar si el dataset existe
        try:
            dataset = client.get_dataset(dataset_name)
            logger.info(f"Dataset bronze encontrado en proyecto {project_id}")
            return {
                'dataset_id': dataset_name,
                'project_id': project_id,
                'exists': True
            }
        except Exception:
            logger.warning(f"Dataset bronze no encontrado en proyecto {project_id}")
            return {
                'dataset_id': dataset_name,
                'project_id': project_id,
                'exists': False
            }
        
    except Exception as e:
        logger.error(f"ERROR al verificar dataset bronze en proyecto {project_id}: {str(e)}")
        print(f"❌ ERROR al verificar dataset bronze en proyecto {project_id}: {str(e)}")
        return None


def authorize_view_in_dataset_api(project_id, dataset_id, silver_project, silver_dataset, silver_view, dry_run=True):
    """
    Autoriza una vista en un dataset usando la API REST de BigQuery
    """
    try:
        access_token = get_access_token()
        if not access_token:
            return None
        
        # URL de la API REST para autorizar vistas en el dataset
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}"
        
        # Primero obtener la configuración actual del dataset
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        print(f"🔧 Autorizando vista en dataset via API REST...")
        print(f"   Dataset: {project_id}:{dataset_id}")
        print(f"   Vista autorizada: {silver_project}:{silver_dataset}.{silver_view}")
        
        if dry_run:
            print(f"🔍 DRY-RUN: GET {url}")
            print(f"   Luego: PATCH {url} con vista autorizada")
            return True
        else:
            # Obtener configuración actual del dataset
            print(f"🚀 OBTENIENDO configuración del dataset: GET {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"❌ ERROR obteniendo dataset: {response.status_code}")
                return False
            
            dataset_info = response.json()
            
            # Agregar la vista autorizada si no existe
            if 'access' not in dataset_info:
                dataset_info['access'] = []
            
            # Verificar si ya existe la autorización
            for access in dataset_info['access']:
                if 'view' in access:
                    view_ref = access['view']
                    if (view_ref.get('projectId') == silver_project and 
                        view_ref.get('datasetId') == silver_dataset and 
                        view_ref.get('tableId') == silver_view):
                        print(f"ℹ️  La vista ya está autorizada, saltando")
                        return "SKIP"
            
            # Agregar nueva autorización de vista
            new_access = {
                "view": {
                    "projectId": silver_project,
                    "datasetId": silver_dataset,
                    "tableId": silver_view
                }
            }
            dataset_info['access'].append(new_access)
            
            # Actualizar el dataset con la nueva autorización
            print(f"🚀 EJECUTANDO: PATCH {url}")
            update_response = requests.patch(url, headers=headers, json=dataset_info)
            
            if update_response.status_code == 200:
                print(f"✅ Vista autorizada exitosamente")
                return True
            else:
                print(f"❌ ERROR: {update_response.status_code}")
                print(f"   Response: {update_response.text}")
                return False
                
    except Exception as e:
        print(f"❌ ERROR al autorizar vista via API: {str(e)}")
        return False


def check_view_authorization_exists_api(project_id, dataset_id, silver_project, silver_dataset, silver_view):
    """
    Verifica si la vista ya está autorizada en el dataset usando la API REST
    """
    try:
        access_token = get_access_token()
        if not access_token:
            return False
        
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}"
        
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            dataset_info = response.json()
            
            # Verificar si la vista ya está autorizada
            if 'access' in dataset_info:
                for access in dataset_info['access']:
                    if 'view' in access:
                        view_ref = access['view']
                        if (view_ref.get('projectId') == silver_project and 
                            view_ref.get('datasetId') == silver_dataset and 
                            view_ref.get('tableId') == silver_view):
                            return True
            
        return False
        
    except Exception as e:
        print(f"❌ ERROR al verificar autorización de vista via API: {str(e)}")
        return False


def validate_silver_view_exists(silver_project, silver_dataset, silver_view):
    """
    Valida que la vista en silver existe antes de intentar autorizarla
    """
    try:
        client = bigquery.Client(project=silver_project)
        
        # Verificar si la vista existe
        try:
            table_ref = client.dataset(silver_dataset).table(silver_view)
            table = client.get_table(table_ref)
            print(f"✅ Vista en silver validada: {silver_project}:{silver_dataset}.{silver_view}")
            return True
        except Exception:
            print(f"❌ ERROR: La vista {silver_project}:{silver_dataset}.{silver_view} no existe")
            return False
            
    except Exception as e:
        print(f"❌ ERROR al validar vista en silver: {str(e)}")
        return False


def check_user_permissions(project_id):
    """
    Verifica que el usuario tiene permisos para modificar datasets en el proyecto
    """
    try:
        client = bigquery.Client(project=project_id)
        
        # Intentar listar datasets para verificar permisos básicos
        datasets = list(client.list_datasets())
        print(f"✅ Permisos verificados para proyecto {project_id}")
        return True
        
    except Exception as e:
        print(f"❌ ERROR de permisos en proyecto {project_id}: {str(e)}")
        return False


def process_dataset_authorization(project_id, dataset_id, silver_project, silver_dataset, silver_view, dry_run, dataset_type):
    """
    Procesa la autorización de vista en un dataset específico
    """
    try:
        logger.info(f"Procesando autorización de vista en dataset {dataset_type} {project_id}:{dataset_id}")
        print(f"🔐 Autorizando vista en dataset {dataset_type}...")
        print(f"   Dataset: {project_id}:{dataset_id}")
        print(f"   Vista en silver: {silver_project}:{silver_dataset}.{silver_view}")
        
        # Verificar si la vista ya está autorizada
        if not dry_run and check_view_authorization_exists_api(project_id, dataset_id, silver_project, silver_dataset, silver_view):
            logger.info(f"Vista ya autorizada en {dataset_type}, saltando")
            print(f"ℹ️  La vista ya está autorizada en {dataset_type}, saltando")
            return {'success': True, 'skipped': True}
        
        # Autorizar vista en dataset via API REST
        result = authorize_view_in_dataset_api(
            project_id=project_id,
            dataset_id=dataset_id,
            silver_project=silver_project,
            silver_dataset=silver_dataset,
            silver_view=silver_view,
            dry_run=dry_run
        )
        
        if result == "SKIP":
            logger.info(f"Vista ya autorizada en {dataset_type} (SKIP), saltando")
            print(f"ℹ️  Vista ya autorizada en {dataset_type}, saltando")
            return {'success': True, 'skipped': True}
        elif result:
            logger.info(f"Vista autorizada exitosamente en {dataset_type}")
            print(f"✅ Vista autorizada exitosamente en {dataset_type}")
            return {'success': True, 'skipped': False}
        else:
            logger.error(f"Error al autorizar vista en {dataset_type}")
            print(f"❌ Error al autorizar vista en {dataset_type}")
            return {'success': False, 'skipped': False}
            
    except Exception as e:
        logger.error(f"ERROR procesando autorización de vista en {dataset_type}: {str(e)}")
        print(f"❌ ERROR procesando autorización de vista en {dataset_type}: {str(e)}")
        return {'success': False, 'skipped': False}


def process_company_view_authorization(company, dry_run=True):
    """
    Procesa la autorización de vista para una empresa específica en ambos datasets:
    - servicetitan_* (raw)
    - bronze
    """
    company_id = company.company_id
    company_name = company.company_name
    company_new_name = company.company_new_name
    project_id = company.company_project_id
    
    print(f"\n{'='*80}")
    print(f"🏢 PROCESANDO EMPRESA: {company_name}")
    print(f"   Project ID: {project_id}")
    print(f"{'='*80}")
    
    results = {
        'company_id': company_id,
        'company_name': company_name,
        'project_id': project_id,
        'servicetitan_dataset_found': False,
        'bronze_dataset_found': False,
        'servicetitan_view_authorized': False,
        'bronze_view_authorized': False,
        'total_errors': 0,
        'error': False
    }
    
    # Configuración del proyecto silver y vistas a autorizar
    silver_project = project_id  # Usar el project_id de la empresa
    silver_dataset = "silver"  # Dataset silver donde están las vistas
    
    # 1. PROCESAR DATASET SERVICETITAN_* (RAW)
    print(f"\n📊 PROCESANDO DATASET SERVICETITAN_* (RAW)...")
    servicetitan_info = check_servicetitan_dataset_exists(project_id)
    
    if servicetitan_info and servicetitan_info['exists']:
        results['servicetitan_dataset_found'] = True
        print(f"✅ Dataset servicetitan encontrado: {project_id}:{servicetitan_info['dataset_id']}")
        
        # Procesar todas las vistas en silver
        servicetitan_views_authorized = 0
        servicetitan_views_total = 0
        
        for silver_view in SILVER_VIEWS:
            print(f"\n   🔐 Procesando vista: {silver_view}")
            # Validar que la vista en silver existe
            if validate_silver_view_exists(silver_project, silver_dataset, silver_view):
                # Autorizar vista en dataset servicetitan
                servicetitan_result = process_dataset_authorization(
                    project_id, servicetitan_info['dataset_id'], 
                    silver_project, silver_dataset, silver_view, 
                    dry_run, "servicetitan"
                )
                servicetitan_views_total += 1
                if servicetitan_result['success']:
                    servicetitan_views_authorized += 1
                if not servicetitan_result['success']:
                    results['total_errors'] += 1
            else:
                print(f"   ❌ Vista {silver_view} no existe en silver, saltando")
                results['total_errors'] += 1
        
        results['servicetitan_view_authorized'] = (servicetitan_views_authorized == servicetitan_views_total and servicetitan_views_total > 0)
        print(f"   📊 Resumen servicetitan: {servicetitan_views_authorized}/{servicetitan_views_total} vistas autorizadas")
    else:
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        print(f"⚠️  No se encontró el dataset '{dataset_name}' en el proyecto {project_id}")
    
    # 2. PROCESAR DATASET BRONZE
    print(f"\n📊 PROCESANDO DATASET BRONZE...")
    bronze_info = check_bronze_dataset_exists(project_id)
    
    if bronze_info and bronze_info['exists']:
        results['bronze_dataset_found'] = True
        print(f"✅ Dataset bronze encontrado: {project_id}:{bronze_info['dataset_id']}")
        
        # Procesar todas las vistas en silver
        bronze_views_authorized = 0
        bronze_views_total = 0
        
        for silver_view in SILVER_VIEWS:
            print(f"\n   🔐 Procesando vista: {silver_view}")
            # Validar que la vista en silver existe
            if validate_silver_view_exists(silver_project, silver_dataset, silver_view):
                # Autorizar vista en dataset bronze
                bronze_result = process_dataset_authorization(
                    project_id, bronze_info['dataset_id'], 
                    silver_project, silver_dataset, silver_view, 
                    dry_run, "bronze"
                )
                bronze_views_total += 1
                if bronze_result['success']:
                    bronze_views_authorized += 1
                if not bronze_result['success']:
                    results['total_errors'] += 1
            else:
                print(f"   ❌ Vista {silver_view} no existe en silver, saltando")
                results['total_errors'] += 1
        
        results['bronze_view_authorized'] = (bronze_views_authorized == bronze_views_total and bronze_views_total > 0)
        print(f"   📊 Resumen bronze: {bronze_views_authorized}/{bronze_views_total} vistas autorizadas")
    else:
        print(f"⚠️  No se encontró el dataset 'bronze' en el proyecto {project_id}")
    
    # Determinar si hubo errores generales
    if results['total_errors'] > 0:
        results['error'] = True
    
    # Resumen de la empresa
    print(f"\n📋 RESUMEN PARA {company_name}:")
    print(f"   Dataset servicetitan: {'✅ Encontrado' if results['servicetitan_dataset_found'] else '❌ No encontrado'}")
    print(f"   Dataset bronze: {'✅ Encontrado' if results['bronze_dataset_found'] else '❌ No encontrado'}")
    print(f"   Vista autorizada en servicetitan: {'✅ Sí' if results['servicetitan_view_authorized'] else '❌ No'}")
    print(f"   Vista autorizada en bronze: {'✅ Sí' if results['bronze_view_authorized'] else '❌ No'}")
    print(f"   Total de errores: {results['total_errors']}")
    
    return results


def dry_run_mode():
    """
    Modo de ejecución en seco - solo muestra los comandos
    """
    print("🔍 MODO DRY-RUN - Solo mostrando comandos de API REST (no se ejecutarán)")
    print("=" * 80)
    
    # Obtener empresas con proyectos
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados")
    print("=" * 80)
    
    total_servicetitan_datasets = 0
    total_bronze_datasets = 0
    total_servicetitan_authorizations = 0
    total_bronze_authorizations = 0
    total_errors = 0
    
    for company in companies:
        result = process_company_view_authorization(company, dry_run=True)
        if result['servicetitan_dataset_found']:
            total_servicetitan_datasets += 1
        if result['bronze_dataset_found']:
            total_bronze_datasets += 1
        if result['servicetitan_view_authorized']:
            total_servicetitan_authorizations += 1
        if result['bronze_view_authorized']:
            total_bronze_authorizations += 1
        if result['error']:
            total_errors += 1
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN GENERAL:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de datasets servicetitan_* encontrados: {total_servicetitan_datasets}")
    print(f"   Total de datasets bronze encontrados: {total_bronze_datasets}")
    print(f"   Total de vistas a autorizar en servicetitan: {total_servicetitan_authorizations}")
    print(f"   Total de vistas a autorizar en bronze: {total_bronze_authorizations}")
    print(f"   Total de errores: {total_errors}")
    print(f"{'='*80}")


def real_execution_mode():
    """
    Modo de ejecución real - ejecuta los comandos
    """
    print("🚀 MODO EJECUCIÓN REAL - Ejecutando comandos de API REST")
    print("⚠️  ADVERTENCIA: Esto autorizará vistas reales en BigQuery")
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
    
    # Verificar permisos en todos los proyectos antes de continuar
    print("🔐 Verificando permisos en todos los proyectos...")
    projects_to_process = set()
    for company in companies:
        if company.company_project_id:
            projects_to_process.add(company.company_project_id)
    
    for project_id in projects_to_process:
        if not check_user_permissions(project_id):
            print(f"❌ No se pueden procesar proyectos sin permisos suficientes")
            return
    
    print("✅ Permisos verificados en todos los proyectos")
    print("=" * 80)
    
    total_servicetitan_datasets = 0
    total_bronze_datasets = 0
    total_servicetitan_authorizations = 0
    total_bronze_authorizations = 0
    total_errors = 0
    successful_companies = 0
    failed_companies = 0
    
    for company in companies:
        try:
            result = process_company_view_authorization(company, dry_run=False)
            if result['servicetitan_dataset_found']:
                total_servicetitan_datasets += 1
            if result['bronze_dataset_found']:
                total_bronze_datasets += 1
            if result['servicetitan_view_authorized']:
                total_servicetitan_authorizations += 1
            if result['bronze_view_authorized']:
                total_bronze_authorizations += 1
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
    print(f"   Total de datasets servicetitan_* encontrados: {total_servicetitan_datasets}")
    print(f"   Total de datasets bronze encontrados: {total_bronze_datasets}")
    print(f"   Total de vistas autorizadas en servicetitan: {total_servicetitan_authorizations}")
    print(f"   Total de vistas autorizadas en bronze: {total_bronze_authorizations}")
    print(f"   Total de errores: {total_errors}")
    print(f"{'='*80}")


def list_datasets_only():
    """
    Solo lista los datasets servicetitan_* y bronze encontrados sin autorizar vistas
    """
    print("📋 MODO LISTADO - Solo mostrando datasets servicetitan_* y bronze encontrados")
    print("   (Para autorizar la vista consolidada en cada dataset via API REST)")
    print("=" * 80)
    
    # Obtener empresas con proyectos
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados")
    print("=" * 80)
    
    total_servicetitan_datasets = 0
    total_bronze_datasets = 0
    
    for company in companies:
        company_id = company.company_id
        company_name = company.company_name
        project_id = company.company_project_id
        
        print(f"\n🏢 EMPRESA: {company_name}")
        print(f"   Project ID: {project_id}")
        
        # Verificar dataset servicetitan_*
        servicetitan_info = check_servicetitan_dataset_exists(project_id)
        if not servicetitan_info or not servicetitan_info['exists']:
            dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
            print(f"   ⚠️  No se encontró el dataset '{dataset_name}'")
        else:
            print(f"   📋 Dataset servicetitan encontrado: {project_id}:{servicetitan_info['dataset_id']}")
            total_servicetitan_datasets += 1
        
        # Verificar dataset bronze
        bronze_info = check_bronze_dataset_exists(project_id)
        if not bronze_info or not bronze_info['exists']:
            print(f"   ⚠️  No se encontró el dataset 'bronze'")
        else:
            print(f"   📋 Dataset bronze encontrado: {project_id}:{bronze_info['dataset_id']}")
            total_bronze_datasets += 1
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de datasets servicetitan_* encontrados: {total_servicetitan_datasets}")
    print(f"   Total de datasets bronze encontrados: {total_bronze_datasets}")
    print(f"{'='*80}")


def main():
    """
    Función principal que permite elegir entre diferentes modos
    """
    print("🔧 SCRIPT DE AUTORIZACIÓN DE MÚLTIPLES VISTAS EN SILVER VIA API REST")
    print("=" * 60)
    print(f"📋 Vistas configuradas: {', '.join(SILVER_VIEWS)}")
    print("=" * 60)
    print("1. Modo LISTADO - Solo mostrar datasets servicetitan_* y bronze encontrados")
    print("2. Modo DRY-RUN - Mostrar comandos de API REST (no se ejecutarán)")
    print("3. Modo EJECUCIÓN REAL - Autorizar múltiples vistas en silver via API REST")
    print("=" * 60)
    
    choice = input("Selecciona el modo (1, 2 o 3): ").strip()
    
    if choice == "1":
        list_datasets_only()
    elif choice == "2":
        dry_run_mode()
    elif choice == "3":
        real_execution_mode()
    else:
        print("❌ Opción inválida. Saliendo...")
        sys.exit(1)


if __name__ == "__main__":
    main() 