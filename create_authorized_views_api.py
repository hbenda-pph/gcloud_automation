#!/usr/bin/env python3
"""
Script para crear vistas autorizadas usando la API REST de BigQuery
"""
import requests
import json
import time
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery
import subprocess
import sys

PROJECT_SOURCE = "platform-partners-qua"
DATASET_NAME = "settings"
TABLE_NAME = "companies"


def get_access_token():
    """
    Obtiene el token de acceso para la API REST
    """
    try:
        credentials, project = default()
        credentials.refresh(Request())
        return credentials.token
    except Exception as e:
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


def authorize_view_in_dataset_api(project_id, dataset_id, central_project, central_dataset, central_view, dry_run=True):
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
        print(f"   Vista autorizada: {central_project}:{central_dataset}.{central_view}")
        
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
                    if (view_ref.get('projectId') == central_project and 
                        view_ref.get('datasetId') == central_dataset and 
                        view_ref.get('tableId') == central_view):
                        print(f"ℹ️  La vista ya está autorizada, saltando")
                        return "SKIP"
            
            # Agregar nueva autorización de vista
            new_access = {
                "view": {
                    "projectId": central_project,
                    "datasetId": central_dataset,
                    "tableId": central_view
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


def check_view_authorization_exists_api(project_id, dataset_id, central_project, central_dataset, central_view):
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
                        if (view_ref.get('projectId') == central_project and 
                            view_ref.get('datasetId') == central_dataset and 
                            view_ref.get('tableId') == central_view):
                            return True
            
        return False
        
    except Exception as e:
        print(f"❌ ERROR al verificar autorización de vista via API: {str(e)}")
        return False


def process_company_view_authorization(company, dry_run=True):
    """
    Procesa la autorización de vista para una empresa específica
    """
    company_id = company.company_id
    company_name = company.company_name
    company_new_name = company.company_new_name
    project_id = company.company_project_id
    
    print(f"\n{'='*80}")
    print(f"🏢 PROCESANDO EMPRESA: {company_name}")
    print(f"   Project ID: {project_id}")
    print(f"{'='*80}")
    
    # Verificar que existe el dataset servicetitan_*
    dataset_info = check_servicetitan_dataset_exists(project_id)
    
    if not dataset_info or not dataset_info['exists']:
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        print(f"⚠️  No se encontró el dataset '{dataset_name}' en el proyecto {project_id}")
        return {
            'company_id': company_id,
            'company_name': company_name,
            'project_id': project_id,
            'dataset_found': False,
            'view_authorized': False,
            'error': True
        }
    
    print(f"📋 Dataset encontrado: {project_id}:{dataset_info['dataset_id']}")
    
    # Configuración del proyecto central y vista consolidada
    central_project = "platform-partners-des"
    central_dataset = "bronze"  #"silver"
    central_view = "consolidated_call" #"vw_consolidated_call"
    
    print(f"\n🔐 Autorizando vista consolidada en dataset...")
    print(f"   Dataset: {project_id}:{dataset_info['dataset_id']}")
    print(f"   Vista consolidada: {central_project}:{central_dataset}.{central_view}")
    
    try:
        # Verificar si la vista ya está autorizada
        if not dry_run and check_view_authorization_exists_api(project_id, dataset_info['dataset_id'], central_project, central_dataset, central_view):
            print(f"ℹ️  La vista ya está autorizada para {company_name}, saltando")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'dataset_found': True,
                'view_authorized': True,  # Ya está autorizada, considerarlo como exitoso
                'error': False
            }
        
        # Autorizar vista en dataset via API REST
        result = authorize_view_in_dataset_api(
            project_id=project_id,
            dataset_id=dataset_info['dataset_id'],
            central_project=central_project,
            central_dataset=central_dataset,
            central_view=central_view,
            dry_run=dry_run
        )
        
        if result == "SKIP":
            print(f"ℹ️  Vista ya autorizada para {company_name}, saltando")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'dataset_found': True,
                'view_authorized': True,  # Ya está autorizada, considerarlo como exitoso
                'error': False
            }
        elif result:
            print(f"✅ Vista autorizada exitosamente para {company_name}")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'dataset_found': True,
                'view_authorized': True,
                'error': False
            }
        else:
            print(f"❌ Error al autorizar vista para {company_name}")
            return {
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id,
                'dataset_found': True,
                'view_authorized': False,
                'error': True
            }
            
    except Exception as e:
        print(f"❌ ERROR procesando autorización de vista para {company_name}: {str(e)}")
        return {
            'company_id': company_id,
            'company_name': company_name,
            'project_id': project_id,
            'dataset_found': True,
            'view_authorized': False,
            'error': True
        }


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
    
    total_datasets = 0
    total_authorizations = 0
    total_errors = 0
    
    for company in companies:
        result = process_company_view_authorization(company, dry_run=True)
        if result['dataset_found']:
            total_datasets += 1
        if result['view_authorized']:
            total_authorizations += 1
        if result['error']:
            total_errors += 1
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN GENERAL:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de datasets servicetitan_* encontrados: {total_datasets}")
    print(f"   Total de vistas a autorizar: {total_authorizations}")
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
    
    total_datasets = 0
    total_authorizations = 0
    total_errors = 0
    successful_companies = 0
    failed_companies = 0
    
    for company in companies:
        try:
            result = process_company_view_authorization(company, dry_run=False)
            if result['dataset_found']:
                total_datasets += 1
            if result['view_authorized']:
                total_authorizations += 1
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
    print(f"   Total de datasets servicetitan_* encontrados: {total_datasets}")
    print(f"   Total de vistas autorizadas: {total_authorizations}")
    print(f"   Total de errores: {total_errors}")
    print(f"{'='*80}")


def list_servicetitan_datasets_only():
    """
    Solo lista los datasets servicetitan_* encontrados sin autorizar vistas
    """
    print("📋 MODO LISTADO - Solo mostrando datasets servicetitan_* encontrados")
    print("   (Para autorizar la vista consolidada en cada dataset via API REST)")
    print("=" * 80)
    
    # Obtener empresas con proyectos
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados")
    print("=" * 80)
    
    total_datasets = 0
    
    for company in companies:
        company_id = company.company_id
        company_name = company.company_name
        project_id = company.company_project_id
        
        print(f"\n🏢 EMPRESA: {company_name}")
        print(f"   Project ID: {project_id}")
        
        # Verificar que existe el dataset servicetitan_*
        dataset_info = check_servicetitan_dataset_exists(project_id)
        
        if not dataset_info or not dataset_info['exists']:
            dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
            print(f"   ⚠️  No se encontró el dataset '{dataset_name}'")
        else:
            print(f"   📋 Dataset encontrado: {project_id}:{dataset_info['dataset_id']}")
            total_datasets += 1
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de datasets servicetitan_* encontrados: {total_datasets}")
    print(f"{'='*80}")


def main():
    """
    Función principal que permite elegir entre diferentes modos
    """
    print("🔧 SCRIPT DE AUTORIZACIÓN DE VISTA CONSOLIDADA VIA API REST")
    print("=" * 60)
    print("1. Modo LISTADO - Solo mostrar datasets servicetitan_* encontrados")
    print("2. Modo DRY-RUN - Mostrar comandos de API REST (no se ejecutarán)")
    print("3. Modo EJECUCIÓN REAL - Autorizar vista consolidada via API REST")
    print("=" * 60)
    
    choice = input("Selecciona el modo (1, 2 o 3): ").strip()
    
    if choice == "1":
        list_servicetitan_datasets_only()
    elif choice == "2":
        dry_run_mode()
    elif choice == "3":
        real_execution_mode()
    else:
        print("❌ Opción inválida. Saliendo...")
        sys.exit(1)


if __name__ == "__main__":
    main() 