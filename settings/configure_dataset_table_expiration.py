#!/usr/bin/env python3
"""
Script para configurar el parámetro "Enable table expiration" (default_table_expiration_ms)
para cada Dataset en todos los Proyectos asociados a compañías.

Este script:
1. Obtiene todas las compañías con proyectos desde BigQuery
2. Para cada proyecto, lista todos los datasets
3. Configura el default_table_expiration_ms para cada dataset
"""
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Forbidden
import logging
import sys
from typing import List, Dict, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('configure_table_expiration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuración
PROJECT_SOURCE = "platform-partners-des"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Valor por defecto para la expiración de tablas (en milisegundos)
# Por defecto: 90 días = 90 * 24 * 60 * 60 * 1000 = 7,776,000,000 ms
# Puede ser modificado según necesidades
DEFAULT_TABLE_EXPIRATION_MS = 90 * 24 * 60 * 60 * 1000  # 90 días en milisegundos


def get_companies_with_projects() -> List[Dict]:
    """
    Obtiene todas las compañías con proyectos desde BigQuery
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
              AND company_project_id != ''
            ORDER BY company_id
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        companies = []
        for row in results:
            companies.append({
                'company_id': row.company_id,
                'company_name': row.company_name,
                'company_new_name': row.company_new_name,
                'project_id': row.company_project_id
            })
        
        logger.info(f"Se encontraron {len(companies)} compañías con proyectos")
        return companies
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo compañías desde BigQuery: {str(e)}")
        print(f"❌ ERROR al obtener empresas con proyectos: {str(e)}")
        return []


def list_datasets_in_project(project_id: str) -> List[str]:
    """
    Lista todos los datasets en un proyecto
    """
    try:
        client = bigquery.Client(project=project_id)
        datasets = list(client.list_datasets())
        dataset_ids = [dataset.dataset_id for dataset in datasets]
        logger.info(f"Se encontraron {len(dataset_ids)} datasets en proyecto {project_id}")
        return dataset_ids
    except Forbidden as e:
        logger.error(f"❌ Permisos insuficientes para proyecto {project_id}: {str(e)}")
        print(f"❌ ERROR de permisos en proyecto {project_id}: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"❌ Error listando datasets en proyecto {project_id}: {str(e)}")
        print(f"❌ ERROR listando datasets en proyecto {project_id}: {str(e)}")
        return []


def get_dataset_expiration(project_id: str, dataset_id: str) -> Optional[int]:
    """
    Obtiene el valor actual de default_table_expiration_ms de un dataset
    """
    try:
        client = bigquery.Client(project=project_id)
        dataset = client.get_dataset(f"{project_id}.{dataset_id}")
        return dataset.default_table_expiration_ms
    except NotFound:
        logger.warning(f"Dataset {project_id}.{dataset_id} no encontrado")
        return None
    except Exception as e:
        logger.error(f"❌ Error obteniendo expiración de dataset {project_id}.{dataset_id}: {str(e)}")
        return None


def configure_dataset_expiration(
    project_id: str, 
    dataset_id: str, 
    expiration_ms: int, 
    dry_run: bool = True
) -> Dict:
    """
    Configura el default_table_expiration_ms para un dataset
    
    Returns:
        Dict con 'success', 'skipped', 'current_value', 'new_value'
    """
    try:
        client = bigquery.Client(project=project_id)
        dataset_ref = f"{project_id}.{dataset_id}"
        
        # Obtener dataset actual
        try:
            dataset = client.get_dataset(dataset_ref)
        except NotFound:
            logger.warning(f"Dataset {dataset_ref} no encontrado")
            return {
                'success': False,
                'skipped': True,
                'error': 'Dataset no encontrado'
            }
        
        current_expiration = dataset.default_table_expiration_ms
        
        # Verificar si ya tiene el valor configurado
        if current_expiration == expiration_ms:
            logger.info(f"Dataset {dataset_ref} ya tiene el valor configurado: {expiration_ms} ms")
            return {
                'success': True,
                'skipped': True,
                'current_value': current_expiration,
                'new_value': expiration_ms
            }
        
        if dry_run:
            logger.info(f"DRY-RUN: Configuraría {dataset_ref} con expiration_ms={expiration_ms}")
            print(f"🔍 DRY-RUN: Configuraría {dataset_ref}")
            print(f"   Valor actual: {current_expiration} ms" if current_expiration else "   Valor actual: None")
            print(f"   Nuevo valor: {expiration_ms} ms ({expiration_ms / (24*60*60*1000):.0f} días)")
            return {
                'success': True,
                'skipped': False,
                'current_value': current_expiration,
                'new_value': expiration_ms
            }
        
        # Configurar el nuevo valor
        dataset.default_table_expiration_ms = expiration_ms
        client.update_dataset(dataset, ['default_table_expiration_ms'])
        
        logger.info(f"✅ Configurado {dataset_ref} con expiration_ms={expiration_ms}")
        print(f"✅ Configurado {dataset_ref}")
        print(f"   Valor anterior: {current_expiration} ms" if current_expiration else "   Valor anterior: None")
        print(f"   Nuevo valor: {expiration_ms} ms ({expiration_ms / (24*60*60*1000):.0f} días)")
        
        return {
            'success': True,
            'skipped': False,
            'current_value': current_expiration,
            'new_value': expiration_ms
        }
        
    except Forbidden as e:
        logger.error(f"❌ Permisos insuficientes para {project_id}.{dataset_id}: {str(e)}")
        print(f"❌ ERROR de permisos en {project_id}.{dataset_id}: {str(e)}")
        return {
            'success': False,
            'skipped': False,
            'error': f'Permisos insuficientes: {str(e)}'
        }
    except Exception as e:
        logger.error(f"❌ Error configurando expiración en {project_id}.{dataset_id}: {str(e)}")
        print(f"❌ ERROR configurando {project_id}.{dataset_id}: {str(e)}")
        return {
            'success': False,
            'skipped': False,
            'error': str(e)
        }


def process_company_datasets(
    company: Dict, 
    expiration_ms: int, 
    dry_run: bool = True
) -> Dict:
    """
    Procesa todos los datasets de una compañía
    """
    company_id = company['company_id']
    company_name = company['company_name']
    project_id = company['project_id']
    
    print(f"\n{'='*80}")
    print(f"🏢 PROCESANDO EMPRESA: {company_name}")
    print(f"   Company ID: {company_id}")
    print(f"   Project ID: {project_id}")
    print(f"{'='*80}")
    
    results = {
        'company_id': company_id,
        'company_name': company_name,
        'project_id': project_id,
        'datasets_found': 0,
        'datasets_configured': 0,
        'datasets_skipped': 0,
        'datasets_failed': 0,
        'errors': []
    }
    
    # Listar todos los datasets del proyecto
    datasets = list_datasets_in_project(project_id)
    
    if not datasets:
        print(f"⚠️  No se encontraron datasets en el proyecto {project_id}")
        results['errors'].append('No se encontraron datasets')
        return results
    
    results['datasets_found'] = len(datasets)
    print(f"📊 Se encontraron {len(datasets)} datasets en el proyecto")
    
    # Procesar cada dataset
    for dataset_id in datasets:
        print(f"\n📁 Procesando dataset: {project_id}.{dataset_id}")
        
        # Obtener valor actual
        current_expiration = get_dataset_expiration(project_id, dataset_id)
        if current_expiration is not None:
            days = current_expiration / (24*60*60*1000) if current_expiration else None
            print(f"   Expiración actual: {current_expiration} ms ({days:.0f} días)" if days else f"   Expiración actual: None")
        
        # Configurar expiración
        result = configure_dataset_expiration(
            project_id=project_id,
            dataset_id=dataset_id,
            expiration_ms=expiration_ms,
            dry_run=dry_run
        )
        
        if result['success']:
            if result.get('skipped'):
                results['datasets_skipped'] += 1
            else:
                results['datasets_configured'] += 1
        else:
            results['datasets_failed'] += 1
            if 'error' in result:
                results['errors'].append(f"{dataset_id}: {result['error']}")
    
    # Resumen de la empresa
    print(f"\n📋 RESUMEN PARA {company_name}:")
    print(f"   Datasets encontrados: {results['datasets_found']}")
    print(f"   Datasets configurados: {results['datasets_configured']}")
    print(f"   Datasets ya configurados (saltados): {results['datasets_skipped']}")
    print(f"   Datasets con errores: {results['datasets_failed']}")
    
    return results


def dry_run_mode(expiration_ms: int):
    """
    Modo de ejecución en seco - solo muestra los cambios que se harían
    """
    print("🔍 MODO DRY-RUN - Solo mostrando cambios (no se ejecutarán)")
    print("=" * 80)
    print(f"⏰ Valor de expiración a configurar: {expiration_ms} ms ({expiration_ms / (24*60*60*1000):.0f} días)")
    print("=" * 80)
    
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados\n")
    
    total_datasets = 0
    total_to_configure = 0
    total_skipped = 0
    total_failed = 0
    
    for company in companies:
        result = process_company_datasets(company, expiration_ms, dry_run=True)
        total_datasets += result['datasets_found']
        total_to_configure += result['datasets_configured']
        total_skipped += result['datasets_skipped']
        total_failed += result['datasets_failed']
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN GENERAL:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de datasets encontrados: {total_datasets}")
    print(f"   Total de datasets a configurar: {total_to_configure}")
    print(f"   Total de datasets ya configurados: {total_skipped}")
    print(f"   Total de datasets con errores: {total_failed}")
    print(f"{'='*80}")


def real_execution_mode(expiration_ms: int):
    """
    Modo de ejecución real - configura la expiración de tablas
    """
    print("🚀 MODO EJECUCIÓN REAL - Configurando expiración de tablas")
    print("⚠️  ADVERTENCIA: Esto modificará la configuración de datasets en BigQuery")
    print("=" * 80)
    print(f"⏰ Valor de expiración a configurar: {expiration_ms} ms ({expiration_ms / (24*60*60*1000):.0f} días)")
    print("=" * 80)
    
    # Confirmación del usuario
    confirm = input("¿Estás seguro de que quieres continuar? (escribe 'SI' para confirmar): ")
    if confirm != "SI":
        print("❌ Ejecución cancelada por el usuario")
        return
    
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados\n")
    
    total_datasets = 0
    total_configured = 0
    total_skipped = 0
    total_failed = 0
    successful_companies = 0
    failed_companies = 0
    
    for company in companies:
        try:
            result = process_company_datasets(company, expiration_ms, dry_run=False)
            total_datasets += result['datasets_found']
            total_configured += result['datasets_configured']
            total_skipped += result['datasets_skipped']
            total_failed += result['datasets_failed']
            
            if result['datasets_failed'] == 0:
                successful_companies += 1
            else:
                failed_companies += 1
                
        except Exception as e:
            failed_companies += 1
            total_failed += 1
            logger.error(f"❌ ERROR procesando empresa {company['company_name']}: {str(e)}")
            print(f"❌ ERROR procesando empresa {company['company_name']}: {str(e)}")
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN FINAL:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Empresas exitosas: {successful_companies}")
    print(f"   Empresas con errores: {failed_companies}")
    print(f"   Total de datasets encontrados: {total_datasets}")
    print(f"   Total de datasets configurados: {total_configured}")
    print(f"   Total de datasets ya configurados: {total_skipped}")
    print(f"   Total de datasets con errores: {total_failed}")
    print(f"{'='*80}")


def list_datasets_only():
    """
    Solo lista los datasets de cada proyecto sin configurar nada
    """
    print("📋 MODO LISTADO - Solo mostrando datasets encontrados")
    print("=" * 80)
    
    companies = get_companies_with_projects()
    
    if not companies:
        print("❌ No se encontraron empresas con proyectos asignados")
        return
    
    print(f"📋 Se encontraron {len(companies)} empresas con proyectos asignados\n")
    
    total_datasets = 0
    
    for company in companies:
        company_name = company['company_name']
        project_id = company['project_id']
        
        print(f"\n🏢 EMPRESA: {company_name}")
        print(f"   Project ID: {project_id}")
        
        datasets = list_datasets_in_project(project_id)
        
        if not datasets:
            print(f"   ⚠️  No se encontraron datasets")
        else:
            print(f"   📊 Datasets encontrados ({len(datasets)}):")
            for dataset_id in datasets:
                expiration = get_dataset_expiration(project_id, dataset_id)
                if expiration:
                    days = expiration / (24*60*60*1000)
                    print(f"      - {dataset_id} (expiración: {expiration} ms = {days:.0f} días)")
                else:
                    print(f"      - {dataset_id} (expiración: No configurada)")
            total_datasets += len(datasets)
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN:")
    print(f"   Empresas procesadas: {len(companies)}")
    print(f"   Total de datasets encontrados: {total_datasets}")
    print(f"{'='*80}")


def get_expiration_value() -> int:
    """
    Solicita al usuario el valor de expiración en días y lo convierte a milisegundos
    """
    print("\n⏰ CONFIGURACIÓN DE EXPIRACIÓN DE TABLAS")
    print("=" * 60)
    print(f"Valor por defecto: {DEFAULT_TABLE_EXPIRATION_MS / (24*60*60*1000):.0f} días")
    print("=" * 60)
    
    while True:
        try:
            days_input = input(f"Ingresa el número de días para la expiración (Enter para usar {DEFAULT_TABLE_EXPIRATION_MS / (24*60*60*1000):.0f} días): ").strip()
            
            if not days_input:
                return DEFAULT_TABLE_EXPIRATION_MS
            
            days = int(days_input)
            if days <= 0:
                print("❌ El número de días debe ser mayor a 0")
                continue
            
            expiration_ms = days * 24 * 60 * 60 * 1000
            return expiration_ms
            
        except ValueError:
            print("❌ Por favor ingresa un número válido")
        except KeyboardInterrupt:
            print("\n❌ Operación cancelada")
            sys.exit(1)


def main():
    """
    Función principal que permite elegir entre diferentes modos
    """
    print("🔧 SCRIPT DE CONFIGURACIÓN DE EXPIRACIÓN DE TABLAS EN DATASETS")
    print("=" * 80)
    print("Este script configura el parámetro 'Enable table expiration'")
    print("(default_table_expiration_ms) para cada Dataset en todos los")
    print("Proyectos asociados a compañías.")
    print("=" * 80)
    print("1. Modo LISTADO - Solo mostrar datasets encontrados")
    print("2. Modo DRY-RUN - Mostrar cambios que se harían (no se ejecutarán)")
    print("3. Modo EJECUCIÓN REAL - Configurar expiración de tablas")
    print("=" * 80)
    
    choice = input("Selecciona el modo (1, 2 o 3): ").strip()
    
    if choice == "1":
        list_datasets_only()
    elif choice == "2":
        expiration_ms = get_expiration_value()
        dry_run_mode(expiration_ms)
    elif choice == "3":
        expiration_ms = get_expiration_value()
        real_execution_mode(expiration_ms)
    else:
        print("❌ Opción inválida. Saliendo...")
        sys.exit(1)


if __name__ == "__main__":
    main()

