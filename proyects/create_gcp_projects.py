#!/usr/bin/env python3
"""
Script mejorado para crear proyectos GCP, recursos BigQuery y cuentas de servicio para Fivetran
"""
from google.api_core.client_options import ClientOptions
from google.cloud import resourcemanager_v3
from google.cloud import bigquery
from google.cloud import serviceusage_v1
from google.cloud import iam
from google.api_core.exceptions import AlreadyExists, PermissionDenied
import argparse
import logging
import time
from typing import Dict, Any, List, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gcp_project_creator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GCPProjectCreator:
    def __init__(self, source_project: str, dataset_name: str, table_name: str):
        self.source_project = source_project
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.bq_client = bigquery.Client(project=source_project)
        
    def get_companies_data(self) -> List[Dict[str, Any]]:
        """Obtiene los datos de compañías desde BigQuery"""
        query = f"""
            SELECT 
                company_id, 
                company_name, 
                company_new_name
            FROM `{self.source_project}.{self.dataset_name}.{self.table_name}`
            ORDER BY company_id
        """
        
        try:
            logger.info(f"Obteniendo datos de compañías desde {self.source_project}.{self.dataset_name}.{self.table_name}")
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            companies = []
            for row in results:
                companies.append({
                    "id": row.company_id,
                    "name": row.company_name,
                    "new_name": row.company_new_name
                })
            
            logger.info(f"Encontradas {len(companies)} compañías activas")
            return companies
            
        except Exception as e:
            logger.error(f"Error al obtener datos de BigQuery: {str(e)}")
            raise
            
    def create_gcp_project(self, company: Dict[str, Any]) -> Optional[str]:
        """Crea un proyecto GCP para una compañía"""
        project_id = f"{company['new_name'].replace(' ', '-').lower()}"
        display_name = project_id
        
        client = resourcemanager_v3.ProjectsClient()
        project = resourcemanager_v3.Project(
            project_id=project_id,
            display_name=display_name,
            labels={
                "company-id": str(company['id']),
                "environment": "production",
                "data-platform": "bigquery"
            }
        )
        
        try:
            logger.info(f"Creando proyecto {project_id} para {company['name']}")
            operation = client.create_project(
                request={"project": project}
            )
            
            while not operation.done():
                time.sleep(5)
                logger.info("Esperando creación de proyecto...")
            
            if operation.result().project_id == project_id:
                logger.info(f"Proyecto {project_id} creado exitosamente")
                return project_id
                
        except AlreadyExists:
            logger.warning(f"Proyecto {project_id} ya existe")
            return project_id
        except PermissionDenied:
            logger.error(f"No tienes permisos para crear proyectos en esta organización")
            return None
        except Exception as e:
            logger.error(f"Error creando proyecto {project_id}: {str(e)}")
            return None
            
    def enable_services(self, project_id: str, services: List[str]) -> bool:
        """Habilita servicios en un proyecto GCP"""
        client = serviceusage_v1.ServiceUsageClient()
        success = True
        
        for service in services:
            service_name = f"{service}.googleapis.com"
            request = serviceusage_v1.EnableServiceRequest(
                name=f"projects/{project_id}/services/{service_name}"
            )
            
            try:
                logger.info(f"Habilitando servicio {service_name} en {project_id}")
                operation = client.enable_service(request=request)
                operation.result()
                logger.info(f"Servicio {service_name} habilitado")
            except Exception as e:
                logger.error(f"Error habilitando {service_name}: {str(e)}")
                success = False
                
        return success
        
    def create_service_account(self, project_id: str) -> Optional[str]:
        """Crea una cuenta de servicio para Fivetran en el proyecto"""
        client = iam.IAMClient()
        service_account_id = "fivetran-service-account"
        service_account_email = f"{service_account_id}@{project_id}.iam.gserviceaccount.com"
        
        try:
            logger.info(f"Creando cuenta de servicio {service_account_email}")
            
            service_account = client.create_service_account(
                name=f"projects/{project_id}",
                service_account_id=service_account_id,
                service_account={
                    "display_name": "Fivetran Service Account",
                    "description": "Cuenta de servicio para integración con Fivetran"
                }
            )
            
            logger.info(f"Cuenta de servicio creada: {service_account.email}")
            return service_account.email
            
        except AlreadyExists:
            logger.warning(f"La cuenta de servicio {service_account_email} ya existe")
            return service_account_email
        except Exception as e:
            logger.error(f"Error creando cuenta de servicio: {str(e)}")
            return None
            
    def assign_roles_to_service_account(self, project_id: str, service_account_email: str) -> bool:
        """Asigna roles a la cuenta de servicio de Fivetran"""
        from google.cloud import resourcemanager_v3 as rm
        
        required_roles = [
            "roles/bigquery.admin",
            "roles/bigquery.connectionUser",
            "roles/bigquery.dataEditor",
            "roles/bigquery.jobUser"
        ]
        
        client = rm.ProjectsClient()
        success = True
        
        for role in required_roles:
            try:
                policy = client.get_iam_policy(
                    request={
                        "resource": f"projects/{project_id}",
                        "options": {"requested_policy_version": 3}
                    }
                )
                
                # Verificar si el binding ya existe
                binding_exists = False
                for binding in policy.bindings:
                    if binding.role == role and f"serviceAccount:{service_account_email}" in binding.members:
                        binding_exists = True
                        break
                
                if not binding_exists:
                    # Crear nuevo binding si no existe
                    new_binding = rm.Binding()
                    new_binding.role = role
                    new_binding.members = [f"serviceAccount:{service_account_email}"]
                    policy.bindings.append(new_binding)
                    
                    # Actualizar política
                    client.set_iam_policy(
                        request={
                            "resource": f"projects/{project_id}",
                            "policy": policy
                        }
                    )
                    logger.info(f"Rol {role} asignado a {service_account_email}")
                
            except Exception as e:
                logger.error(f"Error asignando rol {role}: {str(e)}")
                success = False
                
        return success
        
    def setup_bigquery(self, project_id: str, company: Dict[str, Any]) -> bool:
        """Configura los recursos iniciales de BigQuery"""
        try:
            client = bigquery.Client(project=project_id)
            
            # Crear dataset principal
            dataset_id = f"fivetran"
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = company.get('region', 'US')
            dataset.description = f"Dataset Fivetran para {company['name']}"
            dataset.labels = {
                "company-id": str(company['id']),
                "data-tier": "fivetran"
            }
            
            logger.info(f"Creando dataset {dataset_id} en {project_id}")
            client.create_dataset(dataset)
            
            # Crear datasets adicionales
            additional_datasets = [
                {"id": f"bronze", "description": "Bronze"},
                {"id": f"silver", "description": "Silver"},
                {"id": f"gold", "description": "Gold"},
                {"id": f"management", "description": "Management"},
                {"id": f"settings", "description": "Settings"}
            ]
            
            for ds in additional_datasets:
                new_dataset = bigquery.Dataset(f"{project_id}.{ds['id']}")
                new_dataset.location = company.get('region', 'US')
                new_dataset.description = ds['description']
                client.create_dataset(new_dataset)
                logger.info(f"Dataset {ds['id']} creado")
                
            return True
            
        except Exception as e:
            logger.error(f"Error configurando BigQuery: {str(e)}")
            return False
            
    def run(self):
        """Ejecuta el proceso completo"""
        try:
            companies = self.get_companies_data()
            
            required_services = [
                "bigquery",
                "bigqueryconnection",
                "storage",
                "iam"
            ]
            
            for company in companies:
                logger.info(f"\nProcesando compañía: {company['name']} (ID: {company['id']})")
                
                # 1. Crear proyecto
                project_id = self.create_gcp_project(company)
                if not project_id:
                    continue
                
                # 2. Habilitar servicios
                if not self.enable_services(project_id, required_services):
                    logger.warning(f"Algunos servicios no se habilitaron en {project_id}")
                
                # 3. Crear cuenta de servicio para Fivetran
                sa_email = self.create_service_account(project_id)
                if sa_email:
                    # 4. Asignar roles a la cuenta de servicio
                    if not self.assign_roles_to_service_account(project_id, sa_email):
                        logger.warning(f"Problemas asignando roles a la cuenta de servicio en {project_id}")
                
                # 5. Configurar BigQuery
                if not self.setup_bigquery(project_id, company):
                    logger.warning(f"Problemas configurando BigQuery en {project_id}")
                
                logger.info(f"Configuración completada para {company['name']}\n")
                
        except Exception as e:
            logger.error(f"Error en el proceso principal: {str(e)}")
        finally:
            logger.info("Proceso de creación de proyectos completado")

def main():
    parser = argparse.ArgumentParser(
        description="Automatiza la creación de proyectos GCP para compañías"
    )
    parser.add_argument(
        "--source-project",
        required=True,
        help="ID del proyecto GCP con la tabla de compañías"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Nombre del dataset de BigQuery"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Nombre de la tabla con datos de compañías"
    )
    
    args = parser.parse_args()
    
    creator = GCPProjectCreator(
        source_project=args.source_project,
        dataset_name=args.dataset,
        table_name=args.table
    )
    creator.run()

if __name__ == "__main__":
    main()


#python create_gcp_projects.py --source-project=platform-partners-des --dataset=settings --table=companies
