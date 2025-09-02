#!/usr/bin/env python3
"""
Script para crear proyectos GCP para compañías, leyendo datos desde una tabla de BigQuery.
Se eliminan las secciones de creación de cuentas de servicio y de recursos BigQuery.
"""
from google.cloud import bigquery
from google.cloud import resourcemanager_v3
from google.cloud import service_usage_v1
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
        display_name = project_id[:30]  # GCP limita a 30 caracteres
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
            operation = client.create_project(request={"project": project})
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
        client = service_usage_v1.ServiceUsageClient()
        success = True
        for service in services:
            service_name = f"{service}.googleapis.com"
            request = service_usage_v1.EnableServiceRequest(
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

# Ejemplo de uso:
# python create_gcp_projects_cursor.py --source-project=platform-partners-des --dataset=settings --table=companies
