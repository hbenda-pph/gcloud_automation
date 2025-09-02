#!/usr/bin/env python3
"""
Auditoría de proyectos GCP según la tabla companies.
Verifica:
- Existencia del proyecto
- Activación de la API de BigQuery
- Existencia de los 6 datasets
- Existencia de la cuenta de servicio y su rol
"""
from google.cloud import bigquery
from googleapiclient import discovery
from google.api_core.exceptions import NotFound
import google.auth
import os

PROJECT_SOURCE = "platform-partners-des"
DATASET_NAME = "settings"
TABLE_NAME = "companies"
REQUIRED_DATASETS = ["settings", "fivetran", "bronze", "silver", "gold", "management"]
SERVICE_ACCOUNT_NAME = "fivetran-account-service"
SERVICE_ACCOUNT_ROLE = "roles/bigquery.admin"


def get_companies_to_audit():
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f"""
        SELECT company_id, company_name, company_new_name, company_project_id
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        ORDER BY company_id
    """
    results = client.query(query).result()
    companies = []
    for row in results:
        # Considerar nulo o vacío
        if not row.company_project_id or str(row.company_project_id).strip() == "":
            companies.append({
                "company_id": row.company_id,
                "company_name": row.company_name,
                "company_new_name": row.company_new_name,
                "project_id": None  # No hay project_id registrado
            })
        else:
            companies.append({
                "company_id": row.company_id,
                "company_name": row.company_name,
                "company_new_name": row.company_new_name,
                "project_id": row.company_project_id
            })
    return companies


def project_exists(project_id, crm_service):
    try:
        project = crm_service.projects().get(projectId=project_id).execute()
        return project.get("lifecycleState", "") == "ACTIVE"
    except Exception:
        return False


def bigquery_api_enabled(project_id, serviceusage):
    try:
        resp = serviceusage.services().get(
            name=f"projects/{project_id}/services/bigquery.googleapis.com"
        ).execute()
        return resp.get("state", "") == "ENABLED"
    except Exception:
        return False


def dataset_exists(project_id, dataset_id, bq_client):
    try:
        bq_client.get_dataset(f"{project_id}.{dataset_id}")
        return True
    except NotFound:
        return False
    except Exception:
        return False


def service_account_exists(project_id, sa_name, iam_service):
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    try:
        sa = iam_service.projects().serviceAccounts().get(
            name=f"projects/{project_id}/serviceAccounts/{sa_email}"
        ).execute()
        return True
    except Exception:
        return False


def service_account_has_role(project_id, sa_name, role, crm_service):
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    try:
        policy = crm_service.projects().getIamPolicy(
            resource=project_id, body={}
        ).execute()
        bindings = policy.get("bindings", [])
        for binding in bindings:
            if binding["role"] == role and f"serviceAccount:{sa_email}" in binding.get("members", []):
                return True
        return False
    except Exception:
        return False


def main():
    print("Auditoría de proyectos GCP según tabla companies\n" + "="*60)
    companies = get_companies_to_audit()
    credentials, _ = google.auth.default()
    crm_service = discovery.build("cloudresourcemanager", "v1", credentials=credentials)
    serviceusage = discovery.build("serviceusage", "v1", credentials=credentials)
    iam_service = discovery.build("iam", "v1", credentials=credentials)
    bq_client = bigquery.Client()

    for company in companies:
        project_id = company["project_id"]
        if not project_id:
            print(f"❌ [{company['company_id']}] {company['company_name']} - Sin project_id registrado en la tabla.")
            print("-"*50)
            continue
        print(f"🔎 [{company['company_id']}] {company['company_name']} (project_id: {project_id})")
        missing = []
        # 1. Proyecto
        if not project_exists(project_id, crm_service):
            missing.append("Proyecto NO existe")
        else:
            # 2. API BigQuery
            if not bigquery_api_enabled(project_id, serviceusage):
                missing.append("API BigQuery NO habilitada")
            # 3. Datasets
            for ds in REQUIRED_DATASETS:
                if not dataset_exists(project_id, ds, bq_client):
                    missing.append(f"Dataset '{ds}' NO existe")
            # 4. Cuenta de servicio
            if not service_account_exists(project_id, SERVICE_ACCOUNT_NAME, iam_service):
                missing.append("Cuenta de servicio Fivetran NO existe")
            else:
                # 5. Rol
                if not service_account_has_role(project_id, SERVICE_ACCOUNT_NAME, SERVICE_ACCOUNT_ROLE, crm_service):
                    missing.append("Cuenta de servicio Fivetran SIN rol bigquery.admin")
        if missing:
            print("  Faltantes:")
            for m in missing:
                print(f"   - {m}")
        else:
            print("  ✅ Todo OK")
        print("-"*50)

if __name__ == "__main__":
    main() 
