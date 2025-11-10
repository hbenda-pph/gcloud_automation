#!/usr/bin/env python3
"""
Script para crear y gestionar Custom Roles para usuarios de Google Sheets

Este script crea un rol personalizado que agrupa los permisos necesarios
para trabajar con Google Sheets conectadas a BigQuery, y opcionalmente
puede asignar ese rol a usuarios.

Ventajas de Custom Roles:
- Un solo rol agrupa todos los permisos necesarios
- Fácil de mantener: modificas el rol una vez, aplica a todos
- Mejor para auditoría: roles con nombres descriptivos
- Escalable: agregar permisos sin tocar cada usuario

Uso:
    # 1. Crear el custom role
    python create_custom_role_sheets_analyst.py --project PROJECT_ID --action create
    
    # 2. Ver el custom role
    python create_custom_role_sheets_analyst.py --project PROJECT_ID --action describe
    
    # 3. Actualizar el custom role (agregar más permisos)
    python create_custom_role_sheets_analyst.py --project PROJECT_ID --action update
    
    # 4. Asignar el role a usuarios
    python create_custom_role_sheets_analyst.py --project PROJECT_ID --action assign --users "user1@domain.com,user2@domain.com"
    
    # 5. Listar usuarios con el role
    python create_custom_role_sheets_analyst.py --project PROJECT_ID --action list-users
"""

import argparse
import sys
import subprocess
import json
from typing import List, Dict, Optional
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'custom_role_management_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Definición del Custom Role
CUSTOM_ROLE_ID = "pphSheetsAnalyst"
CUSTOM_ROLE_TITLE = "PPH Sheets Analyst"
CUSTOM_ROLE_DESCRIPTION = "Rol personalizado para analistas que trabajan con Google Sheets conectadas a BigQuery"

# Permisos incluidos en el rol (puedes expandir esto fácilmente)
CUSTOM_ROLE_PERMISSIONS = [
    # Permisos para leer datos (equivalente a dataViewer)
    "bigquery.datasets.get",
    "bigquery.datasets.getIamPolicy",
    "bigquery.models.getData",
    "bigquery.models.getMetadata",
    "bigquery.models.list",
    "bigquery.routines.get",
    "bigquery.routines.list",
    "bigquery.tables.get",
    "bigquery.tables.getData",
    "bigquery.tables.list",
    "bigquery.tables.export",
    
    # Permisos para ejecutar queries (equivalente a jobUser)
    "bigquery.jobs.create",
    "bigquery.jobs.list",
    "bigquery.jobs.get",
    
    # Permisos adicionales útiles para Google Sheets
    "resourcemanager.projects.get",
    
    # Puedes agregar más permisos aquí según necesites
    # Por ejemplo, para saved queries:
    # "bigquery.savedqueries.get",
    # "bigquery.savedqueries.list",
]

class CustomRoleManager:
    """Gestor de Custom Roles"""
    
    def __init__(self, project_id: str, dry_run: bool = False):
        self.project_id = project_id
        self.dry_run = dry_run
        self.role_name = f"projects/{project_id}/roles/{CUSTOM_ROLE_ID}"
        
    def role_exists(self) -> bool:
        """Verifica si el custom role ya existe"""
        try:
            cmd = f"gcloud iam roles describe {CUSTOM_ROLE_ID} --project={self.project_id}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Error verificando existencia del rol: {str(e)}")
            return False
    
    def create_role(self) -> bool:
        """Crea el custom role"""
        
        if self.role_exists():
            logger.warning(f"⚠️  El rol '{CUSTOM_ROLE_ID}' ya existe en el proyecto {self.project_id}")
            logger.info(f"💡 Usa --action update para modificarlo")
            return False
        
        print(f"\n{'='*80}")
        print(f"🎯 CREAR CUSTOM ROLE")
        print(f"{'='*80}")
        print(f"Role ID: {CUSTOM_ROLE_ID}")
        print(f"Título: {CUSTOM_ROLE_TITLE}")
        print(f"Proyecto: {self.project_id}")
        print(f"\n📋 Permisos incluidos ({len(CUSTOM_ROLE_PERMISSIONS)}):")
        for perm in CUSTOM_ROLE_PERMISSIONS:
            print(f"  ✓ {perm}")
        print(f"{'='*80}\n")
        
        # Crear archivo temporal con la configuración del role
        role_config = {
            "title": CUSTOM_ROLE_TITLE,
            "description": CUSTOM_ROLE_DESCRIPTION,
            "stage": "GA",
            "includedPermissions": CUSTOM_ROLE_PERMISSIONS
        }
        
        # Guardar configuración temporal
        config_file = f"role_config_{CUSTOM_ROLE_ID}.yaml"
        yaml_content = f"""title: "{CUSTOM_ROLE_TITLE}"
description: "{CUSTOM_ROLE_DESCRIPTION}"
stage: "GA"
includedPermissions:
"""
        for perm in CUSTOM_ROLE_PERMISSIONS:
            yaml_content += f"- {perm}\n"
        
        try:
            with open(config_file, 'w') as f:
                f.write(yaml_content)
            
            cmd = f"gcloud iam roles create {CUSTOM_ROLE_ID} --project={self.project_id} --file={config_file}"
            
            if self.dry_run:
                logger.info(f"🔍 DRY-RUN: {cmd}")
                logger.info(f"🔍 Archivo de configuración: {config_file}")
                return True
            else:
                logger.info(f"🚀 Creando custom role...")
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                # Limpiar archivo temporal
                import os
                os.remove(config_file)
                
                if result.returncode == 0:
                    logger.info(f"✅ Custom role '{CUSTOM_ROLE_ID}' creado exitosamente!")
                    logger.info(f"📝 Nombre completo: {self.role_name}")
                    return True
                else:
                    logger.error(f"❌ Error creando rol: {result.stderr}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return False
    
    def describe_role(self) -> bool:
        """Muestra información del custom role"""
        
        if not self.role_exists():
            logger.error(f"❌ El rol '{CUSTOM_ROLE_ID}' no existe")
            logger.info(f"💡 Usa --action create para crearlo")
            return False
        
        try:
            cmd = f"gcloud iam roles describe {CUSTOM_ROLE_ID} --project={self.project_id}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"\n{'='*80}")
                print(f"📋 INFORMACIÓN DEL CUSTOM ROLE")
                print(f"{'='*80}")
                print(result.stdout)
                print(f"{'='*80}\n")
                return True
            else:
                logger.error(f"❌ Error: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return False
    
    def update_role(self, new_permissions: Optional[List[str]] = None) -> bool:
        """Actualiza el custom role con nuevos permisos"""
        
        if not self.role_exists():
            logger.error(f"❌ El rol '{CUSTOM_ROLE_ID}' no existe")
            logger.info(f"💡 Usa --action create para crearlo primero")
            return False
        
        # Si no se pasan nuevos permisos, usar los del archivo
        permissions_to_use = new_permissions if new_permissions else CUSTOM_ROLE_PERMISSIONS
        
        print(f"\n{'='*80}")
        print(f"🔄 ACTUALIZAR CUSTOM ROLE")
        print(f"{'='*80}")
        print(f"Role ID: {CUSTOM_ROLE_ID}")
        print(f"\n📋 Permisos a incluir ({len(permissions_to_use)}):")
        for perm in permissions_to_use:
            print(f"  ✓ {perm}")
        print(f"{'='*80}\n")
        
        # Crear archivo temporal con la configuración actualizada
        config_file = f"role_config_{CUSTOM_ROLE_ID}_update.yaml"
        yaml_content = f"""title: "{CUSTOM_ROLE_TITLE}"
description: "{CUSTOM_ROLE_DESCRIPTION}"
stage: "GA"
includedPermissions:
"""
        for perm in permissions_to_use:
            yaml_content += f"- {perm}\n"
        
        try:
            with open(config_file, 'w') as f:
                f.write(yaml_content)
            
            cmd = f"gcloud iam roles update {CUSTOM_ROLE_ID} --project={self.project_id} --file={config_file}"
            
            if self.dry_run:
                logger.info(f"🔍 DRY-RUN: {cmd}")
                return True
            else:
                logger.info(f"🚀 Actualizando custom role...")
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                # Limpiar archivo temporal
                import os
                os.remove(config_file)
                
                if result.returncode == 0:
                    logger.info(f"✅ Custom role '{CUSTOM_ROLE_ID}' actualizado exitosamente!")
                    return True
                else:
                    logger.error(f"❌ Error actualizando rol: {result.stderr}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return False
    
    def assign_role_to_user(self, email: str) -> bool:
        """Asigna el custom role a un usuario"""
        
        if not self.role_exists():
            logger.error(f"❌ El rol '{CUSTOM_ROLE_ID}' no existe")
            logger.info(f"💡 Usa --action create para crearlo primero")
            return False
        
        try:
            member = f"user:{email}"
            cmd = f"gcloud projects add-iam-policy-binding {self.project_id} --member='{member}' --role='{self.role_name}' --condition=None"
            
            if self.dry_run:
                logger.info(f"🔍 DRY-RUN: {cmd}")
                return True
            else:
                logger.info(f"🚀 Asignando rol '{CUSTOM_ROLE_ID}' a {email}...")
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if result.returncode == 0:
                    logger.info(f"✅ Rol asignado exitosamente a {email}")
                    return True
                else:
                    logger.error(f"❌ Error asignando rol a {email}: {result.stderr}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return False
    
    def assign_role_to_users(self, users: List[str]) -> Dict:
        """Asigna el custom role a múltiples usuarios"""
        
        results = {'success': [], 'failed': []}
        
        print(f"\n{'='*80}")
        print(f"👥 ASIGNAR ROLE A USUARIOS")
        print(f"{'='*80}")
        print(f"Role: {CUSTOM_ROLE_ID}")
        print(f"Usuarios: {len(users)}")
        print(f"{'='*80}\n")
        
        for idx, email in enumerate(users, 1):
            email = email.strip()
            logger.info(f"[{idx}/{len(users)}] Procesando: {email}")
            
            if self.assign_role_to_user(email):
                results['success'].append(email)
            else:
                results['failed'].append(email)
        
        # Resumen
        print(f"\n{'='*80}")
        print(f"📊 RESUMEN")
        print(f"{'='*80}")
        print(f"✅ Exitosos: {len(results['success'])}/{len(users)}")
        for email in results['success']:
            print(f"   - {email}")
        
        print(f"\n❌ Fallidos: {len(results['failed'])}/{len(users)}")
        for email in results['failed']:
            print(f"   - {email}")
        print(f"{'='*80}\n")
        
        return results
    
    def list_users_with_role(self) -> List[str]:
        """Lista usuarios que tienen el custom role asignado"""
        
        if not self.role_exists():
            logger.error(f"❌ El rol '{CUSTOM_ROLE_ID}' no existe")
            return []
        
        try:
            cmd = f"gcloud projects get-iam-policy {self.project_id} --flatten='bindings[].members' --filter='bindings.role:{self.role_name}' --format='value(bindings.members)'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                users = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
                
                print(f"\n{'='*80}")
                print(f"👥 USUARIOS CON EL ROL '{CUSTOM_ROLE_ID}'")
                print(f"{'='*80}")
                if users:
                    for idx, user in enumerate(users, 1):
                        print(f"{idx}. {user}")
                else:
                    print("No hay usuarios con este rol asignado")
                print(f"{'='*80}\n")
                
                return users
            else:
                logger.error(f"❌ Error: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return []


def main():
    parser = argparse.ArgumentParser(
        description='Gestiona Custom Roles para usuarios de Google Sheets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Crear el custom role
  python create_custom_role_sheets_analyst.py --project platform-partners-des --action create

  # Ver información del role
  python create_custom_role_sheets_analyst.py --project platform-partners-des --action describe

  # Actualizar el role (después de modificar CUSTOM_ROLE_PERMISSIONS en el código)
  python create_custom_role_sheets_analyst.py --project platform-partners-des --action update

  # Asignar el role a usuarios
  python create_custom_role_sheets_analyst.py --project platform-partners-des --action assign --users "user1@domain.com,user2@domain.com"

  # Listar usuarios con el role
  python create_custom_role_sheets_analyst.py --project platform-partners-des --action list-users

  # Dry-run
  python create_custom_role_sheets_analyst.py --project platform-partners-des --action create --dry-run
        """
    )
    
    parser.add_argument('--project', required=True, help='ID del proyecto GCP')
    parser.add_argument('--action', required=True,
                       choices=['create', 'describe', 'update', 'assign', 'list-users'],
                       help='Acción a realizar')
    parser.add_argument('--users', help='Lista de usuarios separados por coma (requerido si action=assign)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Modo de prueba - muestra qué haría sin ejecutar cambios')
    
    args = parser.parse_args()
    
    # Validar argumentos
    if args.action == 'assign' and not args.users:
        parser.error("--users es requerido cuando action=assign")
    
    # Crear gestor
    manager = CustomRoleManager(
        project_id=args.project,
        dry_run=args.dry_run
    )
    
    # Ejecutar acción
    try:
        if args.action == 'create':
            success = manager.create_role()
            sys.exit(0 if success else 1)
        
        elif args.action == 'describe':
            success = manager.describe_role()
            sys.exit(0 if success else 1)
        
        elif args.action == 'update':
            success = manager.update_role()
            sys.exit(0 if success else 1)
        
        elif args.action == 'assign':
            users = [u.strip() for u in args.users.split(',') if u.strip()]
            if not users:
                logger.error("❌ No se proporcionaron usuarios válidos")
                sys.exit(1)
            results = manager.assign_role_to_users(users)
            sys.exit(0 if len(results['failed']) == 0 else 1)
        
        elif args.action == 'list-users':
            manager.list_users_with_role()
            sys.exit(0)
    
    except KeyboardInterrupt:
        print("\n\n❌ Operación interrumpida por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

