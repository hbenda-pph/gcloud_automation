#!/usr/bin/env python3
"""
Script para configurar Authorized Views en BigQuery

Este script permite que una vista en un proyecto pueda acceder a recursos
de otro proyecto sin que los usuarios finales necesiten permisos directos.

Uso:
    python configure_authorized_views.py \
        --source-project PROJECT_CON_DATOS \
        --source-dataset DATASET_CON_DATOS \
        --authorized-view-project PROJECT_CON_VISTA \
        --authorized-view-dataset DATASET_CON_VISTA \
        --authorized-view-name NOMBRE_VISTA
"""

import argparse
import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'authorized_views_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AuthorizedViewManager:
    """Gestor de Authorized Views"""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.client = bigquery.Client()
    
    def configure_authorized_view(
        self, 
        source_project: str,
        source_dataset: str,
        authorized_view_project: str,
        authorized_view_dataset: str,
        authorized_view_name: str
    ) -> bool:
        """
        Configura una vista como autorizada para acceder a un dataset
        
        Args:
            source_project: Proyecto que contiene los datos (ej: platform-partners-des)
            source_dataset: Dataset con los datos (ej: settings)
            authorized_view_project: Proyecto con la vista (ej: shape-mhs-1)
            authorized_view_dataset: Dataset con la vista (ej: silver)
            authorized_view_name: Nombre de la vista (ej: vw_mi_vista)
        """
        
        print(f"\n{'='*80}")
        print(f"🔐 CONFIGURAR AUTHORIZED VIEW")
        print(f"{'='*80}")
        print(f"Proyecto con datos: {source_project}.{source_dataset}")
        print(f"Vista autorizada: {authorized_view_project}.{authorized_view_dataset}.{authorized_view_name}")
        print(f"{'='*80}\n")
        
        try:
            # Construir referencias
            source_dataset_ref = f"{source_project}.{source_dataset}"
            authorized_view_ref = {
                "projectId": authorized_view_project,
                "datasetId": authorized_view_dataset,
                "tableId": authorized_view_name
            }
            
            # Obtener el dataset actual
            logger.info(f"📋 Obteniendo dataset {source_dataset_ref}...")
            dataset = self.client.get_dataset(source_dataset_ref)
            
            # Verificar si la vista ya está autorizada
            existing_views = dataset.access_entries or []
            view_already_authorized = False
            
            for entry in existing_views:
                if hasattr(entry, 'entity_id') and entry.entity_id == authorized_view_ref:
                    view_already_authorized = True
                    break
            
            if view_already_authorized:
                logger.info(f"ℹ️  La vista ya está autorizada")
                return True
            
            if self.dry_run:
                logger.info(f"🔍 DRY-RUN: Se agregaría la vista como autorizada")
                return True
            
            # Crear nueva entrada de acceso para la vista
            logger.info(f"🚀 Agregando vista a la lista de authorized views...")
            
            # Crear nueva entrada de acceso
            access_entry = bigquery.AccessEntry(
                role=None,  # Las authorized views no tienen rol
                entity_type="view",
                entity_id=authorized_view_ref
            )
            
            # Agregar a las entradas existentes
            entries = list(dataset.access_entries)
            entries.append(access_entry)
            dataset.access_entries = entries
            
            # Actualizar el dataset
            dataset = self.client.update_dataset(dataset, ["access_entries"])
            
            logger.info(f"✅ Vista autorizada exitosamente!")
            logger.info(f"📝 Ahora la vista puede acceder a {source_dataset_ref}")
            
            return True
            
        except NotFound as e:
            logger.error(f"❌ Error: Dataset o vista no encontrado: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def list_authorized_views(self, project: str, dataset: str) -> list:
        """Lista todas las vistas autorizadas en un dataset"""
        
        try:
            dataset_ref = f"{project}.{dataset}"
            logger.info(f"📋 Obteniendo vistas autorizadas de {dataset_ref}...")
            
            dataset_obj = self.client.get_dataset(dataset_ref)
            
            print(f"\n{'='*80}")
            print(f"📋 VISTAS AUTORIZADAS EN {dataset_ref}")
            print(f"{'='*80}")
            
            authorized_views = []
            for entry in dataset_obj.access_entries:
                if hasattr(entry, 'entity_type') and entry.entity_type == 'view':
                    view_ref = entry.entity_id
                    view_full_name = f"{view_ref['projectId']}.{view_ref['datasetId']}.{view_ref['tableId']}"
                    authorized_views.append(view_full_name)
                    print(f"✓ {view_full_name}")
            
            if not authorized_views:
                print("No hay vistas autorizadas configuradas")
            
            print(f"{'='*80}\n")
            
            return authorized_views
            
        except NotFound:
            logger.error(f"❌ Dataset no encontrado: {dataset_ref}")
            return []
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return []
    
    def remove_authorized_view(
        self,
        source_project: str,
        source_dataset: str,
        authorized_view_project: str,
        authorized_view_dataset: str,
        authorized_view_name: str
    ) -> bool:
        """Remueve una vista de la lista de autorizadas"""
        
        try:
            source_dataset_ref = f"{source_project}.{source_dataset}"
            authorized_view_ref = {
                "projectId": authorized_view_project,
                "datasetId": authorized_view_dataset,
                "tableId": authorized_view_name
            }
            
            logger.info(f"🗑️  Removiendo vista autorizada de {source_dataset_ref}...")
            
            dataset = self.client.get_dataset(source_dataset_ref)
            
            # Filtrar la vista a remover
            entries = [
                entry for entry in dataset.access_entries
                if not (hasattr(entry, 'entity_id') and entry.entity_id == authorized_view_ref)
            ]
            
            if len(entries) == len(dataset.access_entries):
                logger.warning(f"⚠️  La vista no estaba en la lista de autorizadas")
                return False
            
            if self.dry_run:
                logger.info(f"🔍 DRY-RUN: Se removería la vista de las autorizadas")
                return True
            
            dataset.access_entries = entries
            dataset = self.client.update_dataset(dataset, ["access_entries"])
            
            logger.info(f"✅ Vista removida exitosamente!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description='Configura Authorized Views en BigQuery',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Configurar una vista como autorizada
  python configure_authorized_views.py \
    --action add \
    --source-project platform-partners-des \
    --source-dataset settings \
    --authorized-view-project shape-mhs-1 \
    --authorized-view-dataset silver \
    --authorized-view-name vw_mi_vista

  # Listar vistas autorizadas
  python configure_authorized_views.py \
    --action list \
    --source-project platform-partners-des \
    --source-dataset settings

  # Remover una vista autorizada
  python configure_authorized_views.py \
    --action remove \
    --source-project platform-partners-des \
    --source-dataset settings \
    --authorized-view-project shape-mhs-1 \
    --authorized-view-dataset silver \
    --authorized-view-name vw_mi_vista

  # Dry-run
  python configure_authorized_views.py \
    --action add \
    --source-project platform-partners-des \
    --source-dataset settings \
    --authorized-view-project shape-mhs-1 \
    --authorized-view-dataset silver \
    --authorized-view-name vw_mi_vista \
    --dry-run
        """
    )
    
    parser.add_argument('--action', required=True,
                       choices=['add', 'list', 'remove'],
                       help='Acción a realizar')
    parser.add_argument('--source-project',
                       help='Proyecto que contiene los datos (requerido para add/remove/list)')
    parser.add_argument('--source-dataset',
                       help='Dataset con los datos (requerido para add/remove/list)')
    parser.add_argument('--authorized-view-project',
                       help='Proyecto con la vista (requerido para add/remove)')
    parser.add_argument('--authorized-view-dataset',
                       help='Dataset con la vista (requerido para add/remove)')
    parser.add_argument('--authorized-view-name',
                       help='Nombre de la vista (requerido para add/remove)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Modo de prueba - muestra qué haría sin ejecutar cambios')
    
    args = parser.parse_args()
    
    # Validar argumentos según acción
    if args.action in ['add', 'remove']:
        if not all([args.source_project, args.source_dataset, 
                   args.authorized_view_project, args.authorized_view_dataset, 
                   args.authorized_view_name]):
            parser.error("add/remove requiere todos los parámetros de source y authorized-view")
    
    if args.action == 'list':
        if not all([args.source_project, args.source_dataset]):
            parser.error("list requiere --source-project y --source-dataset")
    
    # Crear gestor
    manager = AuthorizedViewManager(dry_run=args.dry_run)
    
    # Ejecutar acción
    try:
        if args.action == 'add':
            success = manager.configure_authorized_view(
                source_project=args.source_project,
                source_dataset=args.source_dataset,
                authorized_view_project=args.authorized_view_project,
                authorized_view_dataset=args.authorized_view_dataset,
                authorized_view_name=args.authorized_view_name
            )
            sys.exit(0 if success else 1)
        
        elif args.action == 'list':
            manager.list_authorized_views(
                project=args.source_project,
                dataset=args.source_dataset
            )
            sys.exit(0)
        
        elif args.action == 'remove':
            success = manager.remove_authorized_view(
                source_project=args.source_project,
                source_dataset=args.source_dataset,
                authorized_view_project=args.authorized_view_project,
                authorized_view_dataset=args.authorized_view_dataset,
                authorized_view_name=args.authorized_view_name
            )
            sys.exit(0 if success else 1)
    
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

