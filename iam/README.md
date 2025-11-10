# 🎯 Gestión de Permisos con Custom Roles

## 📖 Introducción

Sistema de gestión de permisos basado en **Custom Roles** para la plataforma de datos con arquitectura medallón.

### ✅ ¿Por qué Custom Roles?

| Aspecto | Roles Predefinidos | **Custom Roles** |
|---------|-------------------|------------------|
| **Permisos** | 2+ roles por usuario | 1 rol personalizado |
| **Mantenimiento** | Modificar cada usuario | Modificar el rol una vez |
| **Auditoría** | Múltiples bindings | Un binding claro |
| **Escalabilidad** | Baja | ⭐ Alta |
| **Flexibilidad** | Limitada | ⭐ Total |
| **Best Practice** | ❌ | ✅ |

---

## 🚀 Quick Start

### **1. Crear el Custom Role**

```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action create
```

Esto crea el rol `pphSheetsAnalyst` con todos los permisos necesarios para trabajar con Google Sheets conectadas a BigQuery.

### **2. Asignar el Role a Usuarios**

**Opción A: Con el script (para múltiples usuarios)**
```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action assign \
  --users "user1@domain.com,user2@domain.com,user3@domain.com"
```

**Opción B: Manualmente desde GCP Console** (recomendado para usuarios individuales)
1. Ir a **IAM & Admin** → **IAM**
2. Click **+ GRANT ACCESS**
3. Ingresar email del usuario
4. Buscar el rol: `pphSheetsAnalyst`
5. Guardar

**Opción C: Comando gcloud directo**
```bash
gcloud projects add-iam-policy-binding platform-partners-des \
  --member="user:analista@domain.com" \
  --role="projects/platform-partners-des/roles/pphSheetsAnalyst"
```

### **3. Verificar**

```bash
# Ver información del rol
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action describe

# Listar usuarios con el rol
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action list-users
```

---

## 📋 Permisos Incluidos en `pphSheetsAnalyst`

El rol personalizado incluye estos permisos por defecto:

### 📊 Lectura de Datos (equivalente a `bigquery.dataViewer`)
```
bigquery.datasets.get
bigquery.datasets.getIamPolicy
bigquery.models.getData
bigquery.models.getMetadata
bigquery.models.list
bigquery.routines.get
bigquery.routines.list
bigquery.tables.get
bigquery.tables.getData
bigquery.tables.list
bigquery.tables.export
```

### ⚡ Ejecución de Queries (equivalente a `bigquery.jobUser`)
```
bigquery.jobs.create
bigquery.jobs.list
bigquery.jobs.get
```

### 🔍 Otros
```
resourcemanager.projects.get
```

### 💡 Puedes agregar más fácilmente:
- `bigquery.savedqueries.*` - Para queries guardadas
- `bigquery.transfers.*` - Para data transfers
- Permisos específicos según necesidades

---

## 🔧 Operaciones Comunes

### Agregar un nuevo usuario

**Manualmente (recomendado):**
- IAM & Admin → IAM → Grant Access → `pphSheetsAnalyst`

**Con script:**
```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action assign \
  --users "nuevo.usuario@domain.com"
```

### Agregar permisos al rol

**Proceso:**
1. Editar `iam/create_custom_role_sheets_analyst.py`
2. Agregar permisos a la lista `CUSTOM_ROLE_PERMISSIONS`:

```python
CUSTOM_ROLE_PERMISSIONS = [
    # Permisos existentes...
    "bigquery.tables.getData",
    
    # 👇 NUEVOS PERMISOS
    "bigquery.savedqueries.create",
    "bigquery.savedqueries.get",
]
```

3. Actualizar el rol:
```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action update
```

✅ **Aplica automáticamente a TODOS los usuarios con el rol**

### Ver quién tiene el rol

```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action list-users
```

---

## 🎬 Workflows Completos

### **Escenario 1: Onboarding de Nuevos Analistas**

```bash
# 1. Crear el custom role (solo una vez)
python iam/create_custom_role_sheets_analyst.py --project platform-partners-des --action create

# 2. Asignar a todos los analistas actuales
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action assign \
  --users "analista1@domain.com,analista2@domain.com,analista3@domain.com"

# 3. Listar quién tiene el rol
python iam/create_custom_role_sheets_analyst.py --project platform-partners-des --action list-users
```

### **Escenario 2: Agregar Permisos a Todos los Analistas**

Imagina que ahora necesitas que los analistas también puedan:
- Crear saved queries
- Exportar a Drive

**Proceso:**

1. Editar `iam/create_custom_role_sheets_analyst.py`:

```python
CUSTOM_ROLE_PERMISSIONS = [
    # Permisos existentes...
    "bigquery.tables.getData",
    "bigquery.jobs.create",
    
    # 👇 NUEVOS PERMISOS
    "bigquery.savedqueries.create",
    "bigquery.savedqueries.get",
    "bigquery.savedqueries.list",
    "bigquery.tables.export",
]
```

2. Ejecutar actualización:

```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action update
```

✅ **¡Listo!** Todos los usuarios con el rol `pphSheetsAnalyst` ahora tienen los nuevos permisos sin tocar cada uno individualmente.

### **Escenario 3: Migrar desde Roles Predefinidos**

Si ya tienes usuarios con `bigquery.dataViewer` + `bigquery.jobUser`:

```bash
# 1. Crear el custom role
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action create

# 2. Obtener lista de usuarios actuales
gcloud projects get-iam-policy PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.role:roles/bigquery.dataViewer" \
  --format="value(bindings.members)" > users_to_migrate.txt

# 3. Asignar el nuevo role
python iam/create_custom_role_sheets_analyst.py \
  --project PROJECT_ID \
  --action assign \
  --users "$(cat users_to_migrate.txt | tr '\n' ',')"

# 4. DESPUÉS de verificar, remover roles antiguos manualmente
```

---

## 📊 Comparación: Roles Predefinidos vs Custom Roles

### **Escenario: 50 analistas usando Google Sheets**

#### Enfoque A: Roles Predefinidos
- 2 roles por usuario = **100 bindings en IAM**
- Para agregar un permiso nuevo:
  - 50 comandos para remover rol viejo
  - 50 comandos para agregar rol nuevo
  - Propenso a errores

#### Enfoque B: Custom Role ⭐
- 1 rol por usuario = **50 bindings en IAM**
- Para agregar un permiso nuevo:
  1. Editar `CUSTOM_ROLE_PERMISSIONS` en el script
  2. Un solo comando: `update`
  - ✅ Aplica a todos automáticamente

**Ganancia:**
- ✅ 50% menos bindings en IAM
- ✅ Mantenimiento 50x más rápido
- ✅ Menos propenso a errores
- ✅ Más fácil de auditar

---

## 🎨 Crear Más Custom Roles

Para diferentes perfiles de usuario, crea copias del script con diferentes configuraciones:

### Ejemplo: Data Engineer Role

```python
CUSTOM_ROLE_ID = "pphDataEngineer"
CUSTOM_ROLE_TITLE = "PPH Data Engineer"
CUSTOM_ROLE_PERMISSIONS = [
    # Lectura completa
    "bigquery.tables.getData",
    "bigquery.datasets.get",
    
    # Escritura en Bronze
    "bigquery.tables.create",
    "bigquery.tables.updateData",
    "bigquery.tables.update",
    
    # Jobs
    "bigquery.jobs.create",
    
    # Storage
    "storage.buckets.get",
    "storage.objects.create",
]
```

### Ejemplo: Gold Curator Role

```python
CUSTOM_ROLE_ID = "pphGoldCurator"
CUSTOM_ROLE_TITLE = "PPH Gold Curator"
CUSTOM_ROLE_PERMISSIONS = [
    # Leer Silver
    "bigquery.tables.getData",
    
    # Escribir Gold
    "bigquery.tables.create",
    "bigquery.tables.update",
    "bigquery.tables.updateData",
    
    # Jobs
    "bigquery.jobs.create",
]
```

### Ejemplo: Company Admin Role

```python
CUSTOM_ROLE_ID = "pphCompanyAdmin"
CUSTOM_ROLE_TITLE = "PPH Company Admin"
CUSTOM_ROLE_PERMISSIONS = [
    "bigquery.datasets.get",
    "bigquery.datasets.update",
    "bigquery.tables.create",
    "bigquery.tables.update",
    "bigquery.tables.get",
    "bigquery.tables.getData",
    "bigquery.tables.list",
    "bigquery.jobs.create",
    "bigquery.jobs.list",
]
```

---

## 🌍 Multi-Proyecto

Para aplicar en múltiples proyectos:

```powershell
# PowerShell
$projects = @(
    "platform-partners-des",
    "platform-partners-qua",
    "platform-partners-pro"
)

foreach ($project in $projects) {
    Write-Host "Creando rol en: $project"
    python iam/create_custom_role_sheets_analyst.py --project $project --action create
}
```

```bash
# Bash
for project in platform-partners-des platform-partners-qua platform-partners-pro; do
    echo "Creando rol en: $project"
    python iam/create_custom_role_sheets_analyst.py --project $project --action create
done
```

---

## 🔍 Auditoría y Mantenimiento

### Ver todos los custom roles del proyecto
```bash
gcloud iam roles list --project=platform-partners-des --format="table(name,title,stage)"
```

### Ver permisos de un usuario específico
```bash
gcloud projects get-iam-policy platform-partners-des \
  --flatten="bindings[].members" \
  --filter="bindings.members:user:EMAIL@domain.com" \
  --format="table(bindings.role)"
```

### Auditar quién tiene qué rol
```bash
# Listar usuarios con el custom role
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action list-users

# O directamente con gcloud
gcloud projects get-iam-policy platform-partners-des \
  --flatten="bindings[].members" \
  --filter="bindings.role:projects/platform-partners-des/roles/pphSheetsAnalyst" \
  --format="value(bindings.members)"
```

### Exportar política IAM completa
```bash
gcloud projects get-iam-policy platform-partners-des \
  --format=json > iam_backup_$(date +%Y%m%d).json
```

---

## 📋 Requisitos

### Dependencias Python
```bash
pip install google-cloud-bigquery google-cloud-resource-manager
```

### Autenticación
```bash
gcloud auth application-default login
```

### Permisos Necesarios
Tu cuenta debe tener uno de estos roles:
- `roles/owner`
- `roles/iam.securityAdmin`
- `roles/resourcemanager.projectIamAdmin`

---

## ⚠️ Testing

Siempre prueba con `--dry-run` primero:

```bash
python iam/create_custom_role_sheets_analyst.py \
  --project platform-partners-des \
  --action create \
  --dry-run
```

---

## 🚨 Troubleshooting

### Error: "Permission denied"
**Solución:** Verifica que tienes permisos de IAM Admin
```bash
gcloud projects get-iam-policy platform-partners-des \
  --flatten="bindings[].members" \
  --filter="bindings.members:$(gcloud config get-value account)"
```

### Error: "Role already exists"
**Solución:** Usa `--action update` en lugar de `create`

### Usuario no puede acceder después de asignar rol
**Posibles causas:**
1. Los permisos tardan 1-5 minutos en propagarse
2. El usuario debe cerrar sesión y volver a entrar
3. Verifica que el rol fue asignado correctamente

---

## 📚 Comandos de Referencia

```bash
# Crear rol
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action create

# Ver info del rol
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action describe

# Actualizar rol
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action update

# Asignar a usuarios
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action assign --users "email1,email2"

# Listar usuarios con el rol
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action list-users

# Dry-run (cualquier acción)
python iam/create_custom_role_sheets_analyst.py --project PROJECT_ID --action ACTION --dry-run
```

---

## 🎯 Roadmap de Roles

### Roles Sugeridos para tu Plataforma

1. ✅ **`pphSheetsAnalyst`** - Analistas con Google Sheets
2. 🔜 **`pphDataEngineer`** - Ingenieros de datos
3. 🔜 **`pphBronzeEditor`** - Escritura en capa Bronze
4. 🔜 **`pphSilverEditor`** - Escritura en capa Silver
5. 🔜 **`pphGoldCurator`** - Curación de capa Gold
6. 🔜 **`pphCompanyAdmin`** - Administradores de compañía tenant
7. 🔜 **`pphSandboxUser`** - Usuarios temporales en Sandbox

---

## 💡 Próximos Pasos

1. ✅ Ejecuta el script para crear `pphSheetsAnalyst`
2. ✅ Asigna a tus usuarios actuales
3. ✅ Verifica que funciona
4. ⭐ Crea más custom roles según necesites
5. ⭐ Documenta tus roles en tu wiki interna

---

## 🔗 Referencias

- [GCP Custom Roles](https://cloud.google.com/iam/docs/creating-custom-roles)
- [BigQuery IAM](https://cloud.google.com/bigquery/docs/access-control)
- [IAM Best Practices](https://cloud.google.com/iam/docs/best-practices)
