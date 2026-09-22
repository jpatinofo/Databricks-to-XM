# Gobierno: catálogos, grupos y permisos

Quién puede ver y hacer qué, escrito una sola vez. Lo que está aquí es lo que está en Unity Catalog; si difieren, el catálogo tiene la razón y este archivo se corrige.

## Catálogos (ambientes)

| Catálogo | Propósito | Quién escribe | Quién lee |
|---|---|---|---|
| `dev` | Desarrollo: ensayar pipelines, features y modelos sobre datos reales | `grp_ingenieria` (bronce, plata), `grp_analitica` (oro, modelos) | `grp_ingenieria`, `grp_analitica` |
| `qa` | Ejecutar el pipeline completo antes de promover; comparar contra prod | `sp_pipeline` únicamente | `grp_ingenieria`, `grp_analitica` |
| `prod` | Datos oficiales que consume negocio y el endpoint | `sp_pipeline` únicamente | `grp_ingenieria`, `grp_analitica`; `grp_negocio` solo `gold_energia` |

## Esquemas por capa

| Esquema | Contenido | Owner (grupo) |
|---|---|---|
| `bronze_energia` | Archivos de XM tal como llegan, append-only, con `_ingested_at`, `_source_file`, `_publication_date`; volumen `landing` | `grp_ingenieria` |
| `silver_energia` | Una fila por serie-día con `demanda_real_kwh` y `perdidas_kwh`, validada; tabla de cuarentena | `grp_ingenieria` |
| `gold_energia` | `features_demanda_diaria`, `pronostico_demanda`, `desempeno_modelo` y vistas para negocio | `grp_analitica` |
| `models` | Modelos registrados en MLflow con alias `champion` / `challenger` | `grp_analitica` |

## Grupos y roles

| Grupo | Quiénes (rol en XM) | Necesita |
|---|---|---|
| `grp_ingenieria` | Ingeniería de datos: ingesta, plata, calidad, operación del pipeline | Escribir bronce y plata en dev; leer todo en qa y prod |
| `grp_analitica` | Científicos y analistas: features, modelos, evaluación | Escribir oro y modelos en dev; leer todo en qa y prod; ejecutar modelos |
| `grp_negocio` | Planeación y operación: consumen el pronóstico | Leer vistas de `prod.gold_energia`; nada más |
| `sp_pipeline` | Service principal que ejecuta jobs y pipelines (clase 13) | Escribir en qa y prod; ninguna persona usa su identidad |

## Matriz de privilegios

Una celda = los privilegios del grupo sobre ese esquema. `NA` si no entra. Todos los grupos tienen `USE CATALOG` y `USE SCHEMA` donde aparece algo distinto de `NA`.

| Grupo | dev.bronze / silver | dev.gold | dev.models | qa.* | prod.bronze / silver | prod.gold | prod.models |
|---|---|---|---|---|---|---|---|
| `grp_ingenieria` | SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION, READ/WRITE VOLUME | SELECT | SELECT | SELECT (EXECUTE en models) | SELECT | SELECT | SELECT, EXECUTE |
| `grp_analitica` | SELECT | SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION | SELECT, EXECUTE, CREATE MODEL | SELECT (EXECUTE en models) | SELECT | SELECT | SELECT, EXECUTE |
| `grp_negocio` | NA | NA | NA | NA | NA | SELECT (vía vistas) | NA |
| `sp_pipeline` | SELECT, MODIFY | SELECT, MODIFY | SELECT, EXECUTE | SELECT, MODIFY, READ/WRITE VOLUME | SELECT, MODIFY, READ/WRITE VOLUME | SELECT, MODIFY | SELECT, EXECUTE, MODIFY |

## Filtros de filas y máscaras

| Tabla | Filtro / máscara | Quién ve todo | Quién ve la versión restringida |
|---|---|---|---|
| `silver_energia.demanda_diaria` | Filtro `solo_regulado(tipo_mercado)`: `is_account_group_member('grp_negocio') = FALSE OR tipo_mercado = 'Regulado'` | `grp_ingenieria`, `grp_analitica`, `sp_pipeline` | `grp_negocio` ve solo mercado regulado |
| `silver_energia.demanda_diaria` | Máscara `mascara_kwh(demanda_real_kwh)`: redondeo a miles para `grp_negocio` | `grp_ingenieria`, `grp_analitica`, `sp_pipeline` | `grp_negocio` ve kWh redondeados |
| `bronze_energia.demanda_raw` (ensayo clase 3, `workspace.c01_jpatinofo`) | Mismas dos funciones con `current_user()` en lugar del grupo | El owner del esquema | Cualquier otro usuario del workspace |

## Secretos

| Secreto | Scope | Quién lo lee | Dónde vive en XM |
|---|---|---|---|
| `simem-api-key` (API de datos abiertos de XM, clase 4) | `xm-demanda` | `sp_pipeline`, `grp_ingenieria` | Azure Key Vault, scope respaldado por Key Vault |
| `github-token` (despliegue por bundle, clase 13) | `xm-cicd` | Service principal de CI/CD | Azure Key Vault / secrets de GitHub Actions |

## Auditoría y linaje

| Pregunta | Cómo se responde |
|---|---|
| ¿Quién tiene acceso a la tabla X? | `SELECT grantee, privilege_type FROM prod.information_schema.table_privileges WHERE table_name = 'X'` |
| ¿Quién leyó la tabla X la semana pasada? | `system.access.audit` filtrando `request_params.full_name_arg` y `event_time`; en XM exportado a Log Analytics |
| ¿De dónde sale la columna Y de oro? | Catalog Explorer → tabla → Lineage (columna), o `system.access.column_lineage` |
| ¿Qué se rompe si cambio la tabla Z? | Lineage aguas abajo de Z (tablas, vistas, notebooks, jobs y modelos que la leen) antes de tocarla |
