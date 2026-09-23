# Lab 4 — Bronce con Auto Loader: 23 publicaciones y una republicación

**Duración:** 65 min · **Modalidad:** individual, en tu propio fork · **Rama:** `feature/c04-ingesta`

## Objetivo

Reemplazar el upload manual por una ingesta real: Auto Loader vigila `landing`, carga solo lo nuevo, no duplica al volver a correr, y cuando llega una republicación con una columna renombrada no pierde ni duplica nada. Cierra el hito 2.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| `DemandaPerdidas.xlsx` en tu volumen `workspace.c01_<usuario>.raw` (lab 1) | Súbelo ahora: Catalog Explorer → tu esquema → Volumes → raw → Upload |
| Git folder en `main` actualizado | GitHub: **Sync fork** → Databricks: botón Git → **Pull** |
| Rama nueva | Git → Create branch → `feature/c04-ingesta` |

## Pasos

Abre `clases/c04-ingesta/notebooks/lab04_bronce_auto_loader`. Widget `usuario` = el sufijo de tu esquema. El notebook sigue esta secuencia; aquí solo está el porqué de cada paso.

### Parte 1 — Cargar sin duplicar (30 min)

1. **Excel → 23 CSV en landing.** Un archivo por `FechaPublicacion`, en `/Volumes/workspace/c01_<usuario>/raw/landing/`. Auto Loader no lee Excel; y así simulamos 23 llegadas separadas.
2. **Auto Loader → bronce, y falla a propósito.** Antes de ejecutar, abre `src/xm_demanda/ingest/bronze.py` y lee `ingestar_bronce`: esquema explícito, `schemaEvolutionMode = rescue`, `_rescued_data`, `availableNow`. Falla porque dos funciones están sin implementar.
3. **Implementar en `src`.** En `bronze.py`: `resolver_alias(columnas)` (función pura; sus tests están en `tests/unit/test_bronze.py`) y `con_metadatos(df)` (`_ingested_at`, `_source_file` desde `_metadata.file_path`, `_publication_date`). La celda de prueba del notebook las valida con un archivo.
4. **Ingesta y prueba de idempotencia.** 145.408 filas y 23 archivos. Vuelve a ejecutar: mismo conteo. Responde la pregunta sobre el checkpoint.

### Parte 2 — Llega `raw_v2` (35 min)

5. **Republicación.** El notebook genera `demanda_2026-08-01_raw_v2.csv`: los últimos 7 días con valores corregidos (+1,5 %) y la columna `Valor` renombrada a `ValorKwh`. Ejecuta la ingesta y mira `_rescued_data`: ahí viajó `ValorKwh`. Si `Valor` llegó nulo, `resolver_alias` no reconoce el alias. Responde la pregunta sobre `schemaEvolutionMode`.
6. **Reconstruir desde landing.** `DROP TABLE` + borrar el checkpoint + ejecutar: 150.350 filas, 24 archivos, 0 nulos. Landing es la fuente de verdad; bronce se reconstruye.
7. **Vista vigente.** `demanda_raw_vigente` con `row_number()` por serie-día-variable ordenado por `_publication_date DESC`: 145.408 filas. Mira cuánto cambió la corrección por día.
8. **ADR-002.** `docs/adr/ADR-002-republicaciones.md`: tres decisiones (acumular vs. sobrescribir, llave de versión, qué es "vigente"), tres alternativas descartadas, estado `aceptada`.
9. **`verificar()`**, luego `ruff check .` y `pytest tests/unit` si trabajas en VS Code. Commit `feat(c04): bronce idempotente con Auto Loader` → push → PR hacia `main` de tu fork → CI en verde → merge. El commit incluye `src/`, `tests/` y `docs/adr/`.

## Criterios de aceptación

- [ ] `demanda_raw_bronze` con 150.350 filas, 24 archivos de origen y `Valor` sin nulos
- [ ] Columnas `_ingested_at`, `_source_file`, `_publication_date` y `_rescued_data`
- [ ] Vista `demanda_raw_vigente` con 145.408 filas; 7 días con dos versiones en bronce
- [ ] `resolver_alias` implementada; `tests/unit/test_bronze.py` en verde (no skip)
- [ ] ADR-002 aceptada, con ≥ 3 alternativas descartadas
- [ ] `verificar()` en OK; PR en verde

**Hito 2 cerrado.**

## Comparar con la solución de referencia

Al cierre se libera el tag `s04`. En GitHub:

`https://github.com/<docente>/Databricks-to-XM/compare/s04...<tu_usuario>:Databricks-to-XM:main`

O en terminal:

```bash
git fetch upstream --tags
git diff s04 -- src/xm_demanda/ingest docs/adr/ADR-002-republicaciones.md clases/c04-ingesta/notebooks
```

## Extensión opcional

Añade a `ALIAS` un renombre que aún no ha pasado (`Fecha_Publicacion` → `FechaPublicacion`), genera un `raw_v3` con ese cambio y comprueba que la ingesta lo absorbe sin tocar el notebook. Luego piensa: ¿qué pasa si el alias cambia de tipo (por ejemplo, `Valor` llega con coma decimal)? ¿Dónde se detecta?

## Problemas frecuentes

| Síntoma | Causa probable |
|---|---|
| `No encuentro src/xm_demanda` | El notebook se abrió fuera del Git folder |
| `NotImplementedError` en el paso 2 | Esperado: implementa las funciones en el paso 3 |
| La celda de prueba dice `_source_file debe venir de _metadata.file_path` | Usaste `input_file_name()` (obsoleto) o un literal; usa `F.col("_metadata.file_path")` |
| Segunda ejecución duplica filas | El checkpoint cambió de ruta o se borró sin borrar la tabla |
| `valor_nulo > 0` tras raw_v2 | `ValorKwh` no está en `ALIAS` o `resolver_alias` no lo devuelve |
| `filas=150350` no coincide en la reconstrucción | Quedó un archivo extra en landing (por ejemplo, ejecutaste dos veces el paso 5 con otro nombre); borra el sobrante |
| `test_bronze.py` sale como `skipped` | `resolver_alias` sigue lanzando `NotImplementedError` |
| Vista vigente ≠ 145.408 | Falta una columna en el `PARTITION BY` (deben ser las 6 de la llave) |

## Así sería en Azure Databricks

| Hoy | En XM |
|---|---|
| Copias raw_v2 al volumen | Job programado descarga del portal XM / API SIMEM a `abfss://landing@…/demanda/` |
| Auto Loader lista el directorio | `cloudFiles.useNotifications = true` con Event Grid |
| Ejecutas el notebook con `availableNow` | Job con *file arrival trigger* sobre la external location |
| Checkpoint en el volumen | Checkpoint en ADLS, respaldado, en el runbook de recuperación |
| Base del SIC: no aplica | Lakeflow Connect (CDC) hacia `bronze_energia` con los mismos metadatos |
