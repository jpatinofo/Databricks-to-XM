# Lab 6 — Plata en formato ancho: MERGE, time travel y dimensión CIIU

**Duración:** 50 min · **Modalidad:** individual, en tu propio fork · **Rama:** `feature/c06-silver`

## Objetivo

Construir `demanda_diaria` (una fila por serie-día, con historial de versiones) y `dim_ciiu`, y cerrar el hito 3. La teoría está dentro del notebook: primero se recorre con la docente sobre sus datos, luego lo ejecutas de principio a fin sobre los tuyos.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| Lab 5 terminado: `workspace.c01_<usuario>.demanda_validada` | Termina el lab 5 hasta el paso 4 (≈15 min si tienes la vista vigente) |
| Git folder en `main` actualizado | GitHub: **Sync fork** → Databricks: Git → **Pull** |
| Rama nueva | Git → Create branch → `feature/c06-silver` |

## Pasos

Abre `clases/c06-silver/notebooks/lab06_silver_modelado`. Widget `usuario`. Ejecuta de arriba abajo; las secciones tienen la teoría y el código juntos.

| Sección | Qué haces | Qué debes ver |
|---|---|---|
| 1 Largo → ancho | Leer el contrato de `demanda_diaria` | — |
| 2 El pivote | Ejecutar las dos versiones (SQL y PySpark) | 145.408 filas largas → 72.704 serie-días, llave única |
| 3 MERGE | Ejecutar dos MERGE iguales y luego una republicación de 3 filas | Historial: v1 72.704 insertadas · v2 0/0 · v3 3 actualizadas. **Responde la pregunta** |
| 4 Time travel | Comparar un serie-día en v2 y v3; anotar la versión actual | Dos valores distintos de `demanda_real_kwh` |
| 5 Dimensión CIIU | **TODO 1:** `normalizar_texto` en `src/xm_demanda/silver/transform.py`. Ejecutar el anti-join. **Responde la pregunta.** **TODO 2:** el alias en `ALIAS_CIIU`. Unir a los hechos | 1 texto sin sección → 0 · `dim_ciiu` con 22 filas · `ciiu_seccion` sin nulos |
| 6 Rendimiento | `CLUSTER BY (fecha, tipo_mercado)` + `OPTIMIZE` | `DESCRIBE DETAIL` antes y después |
| 7 Contrato | Comentarios y tags | Verlo en Catalog Explorer |
| `verificar()` | 11 comprobaciones en OK | Commit `feat(c06): plata en formato ancho con MERGE y dim_ciiu` → push → PR hacia `main` de tu fork → CI en verde → merge |

El commit incluye `src/xm_demanda/silver/transform.py`.

## Criterios de aceptación

- [ ] `demanda_diaria`: 72.704 filas, llave única, columnas en `snake_case`, `ciiu_seccion` sin nulos
- [ ] Historial con un MERGE de 3 actualizadas y uno que no tocó nada
- [ ] `CLUSTER BY (fecha, tipo_mercado)` y comentario de tabla
- [ ] `dim_ciiu` con 22 secciones, todas con `nombre_fuente`
- [ ] `normalizar_texto` implementada y `ALIAS_CIIU` con el alias; `tests/unit/test_silver.py` en verde (sin skip)
- [ ] Las dos preguntas respondidas (secciones 3 y 5)
- [ ] `verificar()` en OK; PR en verde

**Hito 3 cerrado.**

## Comparar con la solución de referencia

Al cierre se libera el tag `s06`. En GitHub:

`https://github.com/<docente>/Databricks-to-XM/compare/s06...<tu_usuario>:Databricks-to-XM:main`

O en terminal:

```bash
git fetch upstream --tags
git diff s06 -- src/xm_demanda/silver clases/c06-silver/notebooks
```

## Extensión opcional

Agrega a `dim_ciiu` un atributo `sector` (primario: A–B; secundario: C–F; terciario: G–U; X: no aplica) desde el CSV y calcula la demanda por sector y mes. Piensa dónde debería vivir ese atributo si el DANE cambia la agrupación.

## Problemas frecuentes

| Síntoma | Causa probable |
|---|---|
| `TABLE_OR_VIEW_NOT_FOUND … demanda_validada` | Falta el lab 5 |
| Historial sin v0 `CREATE` | La tabla ya existía; la celda hace `DROP TABLE` primero, vuelve a ejecutarla |
| v2 muestra `actualizadas = 72704` | Tu MERGE no tiene la condición `s._publication_date > t._publication_date` (mira `transform.py`) |
| `normalizar_texto` devuelve `'EDUCACIÓN'` con tilde | Falta quitar los caracteres combinantes tras `NFKD` |
| `sin_match` sigue con 1 fila tras el alias | La llave del alias debe ser el texto **normalizado** (mayúsculas, sin tildes), no el original |
| `MERGE … INSERT *` falla tras `ADD COLUMNS` | Ejecutaste de nuevo `merge_demanda_diaria` después de agregar `ciiu_seccion`; la fuente no la trae. Es esperado: en la clase 7 el join a la dimensión se hace antes del MERGE |
| `clusteringColumns` vacío en `verificar()` | Falta el `ALTER TABLE … CLUSTER BY`; `OPTIMIZE` solo no lo declara |

## Así sería en Azure Databricks

| Hoy | En XM |
|---|---|
| MERGE lanzado desde el notebook | Tarea del job diario, después del pipeline de calidad |
| Versión de la tabla anotada a mano | Registrada en MLflow por el entrenamiento (clase 9) |
| `dim_ciiu` desde un CSV del repo | Tabla de referencia gobernada, con owner y MERGE cuando el DANE actualiza |
| OPTIMIZE a mano | Predictive optimization; clustering declarado en el bundle |
