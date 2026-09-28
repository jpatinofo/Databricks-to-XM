# Lab 5 — Reglas de calidad, validador y cuarentena

**Duración:** 55 min · **Modalidad:** individual, en tu propio fork · **Rama:** `feature/c05-calidad`

## Objetivo

Definir qué es un dato válido en un solo sitio (`src/xm_demanda/quality/rules.py`), aplicarlo por lotes sobre tu bronce con la misma semántica que el pipeline declarativo (warn / drop / cuarentena / fail), y dejar un reporte de calidad por regla y ejecución.

Free Edition permite **un solo pipeline activo por tipo** en el workspace. Por eso el pipeline `silver_energia` lo corre la docente en `dev` durante la demo, y tú aplicas las mismas reglas con el validador en tu esquema. El código de las reglas es idéntico en los dos caminos.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| Lab 4 terminado: `workspace.c01_<usuario>.demanda_raw_vigente` | Termina el lab 4 hasta el paso 7 (la vista); son 10 minutos si bronce ya existe |
| Git folder en `main` actualizado | GitHub: **Sync fork** → Databricks: Git → **Pull** |
| Rama nueva | Git → Create branch → `feature/c05-calidad` |

## Pasos

Abre `clases/c05-calidad-pipelines/notebooks/lab05_calidad_validador`. Widget `usuario`.

1. **Leer las reglas.** `REGLAS` (formato largo) y `REGLAS_BALANCE` (serie-día). Responde por qué `valor_no_negativo` va a cuarentena y `valor_no_nulo` a drop.
2. **Implementar `evaluar()`** en `src/xm_demanda/quality/validador.py`: agrega `_fallas`, un array con los nombres de las reglas que **no** se cumplen. Seis líneas; pistas en el archivo. La celda de prueba usa seis filas sintéticas y te dice cuál falla.
3. **Validar tu vista vigente.** `aplicar()` devuelve válidas, cuarentena y métricas. Se escriben `demanda_validada`, `demanda_cuarentena` y `calidad_metricas` en tu esquema: 145.408 válidas, 0 en cuarentena, 0 fallas.
4. **Siete filas malas.** Se unen a la vista y se valida: la regla `fail` (fecha del futuro) detiene todo. Responde si esa es la decisión correcta. Se quita esa fila y se valida de nuevo: 145.411 válidas (145.408 + 1 warn + 2 de la serie nueva), 1 en cuarentena, 2 descartadas.
5. **Balance por serie-día.** Formato ancho sobre plata validada y `REGLAS_BALANCE`: 72.705 serie-días válidos, 1 en `balance_cuarentena` (pérdidas > demanda).
6. **Reporte.** `calidad_metricas` con una fila por regla y ejecución; tres ejecuciones acumuladas.
7. **El pipeline.** Lee `notebooks/02_silver_pipeline.py` y responde qué cambia entre validador y pipeline, y cuál usarías en producción.
8. **`verificar()`**, luego commit `feat(c05): reglas de calidad, validador y cuarentena` → push → PR hacia `main` de tu fork → CI en verde → merge. El commit incluye `src/xm_demanda/quality/validador.py`.

## Criterios de aceptación

- [ ] `evaluar()` implementada; la celda de prueba en OK
- [ ] `demanda_validada` con 145.411 filas y columna `_fallas`; `demanda_cuarentena` con 1
- [ ] `balance_serie_dia` con 72.705; `balance_cuarentena` con 1
- [ ] `calidad_metricas` cubre las 7 reglas y tiene ≥ 3 ejecuciones
- [ ] Las tres preguntas respondidas (pasos 1, 4 y 7)
- [ ] `verificar()` en OK; PR en verde

## Comparar con la solución de referencia

Al cierre se libera el tag `s05`. En GitHub:

`https://github.com/<docente>/Databricks-to-XM/compare/s05...<tu_usuario>:Databricks-to-XM:main`

O en terminal:

```bash
git fetch upstream --tags
git diff s05 -- src/xm_demanda/quality clases/c05-calidad-pipelines/notebooks
```

## Extensión opcional

Agrega una regla nueva a `REGLAS` (por ejemplo, `mercado_conocido`: `TipoMercado IN ('Regulado', 'No Regulado')`, acción `drop`) y comprueba que el validador y el pipeline la recogen sin tocar ninguno de los dos: solo cambió el diccionario. Luego agrega un test en `tests/unit/test_validador.py` que la cubra.

Si quieres correr el pipeline tú: al final de la clase, de a uno, crea un pipeline apuntando a `notebooks/02_silver_pipeline.py` con catálogo `workspace`, esquema `c01_<usuario>` y configuration `catalog = workspace`. Antes cambia en el notebook la fuente a tu bronce (`workspace.c01_<usuario>.demanda_raw_bronze`). Bórralo al terminar para liberar el cupo.

## Problemas frecuentes

| Síntoma | Causa probable |
|---|---|
| `TABLE_OR_VIEW_NOT_FOUND … demanda_raw_vigente` | Falta el paso 7 del lab 4 |
| La celda de prueba dice `obtuve [] esperaba ['valor_no_negativo']` | Usaste `F.expr(expr)` en vez de `~F.expr(expr)`: `_fallas` debe listar las reglas que NO se cumplen |
| `obtuve [None, 'valor_no_negativo', …]` | Falta `F.array_compact(...)` para quitar los nulos |
| `ReglaFail` en el paso 3 | Tu bronce tiene fechas futuras: revisa `raw_v2` del lab 4 (FechaPublicacion no es Fecha) |
| `validas=145412` | Quedó una fila sintética de más en la vista vigente (ejecutaste dos veces el paso 4 escribiendo sobre bronce). El paso 4 no toca bronce: solo une en memoria |
| `calidad_metricas` con menos de 3 ejecuciones | Alguna celda de escritura falló; repite los pasos 3, 4 y 5 |
| El pipeline de la docente no arranca | Otro pipeline está activo en el workspace: bórralo o espera |

## Así sería en Azure Databricks

| Hoy | En XM |
|---|---|
| Validador por lotes en tu esquema | El pipeline es la única ruta a plata; el validador queda para pruebas |
| `calidad_metricas` escrita por el notebook | Event log del pipeline → Log Analytics; dashboard por regla y día |
| `fail` se ve en pantalla | Notificación inmediata y el job diario se detiene antes de oro |
| Cuarentena se revisa en clase | Alerta si supera un umbral; ticket al dueño del esquema (`docs/governance.md`) |
