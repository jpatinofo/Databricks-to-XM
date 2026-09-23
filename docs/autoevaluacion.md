# Autoevaluación por hito

No hay calificación. Cada laboratorio incluye una celda `verificar()` que comprueba los criterios de aceptación. Esta lista es el resumen por hito para saber dónde estás.

Cómo compararte con la solución de referencia al cierre de cada clase. Al terminar la clase la docente publica el tag `sNN` en el repositorio original; tú comparas tu `main` contra ese tag.

**Opción A — en GitHub, sin instalar nada.** Abre en el navegador (cambia `<docente>` y `<tu_usuario>`):

`https://github.com/<docente>/Databricks-to-XM/compare/s01...<tu_usuario>:Databricks-to-XM:main`

**Opción B — en terminal:**

```bash
git remote add upstream https://github.com/<docente>/Databricks-to-XM.git   # una sola vez
git fetch upstream --tags
git diff s01 -- <rutas de la clase>
```

Rutas que cambian en cada clase (lo demás del diff es ruido):

| Clase | Tag | Rutas a comparar |
|---|---|---|
| 1 | `s01` | `clases/c01-databricks-lakehouse/notebooks` |
| 2 | `s02` | `docs/architecture.md docs/adr/ADR-001-granularidad-horizonte.md` |
| 3 | `s03` | `docs/governance.md clases/c03-unity-catalog/notebooks` |
| 4 | `s04` | `src/xm_demanda/ingest docs/adr/ADR-002-republicaciones.md clases/c04-ingesta/notebooks` |
| 5 | `s05` | `notebooks/02_silver_pipeline.py src/xm_demanda/quality` |
| 6 | `s06` | `tests/unit src/xm_demanda docs/governance.md` |
| 7 | `s07` | `notebooks/03_gold_features.py src/xm_demanda` |
| 8 | `s08` | `tests src conf .github/workflows` |
| 9 | `s09` | `notebooks/04_train.py src/xm_demanda` |
| 10 | `s10` | `notebooks/04_train.py notebooks/05_batch_inference.py` |
| 11 | `s11` | `notebooks/05_batch_inference.py app resources` |
| 12 | `s12` | `resources databricks.yml conf` |
| 13 | `s13` | `databricks.yml resources .github/workflows ci` |
| 14 | `s14` | `notebooks/06_monitor.py resources` |
| 15 | `s15` | `docs/runbook.md docs/architecture.md` |

No hay una única respuesta correcta; sí hay decisiones sin justificar. Compara el porqué, no el qué.

| Hito | Clase de cierre | Sé que terminé cuando… |
|---|---|---|
| H1 Pregunta analítica | 2 | `verificar()` del lab 1 en verde; `docs/architecture.md` con las tablas por capa y ADR-001 escrita |
| H2 Arquitectura y fuentes | 4 | Bronze idempotente: cargar dos veces el mismo archivo no duplica; el historial conserva las publicaciones |
| H3 Gobierno, permisos y calidad | 6 | Catálogos `dev/qa/prod` con grants; pipeline con expectations y tabla de cuarentena; Silver en formato ancho |
| H4 Pipeline y Gold | 8 | Tabla de features documentada; `pytest tests/unit` y `ruff check .` en verde en CI del fork |
| H5 Modelo, experimento y despliegue | 11 | Modelo registrado en UC con firma y alias `champion`; `pronostico_demanda` poblada por job; endpoint y app responden |
| H6 Operación, CI/CD y consumo | 15 | Bundle desplegado en `dev` y `qa`; CI/CD dispara por PR y por tag; monitor de drift con alerta; runbook escrito |

Niveles orientativos para cada hito, por si quieres ir más allá:

| Nivel | Qué significa |
|---|---|
| Funcional | `verificar()` en verde |
| Sólido | Además: tests para toda función de `src/`, configuración en `conf/`, nada hard-codeado |
| Operable | Además: desplegable por bundle, documentado (ADR/README) y con alerta o monitor |
