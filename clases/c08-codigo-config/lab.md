# Lab 8 — Tests, CI y el PR que debe fallar

**Duración:** 30 min (más el notebook guiado) · **Modalidad:** individual, en tu propio fork · **Ramas:** `feature/c08-tests-ci` y `feature/c08-fuga`

## Objetivo

Cerrar el hito 4: el paquete `xm_demanda` con configuración validada, el contrato de oro como código, pruebas unitarias y de integración pasando en CI, y la evidencia de que CI detiene un error real. La teoría de hoy está en las diapositivas; el notebook la aplica sobre tu repo.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| Lab 7 terminado: `workspace.c01_<usuario>.features_demanda_diaria` y `agregar_calendario` / `agregar_lags` implementadas | Termina el lab 7 (sin ellas, los tests de integración se omiten y `verificar()` marca FALTA) |
| Git folder en `main` actualizado | GitHub: **Sync fork** → Databricks: Git → **Pull** |
| Rama nueva | Git → Create branch → `feature/c08-tests-ci` |

## Pasos

Abre `clases/c08-codigo-config/notebooks/lab08_tests_ci`. Widget `usuario`. La primera celda instala `pytest` y `ruff` en tu sesión.

| Sección | Qué haces | Qué debes ver |
|---|---|---|
| 1 Tres cosas | Comparar la configuración de dev/qa/prod | Solo `catalog`, volúmenes y experimento con `cambia = True` |
| 2 TODO 1 | `validar_config` en `src/xm_demanda/utils/config.py` | dev/qa/prod válidas; `db_password` rechazada con "secreto" |
| 3 TODO 2 | `columnas_contrato` en `src/xm_demanda/features/build.py` | 0 columnas faltan, 0 sobran; `lag_364` y `y_h14` con otro experimento |
| 4 Tests desde Databricks | `ruff` y `pytest tests/unit` | ruff limpio; `pytest → 0`; los skip que quedan son de labs anteriores no terminados |
| 5 TODO 3 | `test_media_7_nunca_incluye_el_dia_t` en `tests/integration/test_features_spark.py` | `pytest tests/integration → 0`, tu test en la lista |
| 6 El PR que debe fallar | Commit + PR de tu trabajo (verde). Luego rama `feature/c08-fuga`, `rowsBetween(-n + 1, 0)`, PR | Checks: `test` verde, `integration` **rojo**. Captura. Cerrar sin mezclar. **Pregunta 1** |
| 7 Reproducibilidad | Versión Delta + commit + tag | **Pregunta 2** |
| `verificar()` | 9 comprobaciones en OK | Commit `feat(c08): validar_config, contrato de oro y test de fuga en integracion` → push → PR → CI con 3 jobs en verde → merge |

## Criterios de aceptación

- [ ] `validar_config` implementada; `tests/unit/test_config.py` en verde sin skip
- [ ] `columnas_contrato` implementada y coincide con tu tabla de oro; `tests/unit/test_features.py` sin skip
- [ ] `test_media_7_nunca_incluye_el_dia_t` escrito con los dos `assert`; `tests/integration` en verde
- [ ] Un PR mezclado con los tres jobs de CI (`test`, `integration` y ruff dentro de `test`) en verde
- [ ] Un PR **cerrado sin mezclar** con `integration` en rojo, y su captura de pantalla en `clases/c08-codigo-config/evidencia_pr_rojo.png` (súbela en el PR bueno)
- [ ] Las dos preguntas respondidas (secciones 6 y 7)
- [ ] `verificar()` en OK

**Hito 4 cerrado.**

## Comparar con la solución de referencia

Al cierre se libera el tag `s08`. En GitHub:

`https://github.com/<docente>/Databricks-to-XM/compare/s08...<tu_usuario>:Databricks-to-XM:main`

O en terminal:

```bash
git fetch upstream --tags
git diff s08 -- src/xm_demanda/utils src/xm_demanda/features tests .github/workflows
```

## Extensión opcional

1. **pre-commit en tu computador.** `pip install -e ".[dev]" && pre-commit install`; intenta commitear un archivo con `token = "abc123..."` y mira cómo `gitleaks` lo detiene antes de que exista el commit.
2. **Un test de integración para plata.** `tests/integration/test_silver_spark.py`: con 4 filas largas, `a_formato_ancho` devuelve 2 filas anchas y `merge_demanda_diaria` dos veces no cambia nada (usa `spark.catalog` con una tabla temporal en formato Delta si tu Spark local lo permite; si no, prueba solo el pivote).
3. **Rama protegida.** En GitHub → Settings → Branches, exige que `test` e `integration` pasen antes de mezclar en `main`. Vuelve a abrir el PR con fuga: ahora el botón de merge no existe.
