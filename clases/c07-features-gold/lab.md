# Lab 7 — Features de oro: calendario, lags, ventanas y horizonte

**Duración:** 50 min · **Modalidad:** individual, en tu propio fork · **Rama:** `feature/c07-features`

## Objetivo

Construir `features_demanda_diaria` (oro): la tabla que va a entrenar el modelo de la clase 9. Misma llave que plata, calendario y festivos, lags y ventanas sin fuga de información, objetivo a 7 días, MERGE idempotente y llave primaria declarada. Abre el hito 4. La teoría está dentro del notebook: primero se recorre con la docente sobre sus datos, luego lo ejecutas de principio a fin sobre los tuyos.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| Lab 6 terminado: `workspace.c01_<usuario>.demanda_diaria` con `ciiu_seccion` sin nulos | Termina el lab 6 hasta la sección 5 (≈20 min si tienes `demanda_validada`) |
| Git folder en `main` actualizado | GitHub: **Sync fork** → Databricks: Git → **Pull** |
| Rama nueva | Git → Create branch → `feature/c07-features` |

## Pasos

Abre `clases/c07-features-gold/notebooks/lab07_features_gold`. Widget `usuario`. Ejecuta de arriba abajo; las secciones tienen la teoría y el código juntos.

| Sección | Qué haces | Qué debes ver |
|---|---|---|
| 1 Oro y contrato | Leer el contrato de `features_demanda_diaria` | 72.704 serie-días, 355 series |
| 2 Calendario | Confirmar el patrón semanal en SQL; cargar `festivos_co.csv` | Dom ≈ 212 GWh, Lun–Vie ≈ 235; 55 festivos, años 2025–2027 |
| 3 TODO 1 | `agregar_calendario` en `src/xm_demanda/features/build.py` | Celda de prueba sin `AssertionError`; ≥ 8 festivos marcados |
| 4 Lags y ventanas | **TODO 2:** `agregar_lags`. Ejecutar ventanas y la prueba de mesa | `media_7` del 22-jun = promedio manual 15–21 jun |
| 5 Fuga y horizonte | Comparar `media_7` con la ventana con fuga. **Responde la pregunta.** Objetivo `y_h7` | 355 series con exactamente 7 nulos al final |
| 6 Banderas | `es_cero`, `dias_con_datos` | Series cortas y con ceros en la tabla |
| 7 Construir y publicar | `construir_features` + dos `merge_features`. **Responde la pregunta.** Llave primaria, `CLUSTER BY`, comentario, tags | Historial con 2 MERGE; constraint visible en Catalog Explorer |
| `verificar()` | 16 comprobaciones en OK | Commit `feat(c07): features de oro con calendario, lags y ventanas` → push → PR → CI en verde → merge |

El commit incluye `src/xm_demanda/features/build.py`. `tests/unit/test_features.py` ya prueba las funciones puras; CI lo corre.

## Criterios de aceptación

- [ ] `features_demanda_diaria`: 72.704 filas, llave única, calendario sin nulos, `es_fin_de_semana` coherente con `dia_semana`
- [ ] `lag_1…lag_28` según `conf/base.yml`; `lag_7` nulo solo en los primeros 7 días de cada serie
- [ ] `media_7` nunca incluye el día `t` (la verificación lo recalcula)
- [ ] `y_h7` nulo solo en los últimos 7 días de cada serie
- [ ] Dos MERGE en el historial; el segundo no insertó filas
- [ ] Llave primaria declarada, `CLUSTER BY (fecha, tipo_mercado)`, comentario de tabla y de columnas
- [ ] `agregar_calendario` y `agregar_lags` implementadas; `pytest tests/unit` en verde
- [ ] Las dos preguntas respondidas (secciones 5 y 7)
- [ ] `verificar()` en OK; PR en verde

**Hito 4 abierto** (se cierra en la clase 8 con el paquete y los tests).

## Comparar con la solución de referencia

Al cierre se libera el tag `s07`. En GitHub:

`https://github.com/<docente>/Databricks-to-XM/compare/s07...<tu_usuario>:Databricks-to-XM:main`

O en terminal:

```bash
git fetch upstream --tags
git diff s07 -- src/xm_demanda/features clases/c07-features-gold/notebooks notebooks/03_gold_features.py
```

## Extensión opcional

1. **Point-in-time.** Construye las features usando solo las filas de bronce con `_publication_date <= fecha_corte` (la vista vigente de la clase 4 con un filtro) y compara `media_7` del último mes con la versión vigente. ¿Cuántos serie-días cambian?
2. **ADR-003.** Documenta en `docs/adr/ADR-003-features-y-fuga.md` la decisión de construir sobre la versión vigente, y en qué caso cambiarías a point-in-time. Plantilla en `docs/adr/template.md`.
3. **Una fuente más.** Agrega `data/reference/temperatura_region.csv` (inventa 3 regiones × 200 días) y una función `agregar_temperatura(df, temp)` con su test. No la uses todavía: en la clase 9 se prueba si aporta.
