# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 8 — Código, configuración y datos separados: tests y CI desde Databricks
# MAGIC
# MAGIC Este notebook es la clase. Hoy no construye una tabla nueva: **endurece lo que ya existe**. Desde la clase 4 el código
# MAGIC vive en `src/`, la configuración en `conf/` y los datos en Unity Catalog; hoy cerramos el hito 4 demostrando que esa
# MAGIC separación se puede probar, automatizar y auditar.
# MAGIC
# MAGIC ```
# MAGIC   src/xm_demanda/            conf/*.yml                 Unity Catalog                tests/
# MAGIC   código: QUÉ se hace        config: DÓNDE y CON QUÉ    datos: lo que llega y        unit: funciones puras, sin Spark
# MAGIC   (ingest, quality,          (catálogo, volúmenes,      lo que se construye          integration: reglas sobre Spark
# MAGIC    silver, features)          lags, horizonte)          (bronce, plata, oro)         verificar(): sobre los datos reales
# MAGIC            │                        │                          │                              │
# MAGIC            └──────────── un PR cambia código y config juntos ───┘      CI corre ruff + unit + integration ┘
# MAGIC ```
# MAGIC
# MAGIC Tres `TODO`: dos funciones puras (`src/xm_demanda/utils/config.py` y `src/xm_demanda/features/build.py`) y un test de
# MAGIC integración (`tests/integration/test_features_spark.py`). Más un PR que **debe fallar**.
# MAGIC Requiere el lab 7: `workspace.c01_<usuario>.features_demanda_diaria` y tu `build.py` con `agregar_calendario` y `agregar_lags`.

# COMMAND ----------

# MAGIC %pip install -q pytest ruff

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

esquema = f"workspace.c01_{usuario}"
T_FEATURES = f"{esquema}.features_demanda_diaria"
print("Tabla de oro del lab 7:", T_FEATURES)

# COMMAND ----------

import importlib
import os
import subprocess
import sys


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: ejecuta este notebook desde tu Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
import xm_demanda.features.build as fb
import xm_demanda.utils.config as cfgmod

importlib.reload(cfgmod)
importlib.reload(fb)
print("Repo:", raiz)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Tres cosas que cambian a ritmos distintos
# MAGIC
# MAGIC ![Código, configuración y datos](./../img/separacion_codigo_config_datos.png)
# MAGIC
# MAGIC Lo que sigue no es teoría nueva: es mirar el repo que ya tienen con esa lente.
# MAGIC
# MAGIC | | Qué es | Dónde vive | Cuándo cambia | Quién lo revisa |
# MAGIC |---|---|---|---|---|
# MAGIC | **Código** | funciones: `a_formato_ancho`, `evaluar`, `construir_features` | `src/xm_demanda/` | cuando cambia la lógica | PR + ruff + tests (CI) |
# MAGIC | **Configuración** | catálogo, volúmenes, lags, ventanas, horizonte | `conf/base.yml` + `conf/<env>.yml` | por ambiente o por experimento | PR (es un diff de 1 línea) |
# MAGIC | **Datos** | bronce, plata, oro, referencia | Unity Catalog, `data/reference/` | todos los días | permisos, expectations, `verificar()` |
# MAGIC
# MAGIC Los notebooks de `notebooks/` son **delgados**: reciben `catalog` como parámetro, cargan la configuración y llaman a `src`.
# MAGIC No tienen lógica propia; por eso se pueden ejecutar igual en `dev`, `qa` y `prod` (clase 12 los convierte en tareas de un job).
# MAGIC
# MAGIC La prueba de que la separación funciona: cargar la configuración de los tres ambientes y ver **qué cambia y qué no**.

# COMMAND ----------

import pandas as pd

ambientes = {env: cfgmod.load_config(env) for env in ["dev", "qa", "prod"]}
comparacion = pd.DataFrame(
    {env: {k: str(v) for k, v in c.items()} for env, c in ambientes.items()}
)
comparacion["cambia"] = comparacion.nunique(axis=1) > 1
display(comparacion.reset_index().rename(columns={"index": "llave"}))

# COMMAND ----------

# MAGIC %md
# MAGIC Solo cambian `catalog`, los volúmenes y la ruta del experimento. El modelo, las tablas y los esquemas son **idénticos** en los
# MAGIC tres: el código que entrenes en `dev` es el mismo que corre en `prod`. Esa fila `cambia = True` es la única diferencia
# MAGIC entre ambientes, y está en un archivo que se revisa por PR, no en un notebook.
# MAGIC
# MAGIC ## 2 · TODO 1 — `validar_config` en `src/xm_demanda/utils/config.py`
# MAGIC
# MAGIC Un YAML acepta cualquier cosa. Dos errores que conviene atrapar **antes** de que un job llegue a Spark:
# MAGIC
# MAGIC 1. Falta una llave requerida (`catalog`, `schemas`, `tablas`, `modelo`, `volumen_landing`): el job fallaría a la mitad con un
# MAGIC    `KeyError` críptico. Mejor un `ValueError` claro al arrancar.
# MAGIC 2. Alguien escribió un secreto en el YAML (`db_password: ...`, `token: ...`). Un secreto en el repo queda en el historial de
# MAGIC    Git para siempre. **La configuración nunca lleva credenciales**: van a un *secret scope* de Databricks (o Key Vault en
# MAGIC    Azure) y se leen con `dbutils.secrets.get(scope, key)` en tiempo de ejecución.
# MAGIC
# MAGIC Completa `validar_config(cfg)`: lanza `ValueError` con la palabra `falta` si falta una llave de `LLAVES_REQUERIDAS`, y con la
# MAGIC palabra `secreto` si alguna llave (a cualquier nivel: usa `_llaves_anidadas`) contiene un fragmento de `PALABRAS_SECRETO`.
# MAGIC Los tests en `tests/unit/test_config.py` ya describen el comportamiento esperado; léelos antes de escribir.

# COMMAND ----------

importlib.reload(cfgmod)
for env, cfg in ambientes.items():
    cfgmod.validar_config(cfg)
    print(f"{env}: configuración válida")

mala = dict(ambientes["dev"])
mala["conexion"] = {"db_password": "hunter2"}
try:
    cfgmod.validar_config(mala)
    raise AssertionError("Debía rechazar db_password")
except ValueError as e:
    print("Rechazada, como debe ser:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC Así se lee un secreto en Databricks (no lo ejecutes: en Free Edition los *secret scopes* se crean con la CLI y no hace falta
# MAGIC para el curso). Lo importante es la forma: el **nombre** del secreto puede estar en la configuración; el **valor**, nunca.
# MAGIC
# MAGIC ```python
# MAGIC # conf/prod.yml                      →   secreto_fuente: {scope: xm-prod, key: api_xm_token}
# MAGIC token = dbutils.secrets.get(scope=cfg["secreto_fuente"]["scope"], key=cfg["secreto_fuente"]["key"])
# MAGIC # Databricks redacta el valor si intentas imprimirlo: [REDACTED]
# MAGIC ```
# MAGIC
# MAGIC ## 3 · TODO 2 — el contrato de oro como código: `columnas_contrato`
# MAGIC
# MAGIC En la clase 7 escribieron el contrato de `features_demanda_diaria` en una tabla de markdown. Un contrato en markdown no se
# MAGIC puede probar. Un contrato **en código** sí: `columnas_contrato(cfg)` devuelve la lista exacta de columnas que la tabla debe
# MAGIC tener, derivada de la configuración. Si alguien cambia `lags` en el YAML, el contrato cambia solo, y `verificar()` y el
# MAGIC notebook de producción comparan la tabla real contra él.
# MAGIC
# MAGIC Orden: llave, medida, calendario, lags, ventanas, banderas, objetivo, `_features_built_at`. Todo lo que necesitas ya está en
# MAGIC `build.py`: `LLAVE`, `MEDIDA`, `COLUMNAS_CALENDARIO`, `nombres_lags`, `nombres_ventanas`, `COLUMNAS_BANDERAS`.

# COMMAND ----------

importlib.reload(fb)
cfg = ambientes["dev"]
contrato = fb.columnas_contrato(cfg)
reales = spark.table(T_FEATURES).columns
print("Contrato:", len(contrato), "columnas | tabla real:", len(reales))
print("Faltan en la tabla:", sorted(set(contrato) - set(reales)))
print("Sobran en la tabla:", sorted(set(reales) - set(contrato)))
assert set(contrato) == set(reales), "La tabla de oro no cumple el contrato"

# Y el contrato sigue a la configuración, sin tocar código:
experimento = {**cfg, "modelo": {**cfg["modelo"], "lags": [1, 7, 364], "horizon_days": 14}}
print("Con otro experimento:", [c for c in fb.columnas_contrato(experimento) if c.startswith(("lag_", "y_h"))])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Correr los tests y el linter desde Databricks
# MAGIC
# MAGIC ![Las pruebas del proyecto](./../img/piramide_pruebas.png)
# MAGIC
# MAGIC No hace falta un computador con Python configurado: `pytest` y `ruff` corren dentro del Git folder, sobre el mismo `src/`
# MAGIC que acaban de editar. Es exactamente lo que hace CI en GitHub, en una máquina limpia, cada vez que abren un PR.
# MAGIC
# MAGIC | Nivel | Qué prueba | Dónde corre | Cuánto tarda |
# MAGIC |---|---|---|---|
# MAGIC | `ruff` | estilo, imports sin usar, líneas largas, errores de sintaxis | pre-commit, CI, aquí | 1 s |
# MAGIC | `tests/unit` | funciones puras: nombres, reglas, config, contrato | CI, local, aquí | 0,1 s |
# MAGIC | `tests/integration` | las reglas del contrato sobre Spark con datos sintéticos | CI (Spark local), aquí (tu sesión) | 20 s |
# MAGIC | `verificar()` | la tabla real de tu esquema | solo aquí | minutos |
# MAGIC
# MAGIC Fíjate en los `skipped`: cada `TODO` pendiente de los labs 5 a 8 aparece como una prueba omitida, no como un error. Así el
# MAGIC repo del curso siempre está en verde y tú ves exactamente qué te falta.

# COMMAND ----------

lint = subprocess.run([sys.executable, "-m", "ruff", "check", "src", "tests"], cwd=raiz, capture_output=True, text=True)
print(lint.stdout or lint.stderr)
assert lint.returncode == 0, "ruff encontró problemas: corrígelos antes del commit"

# COMMAND ----------

import pytest

codigo = pytest.main(["-q", "-p", "no:cacheprovider", "-rs", os.path.join(raiz, "tests", "unit")])
print("pytest unit →", codigo)  # 0 = todo pasó (los skip no cuentan como fallo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · TODO 3 — un test de integración que distingue la ventana correcta de la que tiene fuga
# MAGIC
# MAGIC `tests/integration/test_features_spark.py` construye tres series sintéticas (120, 90 y 60 días, con ceros en la tercera) y
# MAGIC comprueba las **reglas** del contrato de la clase 7: calendario sin nulos, `lag_7` igual al valor de siete filas atrás,
# MAGIC `y_h7` nulo solo al final, banderas. No prueba los valores de XM; prueba que las funciones hacen lo que el contrato dice.
# MAGIC
# MAGIC Abre `tests/integration/conftest.py`: el *fixture* `spark` reutiliza la sesión activa si existe (aquí) y crea una local si no
# MAGIC (CI). Mismo test, dos lugares.
# MAGIC
# MAGIC Falta el test más importante, el de la sección 5 de la clase pasada. Escribe `test_media_7_nunca_incluye_el_dia_t(features)`
# MAGIC al final del archivo, con dos `assert`:
# MAGIC
# MAGIC 1. `media_7` coincide (±1e-6) con `avg` sobre `rowsBetween(-7, -1)` en todas las filas.
# MAGIC 2. `media_7` **no** coincide con `avg` sobre `rowsBetween(-6, 0)` en al menos una fila. Sin este segundo `assert`, un test
# MAGIC    podría pasar tanto con la versión correcta como con la que tiene fuga; un test que no puede fallar no prueba nada.
# MAGIC
# MAGIC Luego ejecuta la celda siguiente: debe aparecer tu test entre los que pasan.

# COMMAND ----------

codigo = pytest.main(["-q", "-p", "no:cacheprovider", "-rs", "-v", os.path.join(raiz, "tests", "integration")])
print("pytest integration →", codigo)
assert codigo == 0, "Hay pruebas de integración fallando"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · El PR que debe fallar
# MAGIC
# MAGIC ![El camino de un cambio](./../img/camino_de_un_cambio.png)
# MAGIC
# MAGIC Hasta hoy todos sus PR han salido en verde, y eso es sospechoso: un revisor que siempre aprueba no está revisando. Vamos a
# MAGIC comprobar que CI de verdad detiene un error, con el error más caro que conocen: una fuga de información.
# MAGIC
# MAGIC | Paso | Dónde | Qué haces |
# MAGIC |---|---|---|
# MAGIC | 1 | Databricks → Git | Primero haz commit y push de tu trabajo de hoy en `feature/c08-tests-ci` (los 3 TODO). Abre el PR y espera el verde. |
# MAGIC | 2 | Databricks → Git | Crea una rama `feature/c08-fuga` desde `main`. |
# MAGIC | 3 | `src/xm_demanda/features/build.py` | En `agregar_ventanas`, cambia `w.rowsBetween(-n, -1)` por `w.rowsBetween(-n + 1, 0)`. Parece inocente. |
# MAGIC | 4 | Aquí | Vuelve a ejecutar la celda de `tests/integration`: tu test debe fallar. Ese es el valor del segundo `assert`. |
# MAGIC | 5 | Databricks → Git | Commit `feat: ventana movil incluyendo el dia` y push. Abre un PR hacia `main`. |
# MAGIC | 6 | GitHub → pestaña Checks | `ruff` verde, `test` verde, `integration` **rojo**. Captura la pantalla: es tu evidencia del lab. |
# MAGIC | 7 | GitHub | Cierra el PR **sin mezclar**. Borra la rama. Vuelve a `main` en Databricks (Git → Pull) y recarga este notebook. |
# MAGIC
# MAGIC Lo que acaban de ver es la diferencia entre "tengo tests" y "los tests me protegen": la fuga pasó por ruff y por las
# MAGIC unitarias, y la paró el test que ustedes escribieron. En XM, ese rojo evita que un modelo con métrica perfecta en el notebook
# MAGIC llegue a producción.

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta 1 (respóndela aquí):** el PR con fuga pasó `ruff` y `tests/unit` y solo falló en `tests/integration`. ¿Qué tipo de
# MAGIC error atrapa cada nivel y por qué una prueba unitaria pura no podía detectar este? Dos o tres frases.
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Reproducibilidad: las cuatro versiones de un pronóstico
# MAGIC
# MAGIC ![Reproducibilidad](./../img/reproducibilidad_pronostico.png)
# MAGIC
# MAGIC Dentro de seis meses alguien, un auditor, el regulador, ustedes mismos, va a preguntar: *¿por qué el pronóstico del 22 de junio
# MAGIC dijo 29,1 GWh?* Responder exige reconstruirlo, y reconstruirlo exige cuatro versiones: la del **código** (commit o tag), la de
# MAGIC la **configuración** (va en el mismo commit), la de los **datos** (versión Delta de la tabla de oro) y la del **modelo**
# MAGIC (run de MLflow, clase 9). Hoy ya tienen tres. La celda siguiente las junta para la tabla de oro de su esquema.

# COMMAND ----------

from pyspark.sql import functions as F

h = spark.sql(f"DESCRIBE HISTORY {T_FEATURES}").orderBy(F.desc("version")).first()
print("Tabla:", T_FEATURES)
print("Versión Delta actual:", h.version, "| operación:", h.operation, "| cuándo:", h.timestamp)
print("Código + configuración: el commit de tu rama en Databricks → Git (arriba a la izquierda) y el tag sNN de la clase")
print("Modelo: run de MLflow (clase 9)")
display(spark.sql(f"SELECT * FROM {T_FEATURES} VERSION AS OF {h.version} WHERE fecha = '2026-06-22' AND codigo_sic_agente = 'ENDD' AND tipo_mercado = 'Regulado'").select("fecha", "demanda_real_kwh", "lag_7", "media_7", "y_h7"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta 2 (respóndela aquí):** `VACUUM` con la retención por defecto (7 días) borra las versiones viejas de la tabla de oro.
# MAGIC ¿Qué pasa con la reproducibilidad del pronóstico de hace seis meses, y qué dos opciones tiene el equipo para protegerla?
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC
# MAGIC Cuando todo esté en OK: commit `feat(c08): validar_config, contrato de oro y test de fuga en integracion` (incluye `src/` y
# MAGIC `tests/`), push, PR hacia `main` de tu fork, **CI en verde con los tres jobs**, merge. El PR rojo de la sección 6 queda cerrado
# MAGIC sin mezclar; su captura es parte del entregable. Al cierre se libera el tag `s08`. **Hito 4 cerrado.**

# COMMAND ----------

def verificar():
    checks = []
    importlib.reload(cfgmod)
    importlib.reload(fb)
    try:
        for env in ["dev", "qa", "prod"]:
            cfgmod.validar_config(cfgmod.load_config(env))
        ok_secreto = False
        try:
            cfgmod.validar_config({**cfgmod.load_config("dev"), "x": {"api_token": "1"}})
        except ValueError as e:
            ok_secreto = "secreto" in str(e)
        checks.append(("validar_config acepta dev/qa/prod y rechaza secretos", ok_secreto))
    except NotImplementedError:
        checks.append(("validar_config implementada", False))
    try:
        cols = fb.columnas_contrato(cfgmod.load_config("dev"))
        checks.append(("columnas_contrato coincide con la tabla real", set(cols) == set(spark.table(T_FEATURES).columns)))
        checks.append(("columnas_contrato sigue a la configuración", "y_h14" in fb.columnas_contrato({**cfgmod.load_config("dev"), "modelo": {**cfgmod.load_config("dev")["modelo"], "horizon_days": 14}})))
    except NotImplementedError:
        checks.append(("columnas_contrato implementada", False))
    ruta_test = os.path.join(raiz, "tests", "integration", "test_features_spark.py")
    with open(ruta_test, encoding="utf-8") as f:
        src_test = f.read()
    checks.append(("test_media_7_nunca_incluye_el_dia_t escrito", "def test_media_7_nunca_incluye_el_dia_t" in src_test and "rowsBetween(-6, 0)" in src_test))
    lint = subprocess.run([sys.executable, "-m", "ruff", "check", "src", "tests"], cwd=raiz, capture_output=True, text=True)
    checks.append(("ruff sin errores", lint.returncode == 0))
    checks.append(("tests/unit en verde", pytest.main(["-q", "-p", "no:cacheprovider", os.path.join(raiz, "tests", "unit")]) == 0))
    checks.append(("tests/integration en verde", pytest.main(["-q", "-p", "no:cacheprovider", os.path.join(raiz, "tests", "integration")]) == 0))
    checks.append(("agregar_ventanas usa rowsBetween(-n, -1) (sin fuga)", "rowsBetween(-n, -1)" in open(os.path.join(raiz, "src", "xm_demanda", "features", "build.py"), encoding="utf-8").read()))

    print()
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/ y tests/). Falta la captura del PR rojo." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
