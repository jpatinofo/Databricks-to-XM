# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5 — Reglas de calidad, validador y cuarentena
# MAGIC
# MAGIC Las reglas viven en `src/xm_demanda/quality/rules.py`. El validador por lotes (`validador.py`) las aplica con la
# MAGIC misma semántica que las expectations del pipeline declarativo (`notebooks/02_silver_pipeline.py`).
# MAGIC Free Edition permite un solo pipeline activo: el pipeline lo corre la docente en `dev`; tú aplicas las mismas reglas
# MAGIC por lotes sobre tu bronce.
# MAGIC
# MAGIC Requiere el lab 4: `workspace.c01_<usuario>.demanda_raw_vigente`.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

esquema = f"workspace.c01_{usuario}"
vigente = f"{esquema}.demanda_raw_vigente"
print("Fuente:", vigente)

# COMMAND ----------

import importlib
import os
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

import xm_demanda.quality.rules as rules
import xm_demanda.quality.validador as validador

importlib.reload(rules)
importlib.reload(validador)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Leer las reglas
# MAGIC `REGLAS` es la única definición de qué es válido: nombre → (expresión SQL, acción). Léelas y responde:
# MAGIC ¿por qué `valor_no_negativo` va a cuarentena y `valor_no_nulo` a drop?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

for nombre, (expr, accion) in {**rules.REGLAS, **rules.REGLAS_BALANCE}.items():
    print(f"{nombre:26} {accion:11} {expr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Implementar `evaluar()` en `validador.py`
# MAGIC Agrega la columna `_fallas`: array con los nombres de las reglas que **no** se cumplen. Pistas en el archivo.
# MAGIC Guarda y ejecuta la celda siguiente: cinco filas sintéticas, una por regla, más una válida.

# COMMAND ----------

importlib.reload(validador)

columnas = ["Fecha", "CodigoVariable", "CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial", "Valor", "FechaPublicacion"]
prueba = spark.createDataFrame(
    [
        ("2026-07-01", "DdaReal", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", 100.0, "2026-07-25"),      # válida
        ("2026-07-01", "DdaReal", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", -5.0, "2026-07-25"),       # valor_no_negativo
        ("2026-07-01", "DdaReal", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", None, "2026-07-25"),       # valor_no_nulo
        ("2026-07-01", "Otra", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", 100.0, "2026-07-25"),         # variable_conocida
        ("2026-07-01", "DdaReal", "EPSC", "EPSC", "Regulado", "EDUCACIÓN", 100.0, "2026-07-25"),           # regulado_sin_clasificar
        ("2030-01-01", "DdaReal", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", 100.0, "2026-07-25"),      # fecha_no_futura
    ],
    columnas,
).withColumn("Fecha", F.to_date("Fecha")).withColumn("FechaPublicacion", F.to_date("FechaPublicacion"))

resultado = [r["_fallas"] for r in validador.evaluar(prueba, rules.REGLAS).select("_fallas").collect()]
esperado = [[], ["valor_no_negativo"], ["valor_no_nulo"], ["variable_conocida"], ["regulado_sin_clasificar"], ["fecha_no_futura"]]
for r, e in zip(resultado, esperado, strict=True):
    assert list(r) == e, f"evaluar: obtuve {list(r)}, esperaba {e}"
print("OK: evaluar() marca exactamente la regla que falla en cada fila")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Validar tu vista vigente
# MAGIC `aplicar()` evalúa, separa por acción y devuelve métricas. Escribe plata validada, cuarentena y métricas en tu esquema.

# COMMAND ----------

T_VALIDADA = f"{esquema}.demanda_validada"
T_CUARENTENA = f"{esquema}.demanda_cuarentena"
T_METRICAS = f"{esquema}.calidad_metricas"


def escribir(validas, cuarentena, metricas, origen):
    validas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_VALIDADA)
    cuarentena.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_CUARENTENA)
    (spark.createDataFrame(metricas).withColumn("origen", F.lit(origen))
        .write.mode("append").option("mergeSchema", "true").saveAsTable(T_METRICAS))
    print(f"validas={validas.count()}  cuarentena={cuarentena.count()}")
    spark.createDataFrame(metricas).select("regla", "accion", "fallas", "evaluadas").display()


validas, cuarentena, metricas = validador.aplicar(spark.table(vigente), rules.REGLAS)
escribir(validas, cuarentena, metricas, "vigente")
# Esperado: validas=145408  cuarentena=0, y 0 fallas en las cinco reglas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Llegan siete filas malas
# MAGIC Simulan errores reales: un valor negativo, un valor nulo, una variable desconocida, un regulado con CIIU,
# MAGIC una fecha del futuro y una serie nueva con pérdidas mayores que la demanda. Se unen a la vista vigente y se valida.

# COMMAND ----------

malas = spark.createDataFrame(
    [
        ("2026-07-20", "DdaReal", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", -5.0, "2026-08-02"),          # cuarentena
        ("2026-07-20", "PerdidasEnergia", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", None, "2026-08-02"),   # drop
        ("2026-07-20", "Otra", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", 12.0, "2026-08-02"),              # drop
        ("2026-07-20", "DdaReal", "ZZZ1", "ZZZ1", "Regulado", "EDUCACIÓN", 50.0, "2026-08-02"),                # warn
        ("2030-01-01", "DdaReal", "EPSC", "EPSC", "Regulado", "SIN CLASIFICAR", 100.0, "2026-08-02"),          # fail
        ("2026-07-20", "DdaReal", "ZZZ2", "ZZZ2", "No Regulado", "EDUCACIÓN", 100.0, "2026-08-02"),            # balance: demanda
        ("2026-07-20", "PerdidasEnergia", "ZZZ2", "ZZZ2", "No Regulado", "EDUCACIÓN", 200.0, "2026-08-02"),    # balance: pérdidas > demanda
    ],
    columnas,
).withColumn("Fecha", F.to_date("Fecha")).withColumn("FechaPublicacion", F.to_date("FechaPublicacion"))
malas = (malas.withColumn("_ingested_at", F.current_timestamp())
         .withColumn("_source_file", F.lit("sintetico_lab05"))
         .withColumn("_publication_date", F.col("FechaPublicacion"))
         .withColumn("_rescued_data", F.lit(None).cast("string")))

con_malas = spark.table(vigente).unionByName(malas, allowMissingColumns=True)
print("Filas a validar:", con_malas.count())

# COMMAND ----------

try:
    validador.aplicar(con_malas, rules.REGLAS)
except validador.ReglaFail as e:
    print("El validador se detuvo, como debe:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** La regla `fail` detuvo todo por una sola fila. ¿Es la decisión correcta para una fecha del futuro? ¿Cuándo preferirías cuarentena?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# Quitamos la fila del futuro (en la vida real: se rechaza el archivo y se avisa a la fuente) y validamos de nuevo
sin_futuro = con_malas.filter("Fecha <= current_date()")
validas, cuarentena, metricas = validador.aplicar(sin_futuro, rules.REGLAS)
escribir(validas, cuarentena, metricas, "vigente+sinteticas")
cuarentena.select("Fecha", "CodigoVariable", "CodigoSICAgente", "Valor", "_fallas").display()
# Esperado: validas=145411 (145.408 + warn + 2 de balance)  cuarentena=1  (drop: 2 filas descartadas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Balance por serie-día: la regla del sector
# MAGIC Formato ancho (una fila por serie-día) sobre plata validada, y `REGLAS_BALANCE`: pérdidas ≤ demanda.

# COMMAND ----------

LLAVE_SERIE = ["CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial"]
balance = spark.table(T_VALIDADA).groupBy("Fecha", *LLAVE_SERIE).agg(
    F.max(F.when(F.col("CodigoVariable") == "DdaReal", F.col("Valor"))).alias("demanda_real_kwh"),
    F.max(F.when(F.col("CodigoVariable") == "PerdidasEnergia", F.col("Valor"))).alias("perdidas_kwh"),
    F.max("_publication_date").alias("_publication_date"),
)
b_validas, b_cuarentena, b_metricas = validador.aplicar(balance, rules.REGLAS_BALANCE)
b_validas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{esquema}.balance_serie_dia")
b_cuarentena.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{esquema}.balance_cuarentena")
(spark.createDataFrame(b_metricas).withColumn("origen", F.lit("balance"))
    .write.mode("append").option("mergeSchema", "true").saveAsTable(T_METRICAS))
print(f"balance validas={b_validas.count()}  cuarentena={b_cuarentena.count()}")
b_cuarentena.display()
# Esperado: validas=72705  cuarentena=1 (la serie ZZZ2 con pérdidas > demanda); perdidas_presentes: 1 warn (ZZZ1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — El reporte de calidad
# MAGIC `calidad_metricas` acumula una fila por regla y ejecución. Es la respuesta a "¿cuántas filas fallaron cada regla y cuándo?".

# COMMAND ----------

spark.sql(f"""
SELECT ejecutado_en, origen, regla, accion, fallas, evaluadas,
       round(fallas / evaluadas * 100, 4) AS pct_fallas
FROM {T_METRICAS}
ORDER BY ejecutado_en DESC, origen, regla
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — El pipeline declarativo
# MAGIC Abre `notebooks/02_silver_pipeline.py`. Usa las mismas `REGLAS`, pero como decoradores de expectation, y el
# MAGIC patrón cuarentena con `_fallas`. La docente lo corrió en `dev` en la demo.
# MAGIC
# MAGIC **Pregunta.** ¿Qué cambia entre el validador por lotes y el pipeline? (reglas, quién decide el orden, dónde quedan
# MAGIC las métricas, qué pasa con `fail`). ¿Cuál usarías en producción y por qué?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación automática

# COMMAND ----------


def verificar():
    checks = []

    def n(t):
        try:
            return spark.table(t).count()
        except Exception:  # noqa: BLE001
            return -1

    checks.append(("demanda_validada con 145.411 filas", n(T_VALIDADA) == 145_411))
    checks.append(("demanda_cuarentena con 1 fila (valor negativo)", n(T_CUARENTENA) == 1))
    checks.append(("balance_serie_dia con 72.705 filas", n(f"{esquema}.balance_serie_dia") == 72_705))
    checks.append(("balance_cuarentena con 1 fila (pérdidas > demanda)", n(f"{esquema}.balance_cuarentena") == 1))
    cols = set(spark.table(T_VALIDADA).columns) if n(T_VALIDADA) >= 0 else set()
    checks.append(("_fallas presente en demanda_validada", "_fallas" in cols))
    try:
        reglas_en_metricas = {r[0] for r in spark.table(T_METRICAS).select("regla").distinct().collect()}
        checks.append(("calidad_metricas cubre las 7 reglas", set(rules.REGLAS) | set(rules.REGLAS_BALANCE) <= reglas_en_metricas))
        ejecuciones = spark.table(T_METRICAS).select("ejecutado_en").distinct().count()
        checks.append(("calidad_metricas con al menos 3 ejecuciones", ejecuciones >= 3))
    except Exception:  # noqa: BLE001
        checks.append(("calidad_metricas existe", False))
    try:
        r = validador.evaluar(prueba, rules.REGLAS).select("_fallas").collect()
        checks.append(("evaluar() implementada", list(r[1]["_fallas"]) == ["valor_no_negativo"]))
    except NotImplementedError:
        checks.append(("evaluar() implementada", False))

    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/xm_demanda/quality)." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
