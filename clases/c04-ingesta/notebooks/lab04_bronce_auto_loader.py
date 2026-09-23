# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 — Bronce con Auto Loader: 23 publicaciones y una republicación
# MAGIC
# MAGIC Conviertes el Excel en 23 archivos (uno por `FechaPublicacion`), dejas que Auto Loader los cargue,
# MAGIC compruebas que correr dos veces no duplica, y resuelves la llegada de `raw_v2`: días ya cargados,
# MAGIC valores corregidos y una columna renombrada.
# MAGIC
# MAGIC El código de ingesta vive en `src/xm_demanda/ingest/bronze.py`. Este notebook solo lo llama.
# MAGIC Las celdas con `TODO` son las que escribes tú (en `bronze.py`, no aquí).

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

esquema = f"workspace.c01_{usuario}"
volumen = f"/Volumes/workspace/c01_{usuario}/raw"
landing = f"{volumen}/landing"
checkpoint = f"{volumen}/_checkpoints/bronze"
tabla = f"{esquema}.demanda_raw_bronze"
print("Landing:", landing, "| Tabla:", tabla)

# COMMAND ----------

# Importar el paquete del repo desde el Git folder (src/ no está en el path por defecto)
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

import importlib

import xm_demanda.ingest.bronze as bronze

importlib.reload(bronze)  # recarga tus cambios en bronze.py sin reiniciar
print("Paquete:", bronze.__file__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Excel → 23 CSV en landing
# MAGIC Un archivo por `FechaPublicacion`. Simula 23 llegadas separadas en el tiempo.

# COMMAND ----------

import pandas as pd

xlsx = [f for f in os.listdir(volumen) if f.lower().endswith(".xlsx")]
assert xlsx, f"No hay .xlsx en {volumen}: sube DemandaPerdidas.xlsx al volumen raw (lab 1)."
df = pd.read_excel(os.path.join(volumen, xlsx[0]))
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
df["FechaPublicacion"] = pd.to_datetime(df["FechaPublicacion"]).dt.date

columnas = [n for n, _ in bronze.COLUMNAS_RAW]  # mismo orden que el esquema declarado
os.makedirs(landing, exist_ok=True)
for pub, parte in df.groupby("FechaPublicacion"):
    parte[columnas].to_csv(f"{landing}/demanda_{pub}.csv", index=False)

archivos = sorted(os.listdir(landing))
print(len(archivos), "archivos:", archivos[:3], "…", archivos[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Auto Loader → bronce
# MAGIC Esquema explícito, columna de rescate, `availableNow`. Se ejecuta con las funciones tal como vienen
# MAGIC (`resolver_alias` y `con_metadatos` aún no están implementadas: verás el error y lo arreglas en el paso 3).
# MAGIC
# MAGIC **Antes de ejecutar:** abre `src/xm_demanda/ingest/bronze.py` y lee `ingestar_bronce`. Ese es todo el pipeline.

# COMMAND ----------

try:
    bronze.ingestar_bronce(spark, landing, tabla, checkpoint)
except Exception as e:  # noqa: BLE001
    print("La ingesta falló. Es esperado la primera vez:", str(e).splitlines()[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Implementar en `src` (y probar)
# MAGIC En `src/xm_demanda/ingest/bronze.py`:
# MAGIC
# MAGIC 1. `resolver_alias(columnas_rescatadas)` → `{alias: canónica}` solo para los alias conocidos (dict `ALIAS`).
# MAGIC 2. `con_metadatos(df)` → agrega `_ingested_at` (`current_timestamp()`), `_source_file` (`_metadata.file_path`) y `_publication_date` (`to_date(FechaPublicacion)`).
# MAGIC
# MAGIC Guarda el archivo y ejecuta la celda siguiente: recarga el módulo y prueba las dos funciones con datos pequeños.

# COMMAND ----------

importlib.reload(bronze)

assert bronze.resolver_alias(["ValorKwh", "Otra"]) == {"ValorKwh": "Valor"}, "resolver_alias: revisa el caso con un alias conocido y uno desconocido"
assert bronze.resolver_alias(["Valor"]) == {}, "resolver_alias: sin alias debe devolver {}"

_prueba = spark.read.csv(f"{landing}/{archivos[0]}", header=True)
_con = bronze.con_metadatos(_prueba)
assert set(bronze.METADATOS) <= set(_con.columns), f"con_metadatos: faltan columnas, tienes {_con.columns}"
assert _con.select("_source_file").first()[0].endswith(archivos[0]), "_source_file debe venir de _metadata.file_path"
print("OK: resolver_alias y con_metadatos funcionan")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Ingesta completa y prueba de idempotencia
# MAGIC Corre la ingesta. Luego córrela **otra vez**. El conteo no debe cambiar.

# COMMAND ----------

bronze.ingestar_bronce(spark, landing, tabla, checkpoint)


def conteo():
    r = spark.sql(f"SELECT count(*) AS filas, count(DISTINCT _source_file) AS archivos, sum(CASE WHEN Valor IS NULL THEN 1 ELSE 0 END) AS valor_nulo FROM {tabla}").first()
    print(f"filas={r.filas}  archivos={r.archivos}  valor_nulo={r.valor_nulo}")
    return r


primera = conteo()

# COMMAND ----------

bronze.ingestar_bronce(spark, landing, tabla, checkpoint)  # otra vez
segunda = conteo()
assert segunda.filas == primera.filas == 145_408, "Correr dos veces cambió el conteo: revisa el checkpoint"
print("Idempotente: 145.408 filas las dos veces")

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** ¿Qué pasaría si borras la carpeta `_checkpoints/bronze` y vuelves a correr? ¿Y si la borras junto con la tabla?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Llega `raw_v2`
# MAGIC XM republica los últimos 7 días con valores corregidos (+1,5 %) y, además, cambió el nombre de la columna
# MAGIC `Valor` → `ValorKwh`. Esta celda genera el archivo y lo deja en landing, como lo haría un job de descarga.

# COMMAND ----------

ultimos = sorted(df["Fecha"].unique())[-7:]
v2 = df[df["Fecha"].isin(ultimos)][columnas].copy()
v2["Valor"] = (v2["Valor"] * 1.015).round(2)
v2["FechaPublicacion"] = pd.to_datetime("2026-08-01").date()
v2 = v2.rename(columns={"Valor": "ValorKwh"})
v2.to_csv(f"{landing}/demanda_2026-08-01_raw_v2.csv", index=False)
print(len(v2), "filas republicadas; columnas:", list(v2.columns))

# COMMAND ----------

bronze.ingestar_bronce(spark, landing, tabla, checkpoint)
tercera = conteo()

spark.sql(f"""
SELECT _source_file, count(*) AS filas, sum(CASE WHEN Valor IS NULL THEN 1 ELSE 0 END) AS valor_nulo,
       min(_rescued_data) AS ejemplo_rescatado
FROM {tabla} WHERE _source_file LIKE '%raw_v2%' GROUP BY ALL
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** Si `valor_nulo` es 0 arriba, `normalizar()` ya resolvió el alias `ValorKwh` usando `_rescued_data` (mira la función).
# MAGIC Si es > 0, algo falta: ¿está `ValorKwh` en el dict `ALIAS`? ¿`resolver_alias` lo reconoce?
# MAGIC
# MAGIC ¿Por qué el valor rescatado llega como texto dentro de un JSON y no como columna? ¿Qué modo de `schemaEvolutionMode` lo habría agregado como columna, y qué riesgo tiene?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Reconstruir bronce desde landing
# MAGIC Landing es la fuente de verdad; bronce se puede reconstruir siempre. Borra tabla y checkpoint y vuelve a correr:
# MAGIC debes llegar al mismo estado (150.350 filas, 24 archivos, 0 nulos). Ese es el segundo sentido de idempotencia.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {tabla}")
dbutils.fs.rm(checkpoint, recurse=True)
bronze.ingestar_bronce(spark, landing, tabla, checkpoint)
cuarta = conteo()
assert cuarta.filas == 150_350 and cuarta.archivos == 24 and cuarta.valor_nulo == 0, "La reconstrucción no coincide"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Vista vigente
# MAGIC Bronce acumula; la vista elige la publicación más reciente por serie-día-variable.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {esquema}.demanda_raw_vigente
COMMENT 'Última publicación por serie-día-variable sobre demanda_raw_bronze'
AS
SELECT * EXCEPT (rn) FROM (
  SELECT *,
    row_number() OVER (
      PARTITION BY Fecha, CodigoVariable, CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
      ORDER BY _publication_date DESC, _ingested_at DESC
    ) AS rn
  FROM {tabla})
WHERE rn = 1
""")

spark.sql(f"""
SELECT (SELECT count(*) FROM {tabla}) AS bronce,
       (SELECT count(*) FROM {esquema}.demanda_raw_vigente) AS vigente,
       (SELECT count(DISTINCT Fecha) FROM {tabla} GROUP BY Fecha HAVING count(DISTINCT _publication_date) > 1 LIMIT 1) IS NOT NULL AS hay_dias_con_dos_versiones
""").display()

# Cuánto cambió la corrección en los 7 días republicados
spark.sql(f"""
SELECT Fecha, count(DISTINCT _publication_date) AS versiones,
       round(sum(CASE WHEN _publication_date = '2026-08-01' THEN Valor END) / sum(CASE WHEN _publication_date <> '2026-08-01' THEN Valor END) - 1, 4) AS cambio_relativo
FROM {tabla} WHERE CodigoVariable = 'DdaReal'
GROUP BY Fecha HAVING versiones > 1 ORDER BY Fecha
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — ADR-002
# MAGIC Abre `docs/adr/ADR-002-republicaciones.md` y escribe las tres decisiones (acumular vs. sobrescribir, llave de versión, qué es vigente)
# MAGIC con al menos una alternativa descartada por cada una. Estado `aceptada`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación automática

# COMMAND ----------

import re


def verificar():
    checks = []
    r = spark.sql(f"SELECT count(*) AS filas, count(DISTINCT _source_file) AS archivos, sum(CASE WHEN Valor IS NULL THEN 1 ELSE 0 END) AS nulos FROM {tabla}").first()
    checks.append(("Bronce con 150.350 filas", r.filas == 150_350))
    checks.append(("Bronce con 24 archivos de origen", r.archivos == 24))
    checks.append(("Sin Valor nulo (alias resuelto)", r.nulos == 0))
    cols = set(spark.table(tabla).columns)
    checks.append(("Metadatos _ingested_at, _source_file, _publication_date", set(bronze.METADATOS) <= cols))
    checks.append(("Columna _rescued_data conservada", bronze.COLUMNA_RESCATE in cols))
    try:
        v = spark.sql(f"SELECT count(*) FROM {esquema}.demanda_raw_vigente").first()[0]
    except Exception:  # noqa: BLE001
        v = -1
    checks.append(("Vista vigente con 145.408 filas", v == 145_408))
    dos = spark.sql(f"SELECT count(*) FROM (SELECT Fecha FROM {tabla} GROUP BY Fecha HAVING count(DISTINCT _publication_date) > 1)").first()[0]
    checks.append(("7 días con dos versiones en bronce", dos == 7))
    try:
        checks.append(("resolver_alias implementada", bronze.resolver_alias(["ValorKwh"]) == {"ValorKwh": "Valor"}))
    except NotImplementedError:
        checks.append(("resolver_alias implementada", False))

    adr = os.path.join(raiz, "docs", "adr", "ADR-002-republicaciones.md")
    existe = os.path.exists(adr)
    checks.append(("ADR-002 existe", existe))
    if existe:
        txt = open(adr, encoding="utf-8").read()
        checks.append(("ADR-002 en estado aceptada", re.search(r"\*\*Estado:\*\*\s*aceptada", txt) is not None))
        decision = re.search(r"## Decisión\s*\n(.*?)\n## ", txt, re.S)
        checks.append(("ADR-002 con decisión escrita (sin <…>)", decision is not None and len(decision.group(1).strip()) > 120 and "<" not in decision.group(1)))
        alt = re.search(r"## Alternativas consideradas\s*\n(.*?)\n## ", txt, re.S)
        n_alt = len(re.findall(r"^\s*[-*]\s", alt.group(1), re.M)) if alt and "<" not in alt.group(1) else 0
        checks.append(("ADR-002 con al menos 3 alternativas descartadas", n_alt >= 3))

    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/, tests/ y docs/adr/)." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
