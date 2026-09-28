# Databricks notebook source
# MAGIC %md
# MAGIC # Preparar `dev` para la demo de la clase 5 (solo docente)
# MAGIC
# MAGIC 1. Deja los 23 CSV del caso en `/Volumes/dev/bronze_energia/landing` (creado en la clase 3).
# MAGIC 2. Corre la ingesta bronce de `src` hacia `dev.bronze_energia.demanda_raw` (la misma del lab 4).
# MAGIC 3. Luego, en la UI: **Jobs & Pipelines → Create → ETL pipeline**, nombre `[xm-demanda] silver_energia`,
# MAGIC    source code `notebooks/02_silver_pipeline.py` (dentro del Git folder), catálogo `dev`, esquema `silver_energia`,
# MAGIC    serverless. Configuration: `catalog = dev`. No lo ejecutes: se ejecuta en vivo.
# MAGIC
# MAGIC Ejecutar desde el Git folder (usa `src/` y `data/raw`).

# COMMAND ----------

import os
import sys

import pandas as pd


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("Ejecuta este notebook desde el Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
from xm_demanda.ingest.bronze import COLUMNAS_RAW, ingestar_bronce  # noqa: E402

landing = "/Volumes/dev/bronze_energia/landing"
checkpoint = "/Volumes/dev/bronze_energia/landing/../_checkpoints/demanda_raw"
tabla = "dev.bronze_energia.demanda_raw"

# COMMAND ----------

df = pd.read_excel(os.path.join(raiz, "data", "raw", "DemandaPerdidas.xlsx"))
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
df["FechaPublicacion"] = pd.to_datetime(df["FechaPublicacion"]).dt.date
columnas = [n for n, _ in COLUMNAS_RAW]
os.makedirs(landing, exist_ok=True)
for pub, parte in df.groupby("FechaPublicacion"):
    parte[columnas].to_csv(f"{landing}/demanda_{pub}.csv", index=False)
print(len(os.listdir(landing)), "archivos en", landing)

# COMMAND ----------

ingestar_bronce(spark, landing, tabla, checkpoint)
spark.sql(f"SELECT count(*) AS filas, count(DISTINCT _source_file) AS archivos FROM {tabla}").display()
# Esperado: 145408 · 23
