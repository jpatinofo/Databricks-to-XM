# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Bronze con Auto Loader desde el volumen landing
# MAGIC Clase 4. Notebook delgado: recibe parámetros del job y llama a `xm_demanda.ingest.bronze`.
# MAGIC En producción lo ejecuta un job (clase 12) con `catalog = dev | qa | prod`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import os
import sys

raiz = os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(raiz, "src"))

from xm_demanda.ingest.bronze import ingestar_bronce
from xm_demanda.utils.config import fqn, load_config

cfg = load_config(catalog)
landing = cfg["volumen_landing"]
tabla = fqn(cfg, "bronze", "bronze_raw")
checkpoint = f"{landing}/../_checkpoints/{cfg['tablas']['bronze_raw']}"
print(landing, "→", tabla)

# COMMAND ----------

ingestar_bronce(spark, landing, tabla, checkpoint)
display(spark.sql(f"SELECT count(*) AS filas, count(DISTINCT _source_file) AS archivos, max(_ingested_at) AS ultima_ingesta FROM {tabla}"))
