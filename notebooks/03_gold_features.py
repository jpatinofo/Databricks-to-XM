# Databricks notebook source
# MAGIC %md
# MAGIC # Construcción de features en oro
# MAGIC Clase 7. Notebook delgado: recibe parámetros del job y llama a `xm_demanda.features.build`.
# MAGIC En producción lo ejecuta un job (clase 12) con `catalog = dev | qa | prod`, después del pipeline de plata.
# MAGIC
# MAGIC Lee `silver_energia.demanda_diaria` y `data/reference/festivos_co.csv`; escribe `gold_energia.features_demanda_diaria`
# MAGIC con MERGE por llave. Lags, ventanas y horizonte vienen de `conf/base.yml` (`modelo`).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import os
import sys

import pandas as pd
from pyspark.sql import functions as F

raiz = os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(raiz, "src"))

from xm_demanda.features.build import LLAVE, construir_features, merge_features
from xm_demanda.utils.config import fqn, load_config

cfg = load_config(catalog)
fuente = fqn(cfg, "silver", "silver_diaria")
destino = fqn(cfg, "gold", "gold_features")
ruta_festivos = os.path.join(raiz, "data", "reference", "festivos_co.csv")
print(fuente, "+ festivos →", destino)

# COMMAND ----------

diaria = spark.table(fuente)
festivos = spark.createDataFrame(pd.read_csv(ruta_festivos, parse_dates=["fecha"]))
festivos = festivos.withColumn("fecha", F.to_date("fecha"))

features = construir_features(diaria, festivos, cfg)
merge_features(spark, features, destino)

# COMMAND ----------

# Contrato mínimo de la feature table: llave primaria (una sola vez; idempotente).
llave_sql = ", ".join(LLAVE)
for c in LLAVE:
    spark.sql(f"ALTER TABLE {destino} ALTER COLUMN {c} SET NOT NULL")
spark.sql(f"ALTER TABLE {destino} DROP CONSTRAINT IF EXISTS pk_features_demanda_diaria")
spark.sql(f"ALTER TABLE {destino} ADD CONSTRAINT pk_features_demanda_diaria PRIMARY KEY ({llave_sql})")

# COMMAND ----------

# Verificación que rompe el job si oro quedó mal (el equivalente de una expectation 'fail').
t = spark.table(destino)
sin_calendario = t.filter("es_festivo IS NULL OR dia_semana IS NULL").count()
assert sin_calendario == 0, f"{sin_calendario} filas sin calendario: ¿falta un año en festivos_co.csv?"
display(spark.sql(f"""
    SELECT count(*) AS filas, min(fecha) AS desde, max(fecha) AS hasta,
           sum(CASE WHEN y_h{cfg['modelo']['horizon_days']} IS NULL THEN 1 ELSE 0 END) AS filas_inferencia,
           max(_features_built_at) AS construido
    FROM {destino}
"""))
