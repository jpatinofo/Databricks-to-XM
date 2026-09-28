# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline declarativo bronce → plata con expectations
# MAGIC Clase 5. Se ejecuta como **pipeline** (Jobs & Pipelines → ETL pipeline), no como notebook.
# MAGIC Configuración del pipeline: `catalog` (dev | qa | prod), esquema destino `silver_energia`.
# MAGIC
# MAGIC Las reglas vienen de `src/xm_demanda/quality/rules.py`: el pipeline no las repite, las importa.
# MAGIC Tablas: `demanda_vigente` → `demanda_validada` / `demanda_cuarentena` → `balance_serie_dia`.

# COMMAND ----------

import os
import sys

try:
    from pyspark import pipelines as dp
except ImportError:  # workspaces con la API anterior
    import dlt as dp

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: el pipeline debe apuntar al notebook dentro del Git folder.")


sys.path.insert(0, os.path.join(_raiz_repo(), "src"))

from xm_demanda.quality.rules import REGLAS, REGLAS_BALANCE  # noqa: E402

catalog = spark.conf.get("catalog", "dev")
bronce = f"{catalog}.bronze_energia.demanda_raw"
LLAVE = ["Fecha", "CodigoVariable", "CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial"]
LLAVE_SERIE = LLAVE[2:]

# COMMAND ----------


def con_expectations(reglas):
    """Convierte el diccionario de reglas en decoradores de expectation según su acción."""
    mapa = {"warn": dp.expect, "drop": dp.expect_or_drop, "fail": dp.expect_or_fail}

    def decorar(fn):
        for nombre, (expr, accion) in reglas.items():
            if accion in mapa:  # cuarentena no es un decorador: es el patrón de abajo
                fn = mapa[accion](nombre, expr)(fn)
        return fn

    return decorar


def marcar_fallas(df, reglas):
    """Columna _fallas con los nombres de las reglas que no se cumplen (mismo criterio que validador.evaluar)."""
    marcas = [F.when(~F.expr(expr), F.lit(n)) for n, (expr, _) in reglas.items()]
    return df.withColumn("_fallas", F.array_compact(F.array(*marcas)))


# COMMAND ----------


@dp.table(name="demanda_vigente", comment="Última publicación por serie-día-variable sobre bronce")
def demanda_vigente():
    w = Window.partitionBy(*LLAVE).orderBy(F.col("_publication_date").desc(), F.col("_ingested_at").desc())
    return spark.read.table(bronce).withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


@dp.table(name="demanda_validada", comment="Filas que cumplen todas las reglas (warn cuenta, no bloquea)")
@con_expectations(REGLAS)
def demanda_validada():
    df = marcar_fallas(spark.read.table("demanda_vigente"), REGLAS)
    cuarentena = [n for n, (_, a) in REGLAS.items() if a == "cuarentena"]
    return df.filter(~F.arrays_overlap("_fallas", F.array(*[F.lit(n) for n in cuarentena])))


@dp.table(name="demanda_cuarentena", comment="Filas con fallas de cuarentena: alguien las revisa")
def demanda_cuarentena():
    df = marcar_fallas(spark.read.table("demanda_vigente"), REGLAS)
    cuarentena = [n for n, (_, a) in REGLAS.items() if a == "cuarentena"]
    return df.filter(F.arrays_overlap("_fallas", F.array(*[F.lit(n) for n in cuarentena])))


@dp.table(name="balance_serie_dia", comment="Una fila por serie-día: demanda y pérdidas. Regla del sector: pérdidas ≤ demanda")
@con_expectations({n: (e, "warn") for n, (e, _) in REGLAS_BALANCE.items()})
def balance_serie_dia():
    df = spark.read.table("demanda_validada")
    return df.groupBy("Fecha", *LLAVE_SERIE).agg(
        F.max(F.when(F.col("CodigoVariable") == "DdaReal", F.col("Valor"))).alias("demanda_real_kwh"),
        F.max(F.when(F.col("CodigoVariable") == "PerdidasEnergia", F.col("Valor"))).alias("perdidas_kwh"),
        F.max("_publication_date").alias("_publication_date"),
    )
