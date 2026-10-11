"""Plata: de bronce validado (largo) a tablas analíticas (ancho) con MERGE idempotente.

`normalizar_texto` y `ALIAS_CIIU` son puros y se prueban sin Spark (tests/unit/test_silver.py).
"""

# Llave de negocio de demanda_diaria: una fila por serie y día.
LLAVE_SERIE = [
    "codigo_sic_agente",
    "mercado_comercializacion",
    "tipo_mercado",
    "clasificacion_industrial",
]
LLAVE = ["fecha", *LLAVE_SERIE]

# Cómo se llaman las columnas de la fuente en plata (snake_case, sin abreviaturas ambiguas).
RENOMBRES = {
    "Fecha": "fecha",
    "CodigoSICAgente": "codigo_sic_agente",
    "MercadoComercializacion": "mercado_comercializacion",
    "TipoMercado": "tipo_mercado",
    "ClasificacionIndustrial": "clasificacion_industrial",
}

# Textos de la fuente que no coinciden con el nombre oficial DANE (digitación, variantes).
# Llave: texto normalizado tal como llega; valor: texto normalizado del nombre oficial.
ALIAS_CIIU: dict[str, str] = {
    # La fuente escribe "ENTRETENEMIENTO" (error de digitación); el DANE, "ENTRETENIMIENTO".
    "ACTIVIDADES ARTISTICAS, DE ENTRETENEMIENTO Y RECREACION": (
        "ACTIVIDADES ARTISTICAS, DE ENTRETENIMIENTO Y RECREACION"
    ),
}


def normalizar_texto(texto: str | None) -> str | None:
    """Mayúsculas, sin tildes ni diéresis, espacios colapsados. Llave de unión para textos.

    >>> normalizar_texto("  Educación  ")
    'EDUCACION'
    >>> normalizar_texto(None) is None
    True
    """
    import unicodedata

    if texto is None:
        return None
    sin_tildes = "".join(
        c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c)
    )
    return " ".join(sin_tildes.split()).upper()


def a_formato_ancho(df):
    """Largo (una fila por serie-día-variable) → ancho (una fila por serie-día).

    Salida: LLAVE + demanda_real_kwh + perdidas_kwh + _publication_date.
    """
    from pyspark.sql import functions as F

    renombrado = df
    for viejo, nuevo in RENOMBRES.items():
        renombrado = renombrado.withColumnRenamed(viejo, nuevo)
    es_demanda = F.col("CodigoVariable") == "DdaReal"
    es_perdidas = F.col("CodigoVariable") == "PerdidasEnergia"
    return renombrado.groupBy(*LLAVE).agg(
        F.max(F.when(es_demanda, F.col("Valor"))).alias("demanda_real_kwh"),
        F.max(F.when(es_perdidas, F.col("Valor"))).alias("perdidas_kwh"),
        F.max("_publication_date").alias("_publication_date"),
    )


def merge_demanda_diaria(spark, df, tabla: str) -> None:
    """Upsert idempotente: inserta serie-días nuevos; actualiza solo si la publicación es más nueva.

    Volver a correr con los mismos datos no toca ninguna fila (0 insertadas, 0 actualizadas).
    """
    if not spark.catalog.tableExists(tabla):
        df.limit(0).write.format("delta").saveAsTable(tabla)
    df.createOrReplaceTempView("_fuente_demanda_diaria")
    condicion = " AND ".join(f"t.{c} = s.{c}" for c in LLAVE)
    spark.sql(f"""
        MERGE INTO {tabla} AS t
        USING _fuente_demanda_diaria AS s
        ON {condicion}
        WHEN MATCHED AND s._publication_date > t._publication_date THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def construir_dim_ciiu(spark, ruta_csv: str, textos_fuente):
    """Dimensión CIIU: una fila por sección oficial, con la llave normalizada de unión.

    `textos_fuente`: DataFrame con la columna `clasificacion_industrial` (valores de la fuente).
    Devuelve (dim, sin_match): la dimensión y los textos de la fuente que no encontraron sección.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    normalizar = F.udf(normalizar_texto, StringType())

    import pandas as pd

    oficial = spark.createDataFrame(pd.read_csv(ruta_csv, dtype=str))
    oficial = oficial.withColumn("llave", normalizar("nombre_oficial"))

    fuente = textos_fuente.select("clasificacion_industrial").distinct()
    fuente = fuente.withColumn("llave_fuente", normalizar("clasificacion_industrial"))
    if ALIAS_CIIU:
        pares = [F.lit(x) for kv in ALIAS_CIIU.items() for x in kv]
        alias = F.create_map(*pares)
        llave = F.coalesce(alias[F.col("llave_fuente")], F.col("llave_fuente"))
    else:
        llave = F.col("llave_fuente")
    fuente = fuente.withColumn("llave", llave)

    dim = oficial.join(fuente, "llave", "left").select(
        "codigo",
        "nombre_oficial",
        F.col("clasificacion_industrial").alias("nombre_fuente"),
        "llave",
    )
    sin_match = fuente.join(oficial, "llave", "left_anti").select("clasificacion_industrial")
    return dim, sin_match
