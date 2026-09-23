"""Ingesta bronce con Auto Loader: esquema explícito, rescate, alias y metadatos.

Las funciones que reciben un DataFrame importan pyspark dentro de la función para que
el módulo se pueda importar (y probar `resolver_alias`) sin Spark instalado.
"""

# Columnas que XM publica hoy y su tipo. Bronce declara lo que espera: lo que no encaje
# va a `_rescued_data`, nunca se pierde en silencio.
COLUMNAS_RAW: list[tuple[str, str]] = [
    ("Fecha", "date"),
    ("CodigoVariable", "string"),
    ("CodigoSICAgente", "string"),
    ("MercadoComercializacion", "string"),
    ("TipoMercado", "string"),
    ("ClasificacionIndustrial", "string"),
    ("Valor", "double"),
    ("FechaPublicacion", "date"),
]

# Nombres alternativos que la fuente ha usado (o usará) para una columna canónica.
# Se completa cada vez que la fuente cambia; queda documentado aquí y en el ADR-002.
ALIAS: dict[str, str] = {
    "ValorKwh": "Valor",
    "valor_kwh": "Valor",
    "Fecha_Publicacion": "FechaPublicacion",
}

COLUMNA_RESCATE = "_rescued_data"
METADATOS = ["_ingested_at", "_source_file", "_publication_date"]


def esquema_raw():
    """StructType explícito de la fuente."""
    from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

    tipos = {"date": DateType(), "string": StringType(), "double": DoubleType()}
    return StructType([StructField(n, tipos[t], True) for n, t in COLUMNAS_RAW])


def resolver_alias(columnas_rescatadas: list[str], alias: dict[str, str] = ALIAS) -> dict[str, str]:
    """Dado el nombre de las columnas que llegaron fuera del esquema, devuelve {alias: canónica}
    solo para las que conocemos. Función pura: se prueba en tests/unit/test_bronze.py.

    >>> resolver_alias(["ValorKwh", "ColumnaDesconocida"])
    {'ValorKwh': 'Valor'}
    """
    # TODO (lab 4, paso 4): implementar.
    raise NotImplementedError("resolver_alias: completa esta función (lab 4, paso 4)")


def normalizar(df):
    """Rellena las columnas canónicas con el valor de sus alias cuando llegaron en `_rescued_data`.

    Ejemplo: raw_v2 trae `ValorKwh` en vez de `Valor`. Con esquema explícito, `Valor` llega nulo y
    `_rescued_data` trae '{"ValorKwh":"123.4", ...}'. Esta función deja `Valor = 123.4`.
    """
    from pyspark.sql import functions as F

    if COLUMNA_RESCATE not in df.columns:
        return df
    tipos = dict(COLUMNAS_RAW)
    for alias, canonica in resolver_alias(list(ALIAS)).items():
        if canonica in df.columns:
            json_alias = F.get_json_object(F.col(COLUMNA_RESCATE), f"$.{alias}")
            rescatado = json_alias.cast(tipos[canonica])
            df = df.withColumn(canonica, F.coalesce(F.col(canonica), rescatado))
    return df


def con_metadatos(df):
    """Agrega `_ingested_at`, `_source_file` (de `_metadata.file_path`) y `_publication_date`."""
    from pyspark.sql import functions as F  # noqa: F401

    # TODO (lab 4, paso 4): implementar. Pista: F.current_timestamp(), F.col("_metadata.file_path"),
    # F.to_date("FechaPublicacion").
    raise NotImplementedError("con_metadatos: completa esta función (lab 4, paso 4)")


def ingestar_bronce(spark, landing: str, tabla: str, checkpoint: str) -> None:
    """Carga a `tabla` (append) todos los archivos nuevos de `landing` y termina (availableNow)."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", COLUMNA_RESCATE)
        .option("header", "true")
        .schema(esquema_raw())
        .load(landing)
        .transform(normalizar)
        .transform(con_metadatos)
    )
    (
        stream.writeStream.option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(tabla)
        .awaitTermination()
    )
