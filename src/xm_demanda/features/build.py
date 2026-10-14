"""Oro: features para pronóstico de demanda diaria, construidas sobre silver.demanda_diaria.

Regla de todas las funciones: una feature en t solo usa información disponible en t
(ventanas que terminan en t-1, calendario conocido de antemano). El objetivo `y_h{h}`
es lead(h) y solo existe para entrenar.

`nombres_lags`, `nombres_ventanas` y `es_fin_de_semana` son puros y se prueban sin Spark.
"""

from xm_demanda.silver.transform import LLAVE, LLAVE_SERIE

MEDIDA = "demanda_real_kwh"


def nombres_lags(lags: list[int]) -> list[str]:
    """[1, 7] → ['lag_1', 'lag_7']. Los lags deben ser enteros positivos únicos."""
    if any((not isinstance(k, int)) or k <= 0 for k in lags):
        raise ValueError(f"Los lags deben ser enteros positivos: {lags}")
    if len(set(lags)) != len(lags):
        raise ValueError(f"Lags repetidos: {lags}")
    return [f"lag_{k}" for k in lags]


def nombres_ventanas(ventanas: list[int]) -> list[str]:
    """[7, 28] → ['media_7', 'media_28', 'desv_7', 'desv_28']."""
    if any((not isinstance(n, int)) or n <= 1 for n in ventanas):
        raise ValueError(f"Las ventanas deben ser enteros > 1: {ventanas}")
    return [f"media_{n}" for n in ventanas] + [f"desv_{n}" for n in ventanas]


def es_fin_de_semana(dia_semana: int) -> bool:
    """dia_semana con la convención de Spark dayofweek: 1 = domingo … 7 = sábado."""
    return dia_semana in (1, 7)


def _ventana_serie():
    from pyspark.sql.window import Window

    return Window.partitionBy(*LLAVE_SERIE).orderBy("fecha")


def agregar_calendario(df, festivos):
    """dia_semana (1=dom…7=sáb), es_fin_de_semana, es_festivo, mes, dia_del_anio.

    `festivos` es un DataFrame con la columna `fecha` (date). Un left join: si un año no está en
    el archivo de referencia, es_festivo queda nulo y verificar() lo detecta.
    """
    from pyspark.sql import functions as F  # noqa: F401

    # TODO (lab 7, sección 3): implementar. Pistas: F.dayofweek, F.month, F.dayofyear,
    # F.col("dia_semana").isin(1, 7), y un left join con festivos.select("fecha", F.lit(True)...).
    raise NotImplementedError("agregar_calendario: completa esta función (lab 7, sección 3)")


def agregar_lags(df, lags: list[int]):
    """lag_k = valor de MEDIDA k días antes, dentro de la misma serie (nulo si no hay historia)."""
    from pyspark.sql import functions as F  # noqa: F401

    # TODO (lab 7, sección 4): implementar con F.lag(MEDIDA, k).over(_ventana_serie()).
    raise NotImplementedError("agregar_lags: completa esta función (lab 7, sección 4)")


def agregar_ventanas(df, ventanas: list[int]):
    """media_n y desv_n sobre los n días ANTERIORES (t-n … t-1). Nunca incluye t."""
    from pyspark.sql import functions as F

    w = _ventana_serie()
    for n in ventanas:
        pasado = w.rowsBetween(-n, -1)
        df = df.withColumn(f"media_{n}", F.avg(MEDIDA).over(pasado))
        df = df.withColumn(f"desv_{n}", F.stddev(MEDIDA).over(pasado))
    return df


def agregar_banderas(df):
    """es_cero (demanda 0: ¿real o falta de medida?) y dias_con_datos acumulados por serie."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = _ventana_serie().rowsBetween(Window.unboundedPreceding, 0)  # desde el inicio de la serie
    df = df.withColumn("es_cero", F.col(MEDIDA) == 0)
    return df.withColumn("dias_con_datos", F.count(MEDIDA).over(w))


def agregar_objetivo(df, horizonte: int):
    """y_h{h} = MEDIDA en t+h. Nulo en los últimos h días de cada serie (filas para inferir)."""
    from pyspark.sql import functions as F

    return df.withColumn(f"y_h{horizonte}", F.lead(MEDIDA, horizonte).over(_ventana_serie()))


def construir_features(df, festivos, cfg: dict):
    """Encadena todo. `cfg` = load_config(...): modelo.{lags, ventanas, horizon_days}."""
    from pyspark.sql import functions as F

    m = cfg["modelo"]
    out = (
        df.transform(lambda d: agregar_calendario(d, festivos))
        .transform(lambda d: agregar_lags(d, m["lags"]))
        .transform(lambda d: agregar_ventanas(d, m["ventanas"]))
        .transform(agregar_banderas)
        .transform(lambda d: agregar_objetivo(d, m["horizon_days"]))
    )
    return out.withColumn("_features_built_at", F.current_timestamp())


def merge_features(spark, df, tabla: str) -> None:
    """Upsert por LLAVE: idempotente; una republicación en plata actualiza sus filas de oro."""
    if not spark.catalog.tableExists(tabla):
        df.limit(0).write.format("delta").saveAsTable(tabla)
    df.createOrReplaceTempView("_fuente_features")
    condicion = " AND ".join(f"t.{c} = s.{c}" for c in LLAVE)
    spark.sql(f"""
        MERGE INTO {tabla} AS t
        USING _fuente_features AS s
        ON {condicion}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
