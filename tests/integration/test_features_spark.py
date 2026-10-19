"""Pruebas de integración de xm_demanda.features sobre datos sintéticos pequeños.

Comprueban las REGLAS del contrato de oro (clase 7), no los valores de XM:
- el calendario no tiene nulos en años cubiertos y sí fuera de ellos,
- lag_k es exactamente el valor de k filas atrás dentro de la misma serie,
- las ventanas nunca incluyen el día t (fuga de información),
- y_h es nulo solo en las últimas h filas de cada serie.
Corren igual en CI (Spark local) y en Databricks (sesión activa).
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from xm_demanda.features import build as fb
from xm_demanda.utils.config import load_config

SERIES = [
    ("ENDD", "BOGOTA-CUNDINAMARCA", "Regulado", "SIN CLASIFICAR"),
    ("EPSC", "ANTIOQUIA", "No Regulado", "EDUCACION"),
    ("XYZ", "CARIBE", "No Regulado", "EXPLOTACION DE MINAS Y CANTERAS"),
]
INICIO = dt.date(2026, 1, 1)
COLS_PLATA = [*fb.LLAVE, fb.MEDIDA, "perdidas_kwh", "_publication_date", "ciiu_seccion"]


def _demanda(i: int, d: int, fecha: dt.date) -> float:
    """Determinista: base por serie, caída de fin de semana, ceros cada 13 días en la tercera."""
    if i == 2 and d % 13 == 0:
        return 0.0
    return 1000.0 + 10 * i + (d % 5) - (150.0 if fecha.weekday() >= 5 else 0.0)


@pytest.fixture(scope="module")
def diaria(spark):
    filas = []
    for i, serie in enumerate(SERIES):
        for d in range(0, 120 - 30 * i):  # series de 120, 90 y 60 días
            fecha = INICIO + dt.timedelta(days=d)
            ciiu = "X" if i == 0 else "P"
            filas.append((fecha, *serie, _demanda(i, d, fecha), 10.0, fecha, ciiu))
    return spark.createDataFrame(filas, COLS_PLATA)


@pytest.fixture(scope="module")
def festivos(spark):
    fechas = [dt.date(2026, 1, 1), dt.date(2026, 1, 12), dt.date(2026, 3, 23), dt.date(2026, 4, 2)]
    return spark.createDataFrame([(f, "festivo") for f in fechas], ["fecha", "nombre"])


@pytest.fixture(scope="module")
def features(diaria, festivos):
    try:
        return fb.construir_features(diaria, festivos, load_config("dev"))
    except NotImplementedError:
        pytest.skip("Pendiente: TODO del lab 7 (agregar_calendario / agregar_lags)")


def test_oro_solo_tiene_las_columnas_del_contrato(features):
    assert not {"perdidas_kwh", "_publication_date"} & set(features.columns)
    assert "ciiu_seccion" in features.columns and "_features_built_at" in features.columns


def test_calendario_sin_nulos_en_anio_cubierto(features):
    assert features.filter("es_festivo IS NULL OR dia_semana IS NULL").count() == 0
    assert features.filter("es_festivo").select("fecha").distinct().count() == 4


def test_calendario_nulo_fuera_de_los_anios_del_archivo(spark, festivos):
    fila = [(dt.date(2030, 1, 1), *SERIES[0], 1.0, 1.0, dt.date(2030, 1, 1), "X")]
    try:
        out = fb.agregar_calendario(spark.createDataFrame(fila, COLS_PLATA), festivos)
    except NotImplementedError:
        pytest.skip("Pendiente: TODO del lab 7")
    assert out.filter("es_festivo IS NULL").count() == 1


def test_lag_7_es_el_valor_de_siete_filas_atras(features):
    w = Window.partitionBy(*fb.LLAVE_SERIE).orderBy("fecha")
    esperado = features.withColumn("_e", F.lag(fb.MEDIDA, 7).over(w))
    assert esperado.filter("lag_7 IS NOT NULL AND lag_7 <> _e").count() == 0
    nulos = features.filter("lag_7 IS NULL").groupBy(*fb.LLAVE_SERIE).count()
    assert nulos.agg(F.max("count")).first()[0] == 7


def test_y_h7_nulo_solo_en_las_ultimas_siete_filas(features):
    nulos = features.filter("y_h7 IS NULL").groupBy(*fb.LLAVE_SERIE).count()
    assert nulos.count() == len(SERIES)
    assert nulos.agg(F.max("count")).first()[0] == 7


def test_banderas(features):
    assert features.filter("es_cero").count() == 5  # 60 días / 13 → d = 0, 13, 26, 39, 52
    maximos = features.groupBy(*fb.LLAVE_SERIE).agg(F.max("dias_con_datos").alias("n"))
    assert sorted(r.n for r in maximos.collect()) == [60, 90, 120]


# TODO (lab 8, sección 5): escribe test_media_7_nunca_incluye_el_dia_t(features).
# Pista: recalcula la media con Window.rowsBetween(-7, -1) y compara con media_7; además,
# comprueba que media_7 NO coincide con la ventana (-6, 0) en todas las filas (así sabes
# que el test distingue la versión correcta de la que tiene fuga).
