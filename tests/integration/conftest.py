"""Sesión de Spark para las pruebas de integración.

- En CI y en local: Spark local (pip install -e ".[spark]"; requiere Java).
- En Databricks: reutiliza la sesión activa del notebook (no crea otra).
Si pyspark no está instalado, las pruebas de esta carpeta se omiten.
"""

import pytest

pyspark = pytest.importorskip("pyspark")


def _sesion_activa():
    """La sesión del notebook de Databricks (clásica o Spark Connect), o None."""
    from pyspark.sql import SparkSession

    activa = SparkSession.getActiveSession()
    if activa is not None:
        return activa
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession

        return ConnectSession.getActiveSession()
    except Exception:  # noqa: BLE001 - sin Spark Connect disponible
        return None


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    activa = _sesion_activa()
    if activa is not None:
        return activa
    return (
        SparkSession.builder.master("local[2]")
        .appName("xm_demanda-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
