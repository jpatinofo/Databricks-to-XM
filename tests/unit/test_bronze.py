import pytest

from xm_demanda.ingest.bronze import ALIAS, COLUMNAS_RAW, METADATOS, resolver_alias


def _resolver(cols):
    try:
        return resolver_alias(cols)
    except NotImplementedError:
        pytest.skip("Pendiente: lab 4, paso 4")


def test_alias_conocido_se_resuelve():
    assert _resolver(["ValorKwh", "ColumnaDesconocida"]) == {"ValorKwh": "Valor"}


def test_sin_alias_devuelve_vacio():
    assert _resolver(["Valor", "Fecha"]) == {}


def test_alias_apuntan_a_columnas_del_esquema():
    canonicas = {n for n, _ in COLUMNAS_RAW}
    assert set(ALIAS.values()) <= canonicas


def test_metadatos_declarados():
    assert METADATOS == ["_ingested_at", "_source_file", "_publication_date"]
