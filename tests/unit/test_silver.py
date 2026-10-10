import re

import pytest

from xm_demanda.silver.transform import ALIAS_CIIU, LLAVE, LLAVE_SERIE, RENOMBRES, normalizar_texto


def _norm(s):
    try:
        return normalizar_texto(s)
    except NotImplementedError:
        pytest.skip("Pendiente: lab 6, paso 5")


def test_normalizar_quita_tildes_y_espacios():
    assert _norm("  Educación  ") == "EDUCACION"
    assert _norm("Actividades   Artísticas") == "ACTIVIDADES ARTISTICAS"


def test_normalizar_nulo():
    assert _norm(None) is None


def test_llave_en_snake_case():
    assert all(re.fullmatch(r"[a-z_]+", c) for c in LLAVE)
    assert LLAVE == ["fecha", *LLAVE_SERIE]


def test_renombres_cubren_la_llave():
    assert set(RENOMBRES.values()) == set(LLAVE)


def test_alias_ciiu_normalizados():
    for k, v in ALIAS_CIIU.items():
        assert k == k.upper() and v == v.upper()
        assert "  " not in k and "  " not in v
