import pytest

from xm_demanda.features.build import (
    COLUMNAS_BANDERAS,
    COLUMNAS_CALENDARIO,
    COLUMNAS_CATEGORICAS,
    MEDIDA,
    columnas_contrato,
    es_fin_de_semana,
    nombres_lags,
    nombres_ventanas,
)
from xm_demanda.silver.transform import LLAVE, LLAVE_SERIE
from xm_demanda.utils.config import load_config


def _contrato(cfg):
    try:
        return columnas_contrato(cfg)
    except NotImplementedError:
        pytest.skip("Pendiente: lab 8, sección 3")


def test_nombres_lags_en_orden():
    assert nombres_lags([1, 7, 14, 28]) == ["lag_1", "lag_7", "lag_14", "lag_28"]


@pytest.mark.parametrize("lags", [[0], [-1], [1, 1], [1.5], ["7"]])
def test_nombres_lags_rechaza_valores_invalidos(lags):
    with pytest.raises(ValueError):
        nombres_lags(lags)


def test_nombres_ventanas_media_y_desviacion():
    assert nombres_ventanas([7, 28]) == ["media_7", "media_28", "desv_7", "desv_28"]


def test_nombres_ventanas_rechaza_ventana_de_uno():
    with pytest.raises(ValueError):
        nombres_ventanas([1, 7])


def test_fin_de_semana_convencion_spark():
    # dayofweek: 1 = domingo … 7 = sábado
    assert es_fin_de_semana(1) and es_fin_de_semana(7)
    assert not any(es_fin_de_semana(d) for d in range(2, 7))


def test_medida_y_llave_de_serie_coherentes_con_plata():
    assert MEDIDA == "demanda_real_kwh"
    assert "fecha" not in LLAVE_SERIE  # la serie se identifica sin la fecha; la fecha ordena


def test_contrato_con_la_configuracion_real():
    cols = _contrato(load_config("dev"))
    assert cols[: len(LLAVE)] == LLAVE and cols[len(LLAVE)] == MEDIDA
    assert set(COLUMNAS_CALENDARIO) <= set(cols) and set(COLUMNAS_BANDERAS) <= set(cols)
    assert set(COLUMNAS_CATEGORICAS) <= set(cols)
    assert not {"perdidas_kwh", "_publication_date"} & set(cols)  # se quedan en plata
    assert {"lag_1", "lag_7", "lag_14", "lag_28", "media_7", "desv_28", "y_h7"} <= set(cols)
    assert cols[-1] == "_features_built_at" and len(cols) == len(set(cols))


def test_contrato_sigue_a_la_configuracion():
    """Cambiar el YAML cambia el contrato: no hay listas de columnas escritas a mano."""
    cfg = load_config("dev")
    cfg["modelo"] = {**cfg["modelo"], "lags": [1, 364], "ventanas": [3], "horizon_days": 14}
    cols = _contrato(cfg)
    assert {"lag_364", "media_3", "desv_3", "y_h14"} <= set(cols)
    assert "lag_7" not in cols and "y_h7" not in cols
