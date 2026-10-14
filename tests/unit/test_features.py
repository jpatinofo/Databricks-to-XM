import pytest

from xm_demanda.features.build import (
    MEDIDA,
    es_fin_de_semana,
    nombres_lags,
    nombres_ventanas,
)
from xm_demanda.silver.transform import LLAVE_SERIE


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
