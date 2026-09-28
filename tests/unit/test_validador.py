import pytest

from xm_demanda.quality.rules import ACCIONES, REGLAS, REGLAS_BALANCE, reglas_por_accion
from xm_demanda.quality.validador import validar_reglas


def test_reglas_del_caso_bien_formadas():
    validar_reglas(REGLAS)
    validar_reglas(REGLAS_BALANCE)


def test_accion_desconocida_se_rechaza():
    with pytest.raises(ValueError):
        validar_reglas({"x": ("Valor > 0", "explotar")})


def test_regla_sin_expresion_se_rechaza():
    with pytest.raises(ValueError):
        validar_reglas({"x": ("", "warn")})


def test_hay_una_regla_fail_y_una_cuarentena():
    assert reglas_por_accion(REGLAS, "fail") == {"fecha_no_futura": "Fecha <= current_date()"}
    assert "valor_no_negativo" in reglas_por_accion(REGLAS, "cuarentena")


def test_todas_las_acciones_usadas_existen():
    usadas = {a for _, a in list(REGLAS.values()) + list(REGLAS_BALANCE.values())}
    assert usadas <= set(ACCIONES)
