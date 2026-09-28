"""Reglas de calidad del caso: la única definición de qué es un dato válido.

Cada regla es (expresión SQL que debe ser verdadera, acción cuando es falsa).
Las usan el pipeline declarativo (notebooks/02_silver_pipeline.py) y el validador por lotes
(xm_demanda.quality.validador). Las funciones puras de abajo se prueban sin Spark.
"""

SIN_CLASIFICAR = "SIN CLASIFICAR"
VARIABLES = ("DdaReal", "PerdidasEnergia")

# Acciones cuando la regla falla:
#   warn        la fila pasa y se cuenta
#   drop        la fila se descarta y se cuenta
#   cuarentena  la fila va a la tabla de cuarentena con el nombre de la regla
#   fail        no se escribe nada; el proceso se detiene
ACCIONES = ("warn", "drop", "cuarentena", "fail")

# Sobre el formato largo (bronce vigente): una fila por serie-día-variable.
REGLAS: dict[str, tuple[str, str]] = {
    "valor_no_nulo": ("Valor IS NOT NULL", "drop"),
    "valor_no_negativo": ("Valor >= 0", "cuarentena"),
    "variable_conocida": ("CodigoVariable IN ('DdaReal', 'PerdidasEnergia')", "drop"),
    "regulado_sin_clasificar": (
        "TipoMercado <> 'Regulado' OR ClasificacionIndustrial = 'SIN CLASIFICAR'",
        "warn",
    ),
    "fecha_no_futura": ("Fecha <= current_date()", "fail"),
}

# Sobre la vista ancha (una fila por serie-día): la regla del sector.
REGLAS_BALANCE: dict[str, tuple[str, str]] = {
    "perdidas_presentes": ("perdidas_kwh IS NOT NULL", "warn"),
    "perdidas_coherentes": (
        "perdidas_kwh IS NULL OR perdidas_kwh <= demanda_real_kwh",
        "cuarentena",
    ),
}


def valor_es_valido(valor: float | None) -> bool:
    """La demanda y las pérdidas no pueden ser negativas ni nulas."""
    return valor is not None and valor >= 0


def clasificacion_es_coherente(tipo_mercado: str, clasificacion: str) -> bool:
    """En el mercado regulado la actividad económica siempre viene sin clasificar."""
    if tipo_mercado == "Regulado":
        return clasificacion == SIN_CLASIFICAR
    return True


def reglas_por_accion(reglas: dict[str, tuple[str, str]], accion: str) -> dict[str, str]:
    """Subconjunto {nombre: expresión} de las reglas con esa acción."""
    return {n: expr for n, (expr, a) in reglas.items() if a == accion}
