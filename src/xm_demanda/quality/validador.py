"""Validador por lotes: aplica las REGLAS a un DataFrame y separa por acción.

Misma semántica que las expectations del pipeline declarativo, para poder ejecutarla en un
esquema propio (Free Edition permite un solo pipeline activo) y en pruebas.
"""

from datetime import UTC, datetime

from xm_demanda.quality.rules import ACCIONES, reglas_por_accion

COLUMNA_FALLAS = "_fallas"


class ReglaFail(Exception):
    """Una regla con acción 'fail' no se cumplió: no se escribe nada."""


def validar_reglas(reglas: dict[str, tuple[str, str]]) -> None:
    """Lanza ValueError si alguna regla está mal formada. Función pura."""
    for nombre, regla in reglas.items():
        if not isinstance(regla, tuple) or len(regla) != 2:
            raise ValueError(f"{nombre}: la regla debe ser (expresión, acción)")
        expr, accion = regla
        if not expr or not isinstance(expr, str):
            raise ValueError(f"{nombre}: la expresión debe ser un texto SQL")
        if accion not in ACCIONES:
            raise ValueError(f"{nombre}: acción '{accion}' no está en {ACCIONES}")


def evaluar(df, reglas: dict[str, tuple[str, str]]):
    """Agrega la columna `_fallas`: array con los nombres de las reglas que NO se cumplen.

    Ejemplo: una fila con Valor = -5 y TipoMercado = 'Regulado' con CIIU 'EDUCACIÓN'
    queda con _fallas = ['valor_no_negativo', 'regulado_sin_clasificar'].
    Una fila válida queda con _fallas = [].
    """
    from pyspark.sql import functions as F  # noqa: F401

    # TODO (lab 5, paso 2): implementar. Pista: F.when(~F.expr(expr), F.lit(nombre)) por regla,
    # F.array(...) y F.array_compact(...) para quitar los nulos.
    raise NotImplementedError("evaluar: completa esta función (lab 5, paso 2)")


def aplicar(df, reglas: dict[str, tuple[str, str]]):
    """Evalúa y separa. Devuelve (validas, cuarentena, metricas).

    - fail: si alguna fila falla una regla 'fail', lanza ReglaFail y no devuelve nada.
    - drop: las filas que fallan una regla 'drop' se descartan.
    - cuarentena: las filas que fallan una regla 'cuarentena' van al segundo DataFrame.
    - warn: la fila pasa; solo se cuenta.
    `metricas` es una lista de dicts: regla, accion, fallas, evaluadas, ejecutado_en.
    """
    from pyspark.sql import functions as F

    validar_reglas(reglas)
    evaluado = evaluar(df, reglas).cache()
    evaluadas = evaluado.count()
    ejecutado_en = datetime.now(UTC)

    fallas = F.explode(F.col(COLUMNA_FALLAS)).alias("regla")
    conteos = evaluado.select(fallas).groupBy("regla").count()
    fallas_por_regla = {r["regla"]: r["count"] for r in conteos.collect()}
    metricas = [
        {
            "regla": n,
            "accion": a,
            "fallas": fallas_por_regla.get(n, 0),
            "evaluadas": evaluadas,
            "ejecutado_en": ejecutado_en,
        }
        for n, (_, a) in reglas.items()
    ]

    con_fail = [n for n in reglas_por_accion(reglas, "fail") if fallas_por_regla.get(n, 0) > 0]
    if con_fail:
        detalle = {n: fallas_por_regla[n] for n in con_fail}
        raise ReglaFail(f"Reglas fail incumplidas (filas): {detalle}")

    def falla_alguna(nombres):
        if not nombres:
            return F.lit(False)
        return F.arrays_overlap(F.col(COLUMNA_FALLAS), F.array(*[F.lit(n) for n in nombres]))

    sin_drop = evaluado.filter(~falla_alguna(list(reglas_por_accion(reglas, "drop"))))
    es_cuarentena = falla_alguna(list(reglas_por_accion(reglas, "cuarentena")))
    cuarentena = sin_drop.filter(es_cuarentena)
    validas = sin_drop.filter(~es_cuarentena)
    return validas, cuarentena, metricas
