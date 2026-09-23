# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Lab 2 — Explorar para diseñar
# MAGIC
# MAGIC Reproduce seis hechos del dato y anota, en cada bloque, la decisión de diseño que abre.
# MAGIC Las respuestas van a `docs/architecture.md` y a `docs/adr/ADR-001-granularidad-horizonte.md`.
# MAGIC
# MAGIC No hay que escribir código nuevo: hay que leer resultados y decidir.

# COMMAND ----------

dbutils.widgets.text("usuario", "jpatinofo")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario en el widget (o 'docente' para usar la tabla compartida)."

tabla = f"workspace.c01_{usuario}.demanda_raw"
llave = ["CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial"]
llave_sql = ", ".join(llave)
print("Tabla:", tabla)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho 1 — Formato largo
# MAGIC ¿Cuántas filas hay por serie-día? Deberían ser exactamente 2 (DdaReal y PerdidasEnergia).

# COMMAND ----------

filas_por_serie_dia = spark.sql(f"""
SELECT n_filas, count(*) AS serie_dias
FROM (
  SELECT Fecha, {llave_sql}, count(*) AS n_filas
  FROM {tabla}
  GROUP BY ALL
)
GROUP BY n_filas ORDER BY n_filas
""")
filas_por_serie_dia.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta de diseño 1.** Plata será una fila por serie-día con `demanda_real_kwh` y `perdidas_kwh`.
# MAGIC ¿Qué debe pasar si un día llega solo una de las dos filas? (nulo, rechazar, cuarentena)
# MAGIC
# MAGIC _Tu respuesta:_ rechazar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho 2 — Publicación con retraso variable
# MAGIC `FechaPublicacion` es cuándo XM publicó el registro; `Fecha` es el día del dato.

# COMMAND ----------

retraso = spark.sql(f"""
SELECT min(d) AS min_dias, percentile(d, 0.5) AS mediana_dias, max(d) AS max_dias
FROM (SELECT datediff(FechaPublicacion, Fecha) AS d FROM {tabla})
""")
retraso.display()

# COMMAND ----------

spark.sql(f"""
SELECT FechaPublicacion, min(Fecha) AS desde, max(Fecha) AS hasta, count(DISTINCT Fecha) AS dias_publicados
FROM {tabla}
GROUP BY FechaPublicacion ORDER BY FechaPublicacion
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta de diseño 2.** Hoy cada `Fecha` tiene una sola `FechaPublicacion`, pero el 25 de julio se publicaron 200 días de golpe.
# MAGIC Cuando el próximo archivo traiga días que ya existen, ¿bronce sobrescribe o acumula? ¿Quién decide cuál versión es la vigente?
# MAGIC
# MAGIC _Tu respuesta:_ Bronce acumula y plata decide cuál es la versión vigente

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho 3 — Series incompletas

# COMMAND ----------

series_incompletas = spark.sql(f"""
WITH por_serie AS (
  SELECT {llave_sql}, count(DISTINCT Fecha) AS dias_con_datos
  FROM {tabla} WHERE CodigoVariable = 'DdaReal'
  GROUP BY ALL
), total AS (SELECT count(DISTINCT Fecha) AS dias_totales FROM {tabla})
SELECT s.*, t.dias_totales - s.dias_con_datos AS dias_faltantes
FROM por_serie s CROSS JOIN total t
WHERE s.dias_con_datos < t.dias_totales
ORDER BY dias_faltantes DESC
""")
series_incompletas.display()
n_incompletas = series_incompletas.count()
print("Series incompletas:", n_incompletas)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta de diseño 3.** ¿Qué hace plata con una serie que aparece o desaparece a mitad de periodo?
# MAGIC (rellenar con cero, marcar `activa = false`, excluir del modelo)
# MAGIC
# MAGIC _Tu respuesta:_ marcar activa=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho 4 — Regulado vs. no regulado

# COMMAND ----------

por_tipo = spark.sql(f"""
SELECT TipoMercado,
       count(DISTINCT {llave_sql}) AS series,
       count(DISTINCT ClasificacionIndustrial) AS ciiu_distintos,
       round(sum(Valor) / (SELECT sum(Valor) FROM {tabla} WHERE CodigoVariable = 'DdaReal'), 3) AS participacion_energia,
       round(percentile(Valor, 0.5)) AS mediana_kwh_dia,
       round(max(Valor)) AS max_kwh_dia
FROM {tabla} WHERE CodigoVariable = 'DdaReal'
GROUP BY TipoMercado
""")
por_tipo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta de diseño 4.** 29 series regulan el 69 % de la energía; 326 series no reguladas son pequeñas y ruidosas.
# MAGIC ¿Un modelo global para todo, uno por tipo de mercado, o uno por serie? ¿Qué métrica de error es justa entre escalas tan distintas?
# MAGIC
# MAGIC _Tu respuesta:_ modelos por tipo de mercado. En cuanto a la metrica de error, según expertos en el tema (IA), se puede realizar mediante WAPE.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho 5 — Ceros

# COMMAND ----------

ceros = spark.sql(f"""
SELECT {llave_sql}, count(*) AS dias_en_cero
FROM {tabla}
WHERE CodigoVariable = 'DdaReal' AND Valor = 0
GROUP BY ALL ORDER BY dias_en_cero DESC
""")
ceros.display()
n_ceros = spark.sql(f"SELECT count(*) FROM {tabla} WHERE CodigoVariable = 'DdaReal' AND Valor = 0").first()[0]
print("Serie-días con demanda 0:", n_ceros)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta de diseño 5.** ¿Un cero es consumo real, ausencia de medida o error? ¿Qué regla de calidad va en plata y qué se hace con la fila (warn, cuarentena, drop)?
# MAGIC
# MAGIC _Tu respuesta:_ El cero probablemente representa una ausencia de medida producto de un posible error en el dispositivo correspondiente. La fila se puede dejar advertida e imputar el promedio del día hasta encontrar la causa raiz del problema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho 6 — Pérdidas

# COMMAND ----------

perdidas = spark.sql(f"""
WITH ancho AS (
  SELECT Fecha, {llave_sql},
         max(CASE WHEN CodigoVariable = 'DdaReal' THEN Valor END) AS demanda,
         max(CASE WHEN CodigoVariable = 'PerdidasEnergia' THEN Valor END) AS perdidas
  FROM {tabla} GROUP BY ALL
)
SELECT count(*) AS serie_dias,
       sum(CASE WHEN perdidas > demanda THEN 1 ELSE 0 END) AS perdidas_mayores_que_demanda,
       round(percentile(perdidas / nullif(demanda, 0), 0.5), 4) AS ratio_mediana,
       round(percentile(perdidas / nullif(demanda, 0), 0.95), 4) AS ratio_p95
FROM ancho
""")
perdidas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta de diseño 6.** Pérdidas ≈ 1,5 % de la demanda y nunca mayores. ¿Va como regla de calidad (`perdidas <= demanda`)? ¿Se pronostican las pérdidas o solo la demanda?
# MAGIC
# MAGIC _Tu respuesta:_ Efectivamente por reglas de calidad, las perdidas deben ser menores a la demanda, no debería existir igualdad. Ambas series pueden ser pronosticadas, pero a fines de operación, agrega mayor valor la demanda.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación automática
# MAGIC
# MAGIC Revisa `docs/architecture.md` y `docs/adr/ADR-001-granularidad-horizonte.md` en tu Git folder.
# MAGIC Ejecuta cuando hayas llenado los dos archivos.

# COMMAND ----------

import os
import re


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "docs", "architecture.md")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro docs/architecture.md: ejecuta este notebook desde tu Git folder.")


def verificar():
    raiz = _raiz_repo()
    checks = []

    arch = open(os.path.join(raiz, "docs", "architecture.md"), encoding="utf-8").read()
    filas = [ln for ln in arch.splitlines() if ln.startswith("|") and not ln.startswith("|---") and "Capa" not in ln]
    tablas = [ln for ln in filas if re.search(r"`\w+_\w+\.\w+`", ln)]
    vacias = [ln for ln in tablas if re.search(r"\|\s*\|", ln) or "<" in ln]
    checks.append(("architecture.md: 5 tablas definidas", len(tablas) >= 5))
    checks.append(("architecture.md: sin celdas vacías ni <marcadores>", len(vacias) == 0))

    adr_path = os.path.join(raiz, "docs", "adr", "ADR-001-granularidad-horizonte.md")
    existe = os.path.exists(adr_path)
    checks.append(("ADR-001 existe", existe))
    if existe:
        adr = open(adr_path, encoding="utf-8").read()
        checks.append(("ADR-001 en estado aceptada", re.search(r"\*\*Estado:\*\*\s*aceptada", adr) is not None))
        decision = re.search(r"## Decisión\s*\n(.*?)\n## ", adr, re.S)
        checks.append(("ADR-001 con decisión escrita", decision is not None and len(decision.group(1).strip()) > 80 and "<" not in decision.group(1)))
        alternativas = re.search(r"## Alternativas consideradas\s*\n(.*?)\n## ", adr, re.S)
        n_alt = len(re.findall(r"^\s*[-*]\s", alternativas.group(1), re.M)) if alternativas and "<" not in alternativas.group(1) else 0
        checks.append(("ADR-001 con al menos 2 alternativas descartadas", n_alt >= 2))

    checks.append(("Exploración ejecutada (hechos 3 y 5)", "n_incompletas" in globals() and "n_ceros" in globals()))

    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()