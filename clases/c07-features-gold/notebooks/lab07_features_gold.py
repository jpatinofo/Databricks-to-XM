# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 7 — Oro: features para pronosticar la demanda
# MAGIC
# MAGIC Este notebook es la clase. La teoría está en las celdas de texto; el código, justo debajo, la ejecuta sobre tus datos.
# MAGIC Se recorre de arriba abajo, en clase con la docente y luego solo.
# MAGIC
# MAGIC **Lo que construyes hoy** (hito 4): la tabla de oro `features_demanda_diaria`, la que va a comer el modelo de la clase 9.
# MAGIC
# MAGIC ```
# MAGIC   demanda_diaria (plata, clase 6)         festivos_co.csv (data/reference)       conf/base.yml
# MAGIC   una fila por serie-día                  fecha, nombre                          lags: [1, 7, 14, 28]
# MAGIC   ┌────────┬────────┬──────────────────┐  ┌────────────┬─────────────────────┐   ventanas: [7, 28]
# MAGIC   │ fecha  │ serie… │ demanda_real_kwh │  │ 2026-07-20 │ Día de la Indep.    │   horizon_days: 7
# MAGIC   └────────┴────────┴──────────────────┘  └────────────┴─────────────────────┘
# MAGIC              │ calendario + lags + ventanas + banderas + objetivo   (src/xm_demanda/features/build.py)
# MAGIC              ▼
# MAGIC   features_demanda_diaria (oro): misma llave que plata, ~20 columnas más, MERGE idempotente
# MAGIC   ┌────────┬────────┬──────────────────┬────────────┬────────────┬───────┬─────────┬────────┬───────┐
# MAGIC   │ fecha  │ serie… │ demanda_real_kwh │ dia_semana │ es_festivo │ lag_7 │ media_7 │ desv_7 │ y_h7  │
# MAGIC   └────────┴────────┴──────────────────┴────────────┴────────────┴───────┴─────────┴────────┴───────┘
# MAGIC ```
# MAGIC
# MAGIC Las celdas con `TODO` se escriben en `src/xm_demanda/features/build.py`, no aquí.
# MAGIC Requiere el lab 6: `workspace.c01_<usuario>.demanda_diaria` (72.704 filas, `ciiu_seccion` sin nulos).

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

esquema = f"workspace.c01_{usuario}"
T_DIARIA = f"{esquema}.demanda_diaria"
T_FEATURES = f"{esquema}.features_demanda_diaria"
print("Fuente:", T_DIARIA, "| Destino:", T_FEATURES)

# COMMAND ----------

import importlib
import os
import sys


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: ejecuta este notebook desde tu Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
import xm_demanda.features.build as fb
from xm_demanda.utils.config import load_config

importlib.reload(fb)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

cfg = load_config("dev")
print("Config del modelo:", cfg["modelo"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Qué es oro y qué es una *feature*
# MAGIC
# MAGIC Plata responde "¿qué pasó?": una fila por serie-día, limpia y con historial. Oro responde una **pregunta concreta de
# MAGIC negocio**: aquí, *¿cuánta demanda tendrá cada serie dentro de 7 días?* (ADR-001: horizonte diario, 7 días, modelo global).
# MAGIC
# MAGIC Una **feature** es una columna que el modelo puede usar para responder esa pregunta. Se calcula a partir de plata y de
# MAGIC fuentes externas (calendario, festivos), con una regla que no se negocia:
# MAGIC
# MAGIC > **En la fila del día `t` solo puede haber información que existía el día `t`.**
# MAGIC
# MAGIC El objetivo (`y_h7`, la demanda de `t+7`) es la excepción visible: solo existe para entrenar y evaluar. En producción, la
# MAGIC fila de hoy tiene `y_h7` nulo y el modelo la rellena.
# MAGIC
# MAGIC ![Oro: fuentes, funciones y contrato](./../img/oro_contrato.png)
# MAGIC
# MAGIC **El contrato de `features_demanda_diaria`:**
# MAGIC
# MAGIC | Grupo | Columnas | Regla |
# MAGIC |---|---|---|
# MAGIC | Llave | `fecha` + las 4 de serie (igual que plata) | única; el MERGE la usa |
# MAGIC | Medida | `demanda_real_kwh` | copiada de plata (el modelo también la usa como `lag_0`) |
# MAGIC | Calendario | `dia_semana`, `es_fin_de_semana`, `es_festivo`, `mes`, `dia_del_anio` | se conoce de antemano: sin nulos, nunca |
# MAGIC | Historia | `lag_1`, `lag_7`, `lag_14`, `lag_28` | nulo solo cuando no hay historia (primeros días de la serie) |
# MAGIC | Ventanas | `media_7`, `desv_7`, `media_28`, `desv_28` | sobre `t-n … t-1`; **nunca incluyen `t`** |
# MAGIC | Banderas | `es_cero`, `dias_con_datos` | contexto de calidad para el modelo |
# MAGIC | Objetivo | `y_h7` | `demanda_real_kwh` en `t+7`; nulo en los últimos 7 días de cada serie |
# MAGIC | Auditoría | `_features_built_at` | cuándo se construyó la fila |
# MAGIC
# MAGIC Lo que **no** hay: columnas de la fuente que el modelo no necesita (`perdidas_kwh`, `_publication_date`). Oro es
# MAGIC estrecho a propósito: cada columna tiene un porqué. Y las listas de lags y ventanas **no están escritas en el código**:
# MAGIC vienen de `conf/base.yml` (`modelo.lags`, `modelo.ventanas`, `modelo.horizon_days`). Cambiar el experimento es cambiar
# MAGIC un archivo de configuración, no reescribir una función (clase 8 profundiza en esto).

# COMMAND ----------

diaria = spark.table(T_DIARIA)
print("Serie-días en plata:", diaria.count())  # 72.704
print("Rango de fechas:", diaria.agg(F.min("fecha"), F.max("fecha")).first())
print("Series:", diaria.select(*fb.LLAVE_SERIE).distinct().count())  # 355

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Sumar fuentes: el calendario explica más de lo que parece
# MAGIC
# MAGIC Antes de escribir una sola feature, mira los datos. La demanda agregada del SIN en 2026 tiene un patrón que cualquier
# MAGIC operador conoce de memoria: cae los fines de semana y cae más los festivos. Un modelo que no sepa qué día es va a
# MAGIC "aprender" ese patrón a medias y a equivocarse justo en los puentes, que es cuando XM más necesita acertar.
# MAGIC
# MAGIC ![Demanda diaria total 2026 con fines de semana y festivos](./../img/demanda_calendario.png)
# MAGIC
# MAGIC En números (calculados sobre esta misma tabla, abajo): el promedio de un lunes a viernes está alrededor de 235 GWh; un
# MAGIC domingo, en 212. Y un **festivo entre semana** queda un 12 % por debajo de un día laboral normal, casi como un domingo.
# MAGIC
# MAGIC ![Demanda por día de semana y efecto festivo](./../img/dia_semana_festivo.png)
# MAGIC
# MAGIC Dos fuentes nuevas entran a oro:
# MAGIC
# MAGIC | Fuente | De dónde sale | Cómo entra |
# MAGIC |---|---|---|
# MAGIC | Calendario (`dia_semana`, `mes`, `dia_del_anio`) | de la propia `fecha`, con funciones de Spark | `F.dayofweek`, `F.month`, `F.dayofyear` |
# MAGIC | Festivos de Colombia | `data/reference/festivos_co.csv` (Ley 51 de 1983: fecha fija, trasladables al lunes y móviles según Semana Santa) | `left join` por `fecha` |
# MAGIC
# MAGIC El archivo de festivos es un **dato de referencia versionado en el repo**, igual que `ciiu_secciones.csv` en la clase 6:
# MAGIC cambia poco, se revisa en un PR y cualquier ambiente lo lee de la misma ruta. Cubre 2025–2027. Cuando llegue 2028 y
# MAGIC nadie lo haya actualizado, `es_festivo` quedará nulo para ese año y `verificar()` (y en producción, una expectation) lo
# MAGIC gritará. Eso es preferible a un `False` silencioso.
# MAGIC
# MAGIC > Convención de Spark que se olvida siempre: `dayofweek` devuelve **1 = domingo … 7 = sábado**. Fin de semana es
# MAGIC > `dia_semana IN (1, 7)`. Ya está encapsulado en `fb.es_fin_de_semana`, con test.

# COMMAND ----------

# Confirma el patrón sobre tus datos: demanda total por día de la semana (SQL, porque es lectura).
diaria.createOrReplaceTempView("diaria")
display(spark.sql("""
SELECT dayofweek(fecha) AS dia_semana,
       CASE dayofweek(fecha) WHEN 1 THEN 'Dom' WHEN 2 THEN 'Lun' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Mié'
                             WHEN 5 THEN 'Jue' WHEN 6 THEN 'Vie' ELSE 'Sáb' END AS nombre,
       ROUND(AVG(total_gwh), 1) AS promedio_gwh
FROM (SELECT fecha, SUM(demanda_real_kwh) / 1e6 AS total_gwh FROM diaria GROUP BY fecha)
GROUP BY 1, 2 ORDER BY 1
"""))

# COMMAND ----------

import pandas as pd

ruta_festivos = os.path.join(raiz, "data", "reference", "festivos_co.csv")
festivos = spark.createDataFrame(pd.read_csv(ruta_festivos, parse_dates=["fecha"]))
festivos = festivos.withColumn("fecha", F.to_date("fecha"))
print("Festivos en el archivo:", festivos.count(), "| años:", [r[0] for r in festivos.select(F.year("fecha")).distinct().orderBy("year(fecha)").collect()])
display(festivos.filter(F.year("fecha") == 2026))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · TODO 1 — `agregar_calendario` en `src/xm_demanda/features/build.py`
# MAGIC
# MAGIC Abre `src/xm_demanda/features/build.py` y completa `agregar_calendario(df, festivos)`. Debe devolver `df` con cinco columnas
# MAGIC nuevas:
# MAGIC
# MAGIC | Columna | Cómo |
# MAGIC |---|---|
# MAGIC | `dia_semana` | `F.dayofweek("fecha")` |
# MAGIC | `es_fin_de_semana` | `F.col("dia_semana").isin(1, 7)` |
# MAGIC | `mes` | `F.month("fecha")` |
# MAGIC | `dia_del_anio` | `F.dayofyear("fecha")` |
# MAGIC | `es_festivo` | `left join` con `festivos.select("fecha", F.lit(True).alias("es_festivo"))` y luego `F.coalesce(F.col("es_festivo"), F.lit(False))` **solo para los años cubiertos por el archivo** |
# MAGIC
# MAGIC Sobre la última fila: si haces `coalesce(…, False)` para todo, un año sin festivos en el archivo queda como "sin festivos"
# MAGIC y nadie se entera. La versión de referencia deja `es_festivo` nulo cuando `year(fecha)` no está entre los años del
# MAGIC archivo. Pista: calcula `anios = festivos.select(F.year("fecha")).distinct()` y usa `F.when(F.year("fecha").isin(...), coalesce).otherwise(None)`,
# MAGIC o un segundo `left join` por año. Cualquiera de las dos vale si la celda de prueba pasa.
# MAGIC
# MAGIC Cuando termines: guarda, vuelve aquí y ejecuta la celda siguiente (recarga el módulo).

# COMMAND ----------

importlib.reload(fb)
con_calendario = fb.agregar_calendario(diaria, festivos)

esperadas = {"dia_semana", "es_fin_de_semana", "es_festivo", "mes", "dia_del_anio"}
assert esperadas <= set(con_calendario.columns), f"Faltan columnas: {esperadas - set(con_calendario.columns)}"
assert con_calendario.count() == diaria.count(), "El join multiplicó o perdió filas: revisa que festivos no tenga fechas repetidas"
assert con_calendario.filter("es_festivo IS NULL").count() == 0, "es_festivo nulo en un año cubierto por el archivo"
n_festivos = con_calendario.filter("es_festivo").select("fecha").distinct().count()
print("Días festivos marcados en tus datos:", n_festivos)
display(con_calendario.filter("es_festivo").groupBy("fecha", "dia_semana").count().orderBy("fecha"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Historia: lags y ventanas con `Window`
# MAGIC
# MAGIC El mejor predictor de la demanda de una serie es su propia demanda reciente. Dos familias de features lo capturan:
# MAGIC
# MAGIC - **Lag `k`**: el valor de la misma serie `k` días antes. `lag_7` = "el mismo día de la semana pasada", que en demanda
# MAGIC   eléctrica vale más que `lag_1` porque respeta el ciclo semanal.
# MAGIC - **Ventana de `n` días**: media y desviación de los `n` días **anteriores** (`t-n … t-1`). La media suaviza; la
# MAGIC   desviación le dice al modelo si la serie está tranquila o agitada.
# MAGIC
# MAGIC ![Lags y ventanas sobre una serie real](./../img/lags_ventanas_serie.png)
# MAGIC
# MAGIC La figura es la serie más grande de la tabla (ENDD, Bogotá-Cundinamarca, regulado) alrededor del 22 de junio de 2026:
# MAGIC `lag_7` mira 7 días atrás, `media_7` promedia los 7 días anteriores (sin incluir el 22), y `y_h7` es lo que queremos
# MAGIC predecir: el 29 de junio.
# MAGIC
# MAGIC En Spark esto se escribe con una **función de ventana**. Tiene tres partes y las tres importan:
# MAGIC
# MAGIC ```python
# MAGIC w = Window.partitionBy(*LLAVE_SERIE).orderBy("fecha")   # 1. dentro de cada serie  2. ordenada por fecha
# MAGIC F.lag("demanda_real_kwh", 7).over(w)                     # 3. mira 7 filas atrás
# MAGIC F.avg("demanda_real_kwh").over(w.rowsBetween(-7, -1))    # 3'. promedia las 7 filas anteriores (no la actual)
# MAGIC ```
# MAGIC
# MAGIC | Si olvidas… | Pasa esto |
# MAGIC |---|---|
# MAGIC | `partitionBy(serie)` | `lag_1` de la primera fila de una serie toma el último día de **otra** serie |
# MAGIC | `orderBy("fecha")` | Spark no sabe qué es "antes"; error o basura |
# MAGIC | que `rowsBetween` cuenta **filas**, no días | si a una serie le falta un día, `lag_7` ya no es "hace 7 días" (por eso existen las banderas de la sección 6) |
# MAGIC
# MAGIC Ya viste `Window` en la clase 4 (`row_number` para la vista vigente) y en la clase 5 (balance por serie-día). Es la
# MAGIC misma herramienta: agrupar sin colapsar filas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 — `agregar_lags` en `src/xm_demanda/features/build.py`
# MAGIC
# MAGIC Completa `agregar_lags(df, lags)`: para cada `k` en `lags`, una columna `lag_{k}` con
# MAGIC `F.lag(fb.MEDIDA, k).over(fb._ventana_serie())`. Usa `fb.nombres_lags(lags)` para los nombres (valida la lista y ya
# MAGIC tiene test). Cuando termines, ejecuta la celda siguiente.

# COMMAND ----------

importlib.reload(fb)
con_lags = fb.agregar_lags(con_calendario, cfg["modelo"]["lags"])
con_ventanas = fb.agregar_ventanas(con_lags, cfg["modelo"]["ventanas"])

assert set(fb.nombres_lags(cfg["modelo"]["lags"])) <= set(con_lags.columns)
serie_top = con_ventanas.filter("codigo_sic_agente = 'ENDD' AND tipo_mercado = 'Regulado' AND mercado_comercializacion = 'BOGOTA-CUNDINAMARCA'")
display(
    serie_top.filter("fecha BETWEEN '2026-06-12' AND '2026-06-29'")
    .select("fecha", "dia_semana", (F.col("demanda_real_kwh") / 1e6).alias("gwh"), (F.col("lag_7") / 1e6).alias("lag_7_gwh"),
            (F.col("media_7") / 1e6).alias("media_7_gwh"), (F.col("desv_7") / 1e6).alias("desv_7_gwh"))
    .orderBy("fecha")
)

# COMMAND ----------

# Prueba de mesa: ¿media_7 del 22 de junio es exactamente el promedio del 15 al 21?
fila = serie_top.filter("fecha = '2026-06-22'").select("media_7").first()
manual = serie_top.filter("fecha BETWEEN '2026-06-15' AND '2026-06-21'").agg(F.avg("demanda_real_kwh")).first()[0]
print(f"media_7 (ventana): {fila.media_7 / 1e6:.4f} GWh | promedio manual 15–21 jun: {manual / 1e6:.4f} GWh")
assert abs(fila.media_7 - manual) < 1e-6, "media_7 no coincide con el promedio de los 7 días anteriores"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Fuga de información y horizonte: el error que no se ve en el entrenamiento
# MAGIC
# MAGIC Una **fuga de información** (*leakage*) es cualquier feature que, en la fila del día `t`, contiene algo que solo se
# MAGIC conoció después de `t`. El modelo entrena con un error minúsculo, la métrica en el notebook es espectacular, y en
# MAGIC producción (donde el futuro no existe todavía) falla. Es el error más caro de la analítica porque **no da error**.
# MAGIC
# MAGIC ![Tres formas de fuga y la versión correcta](./../img/fuga_informacion.png)
# MAGIC
# MAGIC Tres fugas que aparecen en proyectos reales de demanda:
# MAGIC
# MAGIC | Fuga | Cómo se ve en código | Cómo se evita |
# MAGIC |---|---|---|
# MAGIC | La ventana incluye el día `t` | `rowsBetween(-6, 0)` | `rowsBetween(-7, -1)`: termina en `t-1` |
# MAGIC | La ventana mira al futuro | `rowsBetween(-3, 3)` ("centrada", típica en suavizado) | nunca en features de pronóstico |
# MAGIC | La **versión** del dato es futura | usar la demanda republicada por XM 30 días después como si se hubiera conocido el día `t` | *point-in-time*: `_publication_date <= fecha_de_corte`. Plata guarda `_publication_date` justo para esto |
# MAGIC
# MAGIC La tercera es la sutil y es propia de este dominio: XM publica y **republica** (clase 6, ADR-002). Para entrenar con
# MAGIC honestidad se usa lo que se sabía en el momento; para operar hoy, la última versión. En este curso construimos
# MAGIC features sobre la versión vigente (es lo que se usa para operar) y dejamos la variante point-in-time como extensión.
# MAGIC
# MAGIC **El horizonte** cierra el diseño: si el pronóstico es a 7 días, el objetivo de la fila `t` es la demanda de `t+7`
# MAGIC (`F.lead(MEDIDA, 7)`). Eso deja `y_h7` nulo en los últimos 7 días de cada serie: son exactamente las filas que en
# MAGIC producción se mandan al modelo para que las prediga. No son un defecto; son la inferencia.

# COMMAND ----------

# Demostración: la ventana "cómoda" (-6, 0) parece casi igual… y contiene la respuesta del día t.
w = Window.partitionBy(*fb.LLAVE_SERIE).orderBy("fecha")
comparacion = (
    serie_top.withColumn("media_7_con_fuga", F.avg(fb.MEDIDA).over(w.rowsBetween(-6, 0)))
    .filter("fecha BETWEEN '2026-06-20' AND '2026-06-24'")
    .select("fecha", (F.col(fb.MEDIDA) / 1e6).alias("gwh"), (F.col("media_7") / 1e6).alias("media_7_ok"), (F.col("media_7_con_fuga") / 1e6).alias("media_7_con_fuga"))
)
display(comparacion.orderBy("fecha"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta (respóndela aquí, en esta celda):** en la tabla de arriba, `media_7_con_fuga` del 22 de junio incluye la
# MAGIC demanda del propio 22. ¿Por qué el modelo entrenado con esa columna daría un error casi nulo en el notebook y uno
# MAGIC mucho mayor en producción? Escribe dos frases.
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

con_objetivo = fb.agregar_objetivo(con_ventanas, cfg["modelo"]["horizon_days"])
ult = con_objetivo.filter("y_h7 IS NULL").groupBy(*fb.LLAVE_SERIE).count()
print("Series con y_h7 nulo:", ult.count(), "| máximo de filas nulas por serie:", ult.agg(F.max("count")).first()[0])  # 355 | 7

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Series incompletas y ceros: decirle la verdad al modelo
# MAGIC
# MAGIC Hay 355 series y no todas son iguales: 29 del mercado regulado concentran el 69 % de la energía; muchas del no regulado
# MAGIC son un solo cliente grande, con días en cero (¿paró la planta o no llegó la medida?) y series que empiezan a mitad de
# MAGIC año. Dos banderas baratas le dan ese contexto al modelo en vez de esconderlo:
# MAGIC
# MAGIC | Bandera | Qué dice | Por qué no se "arregla" en plata |
# MAGIC |---|---|---|
# MAGIC | `es_cero` | `demanda_real_kwh == 0` | El cero es un dato válido de la fuente; imputarlo sería inventar. Que el modelo decida |
# MAGIC | `dias_con_datos` | cuántas filas tiene la serie hasta `t` | Con 5 días de historia `lag_28` es nulo y `media_28` es de 5 días; el modelo debe saberlo |
# MAGIC
# MAGIC La regla general: **plata no inventa datos; oro describe los que hay**. Imputar (rellenar) es una decisión del modelo,
# MAGIC documentada, no un paso escondido en la transformación.

# COMMAND ----------

con_banderas = fb.agregar_banderas(con_objetivo)
print("Serie-días con demanda cero:", con_banderas.filter("es_cero").count())
display(
    con_banderas.groupBy(*fb.LLAVE_SERIE).agg(F.max("dias_con_datos").alias("dias"), F.sum(F.col("es_cero").cast("int")).alias("ceros"))
    .orderBy("dias").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Construir, publicar y firmar el contrato
# MAGIC
# MAGIC Todo lo anterior, encadenado en una función (`fb.construir_features`) que recibe plata, los festivos y la configuración,
# MAGIC y escribe con **MERGE por llave** (`fb.merge_features`). Igual que en plata: correrlo dos veces con los mismos datos no
# MAGIC cambia nada, y una republicación en plata actualiza solo sus filas de oro. En producción esto es
# MAGIC `notebooks/03_gold_features.py`: el mismo `src`, con `catalog` como parámetro del job (clase 12).
# MAGIC
# MAGIC ```python
# MAGIC df.transform(lambda d: agregar_calendario(d, festivos))   # .transform encadena funciones puras df -> df
# MAGIC   .transform(lambda d: agregar_lags(d, m["lags"]))        # cada una se prueba sola
# MAGIC   .transform(lambda d: agregar_ventanas(d, m["ventanas"]))
# MAGIC   .transform(agregar_banderas)
# MAGIC   .transform(lambda d: agregar_objetivo(d, m["horizon_days"]))
# MAGIC ```
# MAGIC
# MAGIC **Y en Databricks, ¿qué más hay?** Dos cosas que conviene conocer, aunque aquí no las usemos por los límites de Free
# MAGIC Edition:
# MAGIC
# MAGIC - **Feature Engineering en Unity Catalog**: cualquier tabla Delta con **llave primaria** es una *feature table*. El
# MAGIC   entrenamiento (clase 9) puede registrar qué features usó, y el modelo servido (clase 11) las busca solo por llave.
# MAGIC   El primer paso es declarar la llave, y eso sí lo hacemos hoy: `ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY (…)`.
# MAGIC - **Lakehouse Federation**: cuando una fuente externa (por ejemplo, temperatura por región desde un SQL Server o
# MAGIC   Postgres) tiene que entrar a oro, se consulta *en su sitio* desde Unity Catalog, con los mismos permisos, sin copiarla.
# MAGIC   Es la forma limpia de sumar una fuente que otro equipo gobierna.

# COMMAND ----------

importlib.reload(fb)
features = fb.construir_features(diaria, festivos, cfg)
print("Columnas de oro:", len(features.columns))
fb.merge_features(spark, features, T_FEATURES)
fb.merge_features(spark, features, T_FEATURES)  # segunda vez: 0 insertadas... ¿y actualizadas?
display(spark.sql(f"DESCRIBE HISTORY {T_FEATURES}").select("version", "operation", "operationMetrics.numTargetRowsInserted", "operationMetrics.numTargetRowsUpdated"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta (respóndela aquí):** el segundo MERGE muestra 0 insertadas pero 72.704 **actualizadas**. En plata, el segundo
# MAGIC MERGE daba 0/0. ¿Qué diferencia hay entre `merge_features` y `merge_demanda_diaria` que lo explica, y qué columna hace
# MAGIC que las filas "cambien" aunque los datos sean los mismos? ¿Está bien así o lo cambiarías?
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

# Contrato: llave primaria (requisito de feature table), comentario y tags. Se ven en Catalog Explorer.
h = cfg["modelo"]["horizon_days"]
nombre_modelo = cfg["modelo"]["nombre"]
llave_sql = ", ".join(fb.LLAVE)
for c in fb.LLAVE:
    spark.sql(f"ALTER TABLE {T_FEATURES} ALTER COLUMN {c} SET NOT NULL")
spark.sql(f"ALTER TABLE {T_FEATURES} DROP CONSTRAINT IF EXISTS pk_features_demanda_diaria")
spark.sql(f"ALTER TABLE {T_FEATURES} ADD CONSTRAINT pk_features_demanda_diaria PRIMARY KEY ({llave_sql})")
spark.sql(f"ALTER TABLE {T_FEATURES} CLUSTER BY (fecha, tipo_mercado)")
comentario = (
    f"Oro. Features diarias por serie para pronostico a {h} dias (modelo {nombre_modelo}). "
    f"Regla: la fila de t solo usa informacion disponible en t; ventanas sobre t-n..t-1. y_h{h} = objetivo (nulo en inferencia). "
    "Fuente: demanda_diaria + data/reference/festivos_co.csv. Construida por xm_demanda.features.build."
)
spark.sql(f"COMMENT ON TABLE {T_FEATURES} IS '{comentario}'")
spark.sql(f"ALTER TABLE {T_FEATURES} SET TAGS ('capa' = 'oro', 'dominio' = 'energia', 'uso' = 'modelo_{nombre_modelo}', 'clase' = 'c07')")
spark.sql(f"COMMENT ON COLUMN {T_FEATURES}.media_7 IS 'Promedio de demanda_real_kwh en los 7 dias anteriores (t-7..t-1). Nunca incluye t.'")
spark.sql(f"COMMENT ON COLUMN {T_FEATURES}.y_h{h} IS 'Objetivo: demanda_real_kwh en t+{h}. Nulo en los ultimos {h} dias de cada serie (filas de inferencia).'")
display(spark.sql(f"DESCRIBE TABLE EXTENDED {T_FEATURES}").filter("col_name IN ('Comment', 'Clustering Information', 'Table Properties')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC
# MAGIC Cuando todo esté en OK: commit `feat(c07): features de oro con calendario, lags y ventanas` (incluye
# MAGIC `src/xm_demanda/features/build.py`), push, PR hacia `main` de tu fork, CI en verde, merge. Al cierre se libera el tag `s07`.

# COMMAND ----------

import re


def verificar():
    checks = []
    t = spark.table(T_FEATURES)
    n = t.count()
    checks.append(("features_demanda_diaria con 72.704 serie-días", n == 72_704))
    checks.append(("Llave única", t.select(*fb.LLAVE).distinct().count() == n))
    checks.append(("Columnas en snake_case", all(re.fullmatch(r"[a-z0-9_]+", c) for c in t.columns)))
    cal = {"dia_semana", "es_fin_de_semana", "es_festivo", "mes", "dia_del_anio"}
    checks.append(("Calendario completo y sin nulos", cal <= set(t.columns) and t.filter(" OR ".join(f"{c} IS NULL" for c in cal)).count() == 0))
    checks.append(("Festivos marcados (al menos 8 días en tus datos)", t.filter("es_festivo").select("fecha").distinct().count() >= 8))
    checks.append(("es_fin_de_semana coincide con dia_semana IN (1,7)", t.filter("es_fin_de_semana <> (dia_semana IN (1,7))").count() == 0))
    lags = fb.nombres_lags(cfg["modelo"]["lags"])
    checks.append(("Lags de la configuración presentes", set(lags) <= set(t.columns)))
    por_serie = t.filter("lag_7 IS NULL").groupBy(*fb.LLAVE_SERIE).count().agg(F.max("count")).first()[0]
    checks.append(("lag_7 nulo solo en los primeros 7 días de cada serie", por_serie is not None and por_serie <= 7))
    w = Window.partitionBy(*fb.LLAVE_SERIE).orderBy("fecha")
    fuga = t.withColumn("_m", F.avg(fb.MEDIDA).over(w.rowsBetween(-7, -1))).filter("ABS(_m - media_7) > 1e-6").count()
    checks.append(("media_7 nunca incluye el día t", fuga == 0))
    checks.append(("y_h7 nulo solo en los últimos 7 días de cada serie", t.filter("y_h7 IS NULL").groupBy(*fb.LLAVE_SERIE).count().agg(F.max("count")).first()[0] <= 7))
    h = spark.sql(f"DESCRIBE HISTORY {T_FEATURES}").select("operation", "operationMetrics").collect()
    merges = [r.operationMetrics for r in h if r.operation == "MERGE"]
    checks.append(("Al menos dos MERGE en el historial (idempotencia probada)", len(merges) >= 2))
    checks.append(("El segundo MERGE no insertó filas", any(int(m.get("numTargetRowsInserted", 0)) == 0 for m in merges)))
    det = spark.sql(f"DESCRIBE DETAIL {T_FEATURES}").first()
    checks.append(("Comentario de tabla escrito", bool(det.description)))
    checks.append(("Liquid clustering por fecha y tipo_mercado", set(det.clusteringColumns or []) == {"fecha", "tipo_mercado"}))
    pk = spark.sql(f"SHOW CREATE TABLE {T_FEATURES}").first()[0]
    checks.append(("Llave primaria declarada (feature table)", "PRIMARY KEY" in pk.upper()))
    try:
        fb.agregar_calendario(diaria.limit(1), festivos)
        fb.agregar_lags(diaria.limit(1), [1])
        checks.append(("TODO 1 y TODO 2 implementados en src", True))
    except NotImplementedError:
        checks.append(("TODO 1 y TODO 2 implementados en src", False))

    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/xm_demanda/features)." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
