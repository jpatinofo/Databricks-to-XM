# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 6 — Modelado de plata: de reporte a tabla analítica
# MAGIC
# MAGIC Este notebook es la clase. La teoría está en las celdas de texto; el código, justo debajo, la ejecuta sobre tus datos.
# MAGIC Se recorre de arriba abajo, en clase con la docente y luego solo.
# MAGIC
# MAGIC **Lo que construyes hoy** (hito 3):
# MAGIC
# MAGIC ```
# MAGIC   demanda_validada (lab 5, formato largo)         dim_ciiu (data/reference + tus alias)
# MAGIC   ┌──────────┬──────────┬────────┬─────────┐      ┌────────┬────────────────────┬────────┐
# MAGIC   │ Fecha    │ Variable │ Serie… │ Valor   │      │ codigo │ nombre_oficial     │ llave  │
# MAGIC   ├──────────┼──────────┼────────┼─────────┤      ├────────┼────────────────────┼────────┤
# MAGIC   │ 07-20    │ DdaReal  │ EPSC…  │ 1.234,5 │      │ P      │ EDUCACIÓN          │ EDUCA… │
# MAGIC   │ 07-20    │ Perdidas │ EPSC…  │    18,2 │      │ X      │ SIN CLASIFICAR     │ SIN C… │
# MAGIC   └──────────┴──────────┴────────┴─────────┘      └────────┴────────────────────┴────────┘
# MAGIC              │  pivote + MERGE                              │ join por llave normalizada
# MAGIC              ▼                                              ▼
# MAGIC   demanda_diaria (formato ancho, una fila por serie-día, con historial de versiones)
# MAGIC   ┌──────────┬────────┬───────────────────┬──────────────┬──────────────┬───────────────────┐
# MAGIC   │ fecha    │ serie… │ demanda_real_kwh  │ perdidas_kwh │ ciiu_seccion │ _publication_date │
# MAGIC   ├──────────┼────────┼───────────────────┼──────────────┼──────────────┼───────────────────┤
# MAGIC   │ 07-20    │ EPSC…  │           1.234,5 │         18,2 │ X            │ 2026-07-25        │
# MAGIC   └──────────┴────────┴───────────────────┴──────────────┴──────────────┴───────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Las celdas con `TODO` se escriben en `src/xm_demanda/silver/transform.py`, no aquí.
# MAGIC Requiere el lab 5: `workspace.c01_<usuario>.demanda_validada`.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

esquema = f"workspace.c01_{usuario}"
T_VALIDADA = f"{esquema}.demanda_validada"
T_DIARIA = f"{esquema}.demanda_diaria"
T_CIIU = f"{esquema}.dim_ciiu"
print("Fuente:", T_VALIDADA, "| Destinos:", T_DIARIA, T_CIIU)

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
import xm_demanda.silver.transform as silver

importlib.reload(silver)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Por qué plata cambia de forma: largo → ancho
# MAGIC
# MAGIC Bronce guarda lo que llega: **formato largo**, una fila por serie-día-**variable**. Es cómodo para la fuente (agregar
# MAGIC una variable nueva no cambia el esquema) y pésimo para analizar: para saber la demanda y las pérdidas de un día hay que
# MAGIC leer dos filas y cruzarlas.
# MAGIC
# MAGIC Plata sirve a los consumidores: **formato ancho**, una fila por serie-día, una columna por medida. Es la forma que
# MAGIC esperan un modelo (una fila = una observación), un tablero y un analista.
# MAGIC
# MAGIC | | Largo (bronce) | Ancho (plata) |
# MAGIC |---|---|---|
# MAGIC | Grano | serie-día-variable | serie-día |
# MAGIC | Agregar una medida | una fila más | una columna más (cambio de esquema, con ADR) |
# MAGIC | Leer un día | dos filas + cruce | una fila |
# MAGIC | Quién lo prefiere | la fuente, la ingesta | el modelo, el tablero, SQL de negocio |
# MAGIC
# MAGIC **El contrato de `demanda_diaria`** (lo que escribiste en `docs/architecture.md` en la clase 2):
# MAGIC
# MAGIC | Columna | Tipo | Regla |
# MAGIC |---|---|---|
# MAGIC | `fecha` | date | día del dato |
# MAGIC | `codigo_sic_agente`, `mercado_comercializacion`, `tipo_mercado`, `clasificacion_industrial` | string | llave de serie |
# MAGIC | `demanda_real_kwh`, `perdidas_kwh` | double | ≥ 0; pérdidas ≤ demanda |
# MAGIC | `ciiu_seccion` | string | letra A–U o X (sin clasificar); viene de `dim_ciiu` |
# MAGIC | `_publication_date` | date | versión de negocio; decide qué fila gana en el MERGE |
# MAGIC
# MAGIC Convención de nombres: `snake_case`, unidad en el nombre (`_kwh`), sin abreviaturas ambiguas. Los nombres de la
# MAGIC fuente (`CodigoSICAgente`) se quedan en bronce; plata habla el idioma de quien la consume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El pivote, en SQL y en PySpark: cuándo cada uno
# MAGIC
# MAGIC Las dos versiones de abajo producen exactamente la misma tabla. Elegir una no es cuestión de gusto:
# MAGIC
# MAGIC | Usa SQL cuando… | Usa PySpark cuando… |
# MAGIC |---|---|
# MAGIC | La transformación cabe en una consulta y la va a leer alguien de negocio | La lógica se reutiliza (una función en `src`, con test) |
# MAGIC | Es una vista, una expectation, una consulta de auditoría | Hay parámetros, ramas, listas de columnas que cambian |
# MAGIC | Vas a pegarla en un tablero o en un pipeline declarativo en SQL | Vas a encadenar `.transform()` y probarla con pytest |
# MAGIC
# MAGIC En este curso: **SQL para leer y validar, PySpark para construir** (lo que escribe tablas vive en `src`).
# MAGIC
# MAGIC Primero, la fuente: `demanda_validada` sin las filas sintéticas del lab 5. Fíjate en que las excluimos por
# MAGIC `_source_file`: los metadatos de ingesta (clase 4) sirven exactamente para esto.

# COMMAND ----------

fuente = spark.table(T_VALIDADA).filter("_source_file <> 'sintetico_lab05'")
print("Filas fuente (largo):", fuente.count())  # 145.408
fuente.createOrReplaceTempView("fuente_largo")

# COMMAND ----------

# MAGIC %md
# MAGIC **Versión SQL** — `max(CASE WHEN …)` por variable. Se lee de corrido; no se reutiliza.

# COMMAND ----------

ancho_sql = spark.sql("""
SELECT Fecha AS fecha, CodigoSICAgente AS codigo_sic_agente, MercadoComercializacion AS mercado_comercializacion,
       TipoMercado AS tipo_mercado, ClasificacionIndustrial AS clasificacion_industrial,
       max(CASE WHEN CodigoVariable = 'DdaReal' THEN Valor END) AS demanda_real_kwh,
       max(CASE WHEN CodigoVariable = 'PerdidasEnergia' THEN Valor END) AS perdidas_kwh,
       max(_publication_date) AS _publication_date
FROM fuente_largo
GROUP BY ALL
""")
print("Serie-días (SQL):", ancho_sql.count())  # 72.704

# COMMAND ----------

# MAGIC %md
# MAGIC **Versión PySpark** — `silver.a_formato_ancho(df)`: la misma lógica, como función de `src`. Es la que usa el resto del curso.
# MAGIC Abre `src/xm_demanda/silver/transform.py` y compárala con el SQL de arriba: `RENOMBRES` + `groupBy(LLAVE).agg(max(when(...)))`.

# COMMAND ----------

ancho = silver.a_formato_ancho(fuente)
print("Serie-días (PySpark):", ancho.count())
assert ancho.count() == ancho_sql.count() == 72_704
assert ancho.select(*silver.LLAVE).distinct().count() == ancho.count(), "La llave no es única"
ancho.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · MERGE idempotente: la operación central de plata
# MAGIC
# MAGIC Tres formas de escribir una tabla y por qué solo una sirve para plata:
# MAGIC
# MAGIC ```
# MAGIC   overwrite   ──►  borra todo y escribe de nuevo.   Pierde el historial de versiones útil; caro con años de datos.
# MAGIC   append      ──►  agrega filas.                   Duplica en cada corrida; bronce lo hace a propósito, plata no.
# MAGIC   MERGE       ──►  por llave: si existe, actualiza; si no, inserta.   Idempotente si la condición está bien puesta.
# MAGIC ```
# MAGIC
# MAGIC `MERGE` (upsert) compara la fuente con el destino por la **llave de negocio** y decide fila a fila:
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO demanda_diaria AS t
# MAGIC USING fuente AS s
# MAGIC ON t.fecha = s.fecha AND t.codigo_sic_agente = s.codigo_sic_agente AND …      -- la llave
# MAGIC WHEN MATCHED AND s._publication_date > t._publication_date THEN UPDATE SET *  -- solo si llega algo más nuevo
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC La cláusula `AND s._publication_date > t._publication_date` es la que hace la idempotencia: volver a correr con
# MAGIC los mismos datos no actualiza nada, porque nada es más nuevo. Y una republicación (fecha de publicación mayor)
# MAGIC actualiza exactamente las filas que cambiaron. Es la decisión 3 del ADR-002 ("vigente = la publicación más reciente")
# MAGIC materializada.
# MAGIC
# MAGIC Vamos a verlo con los números de `DESCRIBE HISTORY`: cada MERGE deja una versión con cuántas filas insertó y actualizó.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {T_DIARIA}")  # empezamos limpio para leer el historial desde la versión 0
silver.merge_demanda_diaria(spark, ancho, T_DIARIA)  # primera corrida: todo se inserta
silver.merge_demanda_diaria(spark, ancho, T_DIARIA)  # segunda corrida: nada cambia


def historial():
    return spark.sql(f"""
    SELECT version, operation,
           operationMetrics['numTargetRowsInserted'] AS insertadas,
           operationMetrics['numTargetRowsUpdated']  AS actualizadas,
           operationMetrics['numOutputRows']         AS filas_escritas,
           timestamp
    FROM (DESCRIBE HISTORY {T_DIARIA})
    ORDER BY version
    """)


historial().display()
# Esperado: v0 CREATE, v1 MERGE insertadas=72704 actualizadas=0, v2 MERGE insertadas=0 actualizadas=0

# COMMAND ----------

# MAGIC %md
# MAGIC **Llega una republicación.** XM corrige tres serie-días y los publica el 5 de agosto. Solo esas tres filas deben cambiar.

# COMMAND ----------

republicados = (ancho.orderBy("fecha", "codigo_sic_agente").limit(3)
                .withColumn("demanda_real_kwh", F.round(F.col("demanda_real_kwh") * 1.02, 2))
                .withColumn("_publication_date", F.lit("2026-08-05").cast("date")))
silver.merge_demanda_diaria(spark, republicados, T_DIARIA)
historial().display()
# Esperado: v3 MERGE insertadas=0 actualizadas=3
print("Filas en demanda_diaria:", spark.table(T_DIARIA).count())  # sigue en 72.704

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** Si la condición fuera `WHEN MATCHED THEN UPDATE SET *` (sin comparar `_publication_date`), ¿qué mostraría
# MAGIC `actualizadas` en la segunda corrida? ¿Sigue siendo idempotente el resultado? ¿Y el historial?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Time travel: qué versión del dato vio el modelo el día X
# MAGIC
# MAGIC Cada escritura en una tabla Delta crea una **versión**. Los archivos viejos no se borran de inmediato: se pueden
# MAGIC consultar con `VERSION AS OF` o `TIMESTAMP AS OF`. Eso responde tres preguntas que en XM alguien va a hacer:
# MAGIC
# MAGIC | Pregunta | Cómo |
# MAGIC |---|---|
# MAGIC | ¿Qué valor tenía esta serie-día antes de la republicación? | `SELECT … FROM t VERSION AS OF 2` |
# MAGIC | ¿Con qué datos se entrenó el modelo del 15 de agosto? | El entrenamiento guarda `version` de la tabla (clase 9); luego `VERSION AS OF` esa versión |
# MAGIC | Alguien escribió mal y hay que volver atrás | `RESTORE TABLE t TO VERSION AS OF 2` |
# MAGIC
# MAGIC Límite: las versiones viven mientras `VACUUM` no borre sus archivos (por defecto, 7 días de retención). Para
# MAGIC reproducibilidad a largo plazo se guarda una copia o se sube la retención; se decide en el runbook (clase 15).

# COMMAND ----------

llave_ejemplo = republicados.select(*silver.LLAVE).first()
cond = " AND ".join(f"{c} = '{llave_ejemplo[c]}'" for c in silver.LLAVE)
spark.sql(f"""
SELECT 'v2 (antes de la republicación)' AS version, demanda_real_kwh, _publication_date FROM {T_DIARIA} VERSION AS OF 2 WHERE {cond}
UNION ALL
SELECT 'v3 (después)', demanda_real_kwh, _publication_date FROM {T_DIARIA} VERSION AS OF 3 WHERE {cond}
""").display()

version_actual = spark.sql(f"DESCRIBE HISTORY {T_DIARIA}").agg(F.max("version")).first()[0]
print("Versión actual de la tabla (lo que un entrenamiento guardaría):", version_actual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Dimensión CIIU: datos de referencia y texto normalizado
# MAGIC
# MAGIC ### Sector
# MAGIC La **CIIU Rev. 4 A.C.** es la clasificación de actividades económicas que adapta el DANE para Colombia: 21 secciones
# MAGIC (A agricultura … U organizaciones extraterritoriales). En el mercado, el CIIU solo aplica a los usuarios **no regulados**
# MAGIC (empresas que negocian su tarifa); los **regulados** (hogares y pequeños comercios con tarifa CREG) llegan como
# MAGIC `SIN CLASIFICAR`. Por eso la regla `regulado_sin_clasificar` de la clase 5, y por eso el modelo puede usar el sector
# MAGIC económico solo para el 31 % de la energía que es no regulada.
# MAGIC
# MAGIC ### Modelado
# MAGIC Una **dimensión** describe una entidad (aquí, la sección económica); los **hechos** (`demanda_diaria`) la referencian por
# MAGIC una llave corta. Ventajas: la letra `P` ocupa menos que `EDUCACIÓN`, el nombre se corrige en un solo sitio, y se pueden
# MAGIC agregar atributos (sector primario/secundario/terciario) sin tocar los hechos.
# MAGIC
# MAGIC ```
# MAGIC   data/reference/ciiu_secciones.csv  (en el repo, versionado)        fuente: valores distintos de ClasificacionIndustrial
# MAGIC              │ normalizar_texto()                                           │ normalizar_texto() + ALIAS_CIIU
# MAGIC              ▼                                                              ▼
# MAGIC          llave "EDUCACION"  ◄──────────────── join ─────────────────►  llave "EDUCACION"
# MAGIC ```
# MAGIC
# MAGIC El problema real de las dimensiones no es el join, es el **texto**: tildes, mayúsculas, espacios dobles y errores
# MAGIC de digitación. La llave de unión es el texto normalizado, y lo que no coincide se detecta con un **anti-join** y se
# MAGIC resuelve con un alias explícito, no editando la fuente.
# MAGIC
# MAGIC **TODO en `src/xm_demanda/silver/transform.py`:** implementa `normalizar_texto` (mayúsculas, sin tildes, espacios
# MAGIC colapsados; `None` → `None`). Guarda y ejecuta la celda siguiente.

# COMMAND ----------

importlib.reload(silver)
assert silver.normalizar_texto("  Educación  ") == "EDUCACION"
assert silver.normalizar_texto("Actividades   Artísticas, de Entretenimiento") == "ACTIVIDADES ARTISTICAS, DE ENTRETENIMIENTO"
assert silver.normalizar_texto(None) is None
print("OK: normalizar_texto")

# COMMAND ----------

ruta_csv = os.path.join(raiz, "data", "reference", "ciiu_secciones.csv")
dim, sin_match = silver.construir_dim_ciiu(spark, ruta_csv, ancho)
print("Secciones en la dimensión:", dim.count())
print("Textos de la fuente sin sección:")
sin_match.display()
# Esperado la primera vez: 1 texto sin sección. Léelo con cuidado y compáralo con el nombre oficial.

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** ¿Cuál es la diferencia entre el texto de la fuente y el nombre oficial? ¿Es un error de XM, del DANE o de
# MAGIC nadie? ¿Por qué lo resolvemos con un alias en `src` y no corrigiendo la fila en bronce?
# MAGIC
# MAGIC _Tu respuesta:_
# MAGIC
# MAGIC **TODO:** agrega en `ALIAS_CIIU` la entrada `{texto normalizado de la fuente: texto normalizado oficial}` y vuelve a ejecutar.

# COMMAND ----------

importlib.reload(silver)
dim, sin_match = silver.construir_dim_ciiu(spark, ruta_csv, ancho)
assert sin_match.count() == 0, "Todavía hay textos sin sección: revisa ALIAS_CIIU"
dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_CIIU)
spark.table(T_CIIU).orderBy("codigo").display()
# Esperado: 22 filas (21 secciones + X), todas con nombre_fuente

# COMMAND ----------

# MAGIC %md
# MAGIC **Unir la dimensión a los hechos.** `ciiu_seccion` entra a `demanda_diaria` por MERGE, no por overwrite: así el
# MAGIC historial de la tabla sigue intacto. La columna nueva se agrega con `ALTER TABLE` y el MERGE la rellena.

# COMMAND ----------

spark.sql(f"ALTER TABLE {T_DIARIA} ADD COLUMNS (ciiu_seccion STRING)")
spark.sql(f"""
MERGE INTO {T_DIARIA} AS t
USING (
  SELECT d.nombre_fuente, d.codigo FROM {T_CIIU} d WHERE d.nombre_fuente IS NOT NULL
) AS c
ON t.clasificacion_industrial = c.nombre_fuente
WHEN MATCHED THEN UPDATE SET t.ciiu_seccion = c.codigo
""")
spark.sql(f"""
SELECT ciiu_seccion, count(*) AS serie_dias, round(sum(demanda_real_kwh) / 1e6, 1) AS gwh
FROM {T_DIARIA} GROUP BY ALL ORDER BY gwh DESC
""").display()
sin_seccion = spark.table(T_DIARIA).filter("ciiu_seccion IS NULL").count()
print("Serie-días sin sección:", sin_seccion)  # 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Rendimiento sin particiones a mano: liquid clustering, OPTIMIZE y Photon
# MAGIC
# MAGIC **Particionar** (`PARTITIONED BY fecha`) fue la forma clásica de acelerar filtros: una carpeta por valor. Problemas:
# MAGIC hay que elegir la columna al crear la tabla, una columna de alta cardinalidad crea miles de archivos pequeños, y
# MAGIC cambiar de columna es reescribir todo.
# MAGIC
# MAGIC **Liquid clustering** (`CLUSTER BY`) agrupa los datos físicamente por las columnas que más se filtran, sin carpetas,
# MAGIC se puede cambiar con un `ALTER TABLE`, y admite varias columnas. `OPTIMIZE` compacta archivos pequeños y aplica el
# MAGIC clustering. En serverless se ejecuta también automáticamente (predictive optimization); aquí lo lanzamos a mano para verlo.
# MAGIC
# MAGIC **Photon** es el motor de ejecución vectorizado de Databricks (C++). En serverless está siempre activo. Acelera SQL y
# MAGIC DataFrames; **no** acelera UDFs de Python (como `normalizar_texto` en la sección 5): por eso las UDFs se usan para
# MAGIC construir dimensiones pequeñas, no para procesar hechos.
# MAGIC
# MAGIC | Cuándo | Qué |
# MAGIC |---|---|
# MAGIC | Tabla que se filtra por fecha y por tipo de mercado | `CLUSTER BY (fecha, tipo_mercado)` |
# MAGIC | Muchos MERGE pequeños (republicaciones) | `OPTIMIZE` periódico o predictive optimization |
# MAGIC | Lógica fila a fila en Python sobre millones de filas | Reescribir con funciones nativas (`F.*`) para que Photon aplique |

# COMMAND ----------

detalle_antes = spark.sql(f"DESCRIBE DETAIL {T_DIARIA}").select("numFiles", "sizeInBytes", "clusteringColumns").first()
spark.sql(f"ALTER TABLE {T_DIARIA} CLUSTER BY (fecha, tipo_mercado)")
spark.sql(f"OPTIMIZE {T_DIARIA}")
detalle_despues = spark.sql(f"DESCRIBE DETAIL {T_DIARIA}").select("numFiles", "sizeInBytes", "clusteringColumns").first()
print("Antes: ", detalle_antes)
print("Después:", detalle_despues)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · El contrato, dentro del catálogo
# MAGIC Lo mismo que hiciste en la clase 3 con bronce: comentario, comentarios de columna y tags. Plata es la tabla que
# MAGIC más gente va a leer; su documentación vive donde la ven.

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {T_DIARIA} IS 'Demanda real y pérdidas por serie y día (kWh), última publicación vigente. Fuente: demanda_validada. MERGE por llave de serie + fecha.'")
spark.sql(f"ALTER TABLE {T_DIARIA} ALTER COLUMN demanda_real_kwh COMMENT 'Demanda real del día en kWh (publicación vigente)'")
spark.sql(f"ALTER TABLE {T_DIARIA} ALTER COLUMN ciiu_seccion COMMENT 'Sección CIIU Rev. 4 A.C. (A–U); X = sin clasificar (mercado regulado)'")
spark.sql(f"ALTER TABLE {T_DIARIA} SET TAGS ('capa' = 'silver', 'dominio' = 'energia', 'owner' = 'grp_ingenieria', 'sensibilidad' = 'interna')")
spark.sql(f"COMMENT ON TABLE {T_CIIU} IS 'Secciones CIIU Rev. 4 A.C. (DANE) con la llave normalizada usada para unir a demanda_diaria'")
print("Contrato escrito en el catálogo. Ábrelo en Catalog Explorer → tu esquema → demanda_diaria.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación automática

# COMMAND ----------

import re


def verificar():
    checks = []
    t = spark.table(T_DIARIA)
    checks.append(("demanda_diaria con 72.704 serie-días", t.count() == 72_704))
    checks.append(("Llave única (sin duplicados por serie-día)", t.select(*silver.LLAVE).distinct().count() == t.count()))
    checks.append(("Columnas en snake_case", all(re.fullmatch(r"[a-z_]+", c) for c in t.columns)))
    checks.append(("ciiu_seccion sin nulos", t.filter("ciiu_seccion IS NULL").count() == 0))
    h = spark.sql(f"DESCRIBE HISTORY {T_DIARIA}").select("operation", "operationMetrics").collect()
    merges = [r.operationMetrics for r in h if r.operation == "MERGE"]
    actualizadas = [int(m.get("numTargetRowsUpdated", 0)) for m in merges]
    checks.append(("Historial con un MERGE de 3 filas actualizadas (republicación)", 3 in actualizadas))
    checks.append(("Historial con un MERGE que no tocó nada (idempotencia)", any(int(m.get("numTargetRowsUpdated", 0)) == 0 and int(m.get("numTargetRowsInserted", 0)) == 0 for m in merges)))
    det = spark.sql(f"DESCRIBE DETAIL {T_DIARIA}").first()
    checks.append(("Liquid clustering por fecha y tipo_mercado", set(det.clusteringColumns or []) == {"fecha", "tipo_mercado"}))
    checks.append(("Comentario de tabla escrito", bool(det.description)))
    try:
        d = spark.table(T_CIIU)
        checks.append(("dim_ciiu con 22 secciones y todas con nombre_fuente", d.count() == 22 and d.filter("nombre_fuente IS NULL").count() == 0))
    except Exception:  # noqa: BLE001
        checks.append(("dim_ciiu existe", False))
    try:
        checks.append(("normalizar_texto implementada", silver.normalizar_texto("Educación") == "EDUCACION"))
    except NotImplementedError:
        checks.append(("normalizar_texto implementada", False))
    checks.append(("ALIAS_CIIU con el alias del anti-join", len(silver.ALIAS_CIIU) >= 1))

    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/xm_demanda/silver)." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
