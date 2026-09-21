# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 — Gobernar tu propio esquema
# MAGIC
# MAGIC Eres owner de `workspace.c01_<usuario>`. Hoy decides quién lee qué, lo escribes en el catálogo
# MAGIC y lo documentas en `docs/governance.md`.
# MAGIC
# MAGIC Pasos 1 a 5 se hacen en pareja (tú das permisos, tu compañero consulta). Pasos 6 a 10 se hacen solo.
# MAGIC Las celdas con `TODO` son las que escribes tú; el resto se ejecuta tal cual.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
dbutils.widgets.text("companero", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
companero = dbutils.widgets.get("companero").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."
assert "@" in companero, "Escribe el correo completo de tu compañero (con @)."

esquema = f"workspace.c01_{usuario}"
tabla = f"{esquema}.demanda_raw"
yo = spark.sql("SELECT current_user()").first()[0]
print("Esquema:", esquema, "| Tabla:", tabla, "| Yo:", yo, "| Compañero:", companero)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — GRANT a todos y comprobar
# MAGIC Un GRANT sobre el esquema (USE SCHEMA) más un GRANT sobre la tabla (SELECT). Los dos hacen falta: sin USE SCHEMA nadie entra.

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {esquema} TO `account users`")
spark.sql(f"GRANT SELECT ON TABLE {tabla} TO `account users`")
spark.sql(f"SHOW GRANTS ON TABLE {tabla}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Tu compañero** ejecuta ahora, en su propio notebook, contra TU tabla:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT TipoMercado, count(*) AS filas, round(sum(Valor)) AS kwh
# MAGIC FROM workspace.c01_<tu_usuario>.demanda_raw WHERE CodigoVariable = 'DdaReal' GROUP BY ALL
# MAGIC ```
# MAGIC Debe ver las dos filas: Regulado y No Regulado, con cifras completas. Anota cuántos kWh ve en Regulado.

# COMMAND ----------

# La celda que usa el compañero para consultar la tabla de otro (cambia el usuario de la tabla):
# spark.sql("SELECT TipoMercado, count(*) AS filas, round(sum(Valor)) AS kwh FROM workspace.c01_<companero>.demanda_raw WHERE CodigoVariable = 'DdaReal' GROUP BY ALL").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — REVOKE y ver el error
# MAGIC Quita el SELECT. Tu compañero vuelve a ejecutar la consulta y lee el error completo: dice qué privilegio falta y sobre qué objeto.

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON TABLE {tabla} FROM `account users`")
spark.sql(f"SHOW GRANTS ON TABLE {tabla}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** El error de tu compañero, ¿menciona la tabla o el esquema? ¿Por qué el USE SCHEMA que sigue vigente no le alcanza?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Filtro de filas y máscara de columna
# MAGIC Dos funciones SQL en tu esquema. La lógica vive en el catálogo, no en cada notebook.
# MAGIC
# MAGIC - `solo_regulado(tipo)`: devuelve TRUE para ti (owner) siempre; para los demás, solo cuando `tipo = 'Regulado'`.
# MAGIC - `mascara_kwh(v)`: para ti devuelve el valor; para los demás lo redondea a miles (`round(v, -3)`).
# MAGIC
# MAGIC En producción la condición es `is_account_group_member('grp_negocio')` (ver diapositiva 9); hoy, sin poder crear grupos, usamos `current_user()`.

# COMMAND ----------

# TODO: completa las dos funciones. Pista: current_user() devuelve tu correo; la variable `yo` ya lo tiene.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {esquema}.solo_regulado(tipo STRING)
RETURNS BOOLEAN
RETURN <condición>
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {esquema}.mascara_kwh(v DOUBLE)
RETURNS DOUBLE
RETURN <expresión>
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {tabla} SET ROW FILTER {esquema}.solo_regulado ON (TipoMercado)")
spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN Valor SET MASK {esquema}.mascara_kwh")

# Tú, como owner, sigues viendo todo:
spark.sql(f"SELECT TipoMercado, count(*) AS filas, round(sum(Valor)) AS kwh FROM {tabla} WHERE CodigoVariable = 'DdaReal' GROUP BY ALL").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — GRANT otra vez y comparar
# MAGIC Devuelve el SELECT. Tu compañero repite la consulta: ahora ve una sola fila (Regulado) y kWh redondeados a miles.
# MAGIC Misma tabla, distinta vista, sin copiar nada.

# COMMAND ----------

spark.sql(f"GRANT SELECT ON TABLE {tabla} TO `account users`")
spark.sql(f"SHOW GRANTS ON TABLE {tabla}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta.** ¿Qué pasaría si en vez de `account users` el GRANT fuera a un grupo `grp_negocio` que aún no existe? ¿Y si mañana entra una persona nueva a XM: a quién hay que tocar, la tabla o el grupo?
# MAGIC
# MAGIC _Tu respuesta:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Tags y comentarios
# MAGIC Cuatro tags de tabla (`capa`, `dominio`, `owner`, `sensibilidad`), un comentario de tabla y dos de columna.

# COMMAND ----------

# TODO: pon los cuatro tags y los comentarios.
spark.sql(f"ALTER TABLE {tabla} SET TAGS ('capa' = '<…>', 'dominio' = '<…>', 'owner' = '<…>', 'sensibilidad' = '<…>')")
spark.sql(f"COMMENT ON TABLE {tabla} IS '<qué contiene, con qué grano, quién la publica>'")
spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN Valor COMMENT '<…>'")
spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN FechaPublicacion COMMENT '<…>'")

spark.sql(f"SELECT tag_name, tag_value FROM workspace.information_schema.table_tags WHERE schema_name = 'c01_{usuario}' AND table_name = 'demanda_raw'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Vista y linaje
# MAGIC Una vista de solo regulado. Ejecuta una consulta sobre ella y luego abre **Catalog Explorer → tu esquema → demanda_regulado_v → pestaña Lineage**: la flecha desde `demanda_raw` aparece sola.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {esquema}.demanda_regulado_v
COMMENT 'Demanda diaria del mercado regulado por agente (kWh)'
AS
SELECT Fecha, CodigoSICAgente, MercadoComercializacion, ClasificacionIndustrial, Valor AS demanda_kwh, FechaPublicacion
FROM {tabla}
WHERE CodigoVariable = 'DdaReal' AND TipoMercado = 'Regulado'
""")
spark.sql(f"SELECT count(*) AS filas, min(Fecha) AS desde, max(Fecha) AS hasta FROM {esquema}.demanda_regulado_v").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Auditoría: ¿quién tiene qué sobre mi esquema?
# MAGIC Lo mismo que la auditora de XM pediría por correo, resuelto con una consulta.

# COMMAND ----------

spark.sql(f"""
SELECT table_name, grantee, privilege_type
FROM workspace.information_schema.table_privileges
WHERE table_schema = 'c01_{usuario}'
ORDER BY table_name, grantee
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — `docs/governance.md`
# MAGIC Abre el archivo en tu Git folder y llena las tablas: catálogos, esquemas, grupos, matriz de privilegios, filtros y máscaras, secretos, auditoría.
# MAGIC Escribe la solución para XM (tres catálogos, grupos), no lo que hiciste hoy en `workspace`: hoy fue el ensayo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación automática
# MAGIC Ejecuta cuando hayas terminado los pasos 1 a 8.

# COMMAND ----------

import os
import re


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "docs", "governance.md")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro docs/governance.md: ejecuta este notebook desde tu Git folder.")


def verificar():
    sch = f"c01_{usuario}"
    info = "workspace.information_schema"
    checks = []

    def n(sql):
        return spark.sql(sql).count()

    checks.append(("SELECT vigente para `account users` sobre demanda_raw",
                   n(f"SELECT 1 FROM {info}.table_privileges WHERE table_schema = '{sch}' AND table_name = 'demanda_raw' AND privilege_type = 'SELECT' AND grantee = 'account users'") > 0))
    checks.append(("Filtro de filas activo en demanda_raw",
                   n(f"SELECT 1 FROM {info}.row_filters WHERE table_schema = '{sch}' AND table_name = 'demanda_raw'") > 0))
    checks.append(("Máscara activa en la columna Valor",
                   n(f"SELECT 1 FROM {info}.column_masks WHERE table_schema = '{sch}' AND table_name = 'demanda_raw' AND column_name = 'Valor'") > 0))
    tags = {r[0]: r[1] for r in spark.sql(f"SELECT tag_name, tag_value FROM {info}.table_tags WHERE schema_name = '{sch}' AND table_name = 'demanda_raw'").collect()}
    checks.append(("4 tags (capa, dominio, owner, sensibilidad) sin marcadores",
                   {"capa", "dominio", "owner", "sensibilidad"} <= set(tags) and all("<" not in v for v in tags.values())))
    comentario = spark.sql(f"SELECT comment FROM {info}.tables WHERE table_schema = '{sch}' AND table_name = 'demanda_raw'").first()
    checks.append(("Comentario de tabla escrito", comentario is not None and comentario[0] and "<" not in comentario[0]))
    checks.append(("Vista demanda_regulado_v creada",
                   n(f"SELECT 1 FROM {info}.views WHERE table_schema = '{sch}' AND table_name = 'demanda_regulado_v'") > 0))

    raiz = _raiz_repo()
    gov = open(os.path.join(raiz, "docs", "governance.md"), encoding="utf-8").read()
    secciones = ["## Catálogos", "## Esquemas", "## Grupos", "## Matriz de privilegios", "## Filtros", "## Secretos", "## Auditoría"]
    checks.append(("governance.md con sus 7 secciones", all(s in gov for s in secciones)))
    checks.append(("governance.md sin marcadores <…> ni celdas vacías", "<" not in gov and re.search(r"\|\s*\|", gov) is None))
    checks.append(("governance.md menciona dev, qa y prod", all(c in gov for c in ["dev", "qa", "prod"])))

    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
