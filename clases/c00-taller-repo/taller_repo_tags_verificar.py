# Databricks notebook source
# MAGIC %md
# MAGIC # Taller corto — Cómo usamos el repositorio: ramas, tags y `verificar()`
# MAGIC
# MAGIC 
# MAGIC
# MAGIC Tres cosas al salir de aquí:
# MAGIC 1. Saber qué hay en `main`, qué hay en `solution` y qué es un tag `sNN`.
# MAGIC 2. Poder comparar tu trabajo con la solución de referencia en dos clics.
# MAGIC 3. Entender qué mira `verificar()` y por qué no hay calificación.

# COMMAND ----------

dbutils.widgets.text("docente_github", "manularrea")
dbutils.widgets.text("tu_github", "")
docente = dbutils.widgets.get("docente_github").strip()
yo = dbutils.widgets.get("tu_github").strip()
assert docente and yo, "Llena los dos widgets: usuario de GitHub de la docente y el tuyo."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Dos repositorios, dos ramas, un tag por clase
# MAGIC
# MAGIC ```
# MAGIC   github.com/<docente>/Databricks-to-XM        (upstream: el original)
# MAGIC   ├── main      enunciados: lab.md, notebooks con TODO, plantillas <…>
# MAGIC   ├── solution  respuestas de referencia (la docente la actualiza al cierre de cada clase)
# MAGIC   └── tags      s01, s02, s03 … cada uno apunta al commit de solution al cerrar esa clase
# MAGIC
# MAGIC   github.com/<tú>/Databricks-to-XM             (origin: tu fork)
# MAGIC   ├── main      copia de main del original + TUS merges
# MAGIC   └── feature/cNN-<tema>   una rama por clase, se mezcla a main por PR
# MAGIC ```
# MAGIC
# MAGIC Un **tag** es un nombre fijo para un commit. `s03` siempre apuntará al mismo commit aunque `solution` siga avanzando.
# MAGIC Por eso comparamos contra `s03` y no contra `solution`: la clase 3 se compara con la solución de la clase 3, no con la de la 7.
# MAGIC
# MAGIC Los tags **no viajan con el fork**: `Sync fork` y `Pull` traen commits de ramas, no tags. Tu fork no los tiene y no los necesita (ver §3).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El ciclo de cada clase (siempre el mismo)
# MAGIC
# MAGIC | # | Dónde | Qué | Cuándo |
# MAGIC |---|---|---|---|
# MAGIC | 1 | GitHub, tu fork | **Sync fork** (trae los archivos nuevos de la clase desde el original) | Antes de la clase |
# MAGIC | 2 | Databricks, Git folder | Git → rama `main` → **Pull** | Antes de la clase |
# MAGIC | 3 | Databricks, Git folder | Git → **Create branch** `feature/cNN-<tema>` | Al empezar el lab |
# MAGIC | 4 | Notebook del lab | Trabajar; ejecutar `verificar()` hasta que todo sea OK | Durante el lab |
# MAGIC | 5 | Databricks, Git folder | Git → **Commit & push** con el mensaje que dice `lab.md` | Al terminar |
# MAGIC | 6 | GitHub, tu fork | **Compare & pull request** hacia `main` **de tu fork** (no del original) → CI en verde → **Merge** | Al terminar |
# MAGIC | 7 | GitHub, original | La docente publica el tag `sNN` | Cierre de clase |
# MAGIC | 8 | Navegador | Comparar tu `main` con `sNN` (§3) | Después de clase |
# MAGIC
# MAGIC Dos errores frecuentes: abrir el PR contra el repositorio original (GitHub lo propone por defecto: cambia el destino a tu fork), y hacer commit en `main` en vez de en la rama.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Ejercicio A — Comparar con la solución sin instalar nada
# MAGIC La celda arma tu URL de comparación. Ábrela: GitHub muestra el diff entre el tag y tu `main`.

# COMMAND ----------

tags = ["s01", "s02", "s03"]
for t in tags:
    print(f"{t}: https://github.com/{docente}/Databricks-to-XM/compare/{t}...{yo}:Databricks-to-XM:main")

# COMMAND ----------

# MAGIC %md
# MAGIC Cómo leer lo que aparece:
# MAGIC
# MAGIC | En la página | Significa |
# MAGIC |---|---|
# MAGIC | *"This branch is N commits ahead"* | Tu `main` tiene N commits que el tag no tiene: tu trabajo |
# MAGIC | Archivos en rojo/verde | Lo que cambia entre la solución y lo tuyo. Rojo (−) es como está en el tag; verde (+) como está en tu fork |
# MAGIC | Archivos que no esperabas (`notebooks/`, `src/`) | Ruido: la solución de clases posteriores no está en `sNN`, pero tu Sync fork trajo enunciados nuevos. Mira solo las rutas de la clase (tabla en `docs/autoevaluacion.md`) |
# MAGIC | *"There isn't anything to compare"* | Tu fork y el original no comparten historia: el fork se creó mal (no como fork sino como copia). Avísale a la docente |
# MAGIC
# MAGIC No hay una única respuesta correcta en diseño; sí hay decisiones sin justificar. Compara el porqué, no el qué.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Si trabajas en VS Code: los mismos pasos en terminal
# MAGIC
# MAGIC ```bash
# MAGIC git clone https://github.com/<tú>/Databricks-to-XM.git && cd Databricks-to-XM
# MAGIC git remote add upstream https://github.com/<docente>/Databricks-to-XM.git   # una sola vez
# MAGIC
# MAGIC # cada clase
# MAGIC git checkout main && git pull upstream main && git push origin main       # = Sync fork + Pull
# MAGIC git checkout -b feature/c03-gobierno                                         # rama de la clase
# MAGIC # … trabajo …
# MAGIC ruff check . && python -m pytest tests/unit                                  # lo mismo que corre CI
# MAGIC git add -A && git commit -m "feat(c03): gobierno del esquema y governance.md"
# MAGIC git push -u origin feature/c03-gobierno                                      # luego PR en GitHub
# MAGIC
# MAGIC # comparar con la solución
# MAGIC git fetch upstream --tags                                                    # los tags vienen de upstream, no de tu fork
# MAGIC git diff s03 -- docs/governance.md clases/c03-unity-catalog/notebooks
# MAGIC ```
# MAGIC
# MAGIC En el diff: líneas con `-` son la solución, líneas con `+` son las tuyas. `git diff --stat s03` da solo la lista de archivos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Qué es `verificar()` y qué no es
# MAGIC
# MAGIC Cada lab termina con una celda `verificar()`. Es una lista de comprobaciones **sobre cosas reales**: tablas que existen, permisos que están, archivos sin marcadores `<…>`. Imprime `OK` o `FALTA` por cada una. No pone nota, no envía nada a nadie, y puedes ejecutarla las veces que quieras.
# MAGIC
# MAGIC | Mira | Cómo | Ejemplo (clase) |
# MAGIC |---|---|---|
# MAGIC | Tablas y vistas | `spark.table()`, `count()` | `demanda_raw` con 145.408 filas (1) |
# MAGIC | Permisos, filtros, tags | `information_schema` | SELECT vigente para `account users` (3) |
# MAGIC | Documentos del repo | lee el archivo desde el Git folder y busca `<`, secciones, listas | ADR-001 con ≥ 2 alternativas (2) |
# MAGIC | Código en `src/` | importa la función y la llama | `resolver_alias(["ValorKwh"])` (4) |
# MAGIC
# MAGIC Lo que **no** mira: si tu razonamiento es bueno. Eso lo ves tú comparando con `sNN`, y lo discutimos en clase.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Ejercicio B — Una `verificar()` de juguete
# MAGIC Llena el diccionario como si fuera un ADR y ejecuta. Primero va a marcar FALTA; corrige hasta que todo sea OK.
# MAGIC Fíjate en que cada check es una condición concreta y legible: así están escritas las de los labs.

# COMMAND ----------

entregable = {
    "estado": "propuesta",           # debe ser 'aceptada'
    "decision": "<escribe la decisión aquí>",
    "alternativas": ["MERGE en bronce"],   # se piden al menos 2
    "tabla": "workspace.c01_<usuario>.demanda_raw",
}

# COMMAND ----------


def verificar_demo(e):
    checks = [
        ("Estado aceptada", e["estado"] == "aceptada"),
        ("Decisión escrita, sin marcadores <…>", len(e["decision"]) > 40 and "<" not in e["decision"]),
        ("Al menos 2 alternativas descartadas", len(e["alternativas"]) >= 2),
        ("Nombre de tabla sin <usuario> de plantilla", "<" not in e["tabla"]),
    ]
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar_demo(entregable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Ejercicio C — Leer un diff
# MAGIC Esto es lo que verías al comparar un `governance.md` con `s03`. ¿Qué cambió y cuál versión es la de la solución?

# COMMAND ----------

print("""
diff --git a/docs/governance.md b/docs/governance.md
--- a/docs/governance.md
+++ b/docs/governance.md
@@ -31,3 +31,3 @@
 | Grupo | dev.bronze / silver | dev.gold |
-| `grp_negocio` | — | — |
+| `grp_negocio` | SELECT | SELECT |
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura: `a/` (líneas `-`) es el tag `s03`, la solución; `b/` (líneas `+`) es tu fork. La solución no le da a `grp_negocio` acceso a `dev`; tú sí. Ninguna de las dos es "la correcta" por sí sola: la pregunta es si tu tabla de grupos explica por qué negocio necesita leer `dev`. Si no lo explica, esa fila del diff es una decisión sin justificar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Lista de chequeo para salir del taller
# MAGIC
# MAGIC - [ ] Abrí mi URL de comparación con `s03` y entendí qué es ruido y qué es mío
# MAGIC - [ ] Sé en qué rama hago commit (`feature/…`) y hacia dónde va el PR (`main` de mi fork)
# MAGIC - [ ] Sé por qué mi fork no tiene tags y por qué no importa
# MAGIC - [ ] `verificar_demo` me dio todo OK después de corregir el diccionario
# MAGIC - [ ] Sé dónde está la tabla de rutas por clase (`docs/autoevaluacion.md`)
