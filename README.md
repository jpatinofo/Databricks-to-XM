# Soluciones de Analítica con Databricks — XM

**Docente:** Manuela Larrea Gómez

Curso teórico-práctico de 15 clases (2 h cada una, 30 h) para el equipo de analítica de **XM**, operador del Sistema Interconectado Nacional y administrador del Mercado de Energía Mayorista de Colombia.

**A quién va dirigido:** científicos de datos y analistas con posgrado en Data Science que empiezan a usar Databricks y necesitan evolucionar hacia un equipo multidisciplinar capaz de **construir, desplegar y operar** sus propios modelos (ML engineering + MLOps), no solo entrenarlos.

**Qué se llevan al terminar:** un repositorio de referencia con una solución analítica de extremo a extremo (ingesta → gobierno → features → modelo → despliegue → operación) sobre un caso real del sector: **demanda y pérdidas de energía por operador de red, mercado, tipo de mercado y actividad económica (CIIU)**.

---

## Cómo está organizado el curso

| Bloque de cada clase | Duración | Qué pasa |
|---|---|---|
| Teoría | 40 min | Qué se puede hacer con Databricks en ese tema (**la plataforma completa**, no solo lo que permite Free Edition), con ejemplos del sector eléctrico |
| Práctica | 70 min | Laboratorio guiado en **Databricks Free Edition** sobre el caso de XM. Cada lab tiene checklist y criterios de aceptación |
| Cierre | 10 min | Liberación del tag de la solución de referencia, "así sería en Azure/producción", qué viene en la próxima clase |

Principio del curso: **la teoría cubre la plataforma completa; la práctica cubre lo que Free Edition permite.**

**Trabajo autónomo, sin calificación.** Cada participante trabaja en su propio fork. Cada laboratorio termina con una celda `verificar()` que comprueba los criterios de aceptación y dice qué falta. Al cierre de cada clase se libera el tag `sXX` de la rama `solution`, y cada quien compara su versión con la de referencia (`git diff sXX`). El revisor del código es la integración continua (lint y tests), no una persona. La lista de autoevaluación por hito está en `docs/autoevaluacion.md`. Lo que Free Edition no permite (multi-workspace, Azure, GPU, Lakehouse Federation, etc.) se diseña, se diagrama y se documenta igual.

**Hilo conductor:** una única pregunta de negocio desde la clase 1 hasta la 15:

> *Para cada combinación activa de operador de red, mercado de comercialización, tipo de mercado y sector CIIU, ¿cuál es la demanda de energía esperada para los próximos 7 días?*

El algoritmo importa poco. Importa que el equipo sepa **cómo se gobierna, versiona, despliega, monitorea y reentrena** ese modelo.

---

## Estructura del repositorio (por clase)

Cada clase tiene su carpeta `clases/cXX-<tema>/` con:

| Archivo | Contenido |
|---|---|
| `slides.pdf` | Diapositivas de la teoría |
| `lab.md` | Enunciado del laboratorio, checklist y criterios de aceptación |
| `notebooks/` | Notebooks de la clase (versión estudiante; la solución se libera al cierre) |
| `lecturas.md` | Referencias actualizadas: documentación de Databricks y material del sector eléctrico |

El código de la solución vive fuera de las carpetas de clase (`src/`, `resources/`, `conf/`, `tests/`) porque es **un solo producto que crece clase a clase**. El árbol completo está en `docs/estructura-repo.md`.

---

## Plan de las 15 clases

| # | Módulo | Clase | Teoría (plataforma completa) | Práctica (Free Edition) | Hito |
|---|---|---|---|---|---|
| 1 | Fundamentos | Databricks y el Lakehouse para el sector eléctrico | Qué es la Data Intelligence Platform; Lakehouse vs. warehouse vs. lake; Delta Lake; mapa completo de capacidades | Onboarding al workspace, fork del repo, primera tabla Delta con datos de demanda | H1 |
| 2 | Fundamentos | Arquitectura Medallion y patrones analíticos | Bronze/Silver/Gold con contratos; patrones batch, streaming, ML, GenAI; cómo lo hacen operadores de red y de mercado | Diseño de la arquitectura del caso; ADR-001 | H1 ✔ |
| 3 | Gobierno | Unity Catalog: gobierno, seguridad y acceso | Modelo de objetos de UC; grants, grupos, row/column filters, tags, linaje, auditoría; Delta Sharing y Clean Rooms | Catálogos `dev/qa/prod`, grupos, permisos, volumen con datos crudos | H2 |
| 4 | Ingeniería | Ingesta y republicaciones | Auto Loader, Lakeflow Connect, CDC, esquemas explícitos, idempotencia; datos que llegan tarde y se reprocesan | Bronze con Auto Loader; manejo de `FechaPublicacion` (republicaciones) | H2 ✔ |
| 5 | Gobierno + Ing. | Calidad de datos y pipelines declarativos | Lakeflow Declarative Pipelines (ex-DLT), expectations, cuarentena, Lakehouse Monitoring, SLAs de frescura | Pipeline Bronze→Silver con expectations y cuarentena; datos con fallas inyectadas | H3 |
| 6 | Ingeniería | Modelado Silver: de reporte a tabla analítica | Delta `MERGE`, time travel, liquid clustering, dimensiones, SQL vs. PySpark, Photon | Pivote long→wide, dimensión CIIU, "qué vio el modelo el día X" | H3 ✔ |
| 7 | Ingeniería | Sumar fuentes y construir features (Gold) | Feature engineering para series temporales; Feature Store en UC; Lakehouse Federation; fuentes externas típicas del sector | Gold `features_demanda_diaria` con festivos e histórico ampliado; ADR-002 | H4 |
| 8 | Desarrollo | Código, configuración y datos separados | Git folders, ramas, PR, tests, ruff, paquetes Python, notebooks delgados, Databricks Assistant con criterio | Refactor a `src/`, pytest, primer PR con CI como revisor | H4 ✔ |
| 9 | MLOps | Experimentación con MLflow | MLflow 3: experimentos, tracking, evaluación, comparación; baselines honestos; errores típicos (leakage) | Baseline estacional + modelo global LightGBM; métricas por segmento | H5 |
| 10 | MLOps | Registro, versiones y entrenamiento reproducible | Model Registry en UC, alias champion/challenger, firmas, lineage modelo↔datos; "deploy code, not models" | `train.py` parametrizado, registro en UC, promoción por alias | H5 |
| 11 | MLOps | Inferencia batch, Serving y Apps | Cuándo batch, cuándo endpoint, cuándo app; Model Serving, AI Gateway, Databricks Apps, Genie y AI/BI | Job de inferencia batch a Gold; endpoint CPU; app observado vs. estimado | H5 ✔ |
| 12 | Operación | Orquestación con Lakeflow Jobs | DAGs, dependencias, reintentos, alertas, parámetros, triggers por archivo; integración con ADF/Airflow | Job multi-tarea ingesta→features→inferencia con notificaciones | H6 |
| 13 | Operación | Asset Bundles y ambientes | Declarative Automation Bundles: targets, variables, service principals; DEV/QA/PROD en Azure Databricks | `databricks.yml` con 3 targets, deploy a `qa` desde CLI | H6 |
| 14 | Operación | CI/CD y promoción entre ambientes | GitHub Actions y Azure DevOps; pruebas de integración; approvals; promoción de código y de modelos | Pipeline PR→tests→dev; tag→qa→aprobación→prod (`azure-pipelines.yml` de referencia) | H6 |
| 15 | Operación | Monitoreo, drift, reentrenamiento y cierre | Monitoreo de datos y de modelo, drift, políticas de reentrenamiento, runbook, costos, SLOs; demo final | Monitor de drift observado vs. pronóstico; runbook; pipeline completo en verde de punta a punta | H6 ✔ |

Hitos (según el programa del curso): H1 pregunta analítica · H2 arquitectura y fuentes · H3 gobierno, permisos y calidad · H4 pipeline y Gold · H5 modelo, experimento y despliegue · H6 operación, CI/CD y consumo.

---

## Detalle por clase

### Clase 1 — Databricks y el Lakehouse para el sector eléctrico
- **Teoría:** de warehouse y data lake al Lakehouse; Delta Lake (ACID, esquema, versiones); componentes de la plataforma: Unity Catalog, Lakeflow, SQL, Mosaic AI, Apps, Lakebase, Genie; qué es serverless; qué trae Free Edition y qué no.
- **Práctica:** acceso al workspace compartido, fork del repo, Git folder, cargar `DemandaPerdidas.xlsx` a un volumen, crear la primera tabla Delta y consultarla en SQL y PySpark.
- **Sector:** rol de XM en el SIN y el MEM; SIMEM como plataforma de datos abiertos del mercado; AEMO (operador del mercado australiano) como referencia de operador de mercado sobre Databricks.
- **Entregable:** tabla `dev.bronze_energia.demanda_raw` creada desde el repo y primer commit.

### Clase 2 — Arquitectura Medallion y patrones analíticos
- **Teoría:** Bronze/Silver/Gold con contratos (esquema, dueño, frescura); Bronze inmutable con metadatos de ingesta; patrones de solución (batch, streaming, ML, GenAI) y cuándo usar cada uno; anti-patrones frecuentes en equipos que migran notebooks.
- **Práctica:** diseñar la arquitectura del caso (tablas por capa, llaves, granularidad, dueños) y escribirla en `docs/architecture.md`; ADR-001 "granularidad y horizonte".
- **Sector:** EDP E-REDES (operador de distribución de Portugal): ~200.000 series de carga pronosticadas a diario sobre Databricks; qué de ese patrón aplica a 355 series de XM y qué no.
- **Entregable:** `docs/architecture.md` + ADR-001. Hito 1 cerrado.

### Clase 3 — Unity Catalog: gobierno, seguridad y acceso
- **Teoría:** metastore → catálogo → esquema → objeto; volúmenes; grants a grupos; row filters y column masks; tags y clasificación; linaje y auditoría; secretos (secret scopes, Key Vault-backed en Azure); Delta Sharing y Clean Rooms para compartir con otros agentes.
- **Práctica:** crear `dev`, `qa`, `prod`; grupos de rol (ingeniería, ciencia de datos, negocio); grants mínimos; volumen `raw`; un usuario sin permiso rompe un job y se diagnostica.
- **Sector:** confidencialidad de la información por agente y comercializador; trazabilidad exigible por el regulador (CREG) y auditorías; el modelo de transparencia de ENTSO-E como referencia de datos abiertos de mercado.
- **Entregable:** catálogos, grupos y grants documentados en `docs/governance.md`.

### Clase 4 — Ingesta y republicaciones
- **Teoría:** Auto Loader (esquema explícito, evolución, rescate de columnas); Lakeflow Connect para fuentes SaaS y bases de datos; CDC; idempotencia; datos tardíos y republicaciones; metadatos `_ingested_at`, `_source_file`, `_publication_date`.
- **Práctica:** Bronze con Auto Loader desde el volumen; llega `raw_v2` con una columna renombrada y republicaciones de días ya cargados; resolver sin duplicar ni perder la versión anterior.
- **Sector:** por qué la misma fecha se publica varias veces (versiones de liquidación del mercado, corrección de medidas); qué cambia con medición avanzada (AMI) y datos de 15 minutos.
- **Entregable:** `bronze_energia.demanda_raw` idempotente con historial de publicaciones. Hito 2 cerrado.

### Clase 5 — Calidad de datos y pipelines declarativos
- **Teoría:** Lakeflow Declarative Pipelines: tablas materializadas y streaming tables; expectations (warn/drop/fail); cuarentena; Lakehouse Monitoring para perfilar tablas; SLAs de frescura; cuándo usar pipeline declarativo vs. job clásico.
- **Práctica:** pipeline Bronze→Silver con reglas de negocio (`valor ≥ 0`, regulado ⇒ `SIN CLASIFICAR`, pérdidas coherentes con demanda), tabla de cuarentena y métricas de calidad.
- **Sector:** pérdidas técnicas vs. no técnicas; balance de energía como regla de consistencia; costo de una decisión regulatoria tomada sobre datos mal validados.
- **Entregable:** pipeline `silver_energia` con expectations y reporte de calidad.

### Clase 6 — Modelado Silver: de reporte a tabla analítica
- **Teoría:** `MERGE` idempotente; time travel para auditoría y reproducibilidad; liquid clustering y `OPTIMIZE`; diseño de dimensiones; cuándo SQL y cuándo PySpark; Photon.
- **Práctica:** pivote long→wide (`demanda_real`, `perdidas` por serie-día); dimensión CIIU normalizada; consulta "qué versión de los datos vio el modelo el día X".
- **Sector:** clasificación CIIU (DANE) y su uso en el mercado; mercado regulado vs. no regulado y por qué el CIIU solo aplica al segundo.
- **Entregable:** `silver_energia.demanda_diaria` y `silver_energia.dim_ciiu`. Hito 3 cerrado.

### Clase 7 — Sumar fuentes y construir features (Gold)
- **Teoría:** qué es oro y qué es una feature; calendario y festivos como fuente; lags y ventanas con funciones de ventana; fuga de información y horizonte; series incompletas y ceros; Feature Engineering en Unity Catalog (llave primaria, lookups, point-in-time); Lakehouse Federation para consultar fuentes sin moverlas.
- **Práctica:** incorporar `data/reference/festivos_co.csv`; implementar `agregar_calendario` y `agregar_lags` en `src/xm_demanda/features/build.py`; construir `features_demanda_diaria` con MERGE idempotente y llave primaria; ADR-003 (opcional) "features sobre la versión vigente vs. point-in-time".
- **Sector:** variables que mueven la demanda en Colombia: festivos, hidrología y fenómeno de El Niño, temperatura, autogeneración; proyecciones de demanda de la UPME como referencia de largo plazo.
- **Entregable:** tabla de features en Gold con documentación de cada variable. Hito 4 abierto.

### Clase 8 — Código, configuración y datos separados
- **Teoría:** Git folders y repositorios; estrategia de ramas y PR; notebooks delgados + paquete Python; tests unitarios e integración; ruff y pre-commit; configuración por ambiente sin secretos; uso de Databricks Assistant y asistentes de código con verificación.
- **Práctica:** mover lógica a `src/xm_demanda/`; escribir tests con pytest; abrir el primer PR y dejar que CI lo revise; un PR que rompe un test.
- **Sector:** reproducibilidad como requisito ante auditoría regulatoria: poder reconstruir hoy el pronóstico que se publicó hace seis meses.
- **Entregable:** paquete `xm_demanda` con tests pasando en CI. Hito 4 cerrado.

### Clase 9 — Experimentación con MLflow
- **Teoría:** MLflow 3 en Databricks: experimentos, runs, artefactos, `log_input`, evaluación y comparación; baselines honestos; validación temporal; errores típicos (leakage, métricas globales que esconden segmentos).
- **Práctica:** baseline estacional-naive; modelo global LightGBM con lags y categóricas; comparación de runs; métricas por segmento; probar la temperatura y descartarla con evidencia.
- **Sector:** cómo pronostican demanda XM (pronóstico publicado), los operadores europeos (ENTSO-E day-ahead) y AEMO; por qué el baseline estacional es difícil de vencer en demanda eléctrica.
- **Entregable:** experimento `demanda_7d` con runs comparables y modelo candidato. Hito 5 abierto.

### Clase 10 — Registro, versiones y entrenamiento reproducible
- **Teoría:** Model Registry en Unity Catalog; versiones y alias (`champion`/`challenger`); firma y ejemplo de entrada; linaje modelo↔datos↔código; el patrón "deploy code, not models" y cuándo sí promover artefactos.
- **Práctica:** `train.py` parametrizado por configuración; registro del modelo en `dev.models.demanda_7d`; promoción por alias; detectar un modelo entrenado desde `dev` con datos de `prod`.
- **Sector:** trazabilidad de qué modelo produjo qué pronóstico y con qué datos, condición para publicar cifras que usan terceros.
- **Entregable:** modelo registrado y versionado con firma.

### Clase 11 — Inferencia batch, Serving y Apps
- **Teoría:** batch vs. tiempo real vs. app: criterios de decisión; Model Serving (CPU/GPU, escalado, monitoreo de endpoint); AI Gateway; Databricks Apps; Genie y AI/BI Dashboards; lo que Free Edition permite (Serving CPU, 3 apps con caducidad de 24 h).
- **Práctica:** job de inferencia batch a `gold_energia.pronostico_demanda`; endpoint del modelo campeón; app Streamlit "observado vs. estimado" por segmento.
- **Sector:** Octopus Energy y los pronósticos como servicio interno; qué consumidores hay en XM para un pronóstico (planeación, seguimiento, mercado) y qué canal necesita cada uno.
- **Entregable:** tabla de pronósticos, endpoint y app funcionando. Hito 5 cerrado.

### Clase 12 — Orquestación con Lakeflow Jobs
- **Teoría:** jobs multi-tarea, dependencias, parámetros, reintentos, timeouts, notificaciones; triggers por calendario y por llegada de archivo; concurrencia; cuándo integrar Azure Data Factory o Airflow y cuándo no.
- **Práctica:** job `ingesta → calidad → features → inferencia` con alertas; una tarea que falla en silencio se diagnostica desde la UI y los logs.
- **Sector:** alinear el job al ciclo real de publicación de datos del mercado; qué pasa con el pronóstico cuando la fuente llega tarde.
- **Entregable:** job programado con notificaciones. Hito 6 abierto.

### Clase 13 — Asset Bundles y ambientes
- **Teoría:** Declarative Automation Bundles (Databricks Asset Bundles): estructura, targets, variables, recursos; service principals por ambiente; DEV/QA/PROD en Azure Databricks (workspaces separados) vs. su simulación con catálogos en Free Edition.
- **Práctica:** `databricks.yml` con targets `dev/qa/prod`; deploy a `qa` desde la CLI; un deploy con variable faltante.
- **Sector:** qué debe cambiar entre ambientes en una solución de mercado (fuentes, permisos, frecuencias) y qué no debe cambiar nunca (código).
- **Entregable:** bundle desplegado en `dev` y `qa`.

### Clase 14 — CI/CD y promoción entre ambientes
- **Teoría:** pipelines de CI (lint, tests, validate) y CD (deploy por ambiente con approvals); GitHub Actions y Azure DevOps lado a lado; pruebas de integración en `dev`; promoción de código y de modelos; secretos en CI.
- **Práctica:** pipeline PR→tests→deploy dev; tag→deploy qa→aprobación→prod; un merge que se salta CI. `azure-pipelines.yml` equivalente como referencia.
- **Sector:** auditabilidad del despliegue: quién aprobó, qué versión, cuándo.
- **Entregable:** CI/CD operando sobre el propio fork.

### Clase 15 — Monitoreo, drift, reentrenamiento y cierre
- **Teoría:** monitoreo de datos (frescura, volumen, distribución) y de modelo (error observado vs. pronóstico, drift); políticas de reentrenamiento (calendario, por drift, por evento); runbook y dueños; costos y SLOs; tablero AI/BI y espacio Genie para negocio.
- **Práctica:** monitor de drift sobre `pronostico_demanda` vs. real; política de reentrenamiento en el job; runbook; ejecución completa del pipeline en el propio fork; demo de la solución de referencia; backlog post-curso.
- **Sector:** cambios estructurales que degradan un modelo de demanda: El Niño, autogeneración solar, nuevas cargas industriales, cambios regulatorios; cómo se detectan antes de que el negocio los note.
- **Entregable:** solución completa operando en el propio fork. Hito 6 cerrado.

---

## Mapa de capacidades de Databricks cubiertas

| Capacidad | Clase | Práctica en Free Edition |
|---|---|---|
| Delta Lake, time travel, liquid clustering | 1, 6 | Sí |
| Unity Catalog (grants, tags, linaje, volúmenes) | 3 | Sí (un workspace, un metastore) |
| Row filters / column masks | 3 | Sí |
| Delta Sharing, Clean Rooms, Lakehouse Federation | 3, 7 | Solo teoría |
| Auto Loader | 4 | Sí |
| Lakeflow Connect (conectores gestionados) | 4 | Solo teoría |
| Lakeflow Declarative Pipelines + expectations | 5 | Sí (un pipeline activo) |
| Lakehouse Monitoring | 5, 15 | Según disponibilidad; se cubre con monitor propio |
| Feature Store en UC | 7 | Sí |
| Git folders, Assistant | 8 | Sí |
| MLflow 3 (tracking, evaluación) | 9 | Sí |
| Model Registry en UC, alias | 10 | Sí |
| Model Serving | 11 | Sí (CPU, endpoints limitados) |
| Databricks Apps | 11 | Sí (3 apps, 24 h) |
| Genie, AI/BI Dashboards | 11, 15 | Sí |
| Lakeflow Jobs | 12 | Sí (5 tareas concurrentes) |
| Asset Bundles | 13 | Sí (CLI) |
| CI/CD con GitHub Actions / Azure DevOps | 14 | GitHub Actions sí; ADO como referencia |
| Azure Databricks: workspaces, ADLS, Key Vault, redes | 1, 3, 13 | Solo teoría y diagramas |
| Lakebase, Vector Search, Agent Bricks, Mosaic AI | 1, 11 | Mención en el mapa de capacidades |

---

## Requisitos previos

- Python intermedio y SQL. PySpark básico es deseable, no obligatorio (se cubre en las clases 4–7).
- Cuenta en GitHub y en Databricks Free Edition (se envían instrucciones antes de la clase 1).
- Encuesta de nivel previa (5 minutos) para ajustar el peso SQL/PySpark de los laboratorios.

## Uso de los datos

Los datos del caso son de uso exclusivo del curso. Free Edition es de uso no comercial: lo construido aquí es material de aprendizaje y referencia, no un sistema productivo.

## Referencias del sector usadas en el curso

- XM — [Pronóstico de demanda](https://www.xm.com.co/consumo/pronostico-de-demanda) · [Demanda en tiempo real](https://www.xm.com.co/consumo/demanda-en-tiempo-real)
- UPME — [Proyección de demanda de energía eléctrica 2025-2039 (rev. enero 2026)](https://docs.upme.gov.co/DemandayEficiencia/Documents/Proyecciones_de_demanda_de_EE_2025-2039_v2_ene_2026.pdf)
- Databricks — [Plataforma para energía](https://www.databricks.com/solutions/industries/energy) · [EDP E-REDES: pronóstico distribuido de carga](https://community.databricks.com/t5/technical-blog/customer-blog-efficient-distributed-energy-load-forecasting-with/ba-p/92760) · [Free Edition: limitaciones](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
