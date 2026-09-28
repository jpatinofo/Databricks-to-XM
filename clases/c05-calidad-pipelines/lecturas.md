# Lecturas — Clase 5

## Estructura de proyectos y CI/CD

| Tema | Referencia |
|---|---|
| Databricks: buenas prácticas de ingeniería de software para notebooks (módulos, ramas, pytest, jobs desde Git) | https://docs.databricks.com/aws/en/notebooks/best-practices |
| Plantillas de bundles (`databricks bundle init`): default-python, mlops-stacks, dbt-sql… | https://docs.databricks.com/aws/en/dev-tools/bundles/templates |
| Qué es un bundle (Declarative Automation Bundle / Asset Bundle) | https://docs.databricks.com/aws/en/dev-tools/bundles/ |
| CI/CD en Databricks: flujos recomendados | https://docs.databricks.com/aws/en/dev-tools/ci-cd/ |
| CI/CD en Azure Databricks (Azure DevOps y GitHub Actions) | https://learn.microsoft.com/azure/databricks/dev-tools/ci-cd/ |
| MLOps Stacks (plantilla para productos con modelo) | https://docs.databricks.com/aws/en/machine-learning/mlops/mlops-stacks |
| Cookiecutter Data Science (DrivenData) | https://cookiecutter-data-science.drivendata.org/ |
| Kedro (QuantumBlack): estructura y capas de datos | https://docs.kedro.org/ |

## Calidad y pipelines declarativos

| Tema | Referencia |
|---|---|
| Lakeflow Declarative Pipelines: conceptos | https://docs.databricks.com/aws/en/ldp/ |
| Expectations: `expect`, `expect_or_drop`, `expect_or_fail`, `expect_all_*` | https://docs.databricks.com/aws/en/ldp/expectations |
| Patrones avanzados: cuarentena, reglas portables en tablas, validación entre tablas | https://docs.databricks.com/aws/en/ldp/expectation-patterns |
| Referencia Python (`from pyspark import pipelines as dp`) | https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations |
| Buenas prácticas de pipelines | https://docs.databricks.com/aws/en/ldp/best-practices/ |
| Limitaciones de pipelines y de Free Edition (un pipeline activo por tipo) | https://docs.databricks.com/aws/en/getting-started/free-edition-limitations |
| Lakehouse Monitoring (perfilado y frescura, clase 15) | https://docs.databricks.com/aws/en/lakehouse-monitoring/ |

## Casos (publicados por Databricks; verifica las cifras en la fuente)

| Caso | Qué aporta | Referencia |
|---|---|---|
| Block — Spark Declarative Pipelines | +90 % velocidad de desarrollo; de días a horas; reglas de calidad en la entrada | https://databricks.com/customers/block/delta-live-tables |
| E.ON — data lake para 20+ operadores de distribución | 1 TB/día; extraer una vez, distribuir a muchas aplicaciones; 120+ aplicaciones | https://www.databricks.com/customers/eon |
| SSE Airtricity — medidores inteligentes | Miles de millones de filas cada noche con pipelines declarativos | https://www.databricks.com/customers/sse-airtricity |

## Sector eléctrico

| Tema | Referencia |
|---|---|
| CREG — Resolución 015 de 2018: remuneración de la distribución y planes de reducción de pérdidas | https://gestornormativo.creg.gov.co/gestor/entorno/docs/resolucion_creg_0015_2018.htm |
| CREG — pérdidas de energía y planes de reducción (índice normativo, incluye Res. 167 de 2020) | https://gestornormativo.creg.gov.co/gestor/entorno/ee_r_perdidas_energia_planes_reduccion_resolucion_15_2018.html |
| CREG — pérdidas técnicas y no técnicas en el STR | https://gestornormativo.creg.gov.co/gestor/entorno/ee_cto_sistema_transmision_regional_str_perdidas_tecnicas_no_tecnicas.html |
