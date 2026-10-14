# Lecturas — Clase 7

## Databricks / Spark

| Tema | Referencia |
|---|---|
| Funciones de ventana en PySpark (`Window`, `lag`, `lead`, `rowsBetween`) | https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html |
| Feature Engineering en Unity Catalog (feature tables, llave primaria, lookups) | https://docs.databricks.com/aws/en/machine-learning/feature-store/uc/feature-tables-uc |
| Point-in-time lookups para series temporales | https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series |
| Lakehouse Federation: consultar fuentes externas sin moverlas | https://docs.databricks.com/aws/en/query-federation/ |
| Restricciones (`PRIMARY KEY`, `NOT NULL`) en Delta / Unity Catalog | https://docs.databricks.com/aws/en/tables/constraints |
| `DataFrame.transform` para encadenar funciones | https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html |

## Modelado de series temporales

| Tema | Referencia |
|---|---|
| Fuga de información (leakage) en features: qué es y cómo evitarla | https://scikit-learn.org/stable/common_pitfalls.html#data-leakage |
| Features de calendario y lags para pronóstico (capítulo de "Forecasting: Principles and Practice") | https://otexts.com/fpp3/useful-predictors.html |

## Sector

| Tema | Referencia |
|---|---|
| Ley 51 de 1983 (festivos en Colombia: fijos, trasladables al lunes y móviles) | https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=4954 |
| XM — pronóstico de demanda y demanda real del SIN | https://www.xm.com.co |
| UPME — proyecciones de demanda de energía eléctrica y potencia | https://www1.upme.gov.co/DemandayEficiencia/Paginas/Proyecciones-de-demanda.aspx |
| Efecto de El Niño en la operación del SIN (hidrología y generación) | https://www.xm.com.co (informes de operación) |
