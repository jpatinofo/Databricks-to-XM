# Lecturas — Clase 4

## Databricks

| Tema | Referencia |
|---|---|
| Auto Loader: qué es y cómo funciona | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/ |
| Inferencia y evolución de esquema; columna de rescate (`_rescued_data`) | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema |
| Patrones comunes de carga (CSV, JSON, evolución, metadatos) | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/patterns |
| Auto Loader en producción: checkpoints, notificaciones, costos | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/production |
| Columna `_metadata` (file_path, file_modification_time) | https://docs.databricks.com/aws/en/ingestion/file-metadata-column |
| Trigger `availableNow` en Structured Streaming | https://docs.databricks.com/aws/en/structured-streaming/triggers |
| Lakeflow Connect: conectores gestionados (SQL Server, Salesforce, Workday…) | https://docs.databricks.com/aws/en/ingestion/ |
| CDC en pipelines declarativos (AUTO CDC / APPLY CHANGES) | https://docs.databricks.com/aws/en/dlt/cdc |
| Azure: file arrival trigger en jobs | https://learn.microsoft.com/azure/databricks/jobs/file-arrival-triggers |

## Casos del sector (publicados por Databricks; verifica las cifras en la fuente)

| Caso | Qué aporta a la clase | Referencia |
|---|---|---|
| Octopus Energy — liquidación semihoraria (MHHS) | 48× más datos; de 25 000 M a 300 M filas por corrida; US$ 23,63 → 0,48 por fecha de liquidación; Change Data Feed para procesar solo lo que cambió | https://www.databricks.com/blog/scaling-mhhs-how-octopus-energy-achieved-50x-cost-reduction-margin-data-engineering |
| Shell — plataforma de series de tiempo (RTDIP, código abierto) | > 3 M sensores, > 5 billones de mediciones; Delta + liquid clustering | https://www.databricks.com/blog/developing-time-series-lakehouse-shell |
| SSE Airtricity — medidores inteligentes | Miles de millones de filas cada noche con pipelines declarativos (clase 5) | https://www.databricks.com/customers/sse-airtricity |
| UK Power Networks — alertas de medidores en tiempo real | Unity Catalog + Delta Sharing; £45,7 M en 3 años | https://www.databricks.com/customers/uk-power-networks |
| Southern Company — 4,6 M medidores AMI | Ingesta de AMI, SCADA y telemetría | https://www.databricks.com/blog/unlocking-future-energy-smart-meter-innovation |

## Sector eléctrico: por qué el dato se republica

| Tema | Referencia |
|---|---|
| XM — ejemplo de ajuste de liquidación publicado en versión TX3 (marzo 2025, publicado 23 de mayo de 2025) | https://www.xm.com.co/noticias/7884-publicacion-ajuste-1-de-marzo-de-2025-en-version-tx3 |
| XM — procesos de liquidación y facturación del SIC | https://www.xm.com.co/transacciones/liquidaciones/liquidacion-sic/procesos-de-liquidacion-y-facturacion-0 |
| Elexon (Reino Unido) — corridas de liquidación II, SF, R1–R3, RF, DF (hasta 28 meses) | https://www.elexon.co.uk/bsc/glossary/2nd-reconciliation/ |
| CREG — Resolución 101 001 de 2022, infraestructura de medición avanzada (AMI): meta 75 % a 2030, publicación diaria antes de las 8 a. m. | https://gestornormativo.creg.gov.co/gestor/entorno/docs/resolucion_creg_101-1_2022.htm |
| Reglamento (UE) 543/2013 — publicación obligatoria de datos de operadores (ENTSO-E Transparency) | https://eur-lex.europa.eu/eli/reg/2013/543/oj |
