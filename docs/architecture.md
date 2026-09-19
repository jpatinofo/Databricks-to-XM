# Arquitectura de la solución

Pregunta de negocio: para cada combinación activa de operador de red, mercado de comercialización, tipo de mercado y sector CIIU, ¿cuál es la demanda de energía esperada para los próximos 7 días?

Patrón de solución: **batch diario + machine learning**. Los datos llegan por archivo con retraso variable (5 a 205 días) y el pronóstico se necesita una vez al día. No hay caso para streaming.

## Capas y contratos

Llena una fila por tabla. Reemplaza cada `<…>`. El dueño es un rol de XM, no una persona.

| Capa | Tabla | Grano (qué es una fila) | Dueño | Frescura | Garantías |
|---|---|---|---|---|---|
| Bronce | `bronze_energia.demanda_raw` | un registro publicado, tal como llegó | Datalake | Diario | Trazabilidad |
| Plata | `silver_energia.demanda_diaria` | serie-día | Calidad | Diario | Versión actual con calidad |
| Plata | `silver_energia.dim_ciiu` | categoría CIIU | Calidad | Diario | Categorias CIIU vigentes |
| Oro | `gold_energia.features_demanda_diaria` | serie-día | Modelo | Diario | Listo para entrenar |
| Oro | `gold_energia.pronostico_demanda` | serie-día-horizonte | Equipo Demandas | Diario | Listo para usar |

## Llave de serie

`codigo_sic_agente`, `mercado_comercializacion`, `tipo_mercado`, `clasificacion_industrial`. 355 combinaciones activas.

## Decisiones de diseño derivadas de la exploración (lab 2)

| Hecho | Decisión | Dónde se implementa |
|---|---|---|
| Formato largo (2 filas por serie-día) | pivote de las series | Plata (clase 6) |
| Publicación con retraso variable y republicaciones | almacenamiento bronce, registro vigente plata | Bronce (clase 4) / Plata (clase 6) |
| Series incompletas (4 de 355) | activa=false | Plata / features (clase 7) |
| Regulado vs. no regulado | modelo por tipo de mercado | Modelo (clase 9) — ver ADR-001 |
| Ceros (299 serie-días) | marcar advertencia e imputar promedio | Reglas de calidad (clase 5) |
| Pérdidas ≤ demanda | Pérdidas << demanda  | Reglas de calidad (clase 5) |

## Diagrama

```
DemandaPerdidas.xlsx ──▶ [volumen raw] ──▶ bronze_energia.demanda_raw
                                                │
                                                ▼  (pipeline declarativo, reglas de calidad)
                                     silver_energia.demanda_diaria ◀── silver_energia.dim_ciiu
                                                │
                                                ▼  (job de features)
                                  gold_energia.features_demanda_diaria
                                                │
                                                ▼  (job de inferencia, modelo en UC)
                                     gold_energia.pronostico_demanda ──▶ tablero / app
```

En Free Edition todo vive en el catálogo `workspace`; desde la clase 3, en `dev`, `qa` y `prod`. En Azure Databricks cada capa se registra como external location sobre ADLS Gen2.

## ADRs relacionadas

- [ADR-001 — Granularidad y horizonte del pronóstico](adr/ADR-001-granularidad-horizonte.md)
