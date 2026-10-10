# ADR-002: Republicaciones en bronce

- **Estado:** aceptada
- **Fecha:** 2026-10-10
- **Autor:** jpatinofo

## Contexto

XM publica la demanda del SIN día a día, pero el mismo día de dato puede llegar más de una vez al bronce por tres razones que observamos en los 23 archivos de `FechaPublicacion`:

1. **Versiones de liquidación.** XM ajusta la liquidación del SIC en corridas sucesivas (TX1, TX2, TX3…); cada corrida republica días ya publicados con valores corregidos. El caso real: el ajuste del 1 de marzo de 2025 se publicó en versión TX3 el 23 de mayo de 2025 (fuente: xm.com.co).
2. **Retraso de publicación.** En nuestros datos, la distancia entre `Fecha` y `FechaPublicacion` va de 5 a 205 días: un día puede aparecer en una publicación temprana y luego en otra más tardía con medidas corregidas.
3. **Corrección de medidas.** La fuente puede republicar un día para corregir un valor erróneo; el día ya cargado en bronce vuelve a llegar, ahora con el valor ajustado.

En el lab 4 esto se materializó con `raw_v2`: XM republicó los últimos 7 días del archivo original con valores corregidos (+1,5 %) **y** renombró la columna `Valor` → `ValorKwh`. El bronce pasó de 145.408 filas (23 archivos) a 150.350 filas (24 archivos): los 7 días quedaron duplicados con dos versiones, y la columna renombrada viajó en `_rescued_data` como JSON hasta que `resolver_alias` + `normalizar` la mapearon de vuelta a `Valor`.

## Decisión

1. Cuando llega un día que ya existe, bronce **acumula** (append, nunca sobrescribe): Auto Loader con `availableNow` carga solo los archivos nuevos de `landing` y el checkpoint evita releerlos; cada republicación añade filas, no borra las anteriores.
2. La llave de versión de una fila es `(_source_file, _publication_date)`: el archivo de origen dice qué publicación la trajo y `_publication_date` (derivado de `FechaPublicacion`) identifica la versión de liquidación; la llave de negocio —`Fecha, CodigoVariable, CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial`— identifica la serie-día que se repite entre versiones.
3. "Vigente" significa **la publicación más reciente por serie-día** (`row_number()` particionado por las 6 columnas de negocio, ordenado por `_publication_date DESC, _ingested_at DESC`) y lo resuelve una **vista** (`demanda_raw_vigente`) sobre bronce, no plata: bronce guarda todo, la vista elige la última versión.

## Alternativas consideradas

- **Sobrescribir (MERGE o DELETE+INSERT) el día existente cuando llega una republicación.** Descartada: pierde el historial de versiones, impide auditar qué se corrigió y cuándo, y rompe la reconstrucción desde `landing` porque el resultado dependería del orden de carga, no solo de los archivos.
- **Usar `schemaEvolutionMode = addNewColumns` en vez de `rescue`.** Descartada: añadiría `ValorKwh` como columna nueva sin avisar, dejando `Valor` con los valores antiguos y `ValorKwh` con los corregidos; los consumidores recibirían columnas inesperadas y el esquema se contaminaría con campos temporales. Con `rescue` el cambio cae en `_rescued_data` y se decide conscientemente en `normalizar()`.
- **Resolver "vigente" en plata (tabla materializada) en vez de una vista sobre bronce.** Descartada: añade latencia (hay que refrescar la tabla), duplica la lógica de partición/orden en otro lugar y consume almacenamiento extra. La vista es inmediata, no duplica datos y se puede cambiar sin reconstruir.
- **Una tabla separada por publicación (tablas versionadas).** Descartada: multiplica el número de tablas, dificulta consultar la última versión y complica los metadatos. Un solo bronce con `_publication_date` las ordena naturalmente.

## Consecuencias

**Gana:**
- Auditaría completa: cada versión de cada día queda en bronce con `_source_file`, `_publication_date` e `_ingested_at`; se puede reconstruir el estado del SIN en cualquier fecha de publicación.
- Reconstrucción desde `landing`: borrar tabla + checkpoint + re-ejecutar llega al mismo estado (150.350 filas, 24 archivos, 0 nulos). Landing es la fuente de verdad; bronce es derivable.
- Cambios de esquema absorbidos: la columna de rescate atrapa renombres (`Valor` → `ValorKwh`) sin perder datos ni alterar el esquema de la tabla; el dict `ALIAS` los documenta y `normalizar()` los resuelve.
- Idempotencia: correr la ingesta dos veces no cambia el conteo (checkpoint de Auto Loader).

**Cuesta:**
- Crecimiento de bronce: cada republicación añade filas (150.350 vs 145.408 vigentes); el factor de crecimiento depende de la frecuencia de republicación de XM.
- Necesidad de la vista `demanda_raw_vigente`: los consumidores no pueden leer bronce directamente sin repetir la lógica de `row_number()`; la vista centraliza esa lógica.
- Plata (clase 6) consumirá `demanda_raw_vigente`, no bronce: la limpieza y los agregados se hacen sobre la última versión, no sobre el historial.
- Auditoría puede pedir cualquier versión histórica: la respuesta es un `SELECT` con filtro sobre `_publication_date` en bronce, sin salir del lakehouse.
