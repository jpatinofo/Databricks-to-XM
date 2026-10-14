# ADR-003: Features de oro sobre la versión vigente (y no point-in-time)

- **Estado:** propuesta
- **Fecha:** <aaaa-mm-dd>
- **Autor:** <tu nombre>

## Contexto

<XM republica la demanda (ADR-002). Para operar se usa la última versión; para entrenar "con honestidad" habría que usar la versión que se conocía en cada fecha (point-in-time). Qué costaría cada camino en este proyecto.>

## Decisión

<1. Oro se construye sobre `demanda_diaria` (vigente). 2. Las ventanas terminan en t-1 y el objetivo es lead(h). 3. Cuándo se cambiaría a point-in-time.>

## Alternativas consideradas

- <Point-in-time desde bronce con `_publication_date <= fecha_corte`: por qué no ahora>
- <Ventanas que incluyen t: por qué nunca>

## Consecuencias

<Qué gana (simplicidad, misma tabla para entrenar e inferir) y qué cuesta (métrica de entrenamiento levemente optimista si hubo republicaciones grandes).>
