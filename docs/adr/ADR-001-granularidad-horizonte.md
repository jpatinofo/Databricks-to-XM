# ADR-001: Granularidad y horizonte del pronóstico

- **Estado:** aceptada
- **Fecha:** 2026-09-18
- **Autor:** jpatinofo

## Contexto
Variables electricas en granularidad diara, correspondientes a la demanda y perdidas de energía desagregadas por mercado de comercialización y CIIU. ventana historica desde el 2026-01-01 con lotes de carga variables. Se busca desarrollar un modelo de pronostico en un horizonte de 7 días para la variable de demanda. 


## Decisión
Se define realizar un modelo de pronostico por tipo de mercado para una granularidad serie-día, empleando como metrica de error, según conocimiento experto IA, WAPE. el horizonte de demanda a pronosticar corresponderá a 7 días. 

## Alternativas consideradas

- Alternativa 1: No evaluada
- Alternativa 2: No evaluada

## Consecuencias
Se contará con un sistema confiable que permite tener trazabilidad historica de la información disponible, procesamiento y transformación de las observaciones vigentes para un horizonte del pronostico de 7 días. 