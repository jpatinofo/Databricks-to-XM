# Estructura del repositorio

```
Databricks-to-XM/
├── README.md                  Plan del curso (15 clases)
├── CONTRIBUTING.md            Convenciones: ramas, nombres, config, tests
├── databricks.yml             Bundle: targets dev / qa / prod
├── resources/                 Definiciones del bundle (pipeline, jobs)
├── conf/                      Configuración por ambiente, sin secretos
├── src/xm_demanda/            Paquete Python con la lógica (ingest, quality, features, model, utils)
├── notebooks/                 Notebooks delgados, uno por tarea de job
├── tests/unit | integration   pytest
├── app/                       Databricks App (Streamlit)
├── .github/workflows/ci.yml   CI/CD con GitHub Actions
├── ci/azure-pipelines.yml     Equivalente en Azure DevOps (referencia)
├── data/raw                   Dataset del caso
├── data/reference             Tablas de referencia (festivos, CIIU)
├── docs/                      Arquitectura, gobierno, ADRs, runbook, autoevaluación
└── clases/c01 … c15/          Material de cada clase: slides, lab, notebooks, lecturas
```

## Cómo crece el repo

| Clases | Qué se agrega |
|---|---|
| 1–2 | `docs/architecture.md`, ADR-001, primera tabla Delta |
| 3 | `docs/governance.md`, catálogos y grants |
| 4–7 | `src/xm_demanda/ingest`, `quality`, `features`; notebooks 01–03; `resources/pipeline_silver.yml` |
| 8 | `validar_config`, contrato de oro como código, `tests/integration` con Spark, job `integration` en CI, el PR que debe fallar |
| 9–11 | `src/xm_demanda/model`; notebooks 04–05; `app/` |
| 12–14 | `resources/job_inference.yml`, `databricks.yml` completo, CI/CD |
| 15 | notebook 06, `docs/runbook.md` |
