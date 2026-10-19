# Convenciones del repositorio

| Tema | Regla |
|---|---|
| Ramas | `main` del fork es lo entregable. Trabajo en `feature/c<clase>-<tema>`. PR hacia `main` del propio fork; CI (lint + tests) es el revisor |
| Commits | Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:` |
| Nombres en Unity Catalog | `<ambiente>.<capa>_<dominio>.<entidad>` → `dev.silver_energia.demanda_diaria`. snake_case, sin tildes |
| Jobs y pipelines | `[xm-demanda] <verbo>_<objeto>` (el ambiente lo agrega el bundle) |
| Tags en UC | `dominio=energia`, `capa=<bronze|silver|gold>`, `owner=<usuario>` |
| Configuración | Nada hard-codeado. Catálogo, rutas y parámetros salen de `conf/<env>.yml` y variables del bundle |
| Secretos | Solo en secret scopes. Nunca en el repo |
| Notebooks | Delgados: orquestan, no implementan. La lógica vive en `src/xm_demanda/` |
| Modelos | Registrados en UC como `<env>.models.demanda_7d`, con firma y ejemplo de entrada; alias `champion` / `challenger` |
| Tests | `tests/unit` para toda función pura de `src/` (sin Spark); `tests/integration` prueba las reglas sobre Spark con datos sintéticos (local, CI y Databricks); `verificar()` de cada lab comprueba sobre los datos reales |
| Decisiones | Una ADR por decisión de arquitectura en `docs/adr/` (individual, con plantilla; se compara con la de referencia) |

## Flujo de trabajo por clase

1. Crear rama desde `main`.
2. Completar el laboratorio (`clases/cXX/lab.md`) hasta cumplir los criterios de aceptación.
3. `ruff check . && pytest tests/unit` en local (o desde el notebook del lab 8 en Databricks).
4. Abrir PR hacia `main` del fork. CI en verde. Merge.
5. Al cierre de la clase: `git fetch upstream && git diff sXX -- .` para comparar con la solución de referencia.

## Entorno local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # agrega ,spark para correr tests/integration en local (requiere Java 17)
pre-commit install
databricks auth login --host <workspace-url>
```
