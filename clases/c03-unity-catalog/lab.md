# Lab 3 — Gobernar tu propio esquema

**Duración:** 60 min · **Modalidad:** pasos 1–4 en pareja, 5–10 individual · **Rama:** `feature/c03-gobierno`

## Objetivo

Decidir quién lee qué sobre tu tabla, escribirlo en Unity Catalog (grants, filtro de filas, máscara, tags, comentarios) y documentar la política completa de XM en `docs/governance.md`.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| Lab 1 terminado: eres owner de `workspace.c01_<usuario>` con la tabla `demanda_raw` | Créalos ahora con los pasos 4–6 del lab 1 (10 min); sin esquema propio no puedes dar permisos |
| Un compañero con su propio esquema | Emparéjate con quien tengas al lado; si son impares, uno trabaja con la docente |
| Git folder actualizado en `main` | Botón Git → **Pull** (antes, en GitHub: **Sync fork**) |

## Pasos

### Parte 1 — En pareja (25 min)

Los dos hacen lo mismo, cada uno sobre su tabla. Cuando el paso dice "tu compañero consulta", el otro ejecuta la consulta contra TU tabla desde su notebook.

1. Crea la rama `feature/c03-gobierno`. Abre `clases/c03-unity-catalog/notebooks/lab03_gobierno`. Widgets: `usuario` = el sufijo de tu esquema; `companero` = el correo completo de tu compañero.
2. **GRANT y comprobar.** Ejecuta el paso 1: `USE SCHEMA` sobre el esquema y `SELECT` sobre la tabla a `account users`. Tu compañero consulta tu tabla: ve Regulado y No Regulado con cifras completas.
3. **REVOKE y ver el error.** Ejecuta el paso 2. Tu compañero repite la consulta y lee el error completo (`PERMISSION_DENIED`). Responde la pregunta en la celda.
4. **Filtro de filas y máscara.** Completa las dos funciones (`solo_regulado`, `mascara_kwh`) y ejecuta los `ALTER TABLE`. Tú, como owner, sigues viendo todo.
5. **GRANT otra vez.** Ejecuta el paso 4. Tu compañero repite la consulta: ahora ve una sola fila (Regulado) y kWh redondeados a miles. Misma tabla, distinta vista. Responde la pregunta.

### Parte 2 — Individual (35 min)

6. **Tags y comentarios.** Cuatro tags (`capa`, `dominio`, `owner`, `sensibilidad`), un comentario de tabla y dos de columna. Abre Catalog Explorer y comprueba que aparecen.
7. **Vista y linaje.** Crea `demanda_regulado_v`, consúltala y abre Catalog Explorer → la vista → **Lineage**: aparece la flecha desde `demanda_raw`.
8. **Auditoría.** Ejecuta el paso 7 (`information_schema.table_privileges`): es la respuesta a "¿quién tiene acceso?" sin enviar un correo.
9. **`docs/governance.md`.** Llena las siete secciones para la solución de XM: catálogos `dev/qa/prod`, esquemas por capa, grupos, matriz de privilegios, filtros y máscaras, secretos, auditoría. Usa la matriz de la diapositiva 8 como punto de partida y ajústala si no estás de acuerdo (di por qué en la tabla de grupos).
10. Ejecuta `verificar()`. Commit `feat(c03): gobierno del esquema y governance.md` → push → PR hacia `main` de tu fork → CI en verde → merge.

## Criterios de aceptación

- [ ] `demanda_raw` con `SELECT` vigente para `account users`, filtro de filas y máscara sobre `Valor` activos
- [ ] 4 tags y comentario de tabla (sin marcadores `<…>`)
- [ ] Vista `demanda_regulado_v` con linaje visible
- [ ] `docs/governance.md` con las 7 secciones, sin `<…>` ni celdas vacías, y con `dev`, `qa`, `prod`
- [ ] `verificar()` en OK; PR en verde

## Comparar con la solución de referencia

Al cierre se libera el tag `s03`. En GitHub, sin instalar nada:

`https://github.com/<docente>/Databricks-to-XM/compare/s03...<tu_usuario>:Databricks-to-XM:main`

O en terminal:

```bash
git fetch upstream --tags
git diff s03 -- docs/governance.md clases/c03-unity-catalog/notebooks
```

La matriz de la solución es una propuesta razonable, no la única. Si la tuya difiere, lo que importa es que la tabla de grupos diga por qué.

## Extensión opcional

Escribe una máscara que oculte el `CodigoSICAgente` (por ejemplo, `sha2(codigo, 256)`) para todos menos el owner, y aplícala. Piensa qué se rompe aguas abajo: ¿podría plata seguir haciendo el join por agente?

## Problemas frecuentes

| Síntoma | Causa probable |
|---|---|
| `PERMISSION_DENIED … USE SCHEMA` (en tu compañero) | Falta el GRANT sobre el esquema; el SELECT sobre la tabla no basta |
| `PERMISSION_DENIED … SELECT on Table` | Es el error esperado en el paso 3. Si aparece en el paso 5, no se volvió a ejecutar el GRANT |
| `Only the owner can …` | Estás intentando dar permisos sobre el esquema de tu compañero; cada uno gobierna el suyo |
| `PARSE_SYNTAX_ERROR` en las funciones | Quedó un `<condición>` o `<expresión>` sin reemplazar |
| Tu compañero sigue viendo todo tras el filtro | La función devuelve TRUE siempre: revisa que compares `current_user()` con tu correo exacto |
| Lineage vacío | El linaje se captura al ejecutar: consulta la vista una vez y recarga la pestaña |
| `FALTA governance.md menciona dev, qa y prod` | Escribiste lo que hiciste hoy en `workspace`; el documento describe la solución de XM |

## Así sería en Azure Databricks

| Hoy | En XM |
|---|---|
| `current_user()` en el filtro y la máscara | `is_account_group_member('grp_negocio')`: la política sigue al grupo, no a la persona |
| Grupos creados a mano por la docente | Grupos de Entra ID sincronizados con SCIM; el owner de cada esquema es un grupo |
| Tres catálogos en un workspace | Un workspace por ambiente; `prod` enlazado (binding) solo al workspace de prod |
| `information_schema` para auditar | Además `system.access.audit` exportado a Log Analytics con alertas |
