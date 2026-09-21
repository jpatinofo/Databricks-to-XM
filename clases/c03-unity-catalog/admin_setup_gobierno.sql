-- admin_setup_gobierno.sql — lo que solo el administrador del workspace puede hacer.
-- Secciones 1 y 2 se ejecutan ANTES de la clase 3. La sección 3 se hace en vivo en la demo (10 min).
-- Ejecutar celda por celda en un notebook SQL o en el editor SQL, como administrador.

-- =====================================================================
-- Sección 1 · Catálogos por ambiente y esquemas por capa
-- =====================================================================
-- Requiere CREATE CATALOG sobre el metastore (en Free Edition lo tiene el admin del workspace;
-- en XM sobre Azure es un rol de plataforma, no del equipo de analítica).

CREATE CATALOG IF NOT EXISTS dev  COMMENT 'Desarrollo: cualquier persona de ingeniería o analítica escribe';
CREATE CATALOG IF NOT EXISTS qa   COMMENT 'Pruebas: solo escribe el pipeline; las personas leen';
CREATE CATALOG IF NOT EXISTS prod COMMENT 'Producción: solo escribe el pipeline; negocio lee vistas';

-- Mismos esquemas en los tres catálogos: el código no cambia entre ambientes, cambia el catálogo.
CREATE SCHEMA IF NOT EXISTS dev.bronze_energia  COMMENT 'Crudo, append-only, con metadatos de ingesta';
CREATE SCHEMA IF NOT EXISTS dev.silver_energia  COMMENT 'Una fila por serie-día, validada';
CREATE SCHEMA IF NOT EXISTS dev.gold_energia    COMMENT 'Features y pronósticos';
CREATE SCHEMA IF NOT EXISTS dev.models          COMMENT 'Modelos registrados (MLflow en Unity Catalog)';

CREATE SCHEMA IF NOT EXISTS qa.bronze_energia;
CREATE SCHEMA IF NOT EXISTS qa.silver_energia;
CREATE SCHEMA IF NOT EXISTS qa.gold_energia;
CREATE SCHEMA IF NOT EXISTS qa.models;

CREATE SCHEMA IF NOT EXISTS prod.bronze_energia;
CREATE SCHEMA IF NOT EXISTS prod.silver_energia;
CREATE SCHEMA IF NOT EXISTS prod.gold_energia;
CREATE SCHEMA IF NOT EXISTS prod.models;

-- Volumen de aterrizaje para la ingesta (clase 4). Uno por ambiente.
CREATE VOLUME IF NOT EXISTS dev.bronze_energia.landing  COMMENT 'Archivos tal como llegan de XM';
CREATE VOLUME IF NOT EXISTS qa.bronze_energia.landing;
CREATE VOLUME IF NOT EXISTS prod.bronze_energia.landing;

SHOW CATALOGS;

-- Si CREATE CATALOG falla en tu workspace (sin privilegio sobre el metastore), plan B:
-- usa esquemas con prefijo dentro de `workspace`: workspace.dev_bronze_energia, workspace.qa_bronze_energia, …
-- y ajusta conf/{dev,qa,prod}.yml. La matriz de permisos es la misma.

-- =====================================================================
-- Sección 2 · Grupos
-- =====================================================================
-- Los grupos no se crean con SQL. En el workspace: Settings → Identity and access → Groups → Add group.
-- Crear tres grupos y meter a todos los estudiantes en los tres (hoy todos juegan todos los roles):
--   grp_ingenieria   (escribe en dev, lee todo)
--   grp_analitica    (escribe en dev.gold y dev.models, lee el resto)
--   grp_negocio      (solo lee vistas de prod.gold)
-- El service principal sp_pipeline se crea en la clase 13 (Settings → Identity and access → Service principals).
--
-- Comprobar que existen:
SHOW GROUPS;

-- =====================================================================
-- Sección 3 · La matriz de governance.md, en GRANTs  (EN VIVO)
-- =====================================================================
-- Regla: privilegios a grupos, nunca a personas. Herencia: lo que se da sobre el catálogo
-- o el esquema aplica a todo lo que contiene, incluido lo que se cree mañana.

-- 3.1 Entrar: sin USE CATALOG nadie ve nada, aunque tenga SELECT.
GRANT USE CATALOG ON CATALOG dev  TO `grp_ingenieria`, `grp_analitica`;
GRANT USE CATALOG ON CATALOG qa   TO `grp_ingenieria`, `grp_analitica`;
GRANT USE CATALOG ON CATALOG prod TO `grp_ingenieria`, `grp_analitica`, `grp_negocio`;

-- 3.2 dev: ingeniería escribe bronce y plata; analítica escribe oro y modelos; todos leen todo.
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, READ VOLUME, WRITE VOLUME
  ON SCHEMA dev.bronze_energia TO `grp_ingenieria`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION ON SCHEMA dev.silver_energia TO `grp_ingenieria`;
GRANT USE SCHEMA, SELECT ON SCHEMA dev.gold_energia TO `grp_ingenieria`;
GRANT USE SCHEMA, SELECT ON SCHEMA dev.models       TO `grp_ingenieria`;

GRANT USE SCHEMA, SELECT ON SCHEMA dev.bronze_energia TO `grp_analitica`;
GRANT USE SCHEMA, SELECT ON SCHEMA dev.silver_energia TO `grp_analitica`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION ON SCHEMA dev.gold_energia TO `grp_analitica`;
GRANT USE SCHEMA, SELECT, EXECUTE, CREATE MODEL, CREATE FUNCTION ON SCHEMA dev.models TO `grp_analitica`;

-- 3.3 qa y prod: las personas solo leen. Escribe el pipeline (sp_pipeline, clase 13).
GRANT USE SCHEMA, SELECT ON SCHEMA qa.bronze_energia, qa.silver_energia, qa.gold_energia TO `grp_ingenieria`, `grp_analitica`;
GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA qa.models TO `grp_ingenieria`, `grp_analitica`;

GRANT USE SCHEMA, SELECT ON SCHEMA prod.bronze_energia, prod.silver_energia TO `grp_ingenieria`, `grp_analitica`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold_energia TO `grp_ingenieria`, `grp_analitica`, `grp_negocio`;
GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA prod.models TO `grp_ingenieria`, `grp_analitica`;

-- 3.4 Comprobar
SHOW GRANTS ON SCHEMA dev.bronze_energia;
SHOW GRANTS ON SCHEMA prod.gold_energia;

-- 3.5 La demostración: alguien de grp_negocio intenta escribir en prod.
-- (Ejecutar como un estudiante, o mostrar el error en pantalla)
-- CREATE TABLE prod.gold_energia.prueba (x INT);
-- → PERMISSION_DENIED: User does not have CREATE TABLE on Schema 'prod.gold_energia'.
-- Leerlo completo: dice qué privilegio falta y sobre qué objeto. Ese error es la política funcionando.

-- 3.6 Auditoría en una consulta: quién tiene qué sobre prod.
SELECT table_schema, grantee, privilege_type
FROM prod.information_schema.schema_privileges
ORDER BY table_schema, grantee;
