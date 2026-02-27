-- =============================================================================
-- 20_curated.sql
-- Limpieza, tipado, estandarización y creación de llaves (POINT_ID) para joins.
-- =============================================================================

USE DATABASE OPTIMIZING_CLEAN_WATER_SUPPLY;

-- -----------------------------------------------------------------------------
-- 0) Helpers: función para parsear fechas (varios formatos posibles)
-- -----------------------------------------------------------------------------
-- Si tu fecha siempre viene como YYYY-MM-DD, bastaría TRY_TO_DATE(SAMPLE_DATE).
-- Lo dejamos más robusto por si hay "YYYY/MM/DD" u otros.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- 1) Normalización de TRAIN: puntos + targets (tipos correctos)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.TRAIN_BASE AS
SELECT
  -- 1.1) Tipos numéricos robustos
  TRY_TO_DOUBLE(LONGITUDE) AS LONGITUDE,
  TRY_TO_DOUBLE(LATITUDE)  AS LATITUDE,

  -- 1.2) Fecha robusta (prueba varios formatos)
  COALESCE(
    TRY_TO_DATE(SAMPLE_DATE, 'YYYY-MM-DD'),
    TRY_TO_DATE(SAMPLE_DATE, 'YYYY/MM/DD'),
    TRY_TO_DATE(SAMPLE_DATE, 'DD-MM-YYYY'),
    TRY_TO_DATE(SAMPLE_DATE)
  ) AS SAMPLE_DATE,

  -- 1.3) Targets: convertir a números
  TRY_TO_DOUBLE(TOTAL_ALKALINITY) AS TOTAL_ALKALINITY,
  TRY_TO_DOUBLE(ELECTRICAL_CONDUCTANCE) AS ELECTRICAL_CONDUCTANCE,
  TRY_TO_DOUBLE(DISSOLVED_REACTIVE_PHOSPHORUS) AS DISSOLVED_REACTIVE_PHOSPHORUS

FROM RAW.WATER_QUALITY_TRAIN;

-- -----------------------------------------------------------------------------
-- 2) Validaciones mínimas: filas inválidas (solo para revisar)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW CURATED.TRAIN_BASE_INVALID AS
SELECT *
FROM CURATED.TRAIN_BASE
WHERE LONGITUDE IS NULL
   OR LATITUDE IS NULL
   OR SAMPLE_DATE IS NULL;

-- -----------------------------------------------------------------------------
-- 3) Filtro de filas válidas
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.TRAIN_BASE_VALID AS
SELECT *
FROM CURATED.TRAIN_BASE
WHERE LONGITUDE IS NOT NULL
  AND LATITUDE IS NOT NULL
  AND SAMPLE_DATE IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 4) Coordenadas redondeadas + POINT_ID (llave maestra)
-- -----------------------------------------------------------------------------
-- ¿Por qué redondear?
-- Porque lat/lon float entre tablas puede diferir en la 6ta/7ma cifra y romper joins.
-- 5 decimales ~ 1.1 m en latitud (aprox), suficiente para “mismo punto de muestreo”.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.TRAIN_POINTS AS
SELECT
  ROUND(LATITUDE, 5)  AS LAT_R,
  ROUND(LONGITUDE, 5) AS LON_R,
  SAMPLE_DATE,

  -- POINT_ID: hash estable basado en lat/lon redondeados + fecha
  SHA2(
    TO_VARCHAR(ROUND(LATITUDE, 5)) || '|' ||
    TO_VARCHAR(ROUND(LONGITUDE, 5)) || '|' ||
    TO_VARCHAR(SAMPLE_DATE),
    256
  ) AS POINT_ID

FROM CURATED.TRAIN_BASE_VALID;

CREATE OR REPLACE TABLE CURATED.TRAIN_TARGETS AS
SELECT
  SHA2(
    TO_VARCHAR(ROUND(LATITUDE, 5)) || '|' ||
    TO_VARCHAR(ROUND(LONGITUDE, 5)) || '|' ||
    TO_VARCHAR(SAMPLE_DATE),
    256
  ) AS POINT_ID,

  TOTAL_ALKALINITY,
  ELECTRICAL_CONDUCTANCE,
  DISSOLVED_REACTIVE_PHOSPHORUS

FROM CURATED.TRAIN_BASE_VALID;

-- -----------------------------------------------------------------------------
-- 5) Dedupe defensivo (por si el dataset trae duplicados exactos del mismo punto/fecha)
-- -----------------------------------------------------------------------------
-- Estrategia: promediar targets si hay duplicados del mismo POINT_ID.
-- Si prefieres "tomar el primero" lo cambiamos, pero promediar es razonable.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.TRAIN_TARGETS_DEDUP AS
SELECT
  POINT_ID,
  AVG(TOTAL_ALKALINITY) AS TOTAL_ALKALINITY,
  AVG(ELECTRICAL_CONDUCTANCE) AS ELECTRICAL_CONDUCTANCE,
  AVG(DISSOLVED_REACTIVE_PHOSPHORUS) AS DISSOLVED_REACTIVE_PHOSPHORUS
FROM CURATED.TRAIN_TARGETS
GROUP BY POINT_ID;

CREATE OR REPLACE TABLE CURATED.TRAIN_POINTS_DEDUP AS
SELECT DISTINCT
  POINT_ID, LAT_R, LON_R, SAMPLE_DATE
FROM CURATED.TRAIN_POINTS;

-- -----------------------------------------------------------------------------
-- 6) Submission points (sin targets) con la misma lógica de POINT_ID
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.SUBMISSION_POINTS AS
SELECT
  TRY_TO_DOUBLE(LONGITUDE) AS LONGITUDE,
  TRY_TO_DOUBLE(LATITUDE)  AS LATITUDE,
  COALESCE(
    TRY_TO_DATE(SAMPLE_DATE, 'YYYY-MM-DD'),
    TRY_TO_DATE(SAMPLE_DATE, 'YYYY/MM/DD'),
    TRY_TO_DATE(SAMPLE_DATE, 'DD-MM-YYYY'),
    TRY_TO_DATE(SAMPLE_DATE)
  ) AS SAMPLE_DATE
FROM RAW.SUBMISSION_TEMPLATE;

CREATE OR REPLACE TABLE CURATED.SUBMISSION_POINTS_DEDUP AS
SELECT
  ROUND(LATITUDE, 5)  AS LAT_R,
  ROUND(LONGITUDE, 5) AS LON_R,
  SAMPLE_DATE,
  SHA2(
    TO_VARCHAR(ROUND(LATITUDE, 5)) || '|' ||
    TO_VARCHAR(ROUND(LONGITUDE, 5)) || '|' ||
    TO_VARCHAR(SAMPLE_DATE),
    256
  ) AS POINT_ID
FROM CURATED.SUBMISSION_POINTS
WHERE LONGITUDE IS NOT NULL
  AND LATITUDE IS NOT NULL
  AND SAMPLE_DATE IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 7) Checks rápidos (views)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW CURATED.CHECK_COUNTS AS
SELECT 'TRAIN_BASE' AS TABLE_NAME, COUNT(*) AS N FROM CURATED.TRAIN_BASE
UNION ALL
SELECT 'TRAIN_BASE_VALID', COUNT(*) FROM CURATED.TRAIN_BASE_VALID
UNION ALL
SELECT 'TRAIN_INVALID', COUNT(*) FROM CURATED.TRAIN_BASE_INVALID
UNION ALL
SELECT 'TRAIN_POINTS_DEDUP', COUNT(*) FROM CURATED.TRAIN_POINTS_DEDUP
UNION ALL
SELECT 'TRAIN_TARGETS_DEDUP', COUNT(*) FROM CURATED.TRAIN_TARGETS_DEDUP
UNION ALL
SELECT 'SUBMISSION_POINTS_DEDUP', COUNT(*) FROM CURATED.SUBMISSION_POINTS_DEDUP;

-- -----------------------------------------------------------------------------
-- 8) Opcional: Tabla final "CURATED.TRAIN_DATASET" (puntos + targets listos)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.TRAIN_DATASET AS
SELECT
  p.POINT_ID,
  p.LAT_R,
  p.LON_R,
  p.SAMPLE_DATE,
  t.TOTAL_ALKALINITY,
  t.ELECTRICAL_CONDUCTANCE,
  t.DISSOLVED_REACTIVE_PHOSPHORUS
FROM CURATED.TRAIN_POINTS_DEDUP p
JOIN CURATED.TRAIN_TARGETS_DEDUP t
  ON t.POINT_ID = p.POINT_ID;


SELECT * FROM CURATED.CHECK_COUNTS;