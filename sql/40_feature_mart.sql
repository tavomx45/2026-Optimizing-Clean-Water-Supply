-- =============================================================================
-- 40_feature_mart.sql
-- Crea datasets finales para entrenamiento y submission desde tablas FEATURES.
-- =============================================================================

USE DATABASE OPTIMIZING_CLEAN_WATER_SUPPLY;

-- Cambia este string cuando tengas otra versión de features
SET RUN_ID_STR = 'baseline_csv_v1';

-- ---------------------------------------------------------------------------
-- 1) Feature Mart - TRAIN
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE FEATURES.FEATURE_MART_TRAIN AS
SELECT
  -- Llaves y columnas base
  d.POINT_ID,
  d.LAT_R,
  d.LON_R,
  d.SAMPLE_DATE,

  -- Targets
  d.TOTAL_ALKALINITY,
  d.ELECTRICAL_CONDUCTANCE,
  d.DISSOLVED_REACTIVE_PHOSPHORUS,

  -- Features temporales
  MONTH(d.SAMPLE_DATE) AS MONTH,
  DAYOFYEAR(d.SAMPLE_DATE) AS DOY,
  SIN(2 * PI() * DAYOFYEAR(d.SAMPLE_DATE) / 365) AS SIN_DOY,
  COS(2 * PI() * DAYOFYEAR(d.SAMPLE_DATE) / 365) AS COS_DOY,

  -- Features Landsat
  ls.NIR,
  ls.GREEN,
  ls.SWIR16,
  ls.SWIR22,
  ls.NDMI,
  ls.MNDWI,

  -- Features TerraClimate
  tc.PET

FROM CURATED.TRAIN_DATASET d
LEFT JOIN FEATURES.FEAT_LANDSAT_TRAIN ls
  ON ls.POINT_ID = d.POINT_ID
 AND ls.RUN_ID = $RUN_ID_STR
LEFT JOIN FEATURES.FEAT_TERRACLIMATE_TRAIN tc
  ON tc.POINT_ID = d.POINT_ID
 AND tc.RUN_ID = $RUN_ID_STR;

-- ---------------------------------------------------------------------------
-- 2) Feature Mart - SUBMISSION
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE FEATURES.FEATURE_MART_SUBMISSION AS
SELECT
  p.POINT_ID,
  p.LAT_R,
  p.LON_R,
  p.SAMPLE_DATE,

  MONTH(p.SAMPLE_DATE) AS MONTH,
  DAYOFYEAR(p.SAMPLE_DATE) AS DOY,
  SIN(2 * PI() * DAYOFYEAR(p.SAMPLE_DATE) / 365) AS SIN_DOY,
  COS(2 * PI() * DAYOFYEAR(p.SAMPLE_DATE) / 365) AS COS_DOY,

  ls.NIR,
  ls.GREEN,
  ls.SWIR16,
  ls.SWIR22,
  ls.NDMI,
  ls.MNDWI,

  tc.PET

FROM CURATED.SUBMISSION_POINTS_DEDUP p
LEFT JOIN FEATURES.FEAT_LANDSAT_VAL ls
  ON ls.POINT_ID = p.POINT_ID
 AND ls.RUN_ID = $RUN_ID_STR
LEFT JOIN FEATURES.FEAT_TERRACLIMATE_VAL tc
  ON tc.POINT_ID = p.POINT_ID
 AND tc.RUN_ID = $RUN_ID_STR;

-- ---------------------------------------------------------------------------
-- 3) Checks
-- ---------------------------------------------------------------------------

-- Verificar que no se perdieron filas
CREATE OR REPLACE VIEW FEATURES.CHECK_FEATURE_MART_COUNTS AS
SELECT 'FEATURE_MART_TRAIN' AS NAME, COUNT(*) AS N FROM FEATURES.FEATURE_MART_TRAIN
UNION ALL
SELECT 'FEATURE_MART_SUBMISSION', COUNT(*) FROM FEATURES.FEATURE_MART_SUBMISSION;

-- Verificar missing (debería ser 0 o muy bajo con tus outputs)
CREATE OR REPLACE VIEW FEATURES.CHECK_FEATURE_MART_NULLS AS
SELECT
  SUM(IFF(NDMI IS NULL, 1, 0)) AS NULL_NDMI,
  SUM(IFF(MNDWI IS NULL, 1, 0)) AS NULL_MNDWI,
  SUM(IFF(SWIR22 IS NULL, 1, 0)) AS NULL_SWIR22,
  SUM(IFF(PET IS NULL, 1, 0)) AS NULL_PET
FROM FEATURES.FEATURE_MART_TRAIN;
