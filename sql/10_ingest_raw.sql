-- ============================================================================
-- 1) DB + Schemas
-- ============================================================================
CREATE DATABASE IF NOT EXISTS OPTIMIZING_CLEAN_WATER_SUPPLY;
USE DATABASE OPTIMIZING_CLEAN_WATER_SUPPLY;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS FEATURES;
CREATE SCHEMA IF NOT EXISTS ML;

-- ============================================================================
-- 2) File format + Stage
-- ============================================================================
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL','null','')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE STAGE ML_DATA_STAGE
  FILE_FORMAT = CSV_FORMAT;

-- ============================================================================
-- 3) RAW tables (columnas explícitas)
-- ============================================================================

-- Targets / Train base
CREATE OR REPLACE TABLE RAW.WATER_QUALITY_TRAIN (
  LONGITUDE STRING,
  LATITUDE  STRING,
  SAMPLE_DATE STRING,
  TOTAL_ALKALINITY STRING,
  ELECTRICAL_CONDUCTANCE STRING,
  DISSOLVED_REACTIVE_PHOSPHORUS STRING
);

-- Submission template (sin targets)
CREATE OR REPLACE TABLE RAW.SUBMISSION_TEMPLATE (
  LONGITUDE STRING,
  LATITUDE  STRING,
  SAMPLE_DATE STRING,
  TOTAL_ALKALINITY STRING,
  ELECTRICAL_CONDUCTANCE STRING,
  DISSOLVED_REACTIVE_PHOSPHORUS STRING
);

-- Landsat features (ajusta columnas exactas según tu CSV)
CREATE OR REPLACE TABLE RAW.LANDSAT_FEAT_TRAIN (
  LONGITUDE STRING,
  LATITUDE  STRING,
  SAMPLE_DATE STRING,
  NIR STRING,
  GREEN STRING,
  SWIR16 STRING,
  SWIR22 STRING,
  NDMI STRING,
  MNDWI STRING
);

CREATE OR REPLACE TABLE RAW.LANDSAT_FEAT_VAL LIKE RAW.LANDSAT_FEAT_TRAIN;

-- TerraClimate features (el benchmark parece usar solo PET; si tu CSV trae más, agrega columnas)
CREATE OR REPLACE TABLE RAW.TERRACLIMATE_FEAT_TRAIN (
  LONGITUDE STRING,
  LATITUDE  STRING,
  SAMPLE_DATE STRING,
  PET STRING
);

CREATE OR REPLACE TABLE RAW.TERRACLIMATE_FEAT_VAL LIKE RAW.TERRACLIMATE_FEAT_TRAIN;

LIST @ML_DATA_STAGE;

-- ============================================================================
-- 4) Ingestión (comandos)
-- ============================================================================
/*4.1 Subir archivos desde tu repo local (SnowSQL/terminal):*/

-- PUT file://./tables/*.csv @ML_DATA_STAGE AUTO_COMPRESS=TRUE;

-- 4.2 Copiar cada CSV a su tabla (ajusta nombres exactos de archivos):
-- COPY INTO RAW.WATER_QUALITY_TRAIN
-- FROM @ML_DATA_STAGE/water_quality_training_dataset.csv.gz
-- ON_ERROR = 'ABORT_STATEMENT';

-- COPY INTO RAW.SUBMISSION_TEMPLATE
-- FROM @ML_DATA_STAGE/submission_template.csv.gz
-- ON_ERROR = 'ABORT_STATEMENT';

-- COPY INTO RAW.LANDSAT_FEAT_TRAIN
-- FROM @ML_DATA_STAGE/landsat_features_training.csv.gz
-- ON_ERROR = 'ABORT_STATEMENT';

-- COPY INTO RAW.LANDSAT_FEAT_VAL
-- FROM @ML_DATA_STAGE/landsat_features_validation.csv.gz
-- ON_ERROR = 'ABORT_STATEMENT';

-- COPY INTO RAW.TERRACLIMATE_FEAT_TRAIN
-- FROM @ML_DATA_STAGE/terraclimate_features_training.csv.gz
-- ON_ERROR = 'ABORT_STATEMENT';

-- COPY INTO RAW.TERRACLIMATE_FEAT_VAL
-- FROM @ML_DATA_STAGE/terraclimate_features_validation.csv.gz
-- ON_ERROR = 'ABORT_STATEMENT';

