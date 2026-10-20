-- Ejecutar una sola vez desde Hue > Impala, ANTES de arrancar la ingesta.
-- Ejemplo: alumno01 / r01. Sustituye estos valores también en NiFi,
-- validar_iceberg.sql, la ruta HDFS, el topic y el consumer group.
-- Si ya ejecutaste r01, utiliza otra ejecución, tabla y ubicación vacías.
-- Los parámetros #{...} de NiFi NO se sustituyen en el editor SQL de Hue.
-- Requiere un Runtime con Iceberg habilitado en Impala y permisos del taller.

CREATE DATABASE IF NOT EXISTS workshop_alumno01;

-- Sin IF NOT EXISTS: si la tabla existe, detenerse y elegir otra ejecución.
-- Los ocho campos coinciden con operacion_plata.avsc.
-- event_time se conserva como STRING ISO 8601 UTC en esta primera ingesta.
CREATE TABLE workshop_alumno01.operacion_plata_r01 (
  event_id STRING,
  batch_id STRING,
  event_time STRING,
  unit_id STRING,
  ore_tonnes DOUBLE,
  ag_grade_g_t DOUBLE,
  recovery_pct DOUBLE,
  source_system STRING
)
STORED AS ICEBERG
LOCATION '/user/workshop/alumno01/plata/r01/iceberg/operacion_plata'
TBLPROPERTIES ('format-version' = '2');

-- Verificar nombre, formato y ubicación antes de iniciar NiFi.
SHOW CREATE TABLE workshop_alumno01.operacion_plata_r01;
SELECT COUNT(*) AS filas_iniciales
FROM workshop_alumno01.operacion_plata_r01;
-- Esperado: 0. No cargar CSV/JSON directamente en LOCATION.
