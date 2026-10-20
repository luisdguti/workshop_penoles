-- Hue > Impala. Adaptar alumno/ejecución como en crear_tabla_iceberg.sql.
-- Ejecutar cuando PutIceberg haya terminado y no haya errores pendientes.
REFRESH workshop_alumno01.operacion_plata_r01;

SELECT COUNT(*) AS filas, COUNT(DISTINCT event_id) AS eventos_unicos
FROM workshop_alumno01.operacion_plata_r01;
-- Esperado: 66 / 66.

SELECT batch_id, COUNT(*) AS filas
FROM workshop_alumno01.operacion_plata_r01
GROUP BY batch_id
ORDER BY batch_id;
-- lote_001: 22; lote_002: 22; lote_003: 22.

SELECT event_id, COUNT(*) AS repeticiones
FROM workshop_alumno01.operacion_plata_r01
GROUP BY event_id
HAVING COUNT(*) > 1;
-- Esperado: ninguna fila.

SELECT COUNT(*) AS registros_invalidos
FROM workshop_alumno01.operacion_plata_r01
WHERE NOT COALESCE(
  ore_tonnes > 0
  AND ag_grade_g_t > 0
  AND recovery_pct BETWEEN 0 AND 100,
  FALSE
);
-- Esperado: 0.

SELECT event_id, batch_id, unit_id, ore_tonnes, ag_grade_g_t, recovery_pct
FROM workshop_alumno01.operacion_plata_r01
ORDER BY event_id
LIMIT 10;
