# Destino Iceberg del integrador CFM

**Ambiente confirmado: CFM 4.12 / NiFi 2.6.0.** Esta extensión termina la ingesta con una tabla consultable. El alumno crea la tabla desde Hue/Impala y NiFi la carga con PutIceberg. Alcance sugerido: 20 minutos, limitado a **CREATE TABLE → ingesta → SELECT**. Snapshots, time travel, MERGE, evolución de esquema/particiones y mantenimiento pertenecen al taller posterior de Iceberg.

## 1. Preparar la tabla antes de ejecutar NiFi

El instructor confirma que el Runtime del clúster permite crear y consultar Iceberg desde Impala. La versión de CFM no determina la versión de Runtime. Se requiere acceso de NiFi al mismo Hive Metastore usado por Impala, a HDFS y a las credenciales/permisos correspondientes. Autoriza CREATE/SELECT para el alumno y las operaciones de catálogo y escritura HDFS para la identidad de NiFi, según las políticas del ambiente.

Usa una ejecución nueva y una tabla vacía. Si ya procesaste `r01`, cambia coherentemente a `r02`: run.id, input.dir, topic, consumer group, nombre de tabla y ubicación de almacenamiento. Los FlowFiles que antes terminaron en PutHDFS no aparecerán automáticamente en el nuevo processor.

1. Abre Hue, selecciona el editor **Impala** y adapta [crear_tabla_iceberg.sql](crear_tabla_iceberg.sql) a tu alumno/ejecución. Si no puedes crear bases, el instructor debe asignarte una y debes usarla también en NiFi.
2. Ejecuta CREATE DATABASE si corresponde y CREATE TABLE. Si la tabla ya existe, detente y elige una nueva; no borres ni reutilices datos previos.
3. Verifica SHOW CREATE TABLE y el conteo inicial **0**.

El ejemplo usa `workshop_alumno01.operacion_plata_r01`. Los parámetros NiFi deben ser `iceberg.namespace = workshop_alumno01` e `iceberg.table = operacion_plata_r01`: el segundo contiene solo el nombre de la tabla. Hue no interpreta los parámetros `#{...}` de NiFi; el SQL contiene valores explícitos que debes adaptar.

Los ocho nombres/tipos coinciden con `operacion_plata.avsc`: event_time permanece STRING ISO 8601 UTC y las tres medidas son DOUBLE. La tabla no tiene particiones en este ejercicio. Su LOCATION es una carpeta propia, distinta de raw/validos/rechazados. La ruta absoluta se resuelve con la configuración HDFS del clúster; en HA confirma que Impala y NiFi resuelvan el mismo nameservice lógico.

**Crear una tabla Iceberg no registra automáticamente los JSONL existentes.** No copies CSV, JSONL ni archivos Parquet sueltos dentro de su LOCATION. El escritor Iceberg debe registrar lo que escribe para que las consultas lo vean.

## 2. Configurar HiveCatalogService

En el Process Group, añade `HiveCatalogService` con nombre `HiveCatalog_Plata`:

| Propiedad | Valor |
|---|---|
| Hadoop Configuration Resources | `#{hive.config.resources}` |
| Hive Metastore URI | Sin sobrescribir si ya se define en hive-site.xml; si no, la URI Thrift real indicada por el administrador |
| Default Warehouse Location | Sin sobrescribir; el ejercicio ya creó una tabla con LOCATION explícita |

El nuevo parámetro `hive.config.resources` incluye core-site.xml, hdfs-site.xml y hive-site.xml del clúster destino. Confirma las rutas de ejemplo en cada nodo NiFi, su legibilidad y las opciones de seguridad/SASL del metastore. Usa los gateways/configuraciones distribuidos por Cloudera Manager. La URI del metastore **no** es la URL JDBC de HiveServer2.

Aquí HMS sirve como catálogo compartido: relaciona el nombre de la tabla con su metadata vigente. Iceberg mantiene sus archivos de metadata y datos en almacenamiento. Usar HiveCatalogService permite a NiFi e Impala encontrar la misma tabla; no convierte la tabla en una tabla Hive tradicional.

## 3. Extender la salida de válidos

```text
Consumidor Kafka existente
  → MergeRecord_Plata (relación merged)
  → UpdateAttribute_destino
  → PutHDFS_validos
      success → PutIceberg_Plata
                    success → auto-terminate
                    failure → cola ERRORES_PutIceberg
```

En `PutHDFS_validos`, quita el auto-terminate de `success` y conecta esa relación a PutIceberg. Mantén su failure y la rama de rechazados. MergeRecord agrupa primero los records según la sección 6.1 del README. PutIceberg recibe el mismo contenido JSONL agrupado después de escribirlo en HDFS; no hace falta releer los archivos ni añadir un consumidor Kafka.

Configura `PutIceberg_Plata`:

| Propiedad | Valor |
|---|---|
| Record Reader | `JsonTreeReader_Plata`, con `operacion_plata.avsc` explícito |
| Catalog Service | `HiveCatalog_Plata` |
| Catalog Namespace | `#{iceberg.namespace}` |
| Table Name | `#{iceberg.table}` |
| File Format | `PARQUET` |
| Unmatched Column Behavior | `Fail on Unmatched Columns` |
| Kerberos User Service | Servicio autenticado del ambiente cuando corresponda |

Habilita los Controller Services. Configura un único Concurrent Task en PutIceberg para facilitar la observación. No necesita un Record Writer: lee JSONL y escribe archivos de datos mediante Iceberg. PutIceberg exige que la tabla exista; **no ejecuta el CREATE TABLE**. Separa el DDL de preparación del flujo periódico.

Conserva `failure` en una cola inspeccionable y termina `success` de PutIceberg. La extensión hace append, no un UPSERT por event_id. No asumas deduplicación al reenviar datos ni una transacción conjunta HDFS/Iceberg. Si HDFS terminó y Iceberg falla, el JSONL permanece en HDFS: corrige y revisa la tabla antes de repetir una carga. No recircules un error de Iceberg hacia PutHDFS.

## 4. Ejecutar y conciliar

1. Con la tabla vacía y los servicios habilitados, inicia PutIceberg y los destinos, después el resto del flujo y finalmente el emisor de lotes. Conserva las verificaciones HDFS del README.
2. Espera al vaciado de las colas y a que MergeRecord emita sus bins pendientes, incluido el remanente por Max Bin Age; mantén el processor activo y revisa los errores. El success de PutHDFS confirma solo esa escritura; revisa también el de PutIceberg.
3. En Hue/Impala, ejecuta [validar_iceberg.sql](validar_iceberg.sql). REFRESH actualiza la vista de Impala tras la escritura externa; no carga archivos nuevos en la tabla.
4. Deben aparecer **66 filas, 66 event_id distintos y 22 filas por lote**. La consulta de duplicados debe devolver cero filas y la de registros inválidos debe contar cero.
5. Entrega captura del CREATE/SHOW CREATE TABLE, del canvas con PutIceberg y de los resultados SQL, además de la evidencia HDFS del integrador.

Los 66 registros JSONL y las 66 filas Iceberg representan las mismas observaciones; no son 132 eventos diferentes. Los 6 rechazados permanecen fuera de la tabla. Para repetir la prueba de back pressure del README, crea también una tabla y una ubicación Iceberg nuevas y actualiza `iceberg.table` antes de arrancar.

MergeRecord reduce el número de escrituras agrupando records antes de HDFS y PutIceberg. Con poco tráfico seguirán apareciendo archivos pequeños al vencer Max Bin Age; no se garantiza un tamaño Parquet ni un archivo por grupo en cualquier configuración de tabla. El ejercicio valida integración y agrupación, no rendimiento de producción. Conserva la tabla y los JSONL para los módulos siguientes; el diseño de particiones y mantenimiento se explicará allí.

## Referencias y validación

- [PutIceberg de CFM 4.12 / NiFi 2.6.0](https://docs.cloudera.com/cfm/4.12.0/nifi-components-cfm/components/org.apache.nifi/nifi-iceberg-processors-nar/2.6.0.4.12.0.0-907/org.apache.nifi.processors.iceberg.PutIceberg/).
- [Configuración del catálogo y autenticación en CFM 4.12](https://docs.cloudera.com/cfm/4.12.0/nifi-iceberg/cfm-nifi-cdw-iceberg-ingest.pdf). La guía describe CDW; aquí se aplica el catálogo HMS/HDFS del entorno Base del taller, sin requerir desplegar CDW.
- [CREATE TABLE Iceberg en Cloudera Private Cloud Base](https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/iceberg-how-to/topics/iceberg-table-creation.html).
- [Iceberg con Impala](https://impala.apache.org/docs/build/html/topics/impala_iceberg.html).

El material contrasta las propiedades con la documentación de CFM 4.12. Las rutas, políticas, compatibilidad de Runtime y ejecución NiFi → Iceberg deben comprobarse en el clúster del taller; no se afirma una prueba de integración realizada en ese ambiente.
