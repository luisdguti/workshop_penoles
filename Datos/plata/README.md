# Integrador NiFi: operación de plata (simulada)

Construye por tu cuenta un flujo que reciba archivos periódicos, conserve el original en HDFS, clasifique records con SQL y transporte los registros válidos por Kafka hasta HDFS y una tabla Iceberg. El flujo se arma en el canvas; este kit no incluye una solución importable.

**Todos los datos son sintéticos.** Las unidades `UNIDAD_DEMO_A/B`, las mediciones y las fechas son ficticias y no representan operaciones ni resultados de Peñoles. El caso conecta el taller con el procesamiento de mineral de plata; sus rangos sirven para practicar validaciones, no como parámetros de ingeniería metalúrgica. No requiere descargar un dataset externo ni consultar una API. La variante elegida es recepción periódica de archivos.

## 1. Material y contrato

- `datos/lote_001.csv`, `lote_002.csv`, `lote_003.csv`: 24 registros por archivo, 72 en total.
- `operacion_plata.avsc`: esquema Avro para interpretar CSV y JSON; el contenido de salida será JSONL.
- `validos.sql`, `rechazados.sql`: consultas para las dos propiedades dinámicas de QueryRecord.
- `parametros.example.json`: valores de referencia, **no** un Parameter Context importable.
- `enviar_lotes.py`: deposita los tres CSV con 30 segundos de intervalo.
- `validar.py`: verifica los archivos entregados y las salidas descargadas de HDFS.
- `manifest.json`: conteos, IDs rechazados y SHA-256 de los CSV.
- `crear_tabla_iceberg.sql`, `validar_iceberg.sql` e [ICEBERG.md](ICEBERG.md): creación, configuración del destino y consulta básica; extensión para **CFM 4.12 / NiFi 2.6.0**.

| Campo         | Significado                                  | Tipo            |
| ------------- | -------------------------------------------- | --------------- |
| event_id      | Identificador estable de la observación      | string          |
| batch_id      | Lote de origen, incluso después de Kafka     | string          |
| event_time    | Hora de observación ISO 8601 UTC             | string          |
| unit_id       | Unidad ficticia A o B                        | string          |
| ore_tonnes    | Toneladas de mineral procesado               | double nullable |
| ag_grade_g_t  | Ley de plata: gramos por tonelada de mineral | double nullable |
| recovery_pct  | Recuperación de plata, porcentaje            | double nullable |
| source_system | Siempre SIMULADOR_TALLER                     | string          |

CSV UTF-8, delimitador coma, punto decimal y primera línea con encabezados. `NULL` significa dato ausente. Cada lote contiene dos registros deliberadamente inválidos: tonelaje negativo y recuperación mayor de 100 (o ausente en el tercer lote). **Regla:** tonelaje y ley mayores que cero, recuperación entre 0 y 100, ninguna de estas tres medidas nula. Las consultas son complementarias, incluso ante valores nulos.

## 2. Preparación del ambiente

1. Crea un Process Group `Integrador_Plata_<alumno>` y su Parameter Context. Copia los nombres de `parametros.example.json`, sustituyendo usuario, ejecución, rutas y brokers. Los parámetros se referencian como `#{input.dir}`; los atributos como `${filename}`.
2. Pide al administrador un **topic vacío y exclusivo para alumno/ejecución**, por ejemplo `ws.plata.alumno01.r01`, y un consumer group exclusivo. Para simplificar la observación puede tener una partición; replicación y `min.insync.replicas` se ajustan al clúster. No dependas de autocreación de topics.
3. Confirma permisos de publicación/consumo y del grupo para la identidad real de NiFi; configura el protocolo real del clúster, TLS y Kerberos/SASL si corresponden. No uses PLAINTEXT por defecto ni guardes credenciales en este kit.
4. Confirma permisos HDFS para NiFi en `#{hdfs.root}/#{run.id}` y recursos Hadoop accesibles en sus nodos. Configura los servicios/credenciales Kerberos de PutHDFS según el ambiente. El `kinit` de tu terminal **no** autentica por sí solo el proceso NiFi.
5. Crea o autoriza los subdirectorios `raw`, `validos` y `rechazados`; habilita el acceso de lectura para tu usuario de validación. Si los crea NiFi, su identidad debe poder crear directorios en la raíz del ejercicio.
6. Copia el kit al nodo NiFi o a un montaje accesible por NiFi. `input.dir` es una ruta del servidor, no de tu portátil. Ejecuta ListFile y FetchFile en **Primary Node**, sin balanceo de las conexiones entre ambos. Para tolerar cambio de primario, usa un montaje compartido con la misma ruta en todos los nodos.

Antes de arrancar la ingesta, prepara una tabla Iceberg vacía siguiendo [ICEBERG.md](ICEBERG.md). Confirma el Runtime con el instructor y configura `iceberg.namespace`, `iceberg.table` y `hive.config.resources`. Para una nueva ejecución también cambian la tabla y su ubicación.

Usa un único task por processor para esta práctica y back pressure, por ejemplo 100 FlowFiles / 10 MB en las conexiones. Deja FlowFile Expiration en `0 sec`. Crea una cola `ERRORES_<processor>` hacia un Funnel sin salida (sin auto-terminate): permite inspeccionar y conservar errores. Un Funnel puede tener entrada sin conexiones salientes. Detén el origen si crece esa cola y resuelve el motivo antes de reintentar; no construyas un bucle de reintentos sin control.

## 3. Recorrido esperado

```text
ListFile → FetchFile → UpdateAttribute_origen → PutHDFS_raw
  → QueryRecord
      validos    → PublishKafkaRecord → [topic Kafka]
      rechazados → UpdateAttribute_rechazo → PutHDFS_rechazados
      original   → auto-terminate (el CSV ya está en raw)
      failure    → ERRORES_QueryRecord

[topic Kafka] → consumidor Kafka → MergeRecord_Plata
  merged → UpdateAttribute_destino → PutHDFS_validos
  success → PutIceberg_Plata → tabla Iceberg (consultable desde Impala)
```

Kafka desacopla los dos tramos. No dibujes una conexión NiFi entre Publish y Consume. Auto-terminate `success` del publisher solo después del acuse de Kafka; los errores se conservan. Auto-terminate `success` de PutHDFS_rechazados. Conecta `PutHDFS_validos.success` a PutIceberg y termina `PutIceberg.success`; conserva los errores de ambos processors. `PutHDFS_raw.success` sí debe continuar hacia QueryRecord.

## 4. Entrada periódica y archivo original

| Processor              | Configuración                                                                                                                                                               |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ListFile               | Input Directory `#{input.dir}`; File Filter `lote_[0-9]{3}\.csv`; Recurse Subdirectories `false`; Minimum File Age `5 sec`; Run Schedule `10 sec`; Execution `Primary node` |
| FetchFile              | File to Fetch `${absolute.path}/${filename}`; Completion Strategy `None`; Execution `Primary node`                                                                          |
| UpdateAttribute_origen | `source.filename = ${filename}`; `dataset.name = operacion_plata_simulada`; `pipeline.run = #{run.id}`                                                                      |
| PutHDFS_raw            | Hadoop Configuration Resources `#{hadoop.resources}`; Directory `#{hdfs.root}/#{run.id}/raw`; Conflict Resolution Strategy `fail`                                           |

Conecta `success` de FetchFile; conserva `failure`, `not.found` y `permission.denied` en sus colas de error. UpdateAttribute cambia metadatos del FlowFile, **no modifica las columnas del CSV**. No renombres el archivo antes de `PutHDFS_raw`, para conservar `lote_001.csv`, etc.

Cuando el flujo esté listo, desde el servidor ejecuta (reemplaza la ruta por el valor exacto de `input.dir`):

```bash
python3 enviar_lotes.py --destino /tmp/nifi-plata/alumno01/r01/entrada --intervalo 30
```

El script escribe primero `.part` y renombra a `.csv` al terminar. ListFile solo selecciona los CSV completos. No limpies su estado durante la ejecución normal ni copies los archivos de nuevo; así observarás la llegada de tres lotes distintos.

## 5. Controller Services y QueryRecord

Crea y habilita estos tres servicios en el Process Group:

| Servicio                  | Propiedades                                                                                                                                                                                                                                                                                  |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CSVReader_Plata           | Schema Access Strategy: `Use 'Schema Text' Property`; Schema Text: contenido completo de `operacion_plata.avsc`; CSV Format: `Custom Format`; Value Separator: `,`; Treat First Line as Header: `true`; Ignore CSV Header Column Names: `false`; Null String: `NULL`; Character Set: `UTF-8` |
| JsonTreeReader_Plata      | Schema Access Strategy: `Use 'Schema Text' Property`; Schema Text: el mismo `.avsc`                                                                                                                                                                                                          |
| JsonRecordSetWriter_Plata | Schema Access Strategy: `Inherit Record Schema`; Schema Write Strategy: `Do Not Write Schema`; Output Grouping: `One Line Per Object`; Pretty Print JSON: `false`; Suppress Null Values: `Never Suppress`; Compression Format: `none`                                                        |

En QueryRecord configura `Record Reader = CSVReader_Plata`, `Record Writer = JsonRecordSetWriter_Plata` e `Include Zero Record FlowFiles = false`. Agrega las propiedades dinámicas **validos** y **rechazados** con el contenido de sus archivos `.sql`. Sus nombres crean relationships. `FLOWFILE` representa los records del CSV actual; no es una tabla de Hive.

Conserva `failure` para errores de parseo/SQL. Los registros que incumplen las reglas de negocio salen por `rechazados`, no por `failure`. La transformación no elimina campos: event_id, batch_id y source_system viajan dentro del mensaje JSON. Al usar esquema explícito en JsonTreeReader, el consumidor no depende de atributos de un FlowFile anterior.

## 6. Kafka

En ambos processors usa `Kafka Brokers = #{kafka.brokers}`, el mismo topic y la seguridad confirmada del clúster. Selecciona `JsonTreeReader_Plata` y `JsonRecordSetWriter_Plata` como reader/writer.

- **PublishKafkaRecord_2_0:** Topic Name `#{kafka.topic}`; Message Key Field `event_id`; Delivery Guarantee `Guarantee Replicated Delivery`; Use Transactions `false` para este ejercicio; Failure Strategy `Route to Failure`. `validos` de QueryRecord es su entrada. Conserva `failure` en una cola; auto-terminate `success`.
- **ConsumeKafkaRecord_2_0:** Topic Name(s) `#{kafka.topic}`; Topic Name Format `names`; Group ID `#{kafka.group}`; Offset Reset `earliest`. Conecta `success` hacia MergeRecord_Plata y `parse.failure`, si el consumidor la expone, a una cola de error.

`earliest` aplica cuando el grupo no tiene offsets válidos; no reinicia un grupo existente. Cada record publicado es un mensaje; el consumidor puede agrupar varios mensajes en un FlowFile. Los atributos NiFi no cruzan Kafka automáticamente: aquí la trazabilidad está en el contenido. No se requiere enviar headers.

Esta práctica **no garantiza exactly-once de extremo a extremo**. Una publicación parcialmente exitosa y su reintento pueden producir duplicados; también debe considerarse el reprocesamiento en el consumidor. La clave Kafka event_id ayuda a identificar eventos, pero **no deduplica** automáticamente un topic. La conciliación los detecta. No borres mensajes ni offsets para ocultarlos.

## 6.1. Agrupar después de Kafka: MergeRecord

Agrega **MergeRecord_Plata** entre el consumidor Kafka y UpdateAttribute_destino. Si el consumidor entrega un JSON por FlowFile, este paso reúne muchos records antes de escribir. También funciona cuando un FlowFile de entrada ya contiene varios records. La salida del consumidor debe contener los records JSON del contrato (no una envoltura con key/value/metadatos de Kafka).

| Propiedad de MergeRecord | Valor para el laboratorio |
|---|---|
| Record Reader | `JsonTreeReader_Plata` con el esquema explícito existente |
| Record Writer | `JsonRecordSetWriter_Plata`, One Line Per Object, Pretty Print false |
| Merge Strategy | `Bin-Packing Algorithm` |
| Minimum Number of Records | `#{merge.min.records}` = `20` |
| Maximum Number of Records | `#{merge.max.records}` = `1000` |
| Minimum Bin Size | `0 B` |
| Maximum Bin Size | `1 MB` |
| Max Bin Age | `#{merge.max.age}` = `60 sec` |
| Maximum Number of Bins | `5` |
| Correlation Attribute Name | Sin configurar |
| Attribute Strategy | `Keep Only Common Attributes` |

En Scheduling, usa Timer driven, Run Schedule `1 sec` y Concurrent Tasks `1`. Estos valores son didácticos para 66 registros; no son objetivos de tamaño para producción. Los parámetros están incluidos en `parametros.example.json`.

Conecta **merged → UpdateAttribute_destino**, auto-terminate **original** y conserva **failure** en una cola inspeccionable. No conectes original al destino: escribirías los records dos veces. UpdateAttribute_destino queda **después** del merge y asigna `${uuid}.jsonl` al FlowFile combinado; continúa hacia PutHDFS_validos y PutIceberg. Se mantiene JSONL porque PutIceberg usa JsonTreeReader; no cambies aquí el writer a Parquet.

Con Minimum Bin Size = 0 B, el mínimo de 20 records habilita el cierre normal del grupo; **no exige exactamente 20**. No esperes a alcanzar 1000: ese valor es un máximo, no el objetivo de acumulación. Max Bin Age permite emitir el remanente aunque no llegue al mínimo. La edad se cuenta desde que se crea el bin, no desde el último mensaje; la salida se procesa al ejecutarse el processor, sujeta al scheduling y back pressure. No detengas MergeRecord antes de que termine de vaciarse.

No uses event_id como atributo de correlación: separarías los eventos y perderías la agrupación. Tampoco necesitas separar por batch_id: permanece en cada record y puede consultarse después. Este Process Group recibe un único esquema y destino por alumno/ejecución. Si luego se mezclan tablas o ejecuciones, separa los flujos o establece una correlación apropiada antes del merge.

Los bins son locales a cada nodo. Un Concurrent Task no reúne globalmente los mensajes de todo el clúster. El número de archivos depende de las llegadas, distribución, esquema y cierres de bins; no se prometen tres archivos exactos. Los máximos de registros/tamaño son límites flexibles: un FlowFile de entrada no se divide para cumplirlos. El tamaño JSON agrupado tampoco equivale al tamaño Parquet final de Iceberg.

**Evidencia:** inspecciona `record.count`, `merge.count` y `merge.completion.reason` en la relación merged o en Data Provenance. Verifica que hay salidas con múltiples records y menos FlowFiles de salida que entradas cuando estas llegan de uno en uno. La suma de records entregados debe seguir siendo 66; ejecuta las validaciones HDFS e Iceberg existentes. Los rechazos quedan en su rama y no se combinan con los válidos.

**Prueba del remanente:** en una ejecución nueva (rutas, topic/grupo y tabla nuevos), cambia temporalmente `merge.min.records` a `100`, mayor que los 66 registros válidos disponibles. Mantén el máximo en 1000 y Max Bin Age en 60 sec. Envía los tres lotes y deja correr el processor hasta que expiren los bins pendientes; los 66 records deben salir aun sin llegar a 100. Inspecciona el motivo de cierre y vuelve a conciliar. Restaura el mínimo 20 para la práctica normal.

En producción se ajustan tamaño, volumen y espera según la latencia admitida y se mide el tamaño real de los archivos. Un flujo con poco tráfico seguirá emitiendo archivos pequeños al vencer el tiempo; el merge reduce su proliferación, pero no sustituye el mantenimiento de Iceberg. Cambiar solo Maximum File Size de PutIceberg no acumula automáticamente FlowFiles separados.

Referencia: [MergeRecord de CFM 4.12 / NiFi 2.6.0](https://docs.cloudera.com/cfm/4.12.0/nifi-components-cfm/components/org.apache.nifi/nifi-standard-nar/2.6.0.4.12.0.0-907/org.apache.nifi.processors.standard.MergeRecord/).

## 7. Salidas HDFS

Antes de cada PutHDFS final, agrega un UpdateAttribute que defina:

```text
filename = ${uuid}.jsonl
mime.type = application/x-ndjson
pipeline.run = #{run.id}
```

Usa `${uuid}`, estable para ese FlowFile al reintentar, en lugar de generar un nuevo UUID en cada intento. Define PutHDFS_validos con Directory `#{hdfs.root}/#{run.id}/validos` y PutHDFS_rechazados con `#{hdfs.root}/#{run.id}/rechazados`; en ambos configura recursos Hadoop, autenticación y Conflict Resolution Strategy `fail`. Conserva `failure` en ambos. Auto-terminate `success` solo en PutHDFS_rechazados; `PutHDFS_validos.success` continúa a PutIceberg, según [ICEBERG.md](ICEBERG.md). Ante conflicto inspecciona el archivo existente antes de decidir un reproceso: no uses `replace` para ocultarlo.

## 8. Ejecución y evidencia

1. Ejecuta `python3 validar.py` dentro del kit. Debe verificar 72 registros.
2. Verifica que la entrada esté vacía, el topic sea nuevo/vacío y no haya salidas previas para run.id y la tabla Iceberg esté creada y vacía. Inicia primero PutIceberg y el tramo consumidor/HDFS, después el resto de processors y finalmente el script de envío.
3. Observa cada lote: QueryRecord entrega **22 records válidos y 2 rechazados**. Con los tres lotes: **66 válidos y 6 rechazados**. La cantidad de archivos JSONL depende también de MergeRecord y sus bins. Espera a que salgan los grupos pendientes por Max Bin Age, con el processor en ejecución, antes de conciliar.
4. Usa Data Provenance para recorrer un evento desde FetchFile a Publish y el tramo desde Consume a PutHDFS. Los dos tramos no comparten automáticamente el mismo UUID; correlaciónalos por event_id y batch_id del contenido.
5. Espera a que terminen las escrituras y descarga las salidas desde un edge node con cliente HDFS, permisos y autenticación. Sustituye la ruta por tu alumno/ejecución:

```bash
hdfs dfs -ls /user/workshop/alumno01/plata/r01/raw
hdfs dfs -cat '/user/workshop/alumno01/plata/r01/validos/*.jsonl' > validos.jsonl
hdfs dfs -cat '/user/workshop/alumno01/plata/r01/rechazados/*.jsonl' > rechazados.jsonl
python3 validar.py --validos validos.jsonl --rechazados rechazados.jsonl
```

Confirma que cada comando HDFS termina sin error antes de ejecutar el siguiente. Si validas en otra máquina, copia allí ambos JSONL y el kit. No cuentes únicamente líneas de CSV (contienen encabezados), mensajes del panel ni archivos HDFS. El validador compara IDs y contenido, detecta pérdidas/duplicados y requiere 66/6 registros exactos. En raw deben estar los tres CSV originales, íntegros.

6. **Prueba de back pressure sin provocar fallos:** repite el laboratorio con otro run.id, topic/grupo, directorio, tabla Iceberg y ubicación de tabla nuevos. Detén PutHDFS_validos y fija el umbral de objetos de su conexión en 1. Envía los lotes: observa cómo se acumulan FlowFiles y se frena el consumidor. El umbral no es un máximo rígido y puede rebasarse por un batch. Reinicia PutHDFS, espera el drenaje y vuelve a conciliar. No limpies estado ni offsets en una ejecución activa.

Completa también la consulta desde Impala con `validar_iceberg.sql`: **66 filas, 66 IDs distintos y 22 filas por lote**, sin duplicados ni registros inválidos. El validador Python comprueba los JSONL; esta verificación SQL comprueba por separado la tabla.

Entrega: captura del canvas con PutIceberg, CREATE/SHOW CREATE TABLE y resultados SQL, valores no sensibles del Parameter Context, evidencia 72 = 66 + 6, atributos del merge y prueba del remanente, IDs de los rechazos, un ejemplo de trazabilidad y explicación de qué sucede si se detiene el destino. Exporta tu Process Group como flow definition sin credenciales. Las soluciones de los laboratorios anteriores no contienen este integrador.

**Extensión opcional:** usa otro QueryRecord para calcular por unidad `SUM(ore_tonnes * ag_grade_g_t / 1000)` como kg teóricos de plata contenida en el mineral. Es una estimación didáctica por FlowFile, no un balance metalúrgico ni un agregado global del stream. No conectes ese agregado al mismo topic de observaciones. Las transformaciones con Spark y las funciones avanzadas de Iceberg se trabajarán después.

## 9. Tabla Iceberg: creación, carga y consulta

Sigue [ICEBERG.md](ICEBERG.md) antes de arrancar la ingesta: ejecuta el DDL en Hue/Impala, configura HiveCatalogService y PutIceberg, y valida el resultado. PutIceberg no crea la tabla. Se conservan los JSONL; los mismos 66 registros se escriben en Iceberg. No se agregan ejercicios de Kudu, snapshots, MERGE, evolución de esquema ni mantenimiento en esta etapa.

## Referencias de componentes

Propiedades contrastadas con documentación Apache NiFi 1.28; validar la versión instalada antes del taller:

- [QueryRecord](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.28.0/org.apache.nifi.processors.standard.QueryRecord/)
- [CSVReader](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-record-serialization-services-nar/1.28.0/org.apache.nifi.csv.CSVReader/index.html)
- [JsonRecordSetWriter](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-record-serialization-services-nar/1.28.0/org.apache.nifi.json.JsonRecordSetWriter/index.html)
- [PublishKafkaRecord_2_0](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-kafka-2-0-nar/1.28.0/org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_0/index.html)
- [ConsumeKafkaRecord_2_0](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-kafka-2-0-nar/1.28.0/org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_0/index.html)

Validación local disponible: consistencia de CSV/esquema/consultas/conteos y verificador de salidas. La extensión Iceberg está documentada para CFM 4.12 / NiFi 2.6.0 en ICEBERG.md. La ejecución completa requiere el clúster NiFi/Kafka/HDFS/Iceberg/Impala del taller; no se presenta como ejecutada en ese ambiente.
