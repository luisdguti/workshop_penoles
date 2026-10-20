# Datos del taller

`users.txt` está vacío para que el instructor agregue el contenido del ejercicio de NiFi.

## Recorrido principal: tres lotes

Utiliza [plata/datos](plata/datos): 72 registros, 66 válidos y 6 rechazados. Las presentaciones conservan estos resultados esperados. El [README del integrador](plata/README.md) explica cada columna y la relación entre eventos, lotes y unidades.

## Opción de volumen: 100 lotes

Desde la raíz del repositorio, con Python 3.9 o posterior y sin instalar paquetes:

```bash
python3 Datos/plata/generar_lotes_ampliados.py
```

Genera `Datos/plata/ampliado/datos/lote_001.csv` a `lote_100.csv`, más un manifiesto y checksums. Reserva aproximadamente 500 MB. El script no sobrescribe una salida existente. Los datos generados quedan fuera de Git y se pueden reproducir con el mismo comando en otro equipo.

| Concepto | Resultado |
|---|---:|
| Archivos | 100 |
| Registros por archivo | 50 000 |
| Total | 5 000 000 |
| Válidos | 4 995 000 |
| Rechazados | 5 000 |
| Combinaciones de fecha y unidad | 62 |

Conserva las ocho columnas del conjunto pequeño. Los primeros 72 eventos se incluyen en `lote_001` y su `batch_id` se ajusta a ese archivo; el resto utiliza identificadores `AG-VOL-…`. Las observaciones nuevas tienen menor tonelaje por evento y fechas del 3 al 31 de enero de 2026. Sirve para practicar volumen, no para extrapolar producción real. No mezcles ambos conjuntos en la misma ejecución.

### Entregarlos a NiFi sin copiar uno por uno

Ejecuta los comandos en el servidor NiFi o en un equipo que tenga montado el directorio de entrada accesible por NiFi. Genera allí los CSV para evitar transferirlos manualmente. Sustituye la ruta por el `input.dir` de tu Process Group:

```bash
python3 Datos/plata/enviar_100_lotes.py --destino /tmp/nifi-plata/alumno01/r100/entrada --simular
python3 Datos/plata/enviar_100_lotes.py --destino /tmp/nifi-plata/alumno01/r100/entrada --intervalo 1
```

El primer comando verifica archivos, checksums y ausencia de colisiones sin copiar nada. El segundo entrega automáticamente los 100 archivos. ListFile debe usar el filtro `lote_[0-9]{3}\.csv`; el emisor publica cada archivo terminado, excluyendo los temporales `.part` del consumo. La entrada es un directorio local o compartido, no una ruta HDFS. El método de publicación utiliza enlaces duros: el sistema de archivos del destino debe soportarlos.

Usa una ejecución independiente (`r100`): directorio de entrada, rutas HDFS, topic, consumer group, tabla y ubicación Iceberg propios. No reinicies el estado de una ejecución anterior para reutilizarla. Revisa el back pressure y MergeRecord con el instructor, espera que se vacíen las colas y concilia **5 000 000 = 4 995 000 + 5 000**. Los ejercicios de las presentaciones que esperan 66 eventos siguen correspondiendo a los tres lotes pequeños.
