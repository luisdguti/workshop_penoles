# Taller Peñoles — NiFi, Spark e Iceberg

Material para Cloudera Private Cloud Base 7.3.2 y CFM 4.12 / NiFi 2.6.0.
Descarga o clona el repositorio y abre los HTML en el navegador. Algunos recursos externos requieren conexión a Internet.

## OnDemand — preparación individual

1. [NiFi](OnDemand/NiFi.html)
2. [Spark](OnDemand/Spark.html)
3. [Iceberg](OnDemand/Iceberg.html)

## InSitu — taller acompañado

1. [NiFi](InSitu/NiFi.html): ingesta e integrador de operación de plata.
2. [Spark e Iceberg](InSitu/Spark_Iceberg.html): transformación y consumo de la salida de NiFi.

## Actividad de cierre — Caza de errores

Reto individual de 8 minutos sobre NiFi, Spark e Iceberg, con 10 premios. Funciona en el navegador sin conectarse al clúster.

- [Abrir el reto](Actividades/Caza_de_errores.html).
- [Guía para organizar la actividad](Actividades/README.md).
- [Panel del instructor: importar entregas y calcular el ranking](Actividades/Resultados.html).

Descarga el HTML y ábrelo en tu navegador; la vista de archivos de GitHub muestra su código, no ejecuta la actividad.

## Datos

- [users.txt](Datos/users.txt): archivo reservado para el ejercicio de NiFi; **pendiente de completar**.
- [Kit de operación de plata](Datos/plata/README.md): diccionario de columnas, tres CSV, esquema, consultas y configuración de referencia.
- [Datos ampliados](Datos/README.md): generación y entrega automática de 100 CSV.

El recorrido de las presentaciones utiliza **tres lotes: 72 registros, 66 válidos y 6 rechazados**. Son datos sintéticos; no representan operaciones reales de Peñoles. El conjunto ampliado es opcional y sus conteos no sustituyen los criterios de aceptación de esos ejercicios.

```text
OnDemand/                 NiFi.html, Spark.html, Iceberg.html
InSitu/                   NiFi.html, Spark_Iceberg.html
Datos/
  users.txt               Pendiente de completar
  README.md               Uso del conjunto ampliado
  plata/
    datos/                Tres lotes del taller
    generar_lotes_ampliados.py
    enviar_100_lotes.py
    ...                   Esquema, SQL, parámetros y guía del integrador
```

Los CSV ampliados se generan localmente y están excluidos de Git. No se incluyen versiones anteriores, documentos comerciales, PDFs ni herramientas de edición de las presentaciones. Los parámetros incluidos son ejemplos: adapta rutas, usuario y recursos del clúster antes de ejecutar los ejercicios.
