# Caza de errores — actividad de cierre

Actividad individual para 25 participantes, basada en el OnDemand de NiFi, Spark e Iceberg, con una pregunta adicional sobre QueryRecord. El enfoque es conceptual: no incluye back pressure ni linaje en NiFi, ni ejercicios de lógica SQL en Spark. Hay 12 decisiones: seis correctas y seis incorrectas, dos errores por tecnología.

- **Tiempo de juego:** 8 minutos; reserva unos minutos adicionales para recibir las entregas y premiar.
- **Premios:** 10; mayor puntuación primero y menor duración como desempate.
- **Participantes:** [Caza_de_errores.html](Caza_de_errores.html).
- **Instructor:** [Resultados.html](Resultados.html).

Ambos HTML son autónomos: se pueden descargar por separado y abrir con Chrome, Edge o Firefox reciente. No necesitan servidor, paquetes, fuentes externas ni conexión al clúster. En GitHub hay que descargar el archivo, no abrir su vista de código para jugar. También pueden servirse desde un hosting estático; esta actividad no publica ningún sitio automáticamente.

## Antes de empezar

1. Prueba el reto en una ventana privada y ciérrala después. Usa otra sesión limpia para la actividad real: una sesión terminada se conserva para volver a descargar la entrega.
2. Asigna un ID distinto **P01–P25** a cada participante. Cada quien lo ingresará junto con su nombre.
3. Elige un código de ronda, por ejemplo **PENOLES-01**. El prefijo es `PENOLES-`, seguido de 1 a 24 letras de A a Z, números o guiones, por ejemplo `PENOLES-01` o `PENOLES-GRUPO-A`. Ambos formularios convierten automáticamente el código a mayúsculas; no se admiten eñes, acentos ni espacios en el código. Todos deben escribir el mismo código; puedes comunicar las instrucciones por el chat del taller, sin presentación oral.
4. Acuerda el canal para recibir archivos: el chat o espacio de entrega que ya utilicen. El envío no es automático. No se configura ni se utiliza ninguna cuenta externa desde estos HTML.
5. En tu panel, confirma el código de ronda y el número de participantes esperados. Para 25, deja el valor predeterminado.
6. Pide que abran el reto, completen los datos y esperen tu mensaje de inicio. Se permite una sola sesión por participante. Mantén el solucionario cerrado hasta el final.

## Mensaje para compartir con los participantes

> Abre Caza_de_errores.html en tu navegador y completa tu nombre, el ID asignado y el código PENOLES-01. Espera el mensaje de inicio. Tendrás 8 minutos para encontrar seis errores y seleccionar sus correcciones: dos de NiFi, dos de Spark y dos de Iceberg. Hay otras seis decisiones correctas que debes dejar sin marcar. Trabaja individualmente y sin consultar material ni ayudas. Al entregar, descarga tu archivo JSON y envíalo por este canal. Se premian los diez mejores resultados; a igual puntuación, gana el menor tiempo. El resultado solo queda registrado cuando el instructor recibe el archivo.

## Durante el juego

- El reloj se activa al pulsar **Comenzar**, no al abrir el archivo.
- Las decisiones y las opciones aparecen en orden aleatorio para cada participante.
- Se pueden marcar como máximo seis decisiones. Cada error seleccionado con su corrección correcta vale **1 punto**. Una decisión correcta marcada, una opción equivocada o una selección sin corrección vale 0. No hay puntos negativos.
- Antes de entregar se pueden cambiar selecciones y navegar entre tecnologías.
- La entrega manual pide confirmación. Al llegar a cero se cierra automáticamente y conserva las respuestas disponibles.
- Recargar o dejar la pestaña en segundo plano no reinicia el reloj. La sesión se conserva en el mismo navegador cuando permite almacenamiento local. Si el navegador no lo permite, aparece un aviso para no cerrar la página.
- El participante ve su puntuación al terminar, pero no se revela el solucionario. Debe descargar y enviar su archivo `PENOLES-01_Pxx.json`.

## Reunir y premiar

1. Guarda las entregas recibidas en una carpeta local. Si usas `Actividades/entregas/`, está excluida de Git.
2. Abre **Resultados.html**. Selecciona los 25 JSON juntos o impórtalos por tandas. También acepta el archivo de respaldo de entregas generado por el propio panel.
3. Revisa los mensajes de importación. Un archivo de otra ronda, inválido o con respuestas desconocidas se rechaza. Un archivo idéntico repetido no cuenta dos veces. Dos entregas distintas para el mismo ID requieren retirar la anterior antes de admitir la sustitución.
4. El panel recalcula la puntuación a partir de las respuestas; no utiliza una puntuación declarada por el archivo. Ordena por aciertos descendentes y duración ascendente.
5. El ranking es **provisional** hasta completar la cantidad esperada. Si faltó alguien al juego, ajusta esa cantidad a la participación real (entre 10 y 25); no excluyas a quien sí participó y todavía está enviando su archivo.
6. Con todas las entregas, se destacan los diez premiados. Un empate exacto de puntos y milisegundos en el corte del décimo lugar se marca como **Desempatar**: sortea los premios pendientes entre ese grupo con una herramienta acordada. El orden alfabético del ID no decide premios.
7. Descarga el ranking CSV y el respaldo JSON antes de cerrar. Después abre el solucionario del panel para revisar los seis errores y las seis decisiones correctas.

Los nombres y resultados quedan en el navegador y en los archivos descargados; no se envían a un servidor. Evita subir esas entregas personales al repositorio público. Para otra ronda, usa un código nuevo, participantes en sesiones privadas nuevas y el panel vacío después de respaldarlo.

## Versión del cuestionario

Esta edición conceptual usa `penoles-caza-v4`. Comparte el HTML del reto y utiliza el panel de resultados de esta misma edición. El panel rechaza entregas de la versión anterior para no calificarlas con preguntas diferentes. Las sesiones anteriores quedan conservadas bajo sus claves locales y esta edición inicia una sesión nueva.

## Alcance de la calificación

Es una actividad de confianza, no un examen con controles contra manipulación. Al funcionar totalmente en el navegador, el código contiene las soluciones y el cronómetro es local. La validación del panel detecta inconsistencias de formato y recalcula puntos, pero no autentica la identidad ni demuestra que un archivo no fue editado. Para una evaluación formal haría falta un servicio central que controle las sesiones y las entregas.

## Contenido evaluado

| Tecnología | Temas del OnDemand |
|---|---|
| NiFi | Contenido y atributos de un FlowFile, relationships y auto-terminate, Parameter Contexts con `ruta` / `#{ruta}` y consulta de registros con QueryRecord, CSVReader y JsonRecordSetWriter |
| Spark | Driver y executors, evaluación diferida, transformaciones y acciones, particiones e interfaces PySpark/Spark SQL |
| Iceberg | Compactación, retención para time travel, catálogo/metadata y evolución del particionado |
