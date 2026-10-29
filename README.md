# Analisis logistico con Apache Spark y Apache Kafka

Proyecto academico para analizar registros logisticos anonimizados mediante procesamiento por lotes con Apache Spark y procesamiento en tiempo real con Apache Kafka y Spark Structured Streaming.

## Descripcion

La solucion utiliza 586 registros academicos derivados de una base operativa de envios realizados en Colombia entre septiembre de 2024 y octubre de 2026. El conjunto publicado fue anonimizado para excluir identificadores personales y conservar solamente variables logisticas, temporales y economicas.

El proyecto incluye dos componentes:

1. **Procesamiento por lotes:** carga, validacion, limpieza, analisis exploratorio, calculo de indicadores y exportacion de resultados mediante DataFrames de Apache Spark.
2. **Procesamiento en tiempo real:** conversion de cada registro en un evento JSON, publicacion en un topic de Apache Kafka y procesamiento mediante ventanas temporales con Spark Structured Streaming.

## Pregunta de analisis

Que patrones logisticos, temporales y economicos permiten diferenciar los envios entregados de aquellos que presentan devoluciones, cancelaciones o errores, y como pueden Apache Spark, Apache Kafka y Spark Structured Streaming apoyar su analisis historico y seguimiento en tiempo real?

## Estructura del repositorio

```text
analisis-logistico-spark-kafka/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   └── shipments_academico_anonimizado.csv
├── docs/
│   ├── diccionario_datos.md
│   └── metodologia_anonimizacion.md
└── src/
    ├── spark_batch_envios.py
    ├── kafka_producer_envios.py
    └── spark_streaming_consumer_envios.py
```

## Archivos principales

### `src/spark_batch_envios.py`

Carga el archivo CSV mediante un DataFrame de Spark y realiza:

- Definicion explicita del esquema.
- Conteo de registros.
- Revision de identificadores duplicados.
- Revision de valores nulos.
- Aplicacion de reglas basicas de limpieza.
- Distribucion de los estados logisticos.
- Calculo de indicadores generales.
- Analisis por transportadora codificada.
- Analisis por region.
- Comparacion por modalidad de pago.
- Comparacion por modalidad de entrega.
- Analisis por año y mes.
- Exportacion de tablas agregadas en formato CSV.

### `src/kafka_producer_envios.py`

Lee el conjunto anonimizado, convierte cada fila en un evento JSON, agrega una marca temporal en UTC y publica los mensajes en el topic `eventos_envios`.

### `src/spark_streaming_consumer_envios.py`

Consume los eventos del topic `eventos_envios`, interpreta su estructura JSON y calcula indicadores en ventanas de un minuto mediante Spark Structured Streaming.

## Conjunto de datos

El archivo `data/shipments_academico_anonimizado.csv` contiene:

- 586 registros.
- 12 variables.
- Identificadores academicos sin relacion directa con las guias originales.
- Transportadoras codificadas como `T1`, `T2` y `T3`.
- Destinos generalizados por region.
- Atributos temporales sin fecha ni hora exactas.
- Valores monetarios redondeados.

La definicion detallada de las variables se encuentra en [`docs/diccionario_datos.md`](docs/diccionario_datos.md).

La metodologia aplicada para proteger la informacion se encuentra en [`docs/metodologia_anonimizacion.md`](docs/metodologia_anonimizacion.md).

## Proteccion de datos

El repositorio no contiene:

- Numeros de guia originales.
- Nombres de destinatarios.
- Telefonos.
- Direcciones.
- Municipios especificos.
- Fechas y horas exactas.
- Contraseñas o credenciales.

El archivo operativo original fue excluido del repositorio.

## Entorno utilizado

La implementacion fue ejecutada en una maquina virtual con:

- Ubuntu Server 22.04.5 LTS.
- Python 3.
- Java 11.
- Apache Spark 3.5.8.
- Scala 2.12.18.
- Apache Kafka 3.9.2.
- `kafka-python` 3.0.12.

## Requisitos previos

Para reproducir la solucion se requiere:

- Python 3.
- Java compatible con Apache Spark.
- Apache Spark 3.5.8.
- Apache Kafka 3.9.2.
- Acceso local al broker de Kafka en `localhost:9092`.
- Dependencias de Python indicadas en `requirements.txt`.

Instalacion de la dependencia del productor:

```bash
python3 -m pip install -r requirements.txt
```

## Preparacion en BIGDATA

Los programas fueron desarrollados para ejecutarse desde `/home/vboxuser`. Para reproducir el entorno original, copie los archivos a esa ubicacion:

```text
/home/vboxuser/shipments_academico_anonimizado.csv
/home/vboxuser/spark_batch_envios.py
/home/vboxuser/kafka_producer_envios.py
/home/vboxuser/spark_streaming_consumer_envios.py
```

Si se utiliza otra ruta, deben actualizarse las constantes de ubicacion definidas al inicio de los programas.

## Ejecucion del procesamiento por lotes

Ejecute:

```bash
python3 /home/vboxuser/spark_batch_envios.py
```

La aplicacion guarda las tablas resultantes en:

```text
/home/vboxuser/resultados_batch_envios
```

Los resultados exportados incluyen:

- Resumen por estado.
- Indicadores generales.
- Analisis por transportadora.
- Analisis por region.
- Analisis por modalidad de pago.
- Analisis por modalidad de entrega.
- Analisis temporal.

Al final del proceso, el programa espera que se presione `Enter` para mantener disponible temporalmente la interfaz web de Spark en el puerto `4040`.

## Ejecucion del procesamiento en tiempo real

### 1. Iniciar ZooKeeper

Utilice el comando correspondiente a la instalacion local de Apache Kafka.

### 2. Iniciar el broker de Kafka

Mantenga el broker disponible en:

```text
localhost:9092
```

### 3. Crear el topic

Cree el topic `eventos_envios` con una particion y un factor de replicacion igual a uno:

```bash
kafka-topics.sh --create   --topic eventos_envios   --bootstrap-server localhost:9092   --partitions 1   --replication-factor 1
```

La ubicacion de `kafka-topics.sh` puede variar segun la instalacion de Kafka.

### 4. Iniciar el consumidor

El consumidor debe iniciarse antes que el productor porque utiliza `startingOffsets` con el valor `latest`.

```bash
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8   /home/vboxuser/spark_streaming_consumer_envios.py
```

### 5. Iniciar el productor

En otra terminal, ejecute:

```bash
python3 /home/vboxuser/kafka_producer_envios.py
```

El productor publica los 586 registros de manera secuencial en el topic `eventos_envios`.

## Indicadores calculados en streaming

La aplicacion calcula por ventana temporal:

- Total de eventos.
- Entregados.
- Devoluciones.
- Cancelados.
- Eventos con error.
- Eventos en transito.
- Registros sin estado.
- Costo promedio.
- Recaudo acumulado.
- Pedidos contraentrega.
- Total de novedades.
- Tasa de novedades.

La tasa de novedades se define como:

```text
(devoluciones + cancelados + eventos con error) / total de eventos * 100
```

## Resultados de validacion

Durante la ejecucion academica se verifico que:

- Se cargaron 586 registros.
- No se encontraron identificadores duplicados.
- No fue necesario excluir registros mediante las reglas de limpieza aplicadas.
- El productor publico 586 eventos en la particion 0, con offsets del 0 al 585.
- El procesamiento por lotes y la consulta de streaming finalizaron sus calculos previstos.

Como resultado descriptivo, la modalidad de entrega en oficina presento una tasa de entrega del 83,13 %, mientras que la entrega a direccion registro una tasa del 80,17 %. Esta diferencia no demuestra por si misma una relacion causal.

## Consideraciones de reproducibilidad

- Inicie el consumidor antes que el productor.
- No ejecute el productor varias veces si no desea duplicar mensajes en el topic.
- El directorio de checkpoint del consumidor conserva el progreso de la consulta.
- Los resultados pueden variar si se modifica el intervalo del productor, el tamaño de las ventanas o los recursos de la maquina virtual.
- Las rutas configuradas en los programas corresponden al usuario `vboxuser`.

## Limitaciones

- El laboratorio utiliza un solo nodo y una sola particion de Kafka.
- El conjunto publicado fue anonimizado y generalizado.
- Los valores monetarios son aproximados.
- Los resultados son descriptivos y no deben generalizarse automaticamente a toda una operacion logistica.
- Las diferencias observadas entre categorias no implican causalidad.

## Uso academico

Este repositorio fue creado para la Tarea 3 del curso Big Data de la Universidad Nacional Abierta y a Distancia. El contenido se publica con fines academicos y de reproducibilidad tecnica.
