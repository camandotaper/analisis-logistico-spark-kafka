# Diccionario de datos

## Conjunto de datos

**Archivo:** `shipments_academico_anonimizado.csv`

El conjunto de datos contiene 586 registros académicos derivados de una base operativa de envíos realizados en Colombia entre septiembre de 2024 y octubre de 2026. La versión publicada fue anonimizada y transformada para excluir identificadores personales y conservar únicamente las variables necesarias para el análisis logístico, temporal y económico.

## Variables

### `id_evento`

- **Tipo de dato:** texto.
- **Descripción:** identificador académico único asignado a cada registro.
- **Ejemplo:** `ENV0001`.
- **Tratamiento:** sustituye el número de guía original y no permite establecer una relación directa con el envío operativo.

### `transportadora_codigo`

- **Tipo de dato:** texto.
- **Descripción:** código asignado a la transportadora asociada con el envío.
- **Valores posibles:** `T1`, `T2` y `T3`.
- **Tratamiento:** reemplaza el nombre comercial de la transportadora.

### `region_destino`

- **Tipo de dato:** texto.
- **Descripción:** región geográfica generalizada del destino del envío.
- **Valores posibles:** `ANDINA`, `CARIBE`, `PACIFICA`, `ORINOQUIA` y `AMAZONIA`.
- **Tratamiento:** sustituye el municipio y el departamento específicos para reducir el riesgo de identificación.

### `anio`

- **Tipo de dato:** entero.
- **Descripción:** año de creación del registro.
- **Tratamiento:** se deriva de la fecha original sin conservar la fecha completa.

### `mes`

- **Tipo de dato:** entero.
- **Descripción:** número del mes de creación del registro.
- **Rango esperado:** 1 a 12.
- **Tratamiento:** se deriva de la fecha original sin conservar el día exacto.

### `dia_semana`

- **Tipo de dato:** texto.
- **Descripción:** día de la semana en el que se creó el registro.
- **Valores posibles:** `LUNES`, `MARTES`, `MIERCOLES`, `JUEVES`, `VIERNES`, `SABADO` y `DOMINGO`.
- **Tratamiento:** sustituye la fecha exacta de la operación.

### `franja_horaria`

- **Tipo de dato:** texto.
- **Descripción:** intervalo general del día en el que se creó el registro.
- **Valores posibles:** `MADRUGADA`, `MANANA`, `TARDE` y `NOCHE`.
- **Tratamiento:** reemplaza la hora exacta de creación.

### `estado`

- **Tipo de dato:** texto.
- **Descripción:** condición logística registrada para el envío.
- **Valores posibles:** `ENTREGADO`, `DEVOLUCION`, `CANCELADO`, `CON_ERROR`, `EN_TRANSITO` y `SIN_ESTADO`.
- **Tratamiento:** las categorías originales fueron normalizadas para facilitar su procesamiento.

### `costo_envio`

- **Tipo de dato:** entero.
- **Descripción:** valor aproximado del servicio de transporte expresado en pesos colombianos.
- **Unidad:** COP.
- **Tratamiento:** el valor original fue redondeado al millar más cercano.

### `recaudo`

- **Tipo de dato:** entero.
- **Descripción:** valor aproximado registrado para el recaudo del envío.
- **Unidad:** COP.
- **Tratamiento:** el valor original fue redondeado al millar más cercano.

### `contraentrega`

- **Tipo de dato:** booleano.
- **Descripción:** indica si el envío utilizó la modalidad de pago contraentrega.
- **Valores posibles:** `True` o `False`.
- **Interpretación:** `True` corresponde a un pedido contraentrega y `False` a un pedido sin esa modalidad.

### `modalidad_entrega`

- **Tipo de dato:** texto.
- **Descripción:** modalidad general utilizada para recibir el envío.
- **Valores posibles:** `DIRECCION` y `OFICINA`.
- **Tratamiento:** la clasificación se obtuvo localmente a partir de la información operativa y la dirección original fue eliminada posteriormente.

## Variables eliminadas

La versión académica no contiene las siguientes variables del archivo original:

- Número de guía.
- Nombre del destinatario.
- Número telefónico.
- Dirección.
- Municipio específico.
- Fecha y hora exactas.

## Uso previsto

El conjunto de datos se utiliza para:

1. Procesamiento por lotes mediante DataFrames de Apache Spark.
2. Simulación de eventos logísticos en Apache Kafka.
3. Procesamiento en tiempo real mediante Spark Structured Streaming.
4. Cálculo de indicadores de entregas, novedades, costos y recaudos.

## Limitaciones

Los datos fueron transformados para proteger la información personal y operativa. Por esta razón, los valores publicados no deben interpretarse como una reproducción íntegra de la base original. Los resultados obtenidos son descriptivos y corresponden exclusivamente al conjunto académico analizado.
