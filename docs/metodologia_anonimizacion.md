# Metodología de anonimización

## Propósito

El conjunto de datos original corresponde a registros operativos de envíos realizados en Colombia. La fuente contenía información personal y operativa que no debía publicarse en el repositorio, el informe, la presentación ni los videos. Por esta razón, se creó una copia académica anonimizada y transformada que conserva únicamente las variables necesarias para el análisis logístico, temporal y económico.

El archivo resultante se denomina `shipments_academico_anonimizado.csv` y contiene 586 registros.

## Principios aplicados

La preparación del conjunto académico siguió los siguientes criterios:

1. **Minimización:** se conservaron solamente los datos necesarios para los objetivos analíticos de la actividad.
2. **Eliminación de identificadores directos:** se retiraron nombres, teléfonos, direcciones y números de guía.
3. **Generalización:** los destinos específicos y las marcas temporales exactas se sustituyeron por categorías menos precisas.
4. **Seudonimización operativa:** las transportadoras y los identificadores de los envíos fueron reemplazados por códigos académicos.
5. **Reducción de precisión:** los valores monetarios se redondearon al millar más cercano.
6. **Ruptura de la secuencia original:** los registros fueron reorganizados antes de generar los identificadores académicos.
7. **Separación de fuentes:** el archivo original no fue transferido a la máquina virtual ni incorporado al repositorio público.

## Transformaciones realizadas

### Identificador del envío

El número de guía original fue eliminado. Cada registro recibió un identificador académico secuencial con el formato `ENV0001`, `ENV0002` y siguientes. Estos identificadores no guardan una relación directa con las guías operativas.

### Datos del destinatario

Se eliminaron completamente:

- Nombre del destinatario.
- Número telefónico.
- Dirección detallada.

Estos campos no fueron incluidos en la copia académica.

### Transportadora

Los nombres comerciales fueron reemplazados por los códigos `T1`, `T2` y `T3`. La codificación permite comparar el comportamiento de las transportadoras sin publicar sus nombres.

### Destino

El municipio y el departamento específicos fueron reemplazados por una región geográfica general:

- `ANDINA`.
- `CARIBE`.
- `PACIFICA`.
- `ORINOQUIA`.
- `AMAZONIA`.

La generalización conserva una dimensión territorial útil para el análisis y reduce la precisión de la ubicación.

### Fecha y hora

La fecha y la hora exactas fueron eliminadas. En su lugar, se conservaron atributos temporales derivados:

- Año.
- Mes.
- Día de la semana.
- Franja horaria.

Las franjas utilizadas fueron `MADRUGADA`, `MANANA`, `TARDE` y `NOCHE`.

### Estado del envío

Los estados fueron normalizados mediante las categorías:

- `ENTREGADO`.
- `DEVOLUCION`.
- `CANCELADO`.
- `CON_ERROR`.
- `EN_TRANSITO`.
- `SIN_ESTADO`.

La normalización facilita las agrupaciones y el cálculo de indicadores.

### Valores monetarios

Los campos `costo_envio` y `recaudo` se conservaron como valores enteros expresados en pesos colombianos. Para reducir su precisión, los importes fueron redondeados al millar más cercano.

### Modalidad contraentrega

La información original fue convertida a una variable booleana:

- `True`: pedido contraentrega.
- `False`: pedido sin modalidad contraentrega.

### Modalidad de entrega

La dirección original se utilizó únicamente de manera local para clasificar el envío como `DIRECCION` u `OFICINA`. Después de crear esta variable derivada, el texto de la dirección fue eliminado.

### Orden de los registros

Antes de asignar los identificadores académicos, las filas fueron reorganizadas mediante un procedimiento reproducible. Esta acción rompe la correspondencia directa entre el orden del archivo original y el orden de la copia publicada.

## Verificaciones realizadas

Antes de utilizar y publicar la copia académica se verificó que:

- Contuviera 586 registros y una fila de encabezados.
- No incluyera nombres, teléfonos, direcciones ni números de guía.
- No conservara municipios específicos ni fechas y horas exactas.
- Los códigos de transportadora fueran `T1`, `T2` o `T3`.
- Las regiones y los estados pertenecieran a las categorías definidas.
- Las doce variables estuvieran completas en la versión utilizada.
- El archivo pudiera ser leído mediante DataFrames de Apache Spark.

## Uso en el proyecto

La copia anonimizada se utilizó en dos componentes:

1. **Procesamiento por lotes:** carga, validación, transformación, análisis exploratorio y cálculo de indicadores mediante Apache Spark.
2. **Procesamiento en tiempo real:** conversión de cada registro en un evento JSON, publicación en Apache Kafka y consumo mediante Spark Structured Streaming.

## Archivos excluidos del repositorio

No se publican:

- El conjunto de datos operativo original.
- Copias temporales o de respaldo que contengan información sensible.
- Nombres, teléfonos, direcciones o números de guía.
- Capturas que expongan información personal.
- Contraseñas o credenciales de acceso.

## Limitaciones

La anonimización y la generalización disminuyen el nivel de detalle disponible. En consecuencia:

- No es posible analizar municipios o direcciones específicas.
- No se dispone de fechas y horas exactas de operación.
- Los valores monetarios publicados son aproximados.
- Los códigos de transportadora no identifican públicamente empresas concretas.
- Los resultados son descriptivos y corresponden exclusivamente al conjunto académico analizado.
- Las diferencias observadas entre categorías no demuestran por sí mismas relaciones causales.

## Declaración de uso académico

El archivo `shipments_academico_anonimizado.csv` fue preparado exclusivamente para fines académicos. La versión publicada busca permitir la reproducción del análisis sin divulgar los identificadores personales contenidos en la fuente operativa.
