from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    max,
    min,
    round,
    sum,
    when
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType
)

ARCHIVO_CSV = (
    "/home/vboxuser/"
    "shipments_academico_anonimizado.csv"
)

DIRECTORIO_RESULTADOS = (
    "/home/vboxuser/"
    "resultados_batch_envios"
)

spark = (
    SparkSession.builder
    .appName("AnalisisBatchEnvios")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("id_evento", StringType(), False),
    StructField(
        "transportadora_codigo",
        StringType(),
        True
    ),
    StructField("region_destino", StringType(), True),
    StructField("anio", IntegerType(), True),
    StructField("mes", IntegerType(), True),
    StructField("dia_semana", StringType(), True),
    StructField("franja_horaria", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("costo_envio", IntegerType(), True),
    StructField("recaudo", IntegerType(), True),
    StructField("contraentrega", BooleanType(), True),
    StructField(
        "modalidad_entrega",
        StringType(),
        True
    )
])

df = (
    spark.read
    .option("header", "true")
    .schema(esquema)
    .csv(ARCHIVO_CSV)
)

print("\n")
print("=" * 70)
print("ANALISIS POR LOTES DE ENVIOS ANONIMIZADOS")
print("=" * 70)

print("\n1. ESQUEMA DEL DATAFRAME")
df.printSchema()

total_inicial = df.count()

print("\n2. CANTIDAD INICIAL DE REGISTROS")
print(f"Total de registros cargados: {total_inicial}")

duplicados = (
    df.groupBy("id_evento")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("\n3. REVISION DE DUPLICADOS")
print(f"Identificadores duplicados: {duplicados}")

columnas = df.columns

expresiones_nulos = [
    sum(
        when(
            col(nombre_columna).isNull(),
            1
        ).otherwise(0)
    ).alias(nombre_columna)
    for nombre_columna in columnas
]

print("\n4. REVISION DE VALORES NULOS")
df.select(expresiones_nulos).show(
    truncate=False
)

df_limpio = (
    df.dropDuplicates(["id_evento"])
    .filter(col("id_evento").isNotNull())
    .filter(col("estado").isNotNull())
    .filter(col("costo_envio") >= 0)
    .filter(col("recaudo") >= 0)
)

total_limpio = df_limpio.count()
registros_excluidos = total_inicial - total_limpio

print("\n5. RESULTADO DE LA LIMPIEZA")
print(f"Registros iniciales: {total_inicial}")
print(f"Registros finales: {total_limpio}")
print(f"Registros excluidos: {registros_excluidos}")

resumen_estados = (
    df_limpio.groupBy("estado")
    .agg(
        count("*").alias("cantidad")
    )
    .withColumn(
        "porcentaje",
        round(
            col("cantidad")
            / lit(total_limpio)
            * lit(100),
            2
        )
    )
    .orderBy(
        col("cantidad").desc()
    )
)

print("\n6. DISTRIBUCION POR ESTADO")
resumen_estados.show(
    truncate=False
)

indicadores_generales = (
    df_limpio.agg(
        count("*").alias("total_envios"),
        sum(
            when(
                col("estado") == "ENTREGADO",
                1
            ).otherwise(0)
        ).alias("entregados"),
        sum(
            when(
                col("estado") == "DEVOLUCION",
                1
            ).otherwise(0)
        ).alias("devoluciones"),
        sum(
            when(
                col("estado") == "CANCELADO",
                1
            ).otherwise(0)
        ).alias("cancelados"),
        sum(
            when(
                col("estado") == "CON_ERROR",
                1
            ).otherwise(0)
        ).alias("con_error"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio"),
        min("costo_envio").alias("costo_minimo"),
        max("costo_envio").alias("costo_maximo"),
        sum("recaudo").alias("recaudo_total"),
        round(
            avg("recaudo"),
            2
        ).alias("recaudo_promedio"),
        sum(
            when(
                col("contraentrega") == True,
                1
            ).otherwise(0)
        ).alias("pedidos_contraentrega")
    )
    .withColumn(
        "total_novedades",
        col("devoluciones")
        + col("cancelados")
        + col("con_error")
    )
    .withColumn(
        "tasa_entrega_porcentaje",
        round(
            col("entregados")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .withColumn(
        "tasa_novedades_porcentaje",
        round(
            col("total_novedades")
            / col("total_envios")
            * lit(100),
            2
        )
    )
)

print("\n7. INDICADORES GENERALES")
indicadores_generales.show(
    truncate=False,
    vertical=True
)

analisis_transportadora = (
    df_limpio.groupBy(
        "transportadora_codigo"
    )
    .agg(
        count("*").alias("total_envios"),
        sum(
            when(
                col("estado") == "ENTREGADO",
                1
            ).otherwise(0)
        ).alias("entregados"),
        sum(
            when(
                col("estado").isin(
                    "DEVOLUCION",
                    "CANCELADO",
                    "CON_ERROR"
                ),
                1
            ).otherwise(0)
        ).alias("novedades"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio"),
        sum("recaudo").alias("recaudo_total")
    )
    .withColumn(
        "tasa_entrega_porcentaje",
        round(
            col("entregados")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .withColumn(
        "tasa_novedades_porcentaje",
        round(
            col("novedades")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .orderBy(
        col("total_envios").desc()
    )
)

print("\n8. RESULTADOS POR TRANSPORTADORA")
analisis_transportadora.show(
    truncate=False
)

analisis_region = (
    df_limpio.groupBy("region_destino")
    .agg(
        count("*").alias("total_envios"),
        sum(
            when(
                col("estado") == "ENTREGADO",
                1
            ).otherwise(0)
        ).alias("entregados"),
        sum(
            when(
                col("estado").isin(
                    "DEVOLUCION",
                    "CANCELADO",
                    "CON_ERROR"
                ),
                1
            ).otherwise(0)
        ).alias("novedades"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio"),
        sum("recaudo").alias("recaudo_total")
    )
    .withColumn(
        "tasa_entrega_porcentaje",
        round(
            col("entregados")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .withColumn(
        "tasa_novedades_porcentaje",
        round(
            col("novedades")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .orderBy(
        col("total_envios").desc()
    )
)

print("\n9. RESULTADOS POR REGION")
analisis_region.show(
    truncate=False
)

analisis_pago = (
    df_limpio.groupBy("contraentrega")
    .agg(
        count("*").alias("total_envios"),
        sum(
            when(
                col("estado") == "ENTREGADO",
                1
            ).otherwise(0)
        ).alias("entregados"),
        sum(
            when(
                col("estado").isin(
                    "DEVOLUCION",
                    "CANCELADO",
                    "CON_ERROR"
                ),
                1
            ).otherwise(0)
        ).alias("novedades"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio"),
        sum("recaudo").alias("recaudo_total")
    )
    .withColumn(
        "tasa_entrega_porcentaje",
        round(
            col("entregados")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .withColumn(
        "tasa_novedades_porcentaje",
        round(
            col("novedades")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .orderBy("contraentrega")
)

print("\n10. RESULTADOS POR MODALIDAD DE PAGO")
analisis_pago.show(
    truncate=False
)

analisis_modalidad = (
    df_limpio.groupBy("modalidad_entrega")
    .agg(
        count("*").alias("total_envios"),
        sum(
            when(
                col("estado") == "ENTREGADO",
                1
            ).otherwise(0)
        ).alias("entregados"),
        sum(
            when(
                col("estado").isin(
                    "DEVOLUCION",
                    "CANCELADO",
                    "CON_ERROR"
                ),
                1
            ).otherwise(0)
        ).alias("novedades"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio")
    )
    .withColumn(
        "tasa_entrega_porcentaje",
        round(
            col("entregados")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .withColumn(
        "tasa_novedades_porcentaje",
        round(
            col("novedades")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .orderBy("modalidad_entrega")
)

print("\n11. RESULTADOS POR MODALIDAD DE ENTREGA")
analisis_modalidad.show(
    truncate=False
)

analisis_temporal = (
    df_limpio.groupBy(
        "anio",
        "mes"
    )
    .agg(
        count("*").alias("total_envios"),
        sum(
            when(
                col("estado") == "ENTREGADO",
                1
            ).otherwise(0)
        ).alias("entregados"),
        sum(
            when(
                col("estado").isin(
                    "DEVOLUCION",
                    "CANCELADO",
                    "CON_ERROR"
                ),
                1
            ).otherwise(0)
        ).alias("novedades"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio"),
        sum("recaudo").alias("recaudo_total")
    )
    .withColumn(
        "tasa_novedades_porcentaje",
        round(
            col("novedades")
            / col("total_envios")
            * lit(100),
            2
        )
    )
    .orderBy(
        "anio",
        "mes"
    )
)

print("\n12. RESULTADOS POR ANIO Y MES")
analisis_temporal.show(
    50,
    truncate=False
)

resultados = [
    (
        resumen_estados,
        "resumen_estados"
    ),
    (
        indicadores_generales,
        "indicadores_generales"
    ),
    (
        analisis_transportadora,
        "analisis_transportadora"
    ),
    (
        analisis_region,
        "analisis_region"
    ),
    (
        analisis_pago,
        "analisis_modalidad_pago"
    ),
    (
        analisis_modalidad,
        "analisis_modalidad_entrega"
    ),
    (
        analisis_temporal,
        "analisis_temporal"
    )
]

for dataframe, nombre in resultados:
    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(
            f"{DIRECTORIO_RESULTADOS}/{nombre}"
        )
    )

print("\n13. EXPORTACION DE RESULTADOS")
print(
    "Los resultados fueron guardados en:"
)
print(DIRECTORIO_RESULTADOS)

print("\n14. RESUMEN FINAL")
print(f"Registros analizados: {total_limpio}")
print(f"Duplicados encontrados: {duplicados}")
print(f"Registros excluidos: {registros_excluidos}")

print("\n")
print("=" * 70)
print("ANALISIS POR LOTES COMPLETADO")
print("=" * 70)

input(
    "\nPresione Enter despues de tomar las "
    "capturas de Spark Jobs y Stages..."
)

spark.stop()

print("\nSesion de Spark cerrada correctamente.")
