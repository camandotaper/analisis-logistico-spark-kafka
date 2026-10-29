from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    round,
    sum,
    to_timestamp,
    when,
    window
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)

TOPIC = "eventos_envios"
SERVIDOR_KAFKA = "localhost:9092"
CHECKPOINT = "/home/vboxuser/checkpoints/eventos_envios"

spark = (
    SparkSession.builder
    .appName("AnalisisEnviosTiempoReal")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

esquema_evento = StructType([
    StructField("id_evento", StringType(), False),
    StructField("transportadora_codigo", StringType(), True),
    StructField("region_destino", StringType(), True),
    StructField("anio", IntegerType(), True),
    StructField("mes", IntegerType(), True),
    StructField("dia_semana", StringType(), True),
    StructField("franja_horaria", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("costo_envio", LongType(), True),
    StructField("recaudo", LongType(), True),
    StructField("contraentrega", BooleanType(), True),
    StructField("modalidad_entrega", StringType(), True),
    StructField("timestamp_evento", StringType(), True)
])

flujo_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", SERVIDOR_KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

eventos = (
    flujo_kafka
    .selectExpr("CAST(value AS STRING) AS json_evento")
    .select(
        from_json(
            col("json_evento"),
            esquema_evento
        ).alias("datos")
    )
    .select("datos.*")
    .withColumn(
        "tiempo_evento",
        to_timestamp(col("timestamp_evento"))
    )
    .filter(col("tiempo_evento").isNotNull())
)

eventos_con_marca = eventos.withWatermark(
    "tiempo_evento",
    "1 minute"
)

indicadores = (
    eventos_con_marca
    .groupBy(
        window(
            col("tiempo_evento"),
            "1 minute"
        )
    )
    .agg(
        count("*").alias("total_eventos"),
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
        sum(
            when(
                col("estado") == "EN_TRANSITO",
                1
            ).otherwise(0)
        ).alias("en_transito"),
        sum(
            when(
                col("estado") == "SIN_ESTADO",
                1
            ).otherwise(0)
        ).alias("sin_estado"),
        round(
            avg("costo_envio"),
            2
        ).alias("costo_promedio"),
        sum("recaudo").alias("recaudo_acumulado"),
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
        "tasa_novedades_porcentaje",
        round(
            (
                col("total_novedades")
                / col("total_eventos")
            ) * lit(100),
            2
        )
    )
    .select(
        col("window.start").alias("inicio_ventana"),
        col("window.end").alias("fin_ventana"),
        "total_eventos",
        "entregados",
        "devoluciones",
        "cancelados",
        "con_error",
        "en_transito",
        "sin_estado",
        "costo_promedio",
        "recaudo_acumulado",
        "pedidos_contraentrega",
        "total_novedades",
        "tasa_novedades_porcentaje"
    )
)

print("Consumidor de Spark Structured Streaming iniciado.")
print(f"Topic de origen: {TOPIC}")
print("Ventanas de procesamiento: 1 minuto")
print("Presione Ctrl + C para finalizar.\n")

consulta = (
    indicadores.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", "50")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

try:
    consulta.awaitTermination()

except KeyboardInterrupt:
    print("\nFinalizacion solicitada por el usuario.")

finally:
    if consulta.isActive:
        consulta.stop()

    spark.stop()
    print("Consumidor cerrado correctamente.")
