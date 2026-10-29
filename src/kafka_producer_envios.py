import csv
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

ARCHIVO_CSV = "/home/vboxuser/shipments_academico_anonimizado.csv"
TOPIC = "eventos_envios"
SERVIDOR_KAFKA = "localhost:9092"
INTERVALO_SEGUNDOS = 0.5

producer = KafkaProducer(
    bootstrap_servers=SERVIDOR_KAFKA,
    value_serializer=lambda evento: json.dumps(
        evento,
        ensure_ascii=False
    ).encode("utf-8"),
    acks="all",
    retries=5
)

print("Productor conectado con Apache Kafka.")
print(f"Topic de destino: {TOPIC}")
print(f"Archivo de origen: {ARCHIVO_CSV}")
print("Presione Ctrl + C para detener el envio.\n")

eventos_enviados = 0

try:
    with open(ARCHIVO_CSV, mode="r", encoding="utf-8") as archivo:
        lector = csv.DictReader(archivo)

        for fila in lector:
            evento = {
                "id_evento": fila["id_evento"],
                "transportadora_codigo": fila["transportadora_codigo"],
                "region_destino": fila["region_destino"],
                "anio": int(fila["anio"]),
                "mes": int(fila["mes"]),
                "dia_semana": fila["dia_semana"],
                "franja_horaria": fila["franja_horaria"],
                "estado": fila["estado"],
                "costo_envio": int(fila["costo_envio"]),
                "recaudo": int(fila["recaudo"]),
                "contraentrega": (
                    fila["contraentrega"].strip().lower() == "true"
                ),
                "modalidad_entrega": fila["modalidad_entrega"],
                "timestamp_evento": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            resultado = producer.send(
                TOPIC,
                value=evento
            ).get(timeout=10)

            eventos_enviados += 1

            print(
                f"Evento {eventos_enviados:03d} enviado | "
                f"ID: {evento['id_evento']} | "
                f"Estado: {evento['estado']} | "
                f"Particion: {resultado.partition} | "
                f"Offset: {resultado.offset}"
            )

            time.sleep(INTERVALO_SEGUNDOS)

except KeyboardInterrupt:
    print("\nEnvio detenido por el usuario.")

except FileNotFoundError:
    print(f"\nError: no se encontro el archivo {ARCHIVO_CSV}")

except Exception as error:
    print(f"\nError durante el envio: {error}")

finally:
    producer.flush()
    producer.close()

    print(f"\nTotal de eventos enviados: {eventos_enviados}")
    print("Productor cerrado correctamente.")
