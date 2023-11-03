import json
import time
import random
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import AdminClient
from datetime import datetime, timedelta

KAFKA_BROKER = 'kafka:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

maestrosInscritos = set()
stockCarros = {}
STOCK_INICIAL = 10

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def create_topics():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    topics = [
        NewTopic("ingreso", num_partitions=2, replication_factor=1),
        NewTopic("ventas", num_partitions=2, replication_factor=1),
        NewTopic("stock", num_partitions=2, replication_factor=1),
        NewTopic("avisos", num_partitions=2, replication_factor=1)
    ]

    admin_client.create_topics(topics)

create_topics()

def inscripcion():
    rut = input("Introduce el rut del maestro (sin guión): ")
    if rut not in maestrosInscritos:
        nombre = input("Introduce el nombre del maestro: ")
        email = input("Introduce el correo del maestro: ")
        stockCarros[rut] = STOCK_INICIAL
        paid = input("¿Es paid? (s/n): ")
        data = {"nombre": nombre, "email": email, "rut": rut, "stock_restante": STOCK_INICIAL, "paid": paid.lower() == 's'}
        data_str = json.dumps(data)
        maestrosInscritos.add(rut)
        topic = 'ingreso'
        partition = 1 if data.get('paid') else 0
        producer.produce(topic, key=rut, value=str(data_str), partition=partition, callback=delivery_report)
        producer.flush()
        print(f"Vendedor {nombre} con rut {rut} registrado.")
        stockCarros[rut] = {"stock": STOCK_INICIAL, "email": email}
    else:
        print("Error: Maestro ya se encuentra inscrito")

def registroVentas():
    rut = input("Introduce el rut del Maestro: ")
    if rut in maestrosInscritos:
        maestro = input("Introduce el nombre del Maestro: ")
        while True:
            
            stockCarros[rut]["stock"] -= 1

            if stockCarros[rut]["stock"] == 0: ##--------------SOLICITAR STOCK-----------------------
                print(f"El carro con rut {rut} se ha quedado sin stock!")
                
                # Aumentar el stock
                stockCarros[rut]["stock"] += STOCK_INICIAL
                # Notificar al topic stock
                data = {"rut": rut, "stock_adicional": STOCK_INICIAL, "stock_total": stockCarros[rut]["stock"]}
                producer.produce('stock', key=rut, value=str(data), callback=delivery_report)
                producer.flush()
                print(f"Stock añadido para el carro con rut {rut}. Stock total: {stockCarros[rut]}")
                
                continue

            email = stockCarros[rut]["email"]
            fecha_actual = datetime.now().date()
            fecha_actual_iso = fecha_actual.isoformat()
            data = {"maestro": maestro, "rut": rut, "email": email, "fecha_actual_iso": fecha_actual_iso}
            data_str = json.dumps(data)
            producer.produce('ventas', key=rut, value=str(data_str), callback=delivery_report)
            producer.flush()
            print(f"Venta registrada para el maestro {maestro}.")
            time.sleep(random.randint(5,7))
    else:
        print("Error: carro no registrado")


def main():
    while True:
        print("\n--- Menú ---")
        print("1. Inscribir Maestro")
        print("2. Comenzar ventas")
        print("3. Salir")
        choice = input("Elige una opción: ")
        if choice == '1':
            inscripcion()
        elif choice == '2':
            registroVentas()
        elif choice == '3':
            break

if __name__ == "__main__":
    main()
