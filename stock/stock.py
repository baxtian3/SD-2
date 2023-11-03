from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'stock'

maestrosInscritos = set()
stockCarros = {}  # Diccionario para almacenar el stock de cada carro

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['stock'])

def procesar_mensaje(mensaje):
    data = eval(mensaje.value().decode('utf-8'))
    rut = data['rut']

    # Actualizar el stock del carro
    stock_adicional = data.get('stock_adicional', 0)
    if rut not in stockCarros:
        stockCarros[rut] = stock_adicional

    print(f"Stock actualizado para el carro con rut {rut}. Stock total: {stockCarros[rut]}")

def poll_kafka():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of topic {msg.topic()} partition {msg.partition()}")
            else:
                print(f"Error while polling message: {msg.error()}")
        else:
            procesar_mensaje(msg)

if __name__ == "__main__":
    poll_kafka()
