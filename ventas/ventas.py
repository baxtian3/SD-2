import json
import time
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'ventas'


maestrosInscritos = set()
ventasTotales = {}

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['ventas'])

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = 'correo_gmail'  # Reemplazar correo
    msg['To'] = to_email

    # Configuración del servidor SMTP para Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('correo_gmail', 'pass_gmail')  # Reemplazar correo y pass
    server.send_message(msg)
    server.quit()


def procesar_mensaje(mensaje):
    data_str = mensaje.value().decode('utf-8')
    data = json.loads(data_str)

    rut = data['rut']
    email = data['email']
    fecha_venta_iso = data['fecha_actual_iso']
    fecha_venta = datetime.fromisoformat(fecha_venta_iso)
    # Registrar el rut si no está en maestrosInscritos
    if rut not in maestrosInscritos:
        maestrosInscritos.add(rut)
        #ventasTotales[rut] = 0
        ventasTotales[rut] = {"venta_Total": 0,"fecha_inicial": fecha_venta}

    # Sumar las ventas para ese rut
    ventasTotales[rut]["venta_Total"] += 1 
    ganancias =  ventasTotales[rut]["venta_Total"]*1000
    print(f"Total de ventas para el maestro con rut {rut}: {ventasTotales[rut]}." 
                    f"Ganancias: ${ganancias}. ")

    if fecha_venta - ventasTotales[rut]["fecha_inicial"] >= timedelta(seconds = 120):

        ventasTotales[rut]["fecha_inicial"] = fecha_venta
        ganancias =  ventasTotales[rut]["venta_Total"]*1000
        # Enviar el total de ventas para ese rut
        email_body = (f"Total de ventas para el maestro con rut {rut}: {ventasTotales[rut]}." 
                    f"Ganancias: ${ganancias}. ")
        send_email(email, "Registro de venta en MAMOCHI", email_body)

    

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
    
