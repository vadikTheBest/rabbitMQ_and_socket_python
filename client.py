import socket, pika, os, sys, io, pyDes
from keys import k
# Имя файла для отправки
fileName = "initial_db.db"

def import_by_socket():
    sock = socket.socket()
    sock.connect(('localhost', 9090))
    with open(fileName, 'rb') as f:
        byte = f.read(8)
        print(byte, "b")
        d = k.encrypt(byte)
        print(d, "d")
        while byte != b"":       
            sock.send(d)
            byte = f.read(8)
            d = k.encrypt(byte)
    print("Через сокеты отправлено")
    f.close()
    sock.close()

def import_by_rabbitmq():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue='data1', durable=True)
    sizeFile = os.path.getsize(fileName)
    channel.basic_publish(exchange='',
                              routing_key='data1',
                              body=str(sizeFile),
                              properties=pika.BasicProperties(delivery_mode=2,)
                              )
    # Создаем очередь с заданным именем для отправки
    with open(fileName, 'rb') as f:
        byte = f.read(8)
        d = k.encrypt(byte)
        messageBody = d       
        while byte != b"":
            channel.basic_publish(exchange='',
                              routing_key='data1',
                              body=messageBody,
                              properties=pika.BasicProperties(delivery_mode=2,)
                              )
            byte = f.read(8)
            d = k.encrypt(byte)
            messageBody = d
            
    f.close()
    connection.close()

def main():
    print("Добро пожаловать в программу для импортера базы данных SQLite3 используя очередь сообщений RabbitMQ или с помощью сокетов\r\nЧтобы передать информацию через RabbitMQ, введите '1'\r\nЧтобы передать информацию через сокеты, введите '2'\r\n")
    while True:
                line = str(input())
                if line == '1':
                    print("С помощью RabbitMQ")
                    import_by_rabbitmq()
                elif line == '2':
                    print("С помощью сокетов")
                    import_by_socket()
                else:
                    print("Не правильно ввели")

main()
