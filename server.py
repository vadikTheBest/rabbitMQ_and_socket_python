#!/usr/bin/env python
import pika, socket, time, sys, sqlite3, io, normalize, os.path
import sqlite3 as lite
from keys import k
# имя файла, который будем забирать для обработки
fileName = "new_db.db"
data = bytes()
calc = 0
size = int

def get_by_rabbitmq():  
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # забираем из очереди 
    channel.queue_declare(queue='data1', durable=True)
    data = bytes()
    def callback(ch, method, properties, body):
        global data
        global calc
        global size    
        if calc == 0:
                size = int(body)
        if calc != 0:
                data += k.decrypt(body)
                with open(fileName, 'wb') as f:
                        f.write(data)
                f.close
        calc += 1
        if os.path.isfile("new_db.db"):
                if os.path.getsize(fileName) == size:
                        normalize.normalize_bd()
                        os.remove("new_db.db")
        ch.basic_ack(delivery_tag = method.delivery_tag)
        #print(" [x] Received %r" % body)
        return data
        channel.close()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume('data1', callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    channel.start_consuming()
    
def get_by_sockets():
    sock = socket.socket()
    sock.bind(('', 9090))
    sock.listen(1)
    conn, addr = sock.accept()
    print('connected:', addr)
    def getMessage():
        datasock = bytes()
        tmp = conn.recv(16)  
        tmp = k.decrypt(tmp)
        while tmp:
            datasock += tmp
            tmp = conn.recv(16)     
            tmp = k.decrypt(tmp)
            if not tmp:
                return datasock
    datasock = getMessage()
    print(data, "me")
    with open(fileName, 'wb') as f:
            f.write(datasock)
    f.close()
    conn.close()

def main():
    while True:
                line = str(input())
                if line == '1':
                    print("С помощью RabbitMQ")
                    get_by_rabbitmq()                 
                elif line == '2':
                    print("С помощью сокетов")
                    get_by_sockets()
                    normalize.normalize_bd()
                else:
                    print("Не правильно ввели")

main()
