import pika

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the same queue as the producer
channel.queue_declare(queue='Somya')

# Callback function to process messages
def callback(ch, method, properties, body):
    print(f" [x] Received: {body.decode()}")

# Consume messages from the queue
channel.basic_consume(queue='Somya', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit, press CTRL+C')
channel.start_consuming()
