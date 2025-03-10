import pika
import threading

CLIENT_QUEUE = "queue1"  # This client's queue (receives messages)
OTHER_CLIENT_QUEUE = "queue2"  # Other client's queue (sends messages)

# Function to receive messages using a separate connection
def receive_messages():
    try:
        receive_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        receive_channel = receive_connection.channel()
        receive_channel.queue_declare(queue=CLIENT_QUEUE)

        def callback(ch, method, properties, body):
            print(f"\n [📩] Received: {body.decode()}")

        receive_channel.basic_consume(queue=CLIENT_QUEUE, on_message_callback=callback, auto_ack=True)
        print(" [*] Waiting for messages. To exit, press CTRL+C")
        receive_channel.start_consuming()

    except pika.exceptions.AMQPConnectionError:
        print(" [⚠️] Lost connection to RabbitMQ. Retrying...")
        receive_messages()  # Restart listening if disconnected

# Start receiving messages in a separate thread
threading.Thread(target=receive_messages, daemon=True).start()

# Separate connection for sending messages
send_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
send_channel = send_connection.channel()
send_channel.queue_declare(queue=CLIENT_QUEUE)
send_channel.queue_declare(queue=OTHER_CLIENT_QUEUE)

# Function to send messages
while True:
    try:
        msg = input(" [You] Type a message: ")
        if msg.lower() == "exit":
            break  # Exit chat

        send_channel.basic_publish(exchange='', routing_key=OTHER_CLIENT_QUEUE, body=msg)
        print(" [📤] Sent!")
    
    except pika.exceptions.StreamLostError:
        print(" [⚠️] Connection lost while sending. Restarting...")
        send_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        send_channel = send_connection.channel()
        send_channel.queue_declare(queue=CLIENT_QUEUE)
        send_channel.queue_declare(queue=OTHER_CLIENT_QUEUE)

# Close connection
send_connection.close()
