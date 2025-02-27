import pika
import sys

EXCHANGE_NAME = "direct_messages"
CONFIRM_QUEUE = "delivery_confirmations"

if len(sys.argv) != 2:
    print("Usage: python3 receiver.py <receiver_id>")
    sys.exit(1)

receiver_id = sys.argv[1]

# Create connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare exchange and queue
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
channel.queue_declare(queue=receiver_id)
channel.queue_bind(exchange=EXCHANGE_NAME, queue=receiver_id, routing_key=receiver_id)

# Declare confirmation queue
confirm_channel = connection.channel()
confirm_channel.queue_declare(queue=CONFIRM_QUEUE)

# Function to process received messages
def callback(ch, method, properties, body):
    message = body.decode()
    print(f" [📩] {receiver_id} Received: {message}")
    
    # Send confirmation back to sender
    confirm_channel.basic_publish(exchange='', routing_key=CONFIRM_QUEUE, body=f"{receiver_id} got the message")

channel.basic_consume(queue=receiver_id, on_message_callback=callback, auto_ack=True)

print(f" [*] Receiver {receiver_id} is waiting for messages...")
channel.start_consuming()
