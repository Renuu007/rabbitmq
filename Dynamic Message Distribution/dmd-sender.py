import pika
import threading

EXCHANGE_NAME = "direct_messages"
CONFIRM_QUEUE = "delivery_confirmations"

# Create connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare exchange (direct mode)
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

# Declare confirmation queue
channel.queue_declare(queue=CONFIRM_QUEUE)

# Function to listen for delivery confirmations
def listen_for_confirmations():
    confirm_conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    confirm_channel = confirm_conn.channel()
    confirm_channel.queue_declare(queue=CONFIRM_QUEUE)

    def callback(ch, method, properties, body):
        print(f" [✔] Delivery Confirmed: {body.decode()}")

    confirm_channel.basic_consume(queue=CONFIRM_QUEUE, on_message_callback=callback, auto_ack=True)
    print(" [*] Waiting for delivery confirmations...")
    confirm_channel.start_consuming()

# Start confirmation listener in a separate thread
threading.Thread(target=listen_for_confirmations, daemon=True).start()

print("\n[You] Type messages for each receiver.")

while True:
    receiver = input("\n[You] Enter receiver ID (or type 'exit' to quit): ").strip()
    
    if receiver.lower() == "exit":
        break  

    # Ask sender to enter a message
    message = input("[You] Type a message: ").strip()

    # Publish the message with receiver ID as routing key
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=receiver, body=message)
    print(f" [📤] Sent to {receiver}: {message}")

# Close connection
connection.close()
