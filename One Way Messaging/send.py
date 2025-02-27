import pika

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue (Ensures it exists)
channel.queue_declare(queue='Somya')

# Publish a message
message = 'Yo!'  # Define message
channel.basic_publish(exchange='', routing_key='Somya', body=message)

print(f" Sent '{message}'")  # Print the exact message sent

# Close the connection
connection.close()
