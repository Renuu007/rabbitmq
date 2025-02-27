import pika

def on_request(ch, method, props, body):
    """Callback function to process requests and send a response"""
    message = body.decode()
    print(f" [.] Received request: {message}")

    response = f"Reply to '{message}'"  # Process the request and create a response

    # Send response to reply_to queue
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,  # Send response to producer's reply queue
        properties=pika.BasicProperties(correlation_id=props.correlation_id),  # Maintain correlation ID
        body=response
    )
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare an RPC queue for requests
channel.queue_declare(queue='rpc_queue')

# Start consuming requests
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
print(" [*] Waiting for RPC requests...")
channel.start_consuming()

