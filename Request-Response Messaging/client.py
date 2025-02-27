import pika
import uuid

class RpcClient:
    def __init__(self):
        # Connect to RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Declare a callback queue (temporary queue for responses)
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Listen for responses
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)
        
        self.response = None
        self.corr_id = None  # Correlation ID to match requests & responses

    def on_response(self, ch, method, props, body):
        """Callback function to process responses"""
        if self.corr_id == props.correlation_id:  # Check if it's the correct response
            self.response = body.decode()

    def call(self, message):
        """Sends a request and waits for a response"""
        self.response = None
        self.corr_id = str(uuid.uuid4())  # Generate a unique ID for the request

        # Send message to 'rpc_queue' with reply_to and correlation_id
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,  # Where to send the response
                correlation_id=self.corr_id,   # Unique ID to track response
            ),
            body=message
        )

        # Wait for response
        while self.response is None:
            self.connection.process_data_events()

        return self.response  # Return the response to the caller

# Test the RPC client
if __name__ == "__main__":
    rpc_client = RpcClient()
    print(" [x] Sending request...")
    response = rpc_client.call("Hello, Server!")
    print(f" [.] Got response: {response}")
