import pika
from time import sleep
from mongoengine import connect
import json

# Establish a connection to MongoDB
connect('email_contacts')

# Establish a connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')


# Define the function to simulate sending email
def send_email(contact_id):
    print(f"Simulating email send for contact: {contact_id}")
    sleep(2)
    print(f"Email sent for contact: {contact_id}")


# Callback function to process messages from RabbitMQ
def callback(ch, method, properties, body):
    message = json.loads(body)
    contact_id = message['contact_id']
    send_email(contact_id)
    print(f"Marking contact {contact_id} as sent.")

    # Confirm that the message has been processed
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Set up the consumer to listen for messages
channel.basic_consume(queue='email_queue', on_message_callback=callback)

print("Consumer is waiting for messages. To exit press Ctrl+C")
channel.start_consuming()
