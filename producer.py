# producer.py

import pika
from faker import Faker
from mongoengine import connect, Document, StringField, BooleanField, EmbeddedDocument, ListField
from bson import ObjectId
import json

# Initialize Faker for generating fake data
fake = Faker()


# Define the Contact model
class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    is_sent = BooleanField(default=False)


# Establish a connection to MongoDB
connect('email_contacts')

# Establish a connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')


# Generate fake contacts and store them in the database
def generate_fake_contacts(num_contacts):
    for _ in range(num_contacts):
        contact = Contact(
            full_name=fake.name(),
            email=fake.email()
        )
        contact.save()


# Generate fake contacts and send their ObjectIDs to the RabbitMQ queue
def send_contacts_to_queue(num_contacts):
    contacts = Contact.objects(is_sent=False).limit(num_contacts)
    for contact in contacts:
        message = json.dumps({'contact_id': str(contact.id)})
        channel.basic_publish(exchange='', routing_key='email_queue', body=message)
        contact.is_sent = True
        contact.save()


if __name__ == '__main__':
    num_contacts = 10
    generate_fake_contacts(num_contacts)
    send_contacts_to_queue(num_contacts)
    connection.close()
