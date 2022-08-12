from ensurepip import bootstrap
from kafka import KafkaConsumer,KafkaAdminClient
from json import loads


# admin_client = KafkaAdminClient( bootstrap_servers='localhost:9092', client_id = 'Pinterest_producer' )

# Create Consumer to retrieve the messages from the topic
batch_pin_consumer = KafkaConsumer(
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda pinmessage: loads(pinmessage),
    # will read the message from the first
    ## auto_offset_reset = "earliest"
)

# To display list of topics preent in the kafka
print(batch_pin_consumer.topics())

# To subscribe to the correct topic
batch_pin_consumer.subscribe(topics = "pinterest_topic")

for message in batch_pin_consumer:
   print(message)