import sys
import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import IntegerDeserializer

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '***************',
    'sasl.password': '****************************************************',
    'group.id': 'group11',
    'auto.offset.reset': 'latest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-gk071.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('************', '*******************************************')
})

# Fetch the latest Avro schema for the value
subject_name = 'products_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = IntegerDeserializer()
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'products_data' topic
consumer.subscribe(['products_data'])

#filepath created for the Json file based on consumer_id (e.g. 1,2,3,4,5) provided from command line
filepath=f'F:\DE_Bootcamp_GDS\Mod4\Class_2\Class_Assignments\Assignment_Solution\Consumer_Json_Files\Consumer_{sys.argv[1]}.json'
jsonFile = open(filepath, "a")

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message
        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
        data = msg.value()

        # extra 5% off on electronics products
        if(data["category"]=="Electronics"):
            data["price"]=data["price"]*(1-0.05)
        
        data["price"]=round(data["price"],2)
        # make category column values uppercased
        data["category"]=data["category"].upper()
        
        jsonString=json.dumps(data)
        jsonFile.write(jsonString +'\n')
        print(f'Successfully consumed record with key {msg.key()}')

except KeyboardInterrupt:
    jsonFile.close()
except Exception as ex:
    print(f"Error while consuming data: {ex}")
finally:
    consumer.close()