from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import IntegerSerializer

from faker import Faker
import random
import datetime
import mysql.connector as connector
import time

# Define database configuration
dbconfig={
    "host":"localhost",
    "user":"root",
    "password":"*********",
    "database":"kafka_db"
}

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '*************',
    'sasl.password': '********************************'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-gk071.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('*************', '**************************************')
})

# Fetch the latest Avro schema for the value
subject_name = 'products_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "int"}')
key_serializer = IntegerSerializer()
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as an integer
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Produce and Write data to database
def write_to_db(index):
    # Generate 10 product records each time and write to database
    try:
        for i in range(1,11):
            name = fake.word()
            category = random.choice(['Electronics', 'Stationery', 'Apparel', 'Accessories'])
            price = round(random.uniform(50.0, 200.0), 2)
            last_updated = datetime.datetime.now()

            data = (index+i,name,category,price,last_updated)
            cursor.execute(insert_query,data)
            db_conn.commit()

        print("10 rows inserted.. at timestamp ",datetime.datetime.now())
    except Exception as ex:
        print(f"exception thrown while writing records: {ex}")
        raise

# Fetch data from database and write to kafka topic
def write_to_kafka_topic():
    index=0
    global last_read
    try:
        while True:
            # write data to database
            write_to_db(index)
            
            cursor.execute(get_query,(last_read,))
            result=cursor.fetchall()
            print("No of records fetched: ",len(result)) 
            
            for record in result:
                # Create a dictionary from the row values
                value = {'id':record[0], 'name': record[1], 'category':record[2], 'price': record[3], 'last_updated': str(record[4])}
                # Produce to Kafka
                producer.produce(topic='products_data', key=record[0], value=value, on_delivery=delivery_report)
            producer.flush()
            last_read=datetime.datetime.now()
            index=index+10
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("KeyBoard Interrupt..stopping the kafka_producer thread")
    except Exception as ex:
        print(f"exception thrown while reading and producing records: {ex}")
    finally:
        cursor.close()
        db_conn.close()

# database connection object
db_conn = connector.connect(**dbconfig)
cursor=db_conn.cursor()

# faker library used for creating dummy data
fake = Faker()

# query to insert data into database
insert_query='INSERT INTO product (id,name,category,price,last_updated) VALUES(%s,%s,%s,%s,%s)'
# query to fetch data from database based on provided timestamp
get_query='SELECT * FROM product WHERE last_updated > %s'

last_read=datetime.datetime.now()-datetime.timedelta(minutes=5)
write_to_kafka_topic()