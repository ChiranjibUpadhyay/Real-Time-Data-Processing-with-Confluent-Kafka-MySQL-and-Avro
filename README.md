# Real-Time-Data-Processing-with-Confluent-Kafka-MySQL-and-Avro
In this project, I have implemented real time processing of data using confluent kafka

## Background
There is a fictitious e-commerce company called "BuyOnline"
which has a MySQL database that stores product information such as
product ID, name, category, price, and updated timestamp. The
database gets updated frequently with new products and changes in
product information. The company wants to build a real-time system to
stream these updates incrementally to a downstream system for
real-time analytics and business intelligence.

## Objective:
Building a Kafka producer and a consumer
group that work with a MySQL database, Avro serialization, and
multi-partition Kafka topics. The producer will fetch incremental data
from a MySQL table and write Avro serialized data into a Kafka topic.
The consumers will deserialize this data and append it to separate JSON
files.

## Tools Required:
● Python 3.7 or later
● Confluent Kafka Python client
● MySQL Database
● Apache Avro
● A suitable IDE for coding (e.g., PyCharm, Visual Studio Code)
