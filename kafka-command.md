# Kafka Quickstart Guide

This guide provides essential commands and instructions to set up and interact with Kafka using Docker and Kafka CLI tools.

---

## 🔧 Starting Kafka

  1. Open new folder (outside this folder) and add file `docker-compose.yml`

Copy and paste:

```
version: '3'
services:
  zookeeper:
    image: wurstmeister/zookeeper
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    depends_on:
      - zookeeper
```

2. Start Kafka and Zookeper

   Run the following command to start both Kafka and Zookeeper containers in the same terminal you have your docker-compose:

   `docker-compose up`

### ✅ Verifying Containers are Running
Check if the Kafka and Zookeeper containers are running by executing:

`docker ps`

You should see containers for both Kafka and Zookeeper listed as running.

## 📋 Managing Kafka Topics

__1. List All Topics__
   
To list all existing topics, use:

```
kafka-topics.sh --list --bootstrap-server localhost:9092
```

__2. Create a New Topic__

To create a new topic, change <your_topic> to a name of your choice, run:
 
```
kafka-topics.sh --create --topic your_topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

__Example Topics__:

-`handledorders`

-`orderproject`

-`kafkaproject`

__3. Describe a Topic__

To describe a specific topic, use:

```
kafka-topics.sh --zookeeper zookeeper:2181 \
  --describe --topic orders
```
__4. Delete a Topic__

To delete a topic, run:

```
kafka-topics.sh --zookeeper zookeeper:2181 \
  --delete --topic orders
```

## 📦 Consuming Messages

Consume Messages from the Beginning

To consume messages from the beginning of a topic (e.g., handled_orders), run:

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic handled_orders --from-beginning
```

## 📂 Accessing Kafka Terminal in Docker
To execute Kafka CLI commands (e.g., kafka-topics.sh, kafka-console-consumer.sh), follow these steps to access the Kafka container's terminal:

1. Find the Kafka Container Name
Open your terminal and list all running containers with:

`docker ps`

You should see a list of containers. Look for the one running _Kafka_. 

2. Open a Terminal in the Kafka Container

Use the following command to access the Kafka container's terminal:

`docker exec -it kafka /bin/bash`

You are now inside the Kafka container's terminal.

3. Navigate to Kafka Commands

Kafka CLI tools are typically located in the Kafka binary directory. Navigate to this directory:

`cd /opt/kafka/bin`

Now you can run any Kafka command. 

5. Exit the Kafka Container Terminal
When you're done, you can leave the container's terminal by typing:

`exit`

### 🌐 Reference

For more detailed tutorials and resources, visit:

[https://www.conduktor.io/kafka/kafka-consumer-cli-tutorial/](url)
